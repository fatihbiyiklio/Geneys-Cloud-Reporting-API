import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta, time, timezone
import sys
import os
import shutil
import logging
import atexit
import json
import time as pytime
import threading
import uuid
import signal
import traceback
import warnings
import psutil

# Suppress st.cache deprecation warning from streamlit_cookies_manager
warnings.filterwarnings("ignore", message=r".*st\.cache.*deprecated.*")

class _SuppressStCacheDeprecationFilter(logging.Filter):
    def filter(self, record):
        msg = str(record.getMessage())
        return "`st.cache` is deprecated" not in msg

for _logger_name in (
    "streamlit",
    "streamlit.runtime",
    "streamlit.runtime.caching",
    "streamlit.deprecation_util",
):
    logging.getLogger(_logger_name).addFilter(_SuppressStCacheDeprecationFilter())

try:
    import streamlit.deprecation_util as _st_deprecation_util
    _orig_show_deprecation_warning = _st_deprecation_util.show_deprecation_warning

    def _patched_show_deprecation_warning(message, show_in_browser=True, show_once=False):
        # streamlit_cookies_manager still imports @st.cache; skip only this known warning.
        if "`st.cache` is deprecated" in str(message):
            return
        return _orig_show_deprecation_warning(
            message, show_in_browser=show_in_browser, show_once=show_once
        )

    _st_deprecation_util.show_deprecation_warning = _patched_show_deprecation_warning
except Exception:
    pass

from streamlit.runtime import Runtime
from streamlit_cookies_manager import EncryptedCookieManager
from cryptography.fernet import Fernet
from src.data_manager import DataManager
from src.notifications import NotificationManager, AgentNotificationManager, GlobalConversationNotificationManager
from src.auth_manager import AuthManager

# --- AUTH MANAGER ---
auth_manager = AuthManager()

# --- BACKGROUND MONITOR ---
# Disabled: do not auto-exit when sessions drop to zero.

# --- LOGGING ---
MEMORY_LIMIT_MB = int(os.environ.get("GENESYS_MEMORY_LIMIT_MB", "512"))  # Soft limit - trigger cleanup
MEMORY_CLEANUP_COOLDOWN_SEC = int(os.environ.get("GENESYS_MEMORY_CLEANUP_COOLDOWN_SEC", "60"))  # Reduced from 120
MEMORY_HARD_LIMIT_MB = int(os.environ.get("GENESYS_MEMORY_HARD_LIMIT_MB", "768"))  # Hard limit - trigger restart

def _setup_logging():
    logger = logging.getLogger("genesys_app")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    logger.propagate = False
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)
    return logger

logger = _setup_logging()

def _log_exception(prefix, exc_type, exc_value, exc_tb):
    details = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    logger.error("%s: %s", prefix, details)

def _is_expected_system_exit(exc_type, exc_value):
    try:
        if not issubclass(exc_type, SystemExit):
            return False
    except Exception:
        return False
    code = getattr(exc_value, "code", None)
    # sys.exit(1) is used intentionally for app reboot flow.
    return code in (None, 0, 1)

def _sys_excepthook(exc_type, exc_value, exc_tb):
    if _is_expected_system_exit(exc_type, exc_value):
        return
    _log_exception("Unhandled exception", exc_type, exc_value, exc_tb)

def _thread_excepthook(args):
    if _is_expected_system_exit(args.exc_type, args.exc_value):
        return
    _log_exception(f"Thread exception in {args.thread.name}", args.exc_type, args.exc_value, args.exc_traceback)

sys.excepthook = _sys_excepthook
if hasattr(threading, "excepthook"):
    threading.excepthook = _thread_excepthook

if not globals().get("_ATEEXIT_LOG_REGISTERED", False):
    atexit.register(lambda: logger.info("App process exiting"))
    _ATEEXIT_LOG_REGISTERED = True

# --- IMPORTS & PATHS ---
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from src.lang import get_text, STRINGS, DEFAULT_METRICS, ALL_METRICS
from src.monitor import monitor
from src.auth import authenticate
from src.api import GenesysAPI
from src.processor import process_analytics_response, to_excel, to_csv, to_parquet, to_pdf, fill_interval_gaps, process_observations, process_daily_stats, process_user_aggregates, process_user_details, process_conversation_details, apply_duration_formatting

# --- CONFIGURATION ---

SESSION_TTL_SECONDS = 120

def _iter_conversation_pages(api, start_date, end_date, max_records=5000, chunk_days=3, page_size=100):
    """Yield conversation pages with an upper bound on total records to avoid OOM."""
    total = 0
    for page in api.iter_conversation_details(
        start_date,
        end_date,
        chunk_days=chunk_days,
        page_size=page_size,
        max_pages=200,
        order="asc",
    ):
        if not page:
            continue
        if max_records and total + len(page) > max_records:
            page = page[: max_records - total]
        if page:
            yield page
            total += len(page)
        if max_records and total >= max_records:
            break

def _endpoint_reason(endpoint):
    if not endpoint:
        return "Bilinmiyor"
    if "/analytics/queues/observations" in endpoint:
        return "Canli kuyruk metrikleri (DataManager periyodik)"
    if "/analytics/conversations/details" in endpoint:
        return "Detayli konusma raporlari"
    if "/analytics/conversations/aggregates" in endpoint:
        return "Gunluk istatistik ve raporlar (dashboard + raporlar)"
    if "/analytics/users/aggregates" in endpoint:
        return "Kullanici durum agregeleri"
    if "/analytics/users/details" in endpoint:
        return "Kullanici giris/cikis detaylari"
    if "/routing/queues/" in endpoint and endpoint.endswith("/users"):
        return "Kuyruk uyeleri (agent listesi)"
    if "/routing/queues/" in endpoint and endpoint.endswith("/conversations"):
        return "Bekleyen konusmalar (kuyruk konusmalari)"
    if "/notifications/channels" in endpoint:
        return "Notifications kanal/abonelik"
    if endpoint.endswith("/users"):
        return "Kullanici listesi ve/veya status taramasi"
    if "/routing/queues" in endpoint:
        return "Kuyruk listesi"
    if "/routing/wrapupcodes" in endpoint:
        return "Wrap-up kodlari"
    if "/presence/definitions" in endpoint:
        return "Presence haritasi"
    return "Diger"

def _endpoint_source(endpoint):
    if not endpoint:
        return "Bilinmiyor"
    if "/analytics/queues/observations" in endpoint:
        return "Background: DataManager (canli metrik)"
    if "/analytics/conversations/aggregates" in endpoint:
        return "Background + Reports (ortak endpoint)"
    if "/analytics/conversations/details" in endpoint:
        return "Reports (detayli konusma)"
    if "/analytics/users/aggregates" in endpoint:
        return "Reports (kullanici durum agregeleri)"
    if "/analytics/users/details" in endpoint:
        return "Reports (kullanici detaylari)"
    if "/routing/queues/" in endpoint and endpoint.endswith("/users"):
        return "Background: DataManager (kuyruk uyeleri)"
    if "/routing/queues/" in endpoint and endpoint.endswith("/conversations"):
        return "Panel: Bekleyen gorusmeler"
    if "/notifications/channels" in endpoint:
        return "Panel: Notifications"
    if endpoint.endswith("/users"):
        return "Background: DataManager (status tarama)"
    if "/routing/queues" in endpoint:
        return "Init/Settings (kuyruk listesi)"
    if "/routing/wrapupcodes" in endpoint:
        return "Init/Settings (wrapup kodlari)"
    if "/presence/definitions" in endpoint:
        return "Init/Settings (presence haritasi)"
    return "Diger"

def _resolve_resource_path(relative_path):
    candidates = []
    if hasattr(sys, "_MEIPASS"):
        candidates.append(os.path.join(sys._MEIPASS, relative_path))
    candidates.append(os.path.join(os.path.dirname(__file__), relative_path))
    candidates.append(os.path.abspath(relative_path))
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return candidates[0] if candidates else relative_path

def format_status_time(presence_ts, routing_ts):
    """Calculates duration since the most recent status change in HH:MM:SS format."""
    try:
        times = []
        if presence_ts: times.append(datetime.fromisoformat(presence_ts.replace('Z', '+00:00')))
        if routing_ts: times.append(datetime.fromisoformat(routing_ts.replace('Z', '+00:00')))
        if not times: return "00:00:00"
        start_time = max(times)
        diff = datetime.now(timezone.utc) - start_time
        seconds = int(diff.total_seconds())
        if seconds < 0: seconds = 0
        
        hrs = seconds // 3600
        mins = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hrs:02d}:{mins:02d}:{secs:02d}"
    except: return "00:00:00"

def format_duration_seconds(seconds):
    """Formats seconds into HH:MM:SS, returns '-' if None."""
    try:
        if seconds is None:
            return "-"
        seconds = int(max(0, seconds))
        hrs = seconds // 3600
        mins = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hrs:02d}:{mins:02d}:{secs:02d}"
    except:
        return "-"

def _parse_wait_seconds(val):
    """Parses wait duration from timestamps or numeric durations."""
    if val is None:
        return None
    now = datetime.now(timezone.utc)
    try:
        if isinstance(val, str):
            s = val.strip()
            if not s:
                return None
            # Numeric string
            if s.replace(".", "", 1).isdigit():
                val = float(s)
            else:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                return max((now - dt).total_seconds(), 0)
        if isinstance(val, (int, float)):
            v = float(val)
            # Epoch milliseconds
            if v > 1e12:
                return max((now.timestamp() * 1000 - v) / 1000, 0)
            # Epoch seconds
            if v > 1e9:
                return max(now.timestamp() - v, 0)
            # Assume duration in seconds
            return max(v, 0)
    except Exception:
        return None
    return None

def _extract_wait_seconds(conv):
    """Extracts best-effort wait duration in seconds from a queue conversation payload."""
    candidates = []
    def add_candidate(v):
        s = _parse_wait_seconds(v)
        if s is not None:
            candidates.append(s)

    for key in ["queueTime", "queueStartTime", "enqueueTime", "startTime", "conversationStart",
                "waitTime", "timeInQueue", "queueDuration", "waitingTime", "waitSeconds"]:
        if isinstance(conv, dict) and conv.get(key) is not None:
            add_candidate(conv.get(key))

    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        for key in ["queueTime", "queueStartTime", "enqueueTime", "startTime", "connectedTime"]:
            if p.get(key) is not None:
                add_candidate(p.get(key))
        for s in p.get("sessions", []) or []:
            for key in ["queueTime", "queueStartTime", "enqueueTime", "startTime", "connectedTime"]:
                if s.get(key) is not None:
                    add_candidate(s.get(key))
            for seg in s.get("segments", []) or []:
                stype = str(seg.get("segmentType", "")).lower()
                if stype in ["queue", "alert", "acd"] and seg.get("segmentStart") and not seg.get("segmentEnd"):
                    add_candidate(seg.get("segmentStart"))

    if not candidates:
        return None
    return max(candidates)

def _extract_media_type(conv):
    if not isinstance(conv, dict):
        return None
    # Check for callback at conversation level first
    if conv.get("mediaType"):
        mt = conv.get("mediaType").lower()
        if mt == "callback":
            return "callback"
        # Check if this is a callback-originated voice call
        if mt == "voice" and _is_callback_conversation(conv):
            return "callback"
        return conv.get("mediaType")
    participants = conv.get("participants") or conv.get("participantsDetails") or []
    found_media = None
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        # Check for callback purpose
        if purpose == "outbound":
            for s in p.get("sessions", []) or []:
                mt = (s.get("mediaType") or "").lower()
                if mt == "callback":
                    return "callback"
        for s in p.get("sessions", []) or []:
            mt = s.get("mediaType")
            if mt:
                if mt.lower() == "callback":
                    return "callback"
                if not found_media:
                    found_media = mt
    # If voice but callback-originated
    if found_media and found_media.lower() == "voice" and _is_callback_conversation(conv):
        return "callback"
    return found_media

def _is_callback_conversation(conv):
    """Check if conversation originated from a callback request."""
    if not isinstance(conv, dict):
        return False
    # Check participants for callback indicators
    participants = conv.get("participants") or conv.get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose == "outbound":
            # Outbound purpose with agent usually indicates callback
            for s in p.get("sessions", []) or []:
                if s.get("mediaType", "").lower() == "callback":
                    return True
        # Check session-level callback indicators
        for s in p.get("sessions", []) or []:
            if s.get("mediaType", "").lower() == "callback":
                return True
            # Check for callback direction
            direction = s.get("direction", "").lower()
            if direction == "outbound" and (p.get("purpose") or "").lower() in ["agent", "user"]:
                # Agent making outbound call could be callback
                pass
    # Check attributes for callback origin
    attributes = conv.get("attributes") or {}
    for key in attributes:
        if "callback" in key.lower():
            return True
    return False

def _seconds_since(iso_str):
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
        return max((datetime.now(timezone.utc) - dt).total_seconds(), 0)
    except Exception:
        return None

def _has_ivr_participant(conv):
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["ivr", "flow"]:
            return True
    return False

def _session_is_active(session):
    state = (session.get("state") or "").lower()
    if session.get("disconnectedTime"):
        return False
    return state in ["alerting", "connected", "offering", "dialing", "communicating", "contacting"]

def _classify_conversation_state(conv):
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    has_agent = False
    has_queue = False
    has_ivr = False
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        sessions = p.get("sessions", []) or []
        if purpose in ["ivr", "flow"] and not sessions:
            has_ivr = True
        for s in sessions:
            active = _session_is_active(s)
            if not active:
                continue
            if purpose in ["agent", "user"]:
                has_agent = True
            elif purpose in ["acd", "queue"]:
                has_queue = True
            elif purpose in ["ivr", "flow"]:
                has_ivr = True
    if has_agent:
        return "interacting"
    if has_queue:
        return "waiting"
    if has_ivr:
        return "ivr"
    return "unknown"

def _extract_direction_label(conv):
    direction = (conv or {}).get("originatingDirection") or (conv or {}).get("direction")
    if direction:
        direction = str(direction).lower()
    if direction and "inbound" in direction:
        return "Inbound"
    if direction and "outbound" in direction:
        return "Outbound"
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    has_external = False
    has_agent = False
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose == "outbound":
            return "Outbound"
        if purpose in ["external", "customer"]:
            has_external = True
        if purpose in ["agent", "user"]:
            has_agent = True
        for s in p.get("sessions", []) or []:
            sd = (s.get("direction") or "").lower()
            if sd == "inbound":
                return "Inbound"
            if sd == "outbound":
                return "Outbound"
    if has_external and has_agent:
        return "Inbound"
    return None

def _extract_queue_name_from_conv(conv, queue_id_to_name=None):
    queue_id_to_name = queue_id_to_name or {}
    if isinstance(conv, dict):
        qname = conv.get("queueName")
        if qname:
            return qname
        qid = conv.get("queueId")
        if qid and qid in queue_id_to_name:
            return queue_id_to_name.get(qid)
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["acd", "queue"]:
            q_id = p.get("queueId") or p.get("routingQueueId")
            if q_id and q_id in queue_id_to_name:
                return queue_id_to_name.get(q_id)
            name = p.get("name")
            if name:
                return name
            qobj = p.get("queue") or {}
            if isinstance(qobj, dict):
                if qobj.get("name"):
                    return qobj.get("name")
                qid = qobj.get("id")
                if qid and qid in queue_id_to_name:
                    return queue_id_to_name.get(qid)
            for s in p.get("sessions", []) or []:
                qid = s.get("queueId") or s.get("routingQueueId")
                if qid and qid in queue_id_to_name:
                    return queue_id_to_name.get(qid)
                qname = s.get("queueName")
                if qname:
                    return qname
                # Analytics API: queueId is inside segments
                for seg in s.get("segments", []) or []:
                    qid = seg.get("queueId")
                    if qid and qid in queue_id_to_name:
                        return queue_id_to_name.get(qid)
    # Analytics segments
    for seg in (conv or {}).get("segments") or []:
        qname = seg.get("queueName")
        if qname:
            return qname
        qobj = seg.get("queue") or {}
        if isinstance(qobj, dict):
            if qobj.get("name"):
                return qobj.get("name")
            qid = qobj.get("id")
            if qid and qid in queue_id_to_name:
                return queue_id_to_name.get(qid)
        qid = seg.get("queueId")
        if qid and qid in queue_id_to_name:
            return queue_id_to_name.get(qid)
    return None

def _extract_queue_id_from_conv(conv):
    if isinstance(conv, dict):
        qid = conv.get("queueId")
        if qid:
            return qid
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["acd", "queue"]:
            q_id = p.get("queueId") or p.get("routingQueueId")
            if q_id:
                return q_id
            qobj = p.get("queue") or {}
            if isinstance(qobj, dict):
                qid = qobj.get("id")
                if qid:
                    return qid
            # Analytics API: queueId is in sessions > segments
            for s in p.get("sessions", []) or []:
                qid = s.get("queueId") or s.get("routingQueueId")
                if qid:
                    return qid
                for seg in s.get("segments", []) or []:
                    qid = seg.get("queueId")
                    if qid:
                        return qid
    # Conversation-level segments
    for seg in (conv or {}).get("segments") or []:
        qid = seg.get("queueId")
        if qid:
            return qid
        qobj = seg.get("queue") or {}
        if isinstance(qobj, dict):
            qid = qobj.get("id")
            if qid:
                return qid
    return None

def _extract_agent_id_from_conv(conv):
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["agent", "user"]:
            return p.get("userId") or (p.get("user") or {}).get("id") or p.get("participantId")
    return None

def _extract_agent_name_from_conv(conv):
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["agent", "user"]:
            return p.get("name") or (p.get("user") or {}).get("name")
    return None

def _is_generic_queue_name(name):
    if not name:
        return True
    return str(name).strip().lower() in ["aktif", "active"]

def _merge_call(existing, incoming):
    if not existing:
        return dict(incoming) if incoming else {}
    merged = dict(existing)
    def _state_rank(v):
        v = (v or "").lower()
        if v == "interacting":
            return 3
        if v == "waiting":
            return 2
        if v == "ivr":
            return 1
        return 0
    for k, v in (incoming or {}).items():
        if v is None or v == "":
            continue
        if k == "media_type":
            existing_mt = str(existing.get("media_type") or "").lower()
            incoming_mt = str(v).lower()
            # Never downgrade callback to voice
            if existing_mt == "callback" and incoming_mt == "voice":
                continue
            merged[k] = v
            continue
        if k == "queue_name":
            if _is_generic_queue_name(v) and not _is_generic_queue_name(existing.get("queue_name")):
                continue
            merged[k] = v
            continue
        if k == "state":
            if _state_rank(v) >= _state_rank(existing.get("state")):
                merged[k] = v
            continue
        merged[k] = v
    if (merged.get("agent_name") or merged.get("agent_id")) and _state_rank(merged.get("state")) < _state_rank("interacting"):
        merged["state"] = "interacting"
    return merged

def _fetch_conversation_meta(api, conv_id, queue_id_to_name, users_info=None):
    if not api or not conv_id:
        return None
    try:
        conv = api.get_conversation(conv_id)
    except Exception:
        conv = None
    if not conv:
        return None
    if conv.get("conversationEnd"):
        return {
            "conversation_id": conv_id,
            "ended": True,
        }
    queue_id = _extract_queue_id_from_conv(conv)
    queue_name = _extract_queue_name_from_conv(conv, queue_id_to_name)
    agent_id = _extract_agent_id_from_conv(conv)
    agent_name = _extract_agent_name_from_conv(conv)
    if not agent_name and agent_id and users_info:
        agent_name = users_info.get(agent_id, {}).get("name")
    phone = _extract_phone_from_conv(conv)
    direction_label = _extract_direction_label(conv)
    ivr_attrs = _extract_ivr_attributes(conv)
    wg = _extract_workgroup_from_attrs(ivr_attrs) or queue_name
    return {
        "conversation_id": conv_id,
        "queue_id": queue_id,
        "queue_name": queue_name,
        "phone": phone,
        "direction": conv.get("originatingDirection") or conv.get("direction"),
        "direction_label": direction_label,
        "wg": wg,
        "media_type": _extract_media_type(conv),
        "ended": False,
        "agent_id": agent_id,
        "agent_name": agent_name,
    }

def _extract_ivr_attributes(conv):
    """
    Extract IVR/workgroup DTMF selections and attributes from conversation.
    Genesys stores these in:
    - conversation.attributes (custom flow data)
    - participant.attributes (IVR participant data)
    - segment.wrapUpCode (wrap-up selections)
    - flow outcomes and variables
    """
    if not isinstance(conv, dict):
        return {}
    
    result = {}
    
    # 1. Conversation-level attributes (most common for IVR data)
    conv_attrs = conv.get("attributes") or {}
    if conv_attrs:
        for key, val in conv_attrs.items():
            if val:
                result[key] = val
    
    # 2. Check participants for IVR/flow data
    participants = conv.get("participants") or conv.get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        
        # All participant attributes
        p_attrs = p.get("attributes") or {}
        if p_attrs:
            for key, val in p_attrs.items():
                if val:
                    result[key] = val
        
        # Flow/IVR purpose - extract extra data
        if purpose in ["ivr", "flow", "acd"]:
            # Check sessions for flow outcomes
            for s in p.get("sessions") or []:
                s_attrs = s.get("attributes") or {}
                for key, val in s_attrs.items():
                    if val:
                        result[key] = val
                
                # Check segments for flow outcomes
                for seg in s.get("segments") or []:
                    seg_attrs = seg.get("attributes") or {}
                    for key, val in seg_attrs.items():
                        if val:
                            result[key] = val
                    
                    # Also check flowOutcome and flowOutcomeValue
                    if seg.get("flowOutcome"):
                        result["flowOutcome"] = seg.get("flowOutcome")
                    if seg.get("flowOutcomeValue"):
                        result["flowOutcomeValue"] = seg.get("flowOutcomeValue")
    
    return result

def _format_ivr_display(ivr_attrs):
    """Format IVR attributes for display in UI."""
    if not ivr_attrs:
        return None
    
    # Priority order for display - include more patterns
    priority_keys = ["workgroup", "dtmf", "menu", "selection", "departman", "secim", "choice", "option", "priority", "note", "callback"]
    
    # Find the most relevant value to display
    for pkey in priority_keys:
        for key, val in ivr_attrs.items():
            if pkey in key.lower() and val:
                # Format nicely: "ivr.Priority: 50" -> "Priority: 50"
                display_key = key.split(".")[-1] if "." in key else key
                return f"{display_key}: {val}"
    
    # If no priority match, return first non-empty value
    for key, val in ivr_attrs.items():
        if val:
            display_key = key.split(".")[-1] if "." in key else key
            return f"{display_key}: {val}"
    
    return None

def _extract_workgroup_from_attrs(ivr_attrs):
    if not isinstance(ivr_attrs, dict):
        return None
    priority_keys = ["workgroup", "wg", "departman", "department", "menu", "selection", "secim"]
    for pkey in priority_keys:
        for key, val in ivr_attrs.items():
            if not val:
                continue
            key_l = str(key).lower()
            if pkey in key_l:
                return str(val)
    return None

def _build_active_calls(conversations, lang, queue_id_to_name=None, users_info=None):
    items = []
    for conv in conversations or []:
        if conv.get("conversationEnd"):
            continue
        mt = _extract_media_type(conv)
        state = _classify_conversation_state(conv)
        direction_label = _extract_direction_label(conv)
        queue_id = _extract_queue_id_from_conv(conv)
        queue_name = _extract_queue_name_from_conv(conv, queue_id_to_name) or "Aktif"
        agent_id = _extract_agent_id_from_conv(conv)
        agent_name = _extract_agent_name_from_conv(conv)
        if not agent_name and agent_id and users_info:
            agent_name = users_info.get(agent_id, {}).get("name")
        if state == "interacting":
            state_label = get_text(lang, "interacting")
        elif state == "waiting":
            state_label = get_text(lang, "waiting")
        elif state == "ivr":
            state_label = "IVR"
        else:
            state_label = None

        wait_s = _extract_wait_seconds(conv)
        if wait_s is None:
            wait_s = _seconds_since(conv.get("conversationStart"))

        conv_id = conv.get("conversationId") or conv.get("id")
        items.append({
            "conversation_id": conv_id,
            "queue_id": queue_id,
            "queue_name": queue_name,
            "wait_seconds": wait_s,
            "phone": _extract_phone_from_conv(conv),
            "direction_label": direction_label,
            "state_label": state_label,
            "media_type": mt,
            "agent_id": agent_id,
            "agent_name": agent_name,
        })
    return items

def _extract_phone_from_conv(conv):
    """Best-effort phone extraction from queue conversations payload."""
    if not isinstance(conv, dict):
        return None
    def _clean(v):
        return str(v).replace("tel:", "").replace("sip:", "").strip()
    def _is_phone(v):
        if not v:
            return False
        s = _clean(v)
        digits = "".join(ch for ch in s if ch.isdigit())
        return len(digits) >= 7

    participants = conv.get("participants") or conv.get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["external", "customer", "outbound"]:
            for k in ["ani", "addressOther", "address", "name"]:
                v = p.get(k)
                if _is_phone(v):
                    return _clean(v)
            for s in p.get("sessions", []) or []:
                for k in ["ani", "addressOther", "address"]:
                    v = s.get(k)
                    if _is_phone(v):
                        return _clean(v)
    # Fallback to conversation-level fields (avoid dnis/toAddress)
    for k in ["ani", "addressOther", "fromAddress", "callerId"]:
        v = conv.get(k)
        if _is_phone(v):
            return _clean(v)
    return None

st.set_page_config(page_title="Genesys Cloud Reporting", layout="wide")

CREDENTIALS_FILE = "credentials.enc"
KEY_FILE = ".secret.key"
CONFIG_FILE = "dashboard_config.json"
PRESETS_FILE = "presets.json"
ORG_BASE_DIR = "orgs"

def _org_dir(org_code):
    path = os.path.join(ORG_BASE_DIR, org_code)
    os.makedirs(path, exist_ok=True)
    return path

def _org_flag_path(org_code):
    return os.path.join(_org_dir(org_code), ".genesys_logged_out")

def _org_dm_disabled_path(org_code):
    return os.path.join(_org_dir(org_code), ".dm_disabled")

def is_dm_enabled(org_code):
    return not os.path.exists(_org_dm_disabled_path(org_code))

def set_dm_enabled(org_code, enabled: bool):
    path = _org_dm_disabled_path(org_code)
    if enabled:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass
    else:
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write("0")
        except Exception:
            pass


st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [data-testid="stAppViewContainer"] { font-family: 'Inter', sans-serif !important; background-color: #ffffff !important; }
    
    /* Keep rendering stable without forcing GPU/opacity hacks */
    [data-testid="stAppViewContainer"] {
        -webkit-font-smoothing: antialiased !important;
        text-rendering: optimizeLegibility;
    }
    
    /* Disable ALL skeleton loading animations and transitions */
    .stMarkdown, .stDataFrame, [data-testid="column"], 
    .element-container, .stMetric,
    [data-testid="stMetricContainer"], [data-testid="stMetricValue"],
    [data-testid="stMetricLabel"], .agent-card, .call-card {
        animation: none !important;
        transition: none !important;
    }
    
    /* Plotly charts should not have transform/filter overrides */
    .stPlotlyChart {
        animation: none !important;
        transition: none !important;
    }

    /* Keep previous data visible during rerun on the same page */
    [data-stale="true"] {
        opacity: 1 !important;
        filter: none !important;
        -webkit-filter: none !important;
    }
    [data-stale="true"] * {
        opacity: 1 !important;
        filter: none !important;
        -webkit-filter: none !important;
    }

    .stSkeleton, [data-testid="stSkeleton"], [class*="skeleton"] {
        display: none !important;
        visibility: hidden !important;
        opacity: 0 !important;
    }
    div[data-testid="stStatusWidget"] {
        display: none !important;
        visibility: hidden !important;
    }
    
    [data-testid="stAppViewContainer"] > .main { padding-top: 0.5rem !important; }
    [data-testid="stVerticalBlockBorderWrapper"] { background-color: #ffffff !important; border: 1px solid #eef2f6 !important; border-radius: 12px !important; padding: 1rem !important; margin-bottom: 1rem !important; }
    [data-testid="stHorizontalBlock"] { gap: 4px !important; }
    [data-testid="stHorizontalBlock"] { align-items: flex-start !important; }
    [data-testid="stColumn"] { min-width: 0 !important; padding: 0 !important; }
    [data-testid="stMetricContainer"] { background-color: #f8fafb !important; border: 1px solid #f1f5f9 !important; padding: 0.75rem 0.5rem !important; border-radius: 10px !important; text-align: center; }
    [data-testid="stMetricContainer"]:hover { background-color: #f1f5f9 !important; }
    [data-testid="stMetricLabel"] { color: #64748b !important; font-size: 0.75rem !important; font-weight: 600 !important; text-transform: uppercase; letter-spacing: 0.05em; }
    [data-testid="stMetricValue"] { color: #1e293b !important; font-size: 1.6rem !important; font-weight: 700 !important; }
    hr { margin: 1.5rem 0 !important; border-color: #f1f5f9 !important; }
    button[aria-label="Show password text"], button[aria-label="Hide password text"] { display: none !important; }
    h1 { margin-bottom: 0.6rem !important; }
    h2, h3 { margin-top: 0.6rem !important; margin-bottom: 0.4rem !important; }
    [data-testid="stExpander"] details { border-radius: 10px !important; }
    [data-testid="stExpander"] details summary { font-size: 0.9rem !important; padding: 0.35rem 0.6rem !important; }
    .last-update {
        font-size: 0.95rem;
        font-weight: 600;
        color: #64748b;
        text-align: right;
        margin-top: 0.2rem;
        line-height: 1.1;
    }
    .last-update span {
        color: #1e293b;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

# Suppress known noisy browser console warnings from embedded iframes/features
# Using components.html for more reliable script injection
try:
    import streamlit.components.v1 as components
    components.html("""
    <script>
    (function() {
        if (window.__consoleFiltered) return;
        window.__consoleFiltered = true;
        
        const blocked = [
            "Unrecognized feature",
            "allow-scripts and allow-same-origin",
            "ambient-light-sensor",
            "battery",
            "document-domain",
            "layout-animations",
            "legacy-image-formats",
            "oversized-images",
            "vr",
            "wake-lock",
            "sandbox"
        ];
        
        const origWarn = console.warn;
        const origError = console.error;
        
        console.warn = function(...args) {
            try {
                const msg = args.map(a => String(a)).join(" ");
                if (blocked.some(b => msg.includes(b))) return;
            } catch (e) {}
            return origWarn.apply(console, args);
        };
        
        console.error = function(...args) {
            try {
                const msg = args.map(a => String(a)).join(" ");
                if (blocked.some(b => msg.includes(b))) return;
            } catch (e) {}
            return origError.apply(console, args);
        };
    })();
    </script>
    """, height=0, width=0)
except Exception:
    pass

# --- GLOBAL HELPERS (DEFINED FIRST) ---

APP_SESSION_FILE = ".session.enc"
APP_SESSION_COOKIE = "app_session"
APP_SESSION_TTL = 7 * 24 * 3600

def _get_or_create_key():
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f: return f.read()
    key = Fernet.generate_key()
    with open(KEY_FILE, "wb") as f: f.write(key)
    try: os.chmod(KEY_FILE, 0o600)
    except: pass
    return key

def _get_cipher():
    return Fernet(_get_or_create_key())

_cookie_manager = None

def _get_cookie_manager():
    global _cookie_manager
    if _cookie_manager is None:
        key = _get_or_create_key()
        try:
            key_str = key.decode("utf-8")
        except Exception:
            key_str = str(key)
        _cookie_manager = EncryptedCookieManager(prefix="genesys", password=key_str)
    if not _cookie_manager.ready():
        st.stop()
    return _cookie_manager

def load_credentials(org_code):
    org_path = _org_dir(org_code)
    filename = os.path.join(org_path, CREDENTIALS_FILE)
    if not os.path.exists(filename): return {}
    try:
        cipher = _get_cipher()
        with open(filename, "rb") as f: data = f.read()
        creds = json.loads(cipher.decrypt(data).decode('utf-8'))
        # Ensure default values for new fields
        if "utc_offset" not in creds: creds["utc_offset"] = 3
        if "refresh_interval" not in creds: creds["refresh_interval"] = 15
        return creds
    except: return {}

def save_credentials(org_code, client_id, client_secret, region, utc_offset=3, **kwargs):
    cipher = _get_cipher()
    filename = os.path.join(_org_dir(org_code), CREDENTIALS_FILE)
    data = json.dumps({
        "client_id": client_id, 
        "client_secret": client_secret, 
        "region": region,
        "utc_offset": utc_offset,
        "refresh_interval": kwargs.get("refresh_interval", 15)
    }).encode('utf-8')
    with open(filename, "wb") as f: f.write(cipher.encrypt(data))
    try: os.chmod(filename, 0o600)
    except: pass

def delete_credentials(org_code):
    filename = os.path.join(_org_dir(org_code), CREDENTIALS_FILE)
    if os.path.exists(filename): os.remove(filename)

def delete_org_files(org_code):
    org_path = os.path.join(ORG_BASE_DIR, org_code)
    try:
        if os.path.isdir(org_path):
            shutil.rmtree(org_path, ignore_errors=True)
    except Exception:
        pass

def generate_password(length=12):
    """Generate a secure random password."""
    import secrets
    import string
    alphabet = string.ascii_letters + string.digits + "!@#$%"
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# --- APP SESSION MANAGEMENT (REMEMBER ME) ---
def load_app_session():
    try:
        # 1) Try local encrypted session file first (more reliable on refresh)
        if os.path.exists(APP_SESSION_FILE):
            try:
                cipher = _get_cipher()
                raw_file = open(APP_SESSION_FILE, "rb").read()
                session_data = json.loads(cipher.decrypt(raw_file).decode("utf-8"))
                timestamp = session_data.get("timestamp", 0)
                if pytime.time() - timestamp <= APP_SESSION_TTL:
                    return session_data
                # Expired
                os.remove(APP_SESSION_FILE)
            except Exception:
                pass

        # 2) Fallback to cookie
        cookies = _get_cookie_manager()
        raw = cookies.get(APP_SESSION_COOKIE)
        if not raw:
            return None
        session_data = json.loads(raw)
        timestamp = session_data.get("timestamp", 0)
        if pytime.time() - timestamp > APP_SESSION_TTL:
            delete_app_session()
            return None
        return session_data
    except Exception:
        return None

def save_app_session(user_data):
    try:
        # Save to local encrypted session file for robustness
        try:
            cipher = _get_cipher()
            payload = {**user_data, "timestamp": pytime.time()}
            with open(APP_SESSION_FILE, "wb") as f:
                f.write(cipher.encrypt(json.dumps(payload).encode("utf-8")))
            try:
                os.chmod(APP_SESSION_FILE, 0o600)
            except Exception:
                pass
        except Exception:
            pass

        cookies = _get_cookie_manager()
        payload = {**user_data, "timestamp": pytime.time()}
        cookies[APP_SESSION_COOKIE] = json.dumps(payload)
        cookies.save()
    except Exception:
        pass

def delete_app_session():
    # Delete session file first
    try:
        if os.path.exists(APP_SESSION_FILE):
            os.remove(APP_SESSION_FILE)
    except Exception:
        pass
    
    # Delete cookie
    try:
        cookies = _get_cookie_manager()
        if APP_SESSION_COOKIE in cookies:
            del cookies[APP_SESSION_COOKIE]
            cookies.save()
    except Exception:
        pass
    
    # Double-check file is gone
    try:
        if os.path.exists(APP_SESSION_FILE):
            os.remove(APP_SESSION_FILE)
    except Exception:
        pass

def _user_dir(org_code, username):
    base = _org_dir(org_code)
    user_safe = (username or "unknown").replace("/", "_").replace("\\", "_")
    path = os.path.join(base, "users", user_safe)
    os.makedirs(path, exist_ok=True)
    return path

def _current_user():
    return st.session_state.app_user if st.session_state.get("app_user") else None

def load_dashboard_config(org_code):
    user = _current_user()
    if user:
        user_path = _user_dir(org_code, user.get("username"))
        filename = os.path.join(user_path, CONFIG_FILE)
    else:
        org_path = _org_dir(org_code)
        filename = os.path.join(org_path, CONFIG_FILE)
    if not os.path.exists(filename): return {"layout": 1, "cards": []}
    try:
        with open(filename, "r", encoding='utf-8') as f: return json.load(f)
    except: return {"layout": 1, "cards": []}

def save_dashboard_config(org_code, layout, cards):
    user = _current_user()
    if user:
        filename = os.path.join(_user_dir(org_code, user.get("username")), CONFIG_FILE)
    else:
        filename = os.path.join(_org_dir(org_code), CONFIG_FILE)
    try:
        with open(filename, "w", encoding='utf-8') as f: json.dump({"layout": layout, "cards": cards}, f, ensure_ascii=False)
    except: pass

def load_presets(org_code):
    user = _current_user()
    if user:
        filename = os.path.join(_user_dir(org_code, user.get("username")), PRESETS_FILE)
    else:
        org_path = _org_dir(org_code)
        filename = os.path.join(org_path, PRESETS_FILE)
    if not os.path.exists(filename): return []
    try:
        with open(filename, "r", encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except: return []

def save_presets(org_code, presets):
    user = _current_user()
    if user:
        filename = os.path.join(_user_dir(org_code, user.get("username")), PRESETS_FILE)
    else:
        filename = os.path.join(_org_dir(org_code), PRESETS_FILE)
    try:
        with open(filename, "w", encoding='utf-8') as f: json.dump(presets, f, ensure_ascii=False)
    except: pass

def get_all_configs_json():
    org = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
    return json.dumps({"dashboard": load_dashboard_config(org), "report_presets": load_presets(org)}, indent=2)

def import_all_configs(json_data):
    try:
        data = json.loads(json_data)
        org = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
        if "dashboard" in data:
            save_dashboard_config(org, data["dashboard"].get("layout", 1), data["dashboard"].get("cards", []))
        if "report_presets" in data:
            save_presets(org, data["report_presets"])
        return True
    except: return False

# --- SHARED DATA MANAGER (per org, cross-session) ---
@st.cache_resource(show_spinner=False)
def _shared_dm_store():
    return {"lock": threading.Lock(), "data": {}}

def _get_dm_store():
    store = _shared_dm_store()
    if "lock" not in store:
        store["lock"] = threading.Lock()
    if "data" not in store:
        store["data"] = {}
    return store

@st.cache_resource(show_spinner=False)
def _shared_org_session_store():
    return {"lock": threading.Lock(), "orgs": {}}

@st.cache_resource(show_spinner=False)
def _shared_notif_store():
    return {"lock": threading.Lock(), "call": {}, "agent": {}, "global": {}}

@st.cache_resource(show_spinner=False)
def _shared_seed_store():
    return {"lock": threading.Lock(), "orgs": {}}

@st.cache_resource(show_spinner=False)
def _shared_memory_store():
    return {"lock": threading.Lock(), "samples": [], "thread": None, "stop_event": threading.Event(), "last_cleanup_ts": 0, "restart_in_progress": False}

def _silent_restart():
    """Perform a silent restart of the Streamlit app when memory exceeds hard limit.
    Uses process exit to trigger the restart loop in run_app.py."""
    logger.warning(f"Memory exceeded {MEMORY_HARD_LIMIT_MB}MB, initiating silent restart...")
    
    # Full cleanup before restart
    _soft_memory_cleanup()
    
    try:
        import gc
        gc.collect()
        gc.collect()  # Double collect for thorough cleanup
    except Exception:
        pass
    
    # Forcefully terminate process so wrapper can start a fresh Python process.
    # sys.exit(1) only exits the current thread in some Streamlit paths.
    os._exit(1)

def _soft_memory_cleanup():
    """Best-effort cleanup to reduce memory without visible user impact."""
    import gc
    try:
        # Clear seed cache
        seed = _shared_seed_store()
        with seed["lock"]:
            for org in seed.get("orgs", {}).values():
                org["call_seed_data"] = []
                org["ivr_calls_data"] = []
                org["agent_presence"] = {}
                org["agent_routing"] = {}
                org["call_meta"] = {}
                org["daily_stats_cache"] = {}
                org["call_seed_ts"] = 0
                org["call_meta_poll_ts"] = 0
                org["ivr_calls_ts"] = 0
                org["agent_seed_ts"] = 0
    except Exception:
        pass
    
    try:
        # Stop notifications to release WS/thread memory; they will reconnect on next use
        notif = _shared_notif_store()
        with notif["lock"]:
            for key in ["call", "agent", "global"]:
                for nm in notif.get(key, {}).values():
                    try:
                        nm.stop()
                    except Exception:
                        pass
            # Drop references so old manager instances can be garbage-collected.
            notif["call"] = {}
            notif["agent"] = {}
            notif["global"] = {}
    except Exception:
        pass

    try:
        # Clear DataManager caches
        dm_store = _get_dm_store()
        with dm_store["lock"]:
            for dm in dm_store.get("data", {}).values():
                try:
                    dm.obs_data_cache = {}
                    dm.daily_data_cache = {}
                    dm.agent_details_cache = {}
                    dm.queue_members_cache = {}
                    dm.last_member_refresh = 0
                    dm.last_daily_refresh = 0
                    dm.last_update_time = 0
                except Exception:
                    pass
    except Exception:
        pass

    try:
        # Clear org maps cache
        global _org_maps_cache
        _org_maps_cache = {}
    except Exception:
        pass
    
    try:
        # Prune monitor endpoint stats
        from src.monitor import monitor as _mon
        with _mon._lock:
            if len(_mon.api_stats) > 100:
                sorted_stats = sorted(_mon.api_stats.items(), key=lambda x: x[1], reverse=True)
                _mon.api_stats = dict(sorted_stats[:50])
    except Exception:
        pass
    
    # Force garbage collection (double pass for cyclic refs)
    try:
        gc.collect()
        gc.collect()
    except Exception:
        pass


def _periodic_memory_cleanup():
    """Periodic lightweight cleanup - called every 10 minutes regardless of memory pressure.
    Trims caches to prevent gradual memory growth over hours/days of operation."""
    import gc
    
    try:
        # Trim notification manager caches (don't stop them, just trim)
        notif = _shared_notif_store()
        with notif["lock"]:
            for key in ["call", "agent", "global"]:
                for nm in notif.get(key, {}).values():
                    try:
                        if hasattr(nm, '_prune_active_conversations'):
                            nm._prune_active_conversations()
                        if hasattr(nm, '_prune_waiting_calls'):
                            nm._prune_waiting_calls()
                        if hasattr(nm, '_prune_user_caches'):
                            nm._prune_user_caches()
                    except Exception:
                        pass
    except Exception:
        pass
    
    try:
        # Trim seed store - prune old call_meta entries (>10min) and limit data sizes
        seed = _shared_seed_store()
        now = pytime.time()
        with seed["lock"]:
            for org in seed.get("orgs", {}).values():
                # Trim call_meta
                meta = org.get("call_meta", {})
                if len(meta) > 100:
                    stale = [k for k, v in meta.items() if (now - v.get("ts", 0)) > 600]
                    for k in stale:
                        meta.pop(k, None)
                    # Still too big? FIFO trim
                    if len(meta) > 200:
                        sorted_items = sorted(meta.items(), key=lambda x: x[1].get("ts", 0))
                        for k, _ in sorted_items[:len(meta) - 100]:
                            meta.pop(k, None)
                
                # Trim call_seed_data to max 200 entries
                call_seed = org.get("call_seed_data", [])
                if len(call_seed) > 200:
                    org["call_seed_data"] = call_seed[:200]
                
                # Trim ivr_calls_data to max 200 entries
                ivr_calls = org.get("ivr_calls_data", [])
                if len(ivr_calls) > 200:
                    org["ivr_calls_data"] = ivr_calls[:200]
                
                # Trim agent_presence and agent_routing dicts
                for cache_key in ["agent_presence", "agent_routing"]:
                    cache = org.get(cache_key, {})
                    if len(cache) > 100:
                        # Keep only first 100 items
                        keys_to_remove = list(cache.keys())[100:]
                        for k in keys_to_remove:
                            cache.pop(k, None)

                # Trim shared historical daily cache
                daily_cache = org.get("daily_stats_cache", {})
                if daily_cache:
                    stale_daily = [k for k, v in daily_cache.items() if (now - v.get("ts", 0)) > 1800]
                    for k in stale_daily:
                        daily_cache.pop(k, None)
                    if len(daily_cache) > 80:
                        oldest_daily = sorted(daily_cache.items(), key=lambda kv: kv[1].get("ts", 0))[:len(daily_cache) - 60]
                        for k, _ in oldest_daily:
                            daily_cache.pop(k, None)
    except Exception:
        pass
    
    try:
        # Prune stale org sessions
        sess_store = _shared_org_session_store()
        with sess_store["lock"]:
            for org_key in list(sess_store.get("orgs", {}).keys()):
                org_data = sess_store["orgs"][org_key]
                sessions = org_data.get("sessions", {})
                stale_sessions = [s for s, v in sessions.items() if (now - v.get("ts", 0)) > SESSION_TTL_SECONDS]
                for s in stale_sessions:
                    sessions.pop(s, None)
    except Exception:
        pass
    
    try:
        # Trim DataManager error logs
        dm_store = _get_dm_store()
        with dm_store["lock"]:
            for dm in dm_store.get("data", {}).values():
                try:
                    if hasattr(dm, 'error_log') and len(dm.error_log) > 50:
                        dm.error_log = dm.error_log[-50:]
                except Exception:
                    pass
    except Exception:
        pass
    
    try:
        gc.collect()
    except Exception:
        pass

def _safe_autorefresh(*args, **kwargs):
    try:
        from streamlit_autorefresh import st_autorefresh as _st_autorefresh
        return _st_autorefresh(*args, **kwargs)
    except RuntimeError:
        # Runtime not ready yet
        return None
    except Exception:
        return None

def _start_memory_monitor(sample_interval=10, max_samples=720):
    store = _shared_memory_store()
    with store["lock"]:
        if store.get("thread") and store["thread"].is_alive():
            return store
        store["stop_event"].clear()

        def run():
            import gc
            proc = psutil.Process(os.getpid())
            gc_counter = 0
            periodic_cleanup_counter = 0
            full_cleanup_counter = 0
            PERIODIC_CLEANUP_INTERVAL = 30  # Every 30 samples = 5 min (reduced from 10 min)
            FULL_CLEANUP_INTERVAL = 180  # Every 180 samples = 30 min
            
            while not store["stop_event"].is_set():
                try:
                    rss_mb = proc.memory_info().rss / (1024 * 1024)
                except Exception:
                    rss_mb = 0
                try:
                    cpu_pct = proc.cpu_percent(interval=None)
                except Exception:
                    cpu_pct = 0
                ts = datetime.now().isoformat(timespec="seconds")
                sample = {"timestamp": ts, "rss_mb": round(rss_mb, 1), "cpu_pct": round(cpu_pct, 1)}
                with store["lock"]:
                    store["samples"].append(sample)
                    if len(store["samples"]) > max_samples:
                        store["samples"] = store["samples"][-max_samples:]
                    last_cleanup = store.get("last_cleanup_ts", 0)
                
                # Periodic garbage collection every 5 minutes
                gc_counter += 1
                if gc_counter >= 30:  # 30 * 10 sec = 5 min
                    gc.collect()
                    gc_counter = 0
                
                # Periodic lightweight cleanup every 10 minutes (regardless of memory)
                periodic_cleanup_counter += 1
                if periodic_cleanup_counter >= PERIODIC_CLEANUP_INTERVAL:
                    try:
                        _periodic_memory_cleanup()
                    except Exception:
                        pass
                    periodic_cleanup_counter = 0

                # Full cleanup every 30 minutes (regardless of memory)
                full_cleanup_counter += 1
                if full_cleanup_counter >= FULL_CLEANUP_INTERVAL:
                    try:
                        _soft_memory_cleanup()
                    except Exception:
                        pass
                    try:
                        gc.collect()
                        gc.collect()
                    except Exception:
                        pass
                    full_cleanup_counter = 0
                
                # Check if memory exceeds hard limit - trigger silent restart
                if rss_mb >= MEMORY_HARD_LIMIT_MB:
                    restart_in_progress = False
                    with store["lock"]:
                        restart_in_progress = store.get("restart_in_progress", False)
                        if not restart_in_progress:
                            store["restart_in_progress"] = True
                    if not restart_in_progress:
                        logger.warning(f"RSS memory {rss_mb:.1f}MB exceeds hard limit {MEMORY_HARD_LIMIT_MB}MB, triggering silent restart")
                        _silent_restart()
                
                # Soft cleanup at lower threshold
                if rss_mb >= MEMORY_LIMIT_MB and (pytime.time() - last_cleanup) > MEMORY_CLEANUP_COOLDOWN_SEC:
                    _soft_memory_cleanup()
                    with store["lock"]:
                        store["last_cleanup_ts"] = pytime.time()
                for _ in range(max(1, int(sample_interval * 5))):
                    if store["stop_event"].is_set():
                        break
                    pytime.sleep(0.2)

        t = threading.Thread(target=run, daemon=True)
        store["thread"] = t
        t.start()
    return store

def _ensure_seed_org(store, org_code, max_orgs=20):
    # Enforce max orgs to prevent memory bloat from abandoned orgs
    if len(store["orgs"]) > max_orgs and org_code not in store["orgs"]:
        # Remove oldest org (first key)
        oldest_key = next(iter(store["orgs"]), None)
        if oldest_key:
            store["orgs"].pop(oldest_key, None)
    
    org = store["orgs"].setdefault(org_code, {
        "call_seed_ts": 0,
        "call_seed_data": [],
        "call_meta_poll_ts": 0,
        "ivr_calls_ts": 0,
        "ivr_calls_data": [],
        "agent_seed_ts": 0,
        "agent_presence": {},
        "agent_routing": {},
        "call_meta": {},
        "daily_stats_cache": {},
    })
    # Ensure backward-compatible keys
    org.setdefault("call_seed_ts", 0)
    org.setdefault("call_seed_data", [])
    org.setdefault("call_meta_poll_ts", 0)
    org.setdefault("ivr_calls_ts", 0)
    org.setdefault("ivr_calls_data", [])
    org.setdefault("agent_seed_ts", 0)
    org.setdefault("agent_presence", {})
    org.setdefault("agent_routing", {})
    org.setdefault("call_meta", {})
    org.setdefault("daily_stats_cache", {})
    return org

def _get_shared_call_seed(org_code):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        return org.get("call_seed_ts", 0), list(org.get("call_seed_data") or [])

def _reserve_call_seed(org_code, now_ts, min_interval=10):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        last_ts = org.get("call_seed_ts", 0)
        if (now_ts - last_ts) < min_interval:
            return False
        org["call_seed_ts"] = now_ts
        return True

def _update_call_seed(org_code, seed_calls, now_ts, max_items=300):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        org["call_seed_ts"] = now_ts
        data = list(seed_calls or [])
        if len(data) > max_items:
            data = data[:max_items]
        org["call_seed_data"] = data

def _get_shared_call_meta(org_code, max_age_seconds=600):
    store = _shared_seed_store()
    now = pytime.time()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        meta = org.get("call_meta", {})
        stale = [cid for cid, v in meta.items() if (now - v.get("ts", 0)) > max_age_seconds]
        for cid in stale:
            meta.pop(cid, None)
        return dict(meta)

def _update_call_meta(org_code, calls, now_ts, max_items=300):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        meta = org.setdefault("call_meta", {})
        for c in calls or []:
            cid = c.get("conversation_id")
            if not cid:
                continue
            qname = c.get("queue_name")
            qid = c.get("queue_id")
            phone = c.get("phone")
            direction = c.get("direction")
            direction_label = c.get("direction_label")
            wg = c.get("wg")
            agent_id = c.get("agent_id")
            agent_name = c.get("agent_name")
            if not qname and not qid and not phone and not direction and not direction_label and not wg and not agent_id and not agent_name:
                continue
            entry = meta.get(cid, {})
            if qname and not _is_generic_queue_name(qname):
                entry["queue_name"] = qname
            if qid:
                entry["queue_id"] = qid
            if phone:
                entry["phone"] = phone
            if direction:
                entry["direction"] = direction
            if direction_label:
                entry["direction_label"] = direction_label
            if wg:
                entry["wg"] = wg
            if agent_id:
                entry["agent_id"] = agent_id
            if agent_name:
                entry["agent_name"] = agent_name
            entry["ts"] = now_ts
            meta[cid] = entry
        if len(meta) > max_items:
            oldest = sorted(meta.items(), key=lambda kv: kv[1].get("ts", 0))[:len(meta) - max_items]
            for cid, _ in oldest:
                meta.pop(cid, None)

def _reserve_call_meta_poll(org_code, now_ts, min_interval=10):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        last_ts = org.get("call_meta_poll_ts", 0)
        if (now_ts - last_ts) < min_interval:
            return False
        org["call_meta_poll_ts"] = now_ts
        return True

def _get_shared_daily_stats(org_code, cache_key, max_age_seconds=300):
    store = _shared_seed_store()
    now = pytime.time()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        daily_cache = org.setdefault("daily_stats_cache", {})
        stale = [k for k, v in daily_cache.items() if (now - v.get("ts", 0)) > max_age_seconds]
        for k in stale:
            daily_cache.pop(k, None)
        entry = daily_cache.get(cache_key)
        if not entry:
            return None
        return entry.get("data")

def _set_shared_daily_stats(org_code, cache_key, data, now_ts, max_items=60):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        daily_cache = org.setdefault("daily_stats_cache", {})
        daily_cache[cache_key] = {"ts": now_ts, "data": data}
        if len(daily_cache) > max_items:
            oldest = sorted(daily_cache.items(), key=lambda kv: kv[1].get("ts", 0))[:len(daily_cache) - max_items]
            for key, _ in oldest:
                daily_cache.pop(key, None)

def _get_shared_agent_seed(org_code):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        return org.get("agent_seed_ts", 0), dict(org.get("agent_presence") or {}), dict(org.get("agent_routing") or {})

def _reserve_agent_seed(org_code, now_ts, min_interval=60):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        last_ts = org.get("agent_seed_ts", 0)
        if (now_ts - last_ts) < min_interval:
            return False
        org["agent_seed_ts"] = now_ts
        return True

def _merge_agent_seed(org_code, presence_map, routing_map, now_ts, max_items=1000):
    """Merge agent presence/routing data with size limit to prevent memory bloat."""
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        if presence_map:
            org["agent_presence"].update(presence_map)
            # Enforce max size - keep most recent entries
            if len(org["agent_presence"]) > max_items:
                # Remove oldest half when limit exceeded
                keys_to_remove = list(org["agent_presence"].keys())[:len(org["agent_presence"]) - max_items]
                for k in keys_to_remove:
                    org["agent_presence"].pop(k, None)
        if routing_map:
            org["agent_routing"].update(routing_map)
            # Enforce max size
            if len(org["agent_routing"]) > max_items:
                keys_to_remove = list(org["agent_routing"].keys())[:len(org["agent_routing"]) - max_items]
                for k in keys_to_remove:
                    org["agent_routing"].pop(k, None)
        org["agent_seed_ts"] = now_ts

def _get_shared_ivr_calls(org_code):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        return org.get("ivr_calls_ts", 0), list(org.get("ivr_calls_data") or [])

def _reserve_ivr_calls(org_code, now_ts, min_interval=10):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        last_ts = org.get("ivr_calls_ts", 0)
        if (now_ts - last_ts) < min_interval:
            return False
        org["ivr_calls_ts"] = now_ts
        return True

def _update_ivr_calls(org_code, calls, now_ts, max_items=300):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        org["ivr_calls_ts"] = now_ts
        data = list(calls or [])
        if len(data) > max_items:
            data = data[:max_items]
        org["ivr_calls_data"] = data

def _get_session_id():
    if "_session_id" not in st.session_state:
        st.session_state._session_id = str(uuid.uuid4())
    return st.session_state._session_id

def _clear_live_panel_caches(org_code, clear_shared=False):
    try:
        # Clear page-local panel timers/caches
        for k in [
            "_call_panel_last_snapshot_ts",
            "_call_panel_last_meta_poll_ts",
            "_call_panel_last_reconcile_ts",
            "_call_panel_last_reconcile_ids",
        ]:
            st.session_state.pop(k, None)

        if clear_shared:
            # Optional hard cleanup for explicit restart/logout flows.
            store = _shared_seed_store()
            with store["lock"]:
                org_data = store.get("orgs", {}).get(org_code) or {}
                org_data["call_seed_ts"] = 0
                org_data["call_seed_data"] = []
                org_data["call_meta"] = {}
                org_data["call_meta_poll_ts"] = 0
                org_data["daily_stats_cache"] = {}

            notif_store = _shared_notif_store()
            with notif_store["lock"]:
                global_nm = notif_store.get("global", {}).get(org_code)
                if global_nm and hasattr(global_nm, "active_conversations"):
                    with global_nm._lock:
                        global_nm.active_conversations = {}
                call_nm = notif_store.get("call", {}).get(org_code)
                if call_nm and hasattr(call_nm, "waiting_calls"):
                    with call_nm._lock:
                        call_nm.waiting_calls = {}
    except Exception:
        pass

def _prune_admin_group_member_cache(max_entries=40):
    """Keep admin group member caches bounded in session_state."""
    try:
        keys = [k for k in st.session_state.keys() if k.startswith("admin_group_members_")]
        if len(keys) <= max_entries:
            return
        remove_count = len(keys) - max_entries
        for k in keys[:remove_count]:
            st.session_state.pop(k, None)
            st.session_state.pop(f"refresh_{k}", None)
    except Exception:
        pass

def _register_org_session_queues(org_code, queues_map, agent_queues_map, max_orgs=20):
    store = _shared_org_session_store()
    now = pytime.time()
    session_id = _get_session_id()
    with store["lock"]:
        # Enforce max orgs to prevent memory bloat
        if len(store["orgs"]) > max_orgs and org_code not in store["orgs"]:
            # Remove org with no active sessions or oldest
            empty_orgs = [k for k, v in store["orgs"].items() if not v.get("sessions")]
            if empty_orgs:
                store["orgs"].pop(empty_orgs[0], None)
            else:
                oldest_key = next(iter(store["orgs"]), None)
                if oldest_key:
                    store["orgs"].pop(oldest_key, None)
        
        org = store["orgs"].setdefault(org_code, {"sessions": {}})
        org["sessions"][session_id] = {
            "ts": now,
            "queues": queues_map,
            "agent_queues": agent_queues_map
        }
        # Prune stale sessions
        stale = [sid for sid, s in org["sessions"].items() if now - s.get("ts", 0) > SESSION_TTL_SECONDS]
        for sid in stale:
            org["sessions"].pop(sid, None)
        # Build union across active sessions
        union_queues = {}
        union_agent = {}
        for s in org["sessions"].values():
            union_queues.update(s.get("queues", {}))
            union_agent.update(s.get("agent_queues", {}))
        session_count = len(org["sessions"])
    return union_queues, union_agent, session_count

def _get_org_session_stats(org_code):
    store = _shared_org_session_store()
    now = pytime.time()
    with store["lock"]:
        org = store["orgs"].get(org_code, {"sessions": {}})
        stale = [sid for sid, s in org["sessions"].items() if now - s.get("ts", 0) > SESSION_TTL_SECONDS]
        for sid in stale:
            org["sessions"].pop(sid, None)
        union_queues = {}
        union_agent = {}
        for s in org["sessions"].values():
            union_queues.update(s.get("queues", {}))
            union_agent.update(s.get("agent_queues", {}))
        return len(org["sessions"]), len(union_queues), len(union_agent)

def _get_union_session_maps(org_code):
    store = _shared_org_session_store()
    now = pytime.time()
    with store["lock"]:
        org = store["orgs"].get(org_code, {"sessions": {}})
        stale = [sid for sid, s in org["sessions"].items() if now - s.get("ts", 0) > SESSION_TTL_SECONDS]
        for sid in stale:
            org["sessions"].pop(sid, None)
        union_queues = {}
        union_agent = {}
        for s in org["sessions"].values():
            union_queues.update(s.get("queues", {}))
            union_agent.update(s.get("agent_queues", {}))
        return union_queues, union_agent

def _remove_org_session(org_code):
    store = _shared_org_session_store()
    session_id = st.session_state.get("_session_id")
    if not session_id:
        return
    with store["lock"]:
        org = store["orgs"].get(org_code)
        if not org:
            return
        org["sessions"].pop(session_id, None)

def get_shared_data_manager(org_code, max_orgs=20):
    store = _get_dm_store()
    with store["lock"]:
        # Enforce max orgs limit to prevent memory bloat
        if len(store["data"]) > max_orgs and org_code not in store["data"]:
            oldest_key = next(iter(store["data"]), None)
            if oldest_key:
                old_dm = store["data"].pop(oldest_key, None)
                if old_dm:
                    try:
                        old_dm.force_stop()
                    except Exception:
                        pass
        dm = store["data"].get(org_code)
        if dm is None:
            dm = DataManager()
            store["data"][org_code] = dm
    # Apply persisted enabled/disabled state every time
    dm.enabled = is_dm_enabled(org_code)
    return dm

def get_existing_data_manager(org_code):
    store = _get_dm_store()
    with store["lock"]:
        return store["data"].get(org_code)

def remove_shared_data_manager(org_code):
    store = _get_dm_store()
    with store["lock"]:
        dm = store["data"].pop(org_code, None)
    if dm:
        dm.force_stop()

_org_maps_lock = threading.Lock()
_org_maps_cache = {}

def ensure_data_manager():
    org_code = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
    dm = get_shared_data_manager(org_code)
    st.session_state.data_manager = dm
    return dm

def ensure_notifications_manager(max_orgs=20):
    org_code = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
    store = _shared_notif_store()
    with store["lock"]:
        # Enforce max orgs limit
        if len(store["call"]) > max_orgs and org_code not in store["call"]:
            oldest_key = next(iter(store["call"]), None)
            if oldest_key:
                try:
                    store["call"][oldest_key].stop()
                except Exception:
                    pass
                store["call"].pop(oldest_key, None)
        nm = store["call"].get(org_code)
        if nm is None:
            nm = NotificationManager()
            store["call"][org_code] = nm
    return nm

def ensure_agent_notifications_manager(max_orgs=20):
    org_code = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
    store = _shared_notif_store()
    with store["lock"]:
        # Enforce max orgs limit
        if len(store["agent"]) > max_orgs and org_code not in store["agent"]:
            oldest_key = next(iter(store["agent"]), None)
            if oldest_key:
                try:
                    store["agent"][oldest_key].stop()
                except Exception:
                    pass
                store["agent"].pop(oldest_key, None)
        nm = store["agent"].get(org_code)
        if nm is None or not hasattr(nm, "seed_users_missing") or not hasattr(nm, "get_active_calls"):
            nm = AgentNotificationManager()
            store["agent"][org_code] = nm
    return nm

def ensure_global_conversation_manager(max_orgs=20):
    org_code = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
    store = _shared_notif_store()
    with store["lock"]:
        # Enforce max orgs limit
        if len(store["global"]) > max_orgs and org_code not in store["global"]:
            oldest_key = next(iter(store["global"]), None)
            if oldest_key:
                try:
                    store["global"][oldest_key].stop()
                except Exception:
                    pass
                store["global"].pop(oldest_key, None)
        nm = store["global"].get(org_code)
        if nm is None or not hasattr(nm, "get_active_conversations"):
            nm = GlobalConversationNotificationManager()
            store["global"][org_code] = nm
    return nm

def _fetch_org_maps(api):
    users = api.get_users()
    queues = api.get_queues()
    wrapup = api.get_wrapup_codes()
    presence = api.get_presence_definitions()
    users_map = {u['name']: u['id'] for u in users}
    users_info = {
        u['id']: {
            'name': u.get('name', ''),
            'username': u.get('username', ''),
            'email': u.get('email', '')
        } for u in users
    }
    queues_map = {q['name']: q['id'] for q in queues}
    return {
        "users": users,
        "queues": queues,
        "wrapup": wrapup,
        "presence": presence,
        "users_map": users_map,
        "users_info": users_info,
        "queues_map": queues_map,
        # Lightweight copies for cache storage (no raw API lists)
        "_lite": {
            "wrapup": wrapup,
            "presence": presence,
            "users_map": users_map,
            "users_info": users_info,
            "queues_map": queues_map,
        }
    }

def get_shared_org_maps(org_code, api, ttl_seconds=300, force_refresh=False):
    now = pytime.time()
    with _org_maps_lock:
        # Prune old entries to prevent memory growth - reduced from 6h to 30min
        stale = [k for k, v in _org_maps_cache.items() if now - v.get("ts", 0) > 1800]
        for k in stale:
            _org_maps_cache.pop(k, None)
        if len(_org_maps_cache) > 10:  # Reduced from 100 to 10
            oldest = sorted(_org_maps_cache.items(), key=lambda kv: kv[1].get("ts", 0))[:len(_org_maps_cache) - 10]
            for k, _ in oldest:
                _org_maps_cache.pop(k, None)
        entry = _org_maps_cache.get(org_code)
        if entry and not force_refresh and (now - entry.get("ts", 0)) < ttl_seconds:
            return entry
    maps = _fetch_org_maps(api)
    # Store lightweight version in cache (no raw user/queue lists)
    lite = maps.pop("_lite", maps)
    lite_entry = {"ts": now, **lite}
    with _org_maps_lock:
        _org_maps_cache[org_code] = lite_entry
    # Return full maps (including raw lists) for immediate use
    maps["ts"] = now
    return maps

def refresh_data_manager_queues():
    """Calculates optimized agent_queues_map and starts/updates DataManager."""
    if not st.session_state.get('api_client') or not st.session_state.get('queues_map'):
        return
    org_code = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
    if not is_dm_enabled(org_code):
        return
        
    all_dashboard_queues = {} # For metrics (Observations/Daily)
    agent_queues_map = {} # For agent details (First queue of card)
    use_agent_notif = st.session_state.get('use_agent_notifications', True)
    
    if 'dashboard_cards' in st.session_state:
        norm_map = {k.strip(): v for k, v in st.session_state.queues_map.items()}
        for card in st.session_state.dashboard_cards:
            q_list = card.get('queues', [])
            if q_list:
                # 1. Total Metrics Queues
                for q_name in q_list:
                    q_name = q_name.strip()
                    q_id = norm_map.get(q_name)
                    if q_id:
                        all_dashboard_queues[q_name] = q_id
                
                # 2. Agent Details Primary Queue (First in card list)
                primary_q = q_list[0].strip()
                p_id = norm_map.get(primary_q)
                if p_id:
                    if len(agent_queues_map) < 50: # Safety Cap: Max 50 monitored queues for agents
                        agent_queues_map[primary_q] = p_id
    
    # Register this session's queues and compute union across active sessions
    union_queues, union_agent_queues, sess_count = _register_org_session_queues(org_code, all_dashboard_queues, agent_queues_map)
    
    # (debug log removed for build)
    
    # We use all_dashboard_queues for overall metrics (efficiency)
    # Pass empty dicts ({}) if empty, do NOT fall back to 'None' or full map
    st.session_state.data_manager.update_api_client(st.session_state.api_client, st.session_state.get('presence_map'))
    dm_agent_queues = {} if use_agent_notif else union_agent_queues
    st.session_state.data_manager.start(union_queues, dm_agent_queues)

def create_gauge_chart(value, title, height=250):
    try:
        if value is None or not np.isfinite(float(value)):
            value = 0
    except Exception:
        value = 0
    value = float(value)
    fig = go.Figure(go.Indicator(
        mode="gauge", value=value, title={'text': title},
        gauge={'axis': {'range': [0, 100]}, 'bar': {'color': "#00AEC7"},
               'steps': [{'range': [0, 50], 'color': "#ffebee"}, {'range': [50, 80], 'color': "#fff3e0"}, {'range': [80, 100], 'color': "#e8f5e9"}]}))
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=45, b=10), autosize=True)
    # Scale number size with chart height
    num_size = max(12, int(height * 0.12))
    fig.add_annotation(x=0.5, y=0.05, text=f"{value:.0f}", showarrow=False,
                       font=dict(size=num_size, color="#64748b"))
    return fig

def create_donut_chart(data_dict, title, height=300):
    safe_data = {}
    for k, v in (data_dict or {}).items():
        try:
            v = float(v)
        except Exception:
            v = 0
        if np.isfinite(v) and v > 0:
            safe_data[k] = v
    filtered_data = safe_data or {"N/A": 1}
    fig = px.pie(pd.DataFrame(list(filtered_data.items()), columns=['Status', 'Count']), 
                 values='Count', names='Status', title=title, hole=0.6, color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(height=height, margin=dict(l=20, r=20, t=50, b=20))
    return fig

def sanitize_numeric_df(df):
    if df is None or df.empty:
        return df
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            series = pd.to_numeric(df[col], errors="coerce")
            series = series.replace([np.inf, -np.inf], np.nan).fillna(0)
            df[col] = series
    return df

def render_downloads(df, base_name):
    c1, c2, c3, c4 = st.columns(4, gap="small")
    with c1:
        st.download_button("Excel", data=to_excel(df), file_name=f"{base_name}.xlsx", width='stretch')
    with c2:
        st.download_button("CSV", data=to_csv(df), file_name=f"{base_name}.csv", mime="text/csv", width='stretch')
    with c3:
        st.download_button("Parquet", data=to_parquet(df), file_name=f"{base_name}.parquet", mime="application/octet-stream", width='stretch')
    with c4:
        st.download_button("PDF", data=to_pdf(df, title=base_name), file_name=f"{base_name}.pdf", mime="application/pdf", width='stretch')
# --- INITIALIZATION ---
if 'api_client' not in st.session_state: st.session_state.api_client = None
if 'genesys_logged_out' not in st.session_state: st.session_state.genesys_logged_out = False
if 'users_map' not in st.session_state: st.session_state.users_map = {}
if 'queues_map' not in st.session_state: st.session_state.queues_map = {}
if 'users_info' not in st.session_state: st.session_state.users_info = {}
if 'language' not in st.session_state: st.session_state.language = "TR"
if 'app_user' not in st.session_state: st.session_state.app_user = None
if 'wrapup_map' not in st.session_state: st.session_state.wrapup_map = {}

def init_session_state():
    if 'page' not in st.session_state: st.session_state.page = "Dashboard"
    if 'show_agent_panel' not in st.session_state: st.session_state.show_agent_panel = False
    if 'show_call_panel' not in st.session_state: st.session_state.show_call_panel = False
    if 'notifications_manager' not in st.session_state: st.session_state.notifications_manager = None
    if 'agent_notifications_manager' not in st.session_state: st.session_state.agent_notifications_manager = None
    if 'use_agent_notifications' not in st.session_state: st.session_state.use_agent_notifications = True
    if 'logged_in' not in st.session_state: st.session_state.logged_in = False
    if 'last_console_log_count' not in st.session_state: st.session_state.last_console_log_count = 0 # Track logged errors

def log_to_console(message, level='error'):
    """Injects JavaScript to log to browser console."""
    js_code = f"""
    <script>
        console.{level}("☁️ GenesysApp: {message}");
    </script>
    """
    st.markdown(js_code, unsafe_allow_html=True)

init_session_state()

# Ensure shared DataManager is available after login
if st.session_state.app_user and 'data_manager' not in st.session_state:
    ensure_data_manager()
data_manager = st.session_state.get('data_manager')

# Dashboard Config and Credentials will be loaded after login
if st.session_state.app_user and 'dashboard_config_loaded' not in st.session_state:
    org = st.session_state.app_user.get('org_code', 'default')
    config = load_dashboard_config(org)
    st.session_state.dashboard_layout, st.session_state.dashboard_cards = config.get("layout", 1), config.get("cards", [{"id": 0, "title": "", "queues": [], "size": "medium"}])
    st.session_state.dashboard_config_loaded = True

# --- APP LOGIN ---
if not st.session_state.app_user:
    # Try Auto-Login from Encrypted Session File
    saved_session = load_app_session()
    if saved_session:
        st.session_state.app_user = saved_session
        ensure_data_manager()
        st.rerun()

    st.markdown("<h1 style='text-align: center;'>Genesys Reporting API</h1>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("app_login_form"):
            st.subheader(get_text(st.session_state.language, "login_title"))
            u_org = st.text_input(get_text(st.session_state.language, "org_code"), value="default")
            u_name = st.text_input(get_text(st.session_state.language, "username"))
            u_pass = st.text_input(get_text(st.session_state.language, "password"), type="password")
            remember_me = st.checkbox(get_text(st.session_state.language, "remember_me"), value=False, help="Bu cihazda oturumu hatirla.")
            
            if st.form_submit_button(get_text(st.session_state.language, "login"), width='stretch'):
                user_data = auth_manager.authenticate(u_org, u_name, u_pass)
                if user_data:
                    # Exclude password hash from session state and file
                    safe_user_data = {k: v for k, v in user_data.items() if k != 'password'}
                    full_user = {"username": u_name, **safe_user_data}
                    
                    # Handle Remember Me
                    if remember_me:
                        save_app_session(full_user)
                    else:
                        delete_app_session()
                        
                    st.session_state.app_user = full_user
                    ensure_data_manager()
                    st.rerun()
                else:
                    st.error("Hatalı organizasyon, kullanıcı adı veya şifre!")
    st.stop()

# --- AUTO-LOGIN GENESYS ---
if st.session_state.app_user:
    org = st.session_state.app_user.get('org_code', 'default')
    saved_creds = load_credentials(org)
    
    logged_out_flag = os.path.exists(_org_flag_path(org))
    if not st.session_state.api_client and saved_creds and not st.session_state.get('genesys_logged_out') and not logged_out_flag:
        cid, csec, reg = saved_creds.get("client_id"), saved_creds.get("client_secret"), saved_creds.get("region", "mypurecloud.ie")
        if cid and csec:
            client, err = authenticate(cid, csec, reg, org_code=org)
            if client:
                st.session_state.api_client = client
                api = GenesysAPI(client)
                maps = get_shared_org_maps(org, api, ttl_seconds=300)
                users_info_map = maps.get("users_info", {}) or {}
                if users_info_map and not any((u.get("username") or "").strip() for u in users_info_map.values()):
                    maps = get_shared_org_maps(org, api, ttl_seconds=300, force_refresh=True)
                st.session_state.users_map = maps.get("users_map", {})
                st.session_state.users_info = maps.get("users_info", {})
                st.session_state.queues_map = maps.get("queues_map", {})
                st.session_state.wrapup_map = maps.get("wrapup", {})
                st.session_state.presence_map = maps.get("presence", {})
                st.session_state.org_config = saved_creds # Store for later use (refresh interval etc.)
                refresh_data_manager_queues()

    # Ensure DataManager is active and in sync on every rerun
    if st.session_state.get('api_client') and st.session_state.get('queues_map'):
        refresh_data_manager_queues()

# --- SIDEBAR ---
with st.sidebar:
    st.session_state.language = st.selectbox("Dil / Language", ["TR", "EN"])
    lang = st.session_state.language
    st.write(f"Hoş geldiniz, **{st.session_state.app_user['username']}** ({st.session_state.app_user['role']})")
    if st.button(get_text(lang, "logout_app"), type="secondary", width='stretch'):
        try:
            _remove_org_session(st.session_state.app_user.get('org_code', 'default'))
        except Exception:
            pass
        # Clear session file and cookie FIRST
        delete_app_session()
        # Clear all session state
        st.session_state.app_user = None
        st.session_state.api_client = None
        st.session_state.logged_in = False
        st.session_state.genesys_logged_out = True
        if 'dashboard_config_loaded' in st.session_state:
            del st.session_state.dashboard_config_loaded
        if 'data_manager' in st.session_state:
            del st.session_state.data_manager
        st.rerun()
    st.title(get_text(lang, "settings"))
    
    # Define menu options based on role
    menu_options = []
    role = st.session_state.app_user['role']
    menu_options.append(get_text(lang, "menu_metrics_guide"))
    if role in ["Admin", "Manager", "Reports User"]:
        menu_options.append(get_text(lang, "menu_reports"))
    if role in ["Admin", "Manager", "Dashboard User"]:
        menu_options.append(get_text(lang, "menu_dashboard"))
    if role == "Admin":
        menu_options.append(get_text(lang, "menu_users"))
        menu_options.append(get_text(lang, "menu_org_settings"))
        menu_options.append(get_text(lang, "admin_panel"))

    st.session_state.page = st.radio(get_text(lang, "sidebar_title"), menu_options)
    st.write("---")
    st.subheader(get_text(lang, "export_config"))
    st.download_button(label=get_text(lang, "export_config"), data=get_all_configs_json(), file_name=f"genesys_config_{datetime.now().strftime('%Y%m%d')}.json", mime="application/json")
    
    st.write("---")
    st.subheader(get_text(lang, "import_config"))
    up_file = st.file_uploader(get_text(lang, "import_config"), type=["json"])
    if up_file and st.button(get_text(lang, "save"), key="import_btn"):
        if import_all_configs(up_file.getvalue().decode("utf-8")):
            st.success(get_text(lang, "config_imported"))
            if 'dashboard_config_loaded' in st.session_state: del st.session_state.dashboard_config_loaded
        else: st.error(get_text(lang, "config_import_error"))

    # --- ERROR CONSOLE SYNC ---
    # Check DataManager for new errors and log them to browser
    if st.session_state.get('data_manager'): # Use .get() for safety
        dm = st.session_state.data_manager
        with dm._lock:
            current_log_count = len(dm.error_log)
            if current_log_count > st.session_state.last_console_log_count:
                # Log new errors
                for i in range(st.session_state.last_console_log_count, current_log_count):
                    log_to_console(dm.error_log[i])
                
                # Update tracker
                st.session_state.last_console_log_count = current_log_count

# --- MAIN LOGIC ---
lang = st.session_state.language
org = st.session_state.app_user.get('org_code', 'default')
page = st.session_state.page
role = st.session_state.app_user.get('role', 'User')

# On page change, clear live panel caches to avoid stale carry-over between pages.
prev_page = st.session_state.get("_prev_page")
if prev_page is None:
    st.session_state["_prev_page"] = page
elif prev_page != page:
    _clear_live_panel_caches(org)
    st.session_state["_prev_page"] = page

# Block only reports/dashboard if API is missing
if page in [get_text(lang, "menu_reports"), get_text(lang, "menu_dashboard")] and not st.session_state.api_client:
    st.title(get_text(lang, "title"))
    st.info(get_text(lang, "welcome"))
    st.stop()

# Block reports if DataManager is disabled
if page == get_text(lang, "menu_reports") and not is_dm_enabled(org):
    st.warning(get_text(lang, "dm_disabled_reports"))
    st.stop()

# Now handle all pages
if page == get_text(lang, "menu_metrics_guide"):
    st.title(f"📘 {get_text(lang, 'menu_metrics_guide')}")
    ref_path = _resolve_resource_path("METRICS_REFERENCE.md")
    if os.path.exists(ref_path):
        try:
            with open(ref_path, "r", encoding="utf-8") as f:
                st.markdown(f.read())
        except Exception as e:
            st.error(f"Doküman okunamadı: {e}")
    else:
        st.warning(f"Referans dosyası bulunamadı: {ref_path}")

elif page == get_text(lang, "menu_reports"):
    st.title(get_text(lang, "menu_reports"))
    # --- SAVED VIEWS (Compact) ---
    with st.expander(f"📂 {get_text(lang, 'saved_views')}", expanded=False):
        presets = load_presets(org)
        # Single row layout for better alignment
        # Using vertical_alignment="bottom" (Streamlit 1.35+) to align button with inputs
        c_p1, c_p2, c_p3, c_p4 = st.columns([3, 2, 1, 1], gap="small", vertical_alignment="bottom")
        
        with c_p1:
            sel_p = st.selectbox(get_text(lang, "select_view"), [get_text(lang, "no_view_selected")] + [p['name'] for p in presets], key="preset_selector")
        
        def_p = {}
        if sel_p != get_text(lang, "no_view_selected"):
            p = next((p for p in presets if p['name'] == sel_p), None)
            if p:
                def_p = p
                for k in ["type", "names", "metrics", "granularity_label", "fill_gaps"]:
                    if k in p: st.session_state[f"rep_{k[:3]}"] = p[k]
        
        with c_p2:
            p_name_save = st.text_input(get_text(lang, "preset_name"), placeholder=get_text(lang, "preset_name_placeholder"))
            
        with c_p3:
            if st.button(f"💾 {get_text(lang, 'save')}", key="btn_save_view", width='stretch') and p_name_save:
                new_p = {"name": p_name_save, "type": st.session_state.get("rep_typ", "report_agent"), "names": st.session_state.get("rep_nam", []), "metrics": st.session_state.get("rep_met", DEFAULT_METRICS), "granularity_label": st.session_state.get("rep_gra", "Toplam"), "fill_gaps": st.session_state.get("rep_fil", False)}
                presets = [p for p in presets if p['name'] != p_name_save] + [new_p]
                save_presets(org, presets); st.success(get_text(lang, "view_saved")); st.rerun()

        with c_p4:
            can_delete = sel_p != get_text(lang, "no_view_selected")
            if st.button(f"🗑️ {get_text(lang, 'delete_view')}", key="btn_delete_view", width='stretch', disabled=not can_delete) and can_delete:
                presets = [p for p in presets if p['name'] != sel_p]
                save_presets(org, presets); st.success(get_text(lang, "view_deleted")); st.rerun()

    st.divider()

    # --- PRIMARY FILTERS (Always Visible) ---
    c1, c2 = st.columns(2)
    with c1:
        role = st.session_state.app_user['role']
        if role == "Reports User":
            rep_types = ["report_agent", "report_queue", "report_detailed", "report_agent_skill_detail", "report_agent_dnis_skill_detail", "interaction_search", "missed_interactions"]
        else: # Admin, Manager
            rep_types = ["report_agent", "report_queue", "report_detailed", "report_agent_skill_detail", "report_agent_dnis_skill_detail", "interaction_search", "chat_detail", "missed_interactions"]
        
        r_type = st.selectbox(
            get_text(lang, "report_type"), 
            rep_types, 
            format_func=lambda x: get_text(lang, x),
            key="rep_typ", index=rep_types.index(def_p.get("type", "report_agent")) if def_p.get("type", "report_agent") in rep_types else 0)
    with c2:
        is_agent = r_type == "report_agent"
        opts = list(st.session_state.users_map.keys()) if is_agent else list(st.session_state.queues_map.keys())
        if "rep_nam" in st.session_state:
            st.session_state.rep_nam = [n for n in st.session_state.rep_nam if n in opts]
        sel_names = st.multiselect(get_text(lang, "select_agents" if is_agent else "select_workgroups"), opts, key="rep_nam")
        sel_ids = [(st.session_state.users_map if is_agent else st.session_state.queues_map)[n] for n in sel_names]

    # Date & Time Selection (One Row)
    c_d1, c_d2, c_d3, c_d4 = st.columns(4)
    sd = c_d1.date_input("Start Date", datetime.today())
    st_ = c_d2.time_input(get_text(lang, "start_time"), time(0, 0))
    ed = c_d3.date_input("End Date", datetime.today())
    et = c_d4.time_input(get_text(lang, "end_time"), time(23, 59))
    
    # --- ADVANCED FILTERS (Collapsible) ---
    with st.expander(f"⚙️ {get_text(lang, 'advanced_filters')}", expanded=False):
        g1, g2 = st.columns(2)
        gran_opt = {get_text(lang, "total"): "P1D", get_text(lang, "30min"): "PT30M", get_text(lang, "1hour"): "PT1H"}
        sel_gran = g1.selectbox(get_text(lang, "granularity"), list(gran_opt.keys()), key="rep_gra")
        do_fill = g2.checkbox(get_text(lang, "fill_gaps"), key="rep_fil")
        
        # Media Type Filter
        MEDIA_TYPE_OPTIONS = ["voice", "chat", "email", "callback", "message"]
        sel_media_types = st.multiselect(
            get_text(lang, "media_type"),
            MEDIA_TYPE_OPTIONS,
            default=[],
            format_func=lambda x: x.capitalize(),
            key="rep_media",
            help=get_text(lang, "media_type_help")
        )

        if r_type in ["interaction_search", "chat_detail", "missed_interactions"]:
            st.session_state.rep_max_records = st.number_input(
                "Maksimum kayıt (performans için)",
                min_value=100,
                max_value=20000,
                value=int(st.session_state.get("rep_max_records", 5000)),
                step=100,
                help="Yüksek aralıklar bellek kullanımını artırır. Varsayılan 5000 kayıt ile sınırlandırılır."
            )
        if r_type == "chat_detail":
            st.session_state.rep_enrich_limit = st.number_input(
                "Zenginleştirilecek chat sayısı (attributes)",
                min_value=50,
                max_value=5000,
                value=int(st.session_state.get("rep_enrich_limit", 500)),
                step=50,
                help="Her chat için ek API çağrısı yapılır. Limit yükseldikçe bellek ve süre artar."
            )

        # Metrics Selection
        user_metrics = st.session_state.app_user.get('metrics', [])
        selection_options = user_metrics if user_metrics and role != "Admin" else ALL_METRICS
        
        if r_type not in ["interaction_search", "chat_detail", "missed_interactions"]:
            if "last_metrics" in st.session_state and st.session_state.last_metrics:
                auto_def_metrics = [m for m in st.session_state.last_metrics if m in selection_options]
            else:
                auto_def_metrics = [m for m in ["nOffered", "nAnswered", "tAnswered", "tTalk", "tHandle"] if m in selection_options]
            sel_mets = st.multiselect(get_text(lang, "metrics"), selection_options, default=auto_def_metrics, format_func=lambda x: get_text(lang, x), key="rep_met")
        else:
            sel_mets = []

    if r_type == "report_agent_skill_detail":
        st.info(get_text(lang, "skill_report_info"))
    if r_type == "report_agent_dnis_skill_detail":
        st.info(get_text(lang, "dnis_skill_report_info"))

    if r_type == "chat_detail":
            st.info(get_text(lang, "chat_detail_info"))
            if st.button(get_text(lang, "fetch_chat_data"), type="primary", width='stretch'):
             with st.spinner(get_text(lang, "fetching_data")):
                 start_date = datetime.combine(sd, st_) - timedelta(hours=saved_creds.get("utc_offset", 3))
                 end_date = datetime.combine(ed, et) - timedelta(hours=saved_creds.get("utc_offset", 3))
                 
                 api = GenesysAPI(st.session_state.api_client)
                 max_records = int(st.session_state.get("rep_max_records", 5000))
                 dfs = []
                 total_rows = 0
                 u_offset = saved_creds.get("utc_offset", 3)
                 skill_lookup = st.session_state.get("skills_map", {}) or api.get_routing_skills()
                 st.session_state.skills_map = skill_lookup
                 language_lookup = api.get_languages()
                 if language_lookup:
                     st.session_state.languages_map = language_lookup
                 else:
                     language_lookup = st.session_state.get("languages_map", {})

                 for page in _iter_conversation_pages(api, start_date, end_date, max_records=max_records, chunk_days=3):
                     df_chunk = process_conversation_details(
                         {"conversations": page},
                         st.session_state.users_info,
                         st.session_state.queues_map,
                         st.session_state.wrapup_map,
                         include_attributes=True,
                         utc_offset=u_offset,
                         skill_map=skill_lookup,
                         language_map=language_lookup
                     )
                     if not df_chunk.empty:
                         dfs.append(df_chunk)
                         total_rows += len(df_chunk)
                 df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                 if max_records and total_rows >= max_records:
                     st.warning(f"Maksimum kayıt limiti ({max_records}) uygulandı. Daha geniş aralıklar için limiti artırabilirsiniz.")
                 
                 if not df.empty:
                     # Filter for Chat/Message types FIRST to reduce API calls
                     chat_types = ['chat', 'message', 'webchat', 'whatsapp', 'facebook', 'twitter', 'line', 'telegram']
                     df_chat = df[df['MediaType'].isin(chat_types)].copy()

                     if not df_chat.empty:
                         st.info(get_text(lang, "fetching_details_info").format(len(df_chat)))
                         
                         # Create a progress bar
                         progress_bar = st.progress(0)
                         enrich_limit = int(st.session_state.get("rep_enrich_limit", 500))
                         if enrich_limit and len(df_chat) > enrich_limit:
                             st.warning(f"Zenginleştirme limiti uygulandı: ilk {enrich_limit} kayıt.")
                             df_chat = df_chat.head(enrich_limit).copy()
                         total_chats = len(df_chat)
                         
                         # Prepare a list to collect updated attributes
                         enrichment_data = []

                         # Use the helper to fetch
                         api_instance = GenesysAPI(st.session_state.api_client)

                         for index, (idx, row) in enumerate(df_chat.iterrows()):
                             conv_id = row['Id']
                             # Update progress
                             progress_bar.progress((index + 1) / total_chats)
                             
                             try:
                                 # ENRICHMENT: Call Standard Conversation API
                                 # This is necessary because Analytics API often omits 'attributes' (Participant Data)
                                 full_conv = api_instance._get(f"/api/v2/conversations/{conv_id}")
                                 
                                 attrs = {}
                                 if full_conv and 'participants' in full_conv:
                                     # Look for customer participant who usually holds the attributes
                                     # Prioritize customer, then check others
                                     found_attrs = False
                                     for p in full_conv['participants']:
                                         if p.get('purpose') == 'customer' and 'attributes' in p and p['attributes']:
                                             attrs = p['attributes']
                                             found_attrs = True
                                             break
                                     
                                     if not found_attrs:
                                          for p in full_conv['participants']:
                                              if 'attributes' in p and p['attributes']:
                                                  attrs = p['attributes']
                                                  break
                                 
                                 # Append dict to list
                                 enrichment_data.append(attrs)
                                 
                             except Exception as e:
                                 # If individual fetch fails
                                 enrichment_data.append({})
                         
                         progress_bar.empty()
                         
                         # Merge attributes into the DataFrame
                         # usage of 'at' for direct assignment
                         for i, attrs in enumerate(enrichment_data):
                             original_index = df_chat.index[i]
                             for k, v in attrs.items():
                                 # Determine content to set. If it's a date or number, pandas might complain if column is object type,
                                 # but usually it handles it. 
                                 # We cast to string if needed to be safe? No, let pandas handle types.
                                 df_chat.at[original_index, k] = v
                     
                     if df_chat.empty and not df.empty:
                         st.warning("Seçilen tarih aralığında hiç 'Chat/Mesaj' kaydı bulunamadı. (Sesli çağrılar hariç tutuldu)")
                     elif not df_chat.empty:
                         # Display
                         st.dataframe(df_chat, width='stretch')
                         render_downloads(df_chat, f"chat_detail_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
                     else:
                         st.warning(get_text(lang, "no_data"))
                 else:
                     st.warning(get_text(lang, "no_data"))
                 try:
                     import gc as _gc
                     _gc.collect()
                 except Exception:
                     pass

    # --- MISSED INTERACTIONS REPORT ---
    if r_type == "missed_interactions":
        st.info(get_text(lang, "missed_interactions_info"))
        
        # Dynamic Column Selection (Reuse interaction cols)
        from src.lang import INTERACTION_COLUMNS
        default_cols = [c for c in INTERACTION_COLUMNS if c not in ["col_attributes", "col_media"]]
         # Let user select
        selected_cols_keys = st.multiselect(get_text(lang, "select_columns"), INTERACTION_COLUMNS, default=INTERACTION_COLUMNS, format_func=lambda x: get_text(lang, x))

        if st.button(get_text(lang, "fetch_missed_report"), type="primary", width='stretch'):
             with st.spinner(get_text(lang, "fetching_data")):
                 # Fetch data
                 # We need to fetch conversation details
                 # api = GenesysAPI(st.session_state.api_client) # already initialized above if needed, but let's re-init
                 api = GenesysAPI(st.session_state.api_client)
                 
                 s_dt = datetime.combine(sd, st_) - timedelta(hours=saved_creds.get("utc_offset", 3))
                 e_dt = datetime.combine(ed, et) - timedelta(hours=saved_creds.get("utc_offset", 3))
                 
                 # Get details
                 max_records = int(st.session_state.get("rep_max_records", 5000))
                 dfs = []
                 total_rows = 0
                 skill_lookup = st.session_state.get("skills_map", {}) or api.get_routing_skills()
                 st.session_state.skills_map = skill_lookup
                 language_lookup = api.get_languages()
                 if language_lookup:
                     st.session_state.languages_map = language_lookup
                 else:
                     language_lookup = st.session_state.get("languages_map", {})

                 for page in _iter_conversation_pages(api, s_dt, e_dt, max_records=max_records, chunk_days=3):
                     df_chunk = process_conversation_details(
                         {"conversations": page},
                         user_map=st.session_state.users_info,
                         queue_map=st.session_state.queues_map,
                         wrapup_map=st.session_state.wrapup_map,
                         include_attributes=True,
                         utc_offset=saved_creds.get("utc_offset", 3),
                         skill_map=skill_lookup,
                         language_map=language_lookup
                     )
                     if not df_chunk.empty:
                         dfs.append(df_chunk)
                         total_rows += len(df_chunk)
                 df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                 if max_records and total_rows >= max_records:
                     st.warning(f"Maksimum kayıt limiti ({max_records}) uygulandı. Daha geniş aralıklar için limiti artırabilirsiniz.")
                 
                 if not df.empty:
                     # Filter for MISSED Only
                     # Condition: ConnectionStatus is NOT "Cevaplandı" or "Ulaşıldı" or "Bağlandı"
                     # OR strictly match "Kaçan/Cevapsız", "Ulaşılamadı", "Bağlanamadı"
                     # STRICT REQUIREMENT: Only Inbound
                     
                     missed_statuses = ["Kaçan/Cevapsız", "Ulaşılamadı", "Bağlanamadı", "Missed", "Unreachable"]
                     # Filter logic
                     if "ConnectionStatus" in df.columns and "Direction" in df.columns:
                         # Use isin for stricter matching if possible, or string contains for flexibility
                         # And Filter for Inbound
                         df_missed = df[
                             (df["ConnectionStatus"].isin(missed_statuses)) & 
                             (df["Direction"].astype(str).str.lower() == "inbound")
                         ]
                     else:
                         df_missed = pd.DataFrame() # Should not happen

                     if not df_missed.empty:
                         # Rename columns
                         col_map_internal = {
                             "Direction": "col_direction",
                             "Ani": "col_ani",
                             "Dnis": "col_dnis",
                             "Wrapup": "col_wrapup",
                             "MediaType": "col_media",
                             "Duration": "col_duration",
                             "DisconnectType": "col_disconnect",
                             "Alert": "col_alert",
                             "HoldCount": "col_hold_count",
                             "ConnectionStatus": "col_connection",
                             "Start": "start_time",
                             "End": "end_time",
                             "Agent": "col_agent",
                             "Username": "col_username",
                             "Queue": "col_workgroup",
                             "Skill": "col_skill",
                             "Language": "col_language"
                         }
                         
                         final_cols = [k for k, v in col_map_internal.items() if v in selected_cols_keys]
                         df_filtered = df_missed[final_cols]
                         rename_final = {k: get_text(lang, col_map_internal[k]) for k in final_cols}
                         
                         final_df = df_filtered.rename(columns=rename_final)
                         
                         st.success(f"{len(final_df)} adet kaçan etkileşim bulundu.")
                         st.dataframe(final_df, width='stretch')
                         render_downloads(final_df, f"missed_interactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
                     else:
                         st.warning("Seçilen kriterlere uygun kaçan çağrı/etkileşim bulunamadı.")
                 else:
                     st.warning(get_text(lang, "no_data"))
                 try:
                     import gc as _gc
                     _gc.collect()
                 except Exception:
                     pass

    # --- INTERACTION SEARCH ---
    if r_type == "interaction_search":
        st.info(get_text(lang, "interaction_search_info"))
        
        # Dynamic Column Selection
        from src.lang import INTERACTION_COLUMNS
        default_cols = [c for c in INTERACTION_COLUMNS if c not in ["col_media", "col_wrapup"]] # Default subset
        
        # Allow user to customize columns if needed
        selected_cols_keys = st.multiselect(get_text(lang, "select_columns"), INTERACTION_COLUMNS, default=INTERACTION_COLUMNS, format_func=lambda x: get_text(lang, x))
        
        if st.button(get_text(lang, "fetch_interactions"), type="primary", width='stretch'):
             with st.spinner(get_text(lang, "fetching_data")):
                 start_date = datetime.combine(sd, st_) - timedelta(hours=saved_creds.get("utc_offset", 3))
                 end_date = datetime.combine(ed, et) - timedelta(hours=saved_creds.get("utc_offset", 3))
                 # Fetch data
                 api = GenesysAPI(st.session_state.api_client)
                 max_records = int(st.session_state.get("rep_max_records", 5000))
                 dfs = []
                 total_rows = 0
                 skill_lookup = st.session_state.get("skills_map", {}) or api.get_routing_skills()
                 st.session_state.skills_map = skill_lookup
                 language_lookup = api.get_languages()
                 if language_lookup:
                     st.session_state.languages_map = language_lookup
                 else:
                     language_lookup = st.session_state.get("languages_map", {})

                 for page in _iter_conversation_pages(api, start_date, end_date, max_records=max_records, chunk_days=3):
                     df_chunk = process_conversation_details(
                         {"conversations": page},
                         st.session_state.users_info,
                         st.session_state.queues_map,
                         st.session_state.wrapup_map,
                         utc_offset=saved_creds.get("utc_offset", 3),
                         skill_map=skill_lookup,
                         language_map=language_lookup
                     )
                     if not df_chunk.empty:
                         dfs.append(df_chunk)
                         total_rows += len(df_chunk)
                 df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                 if max_records and total_rows >= max_records:
                     st.warning(f"Maksimum kayıt limiti ({max_records}) uygulandı. Daha geniş aralıklar için limiti artırabilirsiniz.")
                 
                 if not df.empty:
                     # Rename columns first to internal keys then to display names
                     col_map_internal = {
                         "Direction": "col_direction",
                         "Ani": "col_ani",
                         "Dnis": "col_dnis",
                         "Wrapup": "col_wrapup",
                         "MediaType": "col_media",
                         "Duration": "col_duration",
                         "DisconnectType": "col_disconnect",
                         "Alert": "col_alert",
                         "HoldCount": "col_hold_count",
                         "ConnectionStatus": "col_connection",
                         "Start": "start_time",
                         "End": "end_time",
                         "Agent": "col_agent",
                         "Username": "col_username",
                         "Queue": "col_workgroup",
                         "Skill": "col_skill",
                         "Language": "col_language"
                     }
                     
                     # Filter columns based on selection
                     final_cols = [k for k, v in col_map_internal.items() if v in selected_cols_keys]
                     df_filtered = df[final_cols]
                     
                     # Rename to Display Names
                     rename_final = {k: get_text(lang, col_map_internal[k]) for k in final_cols}
                     
                     df_filtered = apply_duration_formatting(df_filtered.copy())
                     final_df = df_filtered.rename(columns=rename_final)
                     st.dataframe(final_df, width='stretch')
                     render_downloads(final_df, f"interactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
                 else:
                     st.warning(get_text(lang, "no_data"))
                 try:
                     import gc as _gc
                     _gc.collect()
                 except Exception:
                     pass

    # --- STANDARD REPORTS ---
    elif r_type not in ["chat_detail", "missed_interactions"] and st.button(get_text(lang, "fetch_report"), type="primary", width='stretch'):
        if not sel_mets: st.warning("Lütfen metrik seçiniz.")
        else:
            unsupported_aggregate_metrics = {"tOrganizationResponse", "tAcdWait", "nConsultConnected", "nConsultAnswered"}
            dropped_metrics = [m for m in sel_mets if m in unsupported_aggregate_metrics]
            sel_mets_effective = [m for m in sel_mets if m not in unsupported_aggregate_metrics]
            if dropped_metrics:
                st.warning(f"Bu metrikler aggregate endpoint tarafından desteklenmiyor ve çıkarıldı: {', '.join(dropped_metrics)}")
            if not sel_mets_effective:
                st.warning("Desteklenen bir metrik seçiniz.")
                st.stop()

            # Auto-save last used metrics
            st.session_state.last_metrics = sel_mets
            with st.spinner(get_text(lang, "fetching_data")):
                api = GenesysAPI(st.session_state.api_client)
                s_dt, e_dt = datetime.combine(sd, st_) - timedelta(hours=saved_creds.get("utc_offset", 3)), datetime.combine(ed, et) - timedelta(hours=saved_creds.get("utc_offset", 3))
                is_skill_detailed = r_type == "report_agent_skill_detail"
                is_dnis_skill_detailed = r_type == "report_agent_dnis_skill_detail"
                is_queue_skill = r_type == "report_queue"
                r_kind = "Agent" if r_type == "report_agent" else ("Workgroup" if r_type == "report_queue" else "Detailed")
                g_by = ['userId'] if r_kind == "Agent" else ((['queueId', 'requestedRoutingSkillId', 'requestedLanguageId'] if is_queue_skill else ['queueId']) if r_kind == "Workgroup" else (['userId', 'dnis', 'requestedRoutingSkillId', 'requestedLanguageId', 'queueId'] if is_dnis_skill_detailed else (['userId', 'requestedRoutingSkillId', 'requestedLanguageId', 'queueId'] if is_skill_detailed else ['userId', 'queueId'])))
                f_type = 'user' if r_kind == "Agent" else 'queue'
                
                resp = api.get_analytics_conversations_aggregate(s_dt, e_dt, granularity=gran_opt[sel_gran], group_by=g_by, filter_type=f_type, filter_ids=sel_ids or None, metrics=sel_mets_effective, media_types=sel_media_types or None)
                q_lookup = {v: k for k, v in st.session_state.queues_map.items()}
                skill_lookup = {}
                language_lookup = {}
                if is_skill_detailed or is_dnis_skill_detailed or is_queue_skill:
                    skill_lookup = st.session_state.get("skills_map", {}) or api.get_routing_skills()
                    st.session_state.skills_map = skill_lookup
                    language_lookup = api.get_languages()
                    if language_lookup:
                        st.session_state.languages_map = language_lookup
                    else:
                        language_lookup = st.session_state.get("languages_map", {})
                
                # For detailed report, we still need users_info for userId lookup, even though filter is queue
                lookup_map = st.session_state.users_info if r_kind in ["Agent", "Detailed"] else q_lookup
                report_type_key = "detailed_dnis_skill" if is_dnis_skill_detailed and r_kind == "Detailed" else ("detailed_skill" if is_skill_detailed and r_kind == "Detailed" else ("workgroup_skill" if is_queue_skill and r_kind == "Workgroup" else r_kind.lower()))
                df = process_analytics_response(
                    resp,
                    lookup_map,
                    report_type_key,
                    queue_map=q_lookup,
                    utc_offset=saved_creds.get("utc_offset", 3),
                    skill_map=skill_lookup,
                    language_map=language_lookup
                )
                
                if df.empty and is_agent:
                    agent_data = []
                    for uid in (sel_ids or st.session_state.users_info.keys()):
                        u = st.session_state.users_info.get(uid, {})
                        row = {"Name": u.get('name', uid), "Username": u.get('username', "").split('@')[0], "Id": uid}
                        if r_kind == "Detailed": row.update({"WorkgroupName": "-", "AgentName": row["Name"], "Id": f"{uid}|-"})
                        agent_data.append(row)
                    df = pd.DataFrame(agent_data)
                
                if not df.empty:
                    p_keys = ["tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue", "col_staffed_time", "nNotResponding"]
                    if any(m in sel_mets_effective for m in p_keys) and is_agent:
                        p_map = process_user_aggregates(api.get_user_aggregates(s_dt, e_dt, sel_ids or list(st.session_state.users_info.keys())), st.session_state.get('presence_map'))
                        for pk in ["tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue", "StaffedTime", "nNotResponding"]:
                            df[pk if pk != "StaffedTime" and pk != "nNotResponding" else ("col_staffed_time" if pk == "StaffedTime" else "nNotResponding")] = df["Id"].apply(lambda x: p_map.get(x.split('|')[0] if '|' in x else x, {}).get(pk, 0))
                    
                    if any(m in sel_mets_effective for m in ["col_login", "col_logout"]) and is_agent:
                        u_offset = saved_creds.get("utc_offset", 3)
                        d_map = process_user_details(api.get_user_status_details(s_dt, e_dt, sel_ids or list(st.session_state.users_info.keys())), utc_offset=u_offset)
                        if "col_login" in sel_mets: df["col_login"] = df["Id"].apply(lambda x: d_map.get(x.split('|')[0] if '|' in x else x, {}).get("Login", "N/A"))
                        if "col_logout" in sel_mets: df["col_logout"] = df["Id"].apply(lambda x: d_map.get(x.split('|')[0] if '|' in x else x, {}).get("Logout", "N/A"))

                    if do_fill and gran_opt[sel_gran] != "P1D": df = fill_interval_gaps(df, datetime.combine(sd, st_), datetime.combine(ed, et), gran_opt[sel_gran])

                    if is_dnis_skill_detailed and r_kind == "Detailed":
                        base = ["AgentName", "Username", "Dnis", "SkillName", "LanguageName", "WorkgroupName"]
                    elif is_skill_detailed and r_kind == "Detailed":
                        base = ["AgentName", "Username", "SkillName", "LanguageName", "WorkgroupName"]
                    elif is_queue_skill and r_kind == "Workgroup":
                        base = ["Name", "SkillName", "LanguageName"]
                    else:
                        base = (["AgentName", "Username", "WorkgroupName"] if r_kind == "Detailed" else (["Name", "Username"] if is_agent else ["Name"]))
                    if "Interval" in df.columns: base = ["Interval"] + base
                    for sm in sel_mets_effective:
                        if sm not in df.columns: df[sm] = 0
                    # Avoid duplicates if AvgHandle is already in sel_mets
                    mets_to_show = [m for m in sel_mets_effective if m in df.columns]
                    if "AvgHandle" in df.columns and "AvgHandle" not in mets_to_show:
                        mets_to_show.append("AvgHandle")
                    final_df = df[[c for c in base if c in df.columns] + mets_to_show]
                    
                    # Apply duration formatting
                    final_df = apply_duration_formatting(final_df)

                    rename = {"Interval": get_text(lang, "col_interval"), "AgentName": get_text(lang, "col_agent"), "Username": get_text(lang, "col_username"), "WorkgroupName": get_text(lang, "col_workgroup"), "Name": get_text(lang, "col_agent" if is_agent else "col_workgroup"), "AvgHandle": get_text(lang, "col_avg_handle"), "col_staffed_time": get_text(lang, "col_staffed_time"), "col_login": get_text(lang, "col_login"), "col_logout": get_text(lang, "col_logout"), "SkillName": get_text(lang, "col_skill"), "SkillId": get_text(lang, "col_skill_id"), "LanguageName": get_text(lang, "col_language"), "LanguageId": get_text(lang, "col_language_id"), "Dnis": get_text(lang, "col_dnis")}
                    rename.update({m: get_text(lang, m) for m in sel_mets_effective if m not in rename})
                    df_out = final_df.rename(columns=rename)
                    st.dataframe(df_out, width='stretch')

                    # Queue report chart based on selected interval
                    if r_kind == "Workgroup":
                        try:
                            if "Interval" in df.columns:
                                metric_for_chart = mets_to_show[0] if mets_to_show else None
                                if metric_for_chart and metric_for_chart in df.columns:
                                    chart_df = df[["Interval", metric_for_chart]].copy()
                                    chart_df["Interval"] = pd.to_datetime(chart_df["Interval"], errors="coerce")
                                    chart_df = chart_df.dropna()
                                    if not chart_df.empty:
                                        st.subheader(get_text(lang, "daily_stat"))
                                        st.line_chart(chart_df.set_index("Interval"))
                        except Exception:
                            pass
                    render_downloads(df_out, f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
                else: st.warning(get_text(lang, "no_data"))

elif page == get_text(lang, "menu_users") and role == "Admin":
    st.title(f"👤 {get_text(lang, 'menu_users')}")
    
    with st.expander(f"➕ {get_text(lang, 'add_new_user')}", expanded=True):
        # Auto password generator
        col_gen1, col_gen2 = st.columns([3, 1])
        with col_gen2:
            if st.button(f"🔐 {get_text(lang, 'generate_password_btn')}", key="gen_pw_btn"):
                st.session_state.generated_password = generate_password(12)
        
        generated_pw = st.session_state.get("generated_password", "")
        if generated_pw:
            col_gen1.success(f"Oluşturulan Şifre: **{generated_pw}**")
        
        with st.form("add_user_form"):
            new_un = st.text_input(get_text(lang, "username"))
            new_pw = st.text_input(get_text(lang, "password"), type="password", value=generated_pw, help=get_text(lang, "password_help"))
            new_role = st.selectbox(get_text(lang, "role"), ["Admin", "Manager", "Reports User", "Dashboard User"])
            
            from src.lang import ALL_METRICS
            new_mets = st.multiselect(get_text(lang, "allowed_metrics"), ALL_METRICS, format_func=lambda x: get_text(lang, x))
            
            if st.form_submit_button(get_text(lang, "add"), width='stretch'):
                if new_un and new_pw:
                    org = st.session_state.app_user.get('org_code', 'default')
                    success, msg = auth_manager.add_user(org, new_un, new_pw, new_role, new_mets)
                    if success: 
                        st.session_state.generated_password = ""  # Clear after use
                        st.success(msg)
                        st.rerun()
                    else: st.error(msg)
                else: st.warning("Ad ve şifre gereklidir.")
    
    st.write("---")
    st.subheader("Mevcut Kullanıcılar")
    org = st.session_state.app_user.get('org_code', 'default')
    all_users = auth_manager.get_all_users(org)
    for uname, udata in all_users.items():
        col1, col2, col3, col4 = st.columns([2, 2, 4, 1])
        col1.write(f"**{uname}**")
        col2.write(f"Rol: {udata.get('role', 'User')}")
        col3.write(f"Metrikler: {', '.join(udata.get('metrics', [])) if udata.get('metrics') else 'Hepsi'}")
        
        # Action Buttons Column
        with col4:
            if uname != "admin": # Don't delete self
                if st.button("🗑️", key=f"del_user_{uname}", help="Kullanıcıyı Sil"):
                    auth_manager.delete_user(org, uname)
                    st.rerun()
        
        # Password Reset Section
        with st.expander(f"🔑 Şifre Sıfırla: {uname}"):
            with st.form(key=f"reset_pw_form_{uname}"):
                new_reset_pw = st.text_input("Yeni Şifre", type="password", key=f"new_pw_{uname}")
                if st.form_submit_button("Güncelle"):
                    if new_reset_pw:
                        success, msg = auth_manager.reset_password(org, uname, new_reset_pw)
                        if success: st.success(msg)
                        else: st.error(msg)
                    else:
                        st.warning("Lütfen yeni şifre girin.")
        st.write("---")
    
elif st.session_state.page == get_text(lang, "menu_org_settings") and role == "Admin":
    st.title(f"🏢 {get_text(lang, 'menu_org_settings')}")
    org = st.session_state.app_user.get('org_code', 'default')
    conf = load_credentials(org)

    # --- ORGANIZATION MANAGEMENT ---
    org_list = auth_manager.get_organizations()
    if org == "default":
        st.subheader(get_text(lang, "org_management"))
        if org_list:
            st.dataframe(pd.DataFrame({get_text(lang, "organization"): sorted(org_list)}), width='stretch')
        else:
            st.info(get_text(lang, "no_orgs_found"))

        c_org1, c_org2 = st.columns(2)
        with c_org1:
            with st.form("add_org_form"):
                st.markdown(f"**{get_text(lang, 'org_add')}**")
                new_org = st.text_input(get_text(lang, "org_code_label"), value="")
                new_admin = st.text_input(get_text(lang, "admin_username"), value="admin")
                new_admin_pw = st.text_input(get_text(lang, "admin_password"), type="password")
                if st.form_submit_button(get_text(lang, "create_org"), width='stretch'):
                    if not new_org or not new_admin or not new_admin_pw:
                        st.warning("Organization code, admin username and password are required.")
                    else:
                        ok, msg = auth_manager.add_organization(new_org, new_admin, new_admin_pw)
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)

        with c_org2:
            with st.form("delete_org_form"):
                st.markdown(f"**{get_text(lang, 'org_delete')}**")
                deletable_orgs = [o for o in org_list if o not in ["default", org]]
                if deletable_orgs:
                    del_org = st.selectbox(get_text(lang, "organization"), options=deletable_orgs)
                else:
                    del_org = ""
                    st.info(get_text(lang, "no_deletable_orgs"))
                confirm = st.checkbox(get_text(lang, "confirm_delete_org"))
                if st.form_submit_button(get_text(lang, "delete_org"), width='stretch'):
                    if not del_org:
                        st.warning("No organization selected.")
                    elif not confirm:
                        st.warning("Please confirm deletion.")
                    else:
                        ok, msg = auth_manager.delete_organization(del_org)
                        if ok:
                            delete_org_files(del_org)
                            remove_shared_data_manager(del_org)
                            try:
                                store = _shared_org_session_store()
                                with store["lock"]:
                                    store["orgs"].pop(del_org, None)
                            except Exception:
                                pass
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
    else:
        st.subheader(get_text(lang, "organization"))
        st.info(get_text(lang, "org_restricted"))

    # --- ORG DATA MANAGER (CURRENT ORG) ---
    st.subheader(get_text(lang, "org_dm_title"))
    current_org = st.session_state.app_user.get('org_code', 'default')
    if current_org == "default":
        org_sel = st.selectbox(get_text(lang, "org_dm_select"), options=sorted(org_list), index=sorted(org_list).index(current_org) if current_org in org_list else 0)
    else:
        org_sel = current_org
        st.write(f"{get_text(lang, 'organization')}: {org_sel}")
    if org_sel == current_org and st.session_state.get('data_manager'):
        dm = st.session_state.data_manager
    else:
        dm = get_existing_data_manager(org_sel)
    running = dm.is_running() if dm else False
    metric_q = len(dm.queues_map) if dm else 0
    agent_q = len(dm.agent_queues_map) if dm else 0
    last_upd = datetime.fromtimestamp(dm.last_update_time).strftime('%H:%M:%S') if dm and dm.last_update_time else "Never"
    sess_count, union_metric_q, union_agent_q = _get_org_session_stats(org_sel)
    c_dm1, c_dm2, c_dm3, c_dm4 = st.columns(4)
    c_dm1.metric(get_text(lang, "organization"), org_sel)
    c_dm2.metric(get_text(lang, "org_dm_running"), "Yes" if running else "No")
    c_dm3.metric(get_text(lang, "org_dm_metric_queues"), metric_q)
    c_dm4.metric(get_text(lang, "org_dm_last_update"), last_upd)
    st.caption(get_text(lang, "org_dm_agent_queues") + f": {agent_q}")
    st.caption(f"Aktif Oturum: {sess_count} | Birlesik MetricQ: {union_metric_q} | Birlesik AgentQ: {union_agent_q}")
    if org_sel != current_org and not running:
        st.info(get_text(lang, "org_dm_requires_session"))
    if org_sel == current_org and not st.session_state.get('api_client'):
        st.info(get_text(lang, "genesys_not_connected"))
    c_dm5, c_dm6 = st.columns(2)
    with c_dm5:
        if st.button(get_text(lang, "org_dm_start")):
            if org_sel == current_org:
                set_dm_enabled(current_org, True)
                ensure_data_manager()
                if st.session_state.get('data_manager'):
                    st.session_state.data_manager.resume()
                if st.session_state.get('api_client') and st.session_state.get('queues_map'):
                    refresh_data_manager_queues()
            else:
                set_dm_enabled(org_sel, True)
                dm = get_shared_data_manager(org_sel)
                if dm and dm.is_running() is False:
                    dm.resume()
                    # Start with empty queues; will populate on next active session for that org
                    dm.start({}, {})
            st.rerun()
    with c_dm6:
        if st.button(get_text(lang, "org_dm_stop")):
            dm = get_existing_data_manager(org_sel)
            if dm and dm.is_running():
                dm.force_stop()
            set_dm_enabled(org_sel, False)
            st.rerun()

    with st.form("org_settings_form"):
        st.subheader(get_text(lang, "general_settings"))
        # UTC Offset
        u_off = st.number_input(get_text(lang, "utc_offset"), value=int(conf.get("utc_offset", 3)), step=1)
        # Refresh Interval
        ref_i = st.number_input(get_text(lang, "refresh_interval"), value=int(conf.get("refresh_interval", 10)), min_value=1, max_value=300, step=1, help=get_text(lang, "seconds_label"))
        
        if st.form_submit_button(get_text(lang, "save"), width='stretch'):
            # Update credentials file (preserving sensitive data)
            save_credentials(org, conf.get("client_id"), conf.get("client_secret"), conf.get("region"), utc_offset=u_off, refresh_interval=ref_i)
            # Update session state for immediate effect
            st.session_state.org_config = load_credentials(org)
            st.session_state.data_manager.update_settings(u_off, ref_i)
            st.success(get_text(lang, "view_saved"))
            st.rerun()

    # --- GENESYS API SETTINGS (ORG-SCOPED) ---
    st.subheader(get_text(lang, "genesys_api_creds"))
    c_id = st.text_input("Client ID", value=conf.get("client_id", ""), type="password")
    c_sec = st.text_input("Client Secret", value=conf.get("client_secret", ""), type="password")
    regions = ["mypurecloud.ie", "mypurecloud.com", "mypurecloud.de"]
    region = st.selectbox("Region", regions, index=regions.index(conf.get("region", "mypurecloud.ie")) if conf.get("region") in regions else 0)
    remember = st.checkbox(get_text(lang, "remember_me"), value=bool(conf))
    
    if st.button(get_text(lang, "login_genesys")):
        if c_id and c_sec:
            with st.spinner("Authenticating..."):
                client, err = authenticate(c_id, c_sec, region, org_code=org)
                if client:
                    st.session_state.api_client = client
                    st.session_state.genesys_logged_out = False
                    try:
                        if os.path.exists(_org_flag_path(org)):
                            os.remove(_org_flag_path(org))
                    except Exception:
                        pass
                    # Use existing offsets/intervals if available
                    cur_off = conf.get("utc_offset", 3)
                    cur_ref = conf.get("refresh_interval", 10)
                    if remember: save_credentials(org, c_id, c_sec, region, utc_offset=cur_off, refresh_interval=cur_ref)
                    else: delete_credentials(org)
                    
                    api = GenesysAPI(client)
                    maps = get_shared_org_maps(org, api, ttl_seconds=300, force_refresh=True)
                    st.session_state.users_map = maps.get("users_map", {})
                    st.session_state.users_info = maps.get("users_info", {})
                    st.session_state.queues_map = maps.get("queues_map", {})
                    st.session_state.wrapup_map = maps.get("wrapup", {})
                    st.session_state.presence_map = maps.get("presence", {})
                    st.session_state.org_config = conf if conf else load_credentials(org)
                    
                    st.session_state.data_manager.update_api_client(client, st.session_state.presence_map)
                    st.session_state.data_manager.update_settings(cur_off, cur_ref)
                    # Start with empty queues to prevent full fetch
                    st.session_state.data_manager.start({}, {}) 
                    st.rerun()
                else: st.error(f"Error: {err}")
    
    if st.session_state.api_client:
        st.divider()
        if st.button(get_text(lang, "logout_genesys"), type="primary"):
            # Stop background fetch for this org and clear API client
            dm = st.session_state.get('data_manager')
            if dm:
                dm.force_stop()
                dm.queues_map = {}
                dm.agent_queues_map = {}
                dm.obs_data_cache = {}
                dm.daily_data_cache = {}
                dm.agent_details_cache = {}
                dm.queue_members_cache = {}
            st.session_state.api_client = None
            st.session_state.queues_map = {}
            st.session_state.users_map = {}
            st.session_state.users_info = {}
            st.session_state.presence_map = {}
            with _org_maps_lock:
                _org_maps_cache.pop(org, None)
            try:
                token_cache = os.path.join("orgs", org, ".token_cache.json")
                if os.path.exists(token_cache):
                    os.remove(token_cache)
            except Exception:
                pass
            st.session_state.genesys_logged_out = True
            set_dm_enabled(org, False)
            try:
                with open(_org_flag_path(org), "w", encoding="utf-8") as f:
                    f.write("1")
            except Exception:
                pass
            
            # Also logout from app profile
            try:
                _remove_org_session(st.session_state.app_user.get('org_code', 'default'))
            except Exception:
                pass
            delete_app_session()
            st.session_state.app_user = None
            st.session_state.logged_in = False
            if 'dashboard_config_loaded' in st.session_state:
                del st.session_state.dashboard_config_loaded
            if 'data_manager' in st.session_state:
                del st.session_state.data_manager
            
            st.rerun()

elif st.session_state.page == get_text(lang, "admin_panel") and role == "Admin":
    st.title(f"🛡️ {get_text(lang, 'admin_panel')}")
    
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([f"📊 {get_text(lang, 'api_usage')}", f"📋 {get_text(lang, 'error_logs')}", "🧪 Diagnostics", f"🔌 {get_text(lang, 'manual_disconnect')}", f"👥 {get_text(lang, 'group_management')}", "🔍 Kullanıcı Arama"])
    
    with tab1:
        stats = monitor.get_stats()
        st.subheader(get_text(lang, "general_status"))
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(get_text(lang, "total_api_calls"), stats["total_calls"])
        c2.metric(get_text(lang, "total_errors"), stats["error_count"])
        uptime_hours = stats["uptime_seconds"] / 3600
        c3.metric(get_text(lang, "uptime"), f"{uptime_hours:.1f} sa")
        avg_rate = monitor.get_avg_rate_per_minute()
        recent_rate = monitor.get_rate_per_minute(minutes=1)
        c4.metric("API/Dk", f"{recent_rate:.1f}", help=f"Son 1 dk: {recent_rate:.1f} | Ortalama: {avg_rate:.1f}")
        
        st.divider()
        st.subheader(get_text(lang, "endpoint_usage"))
        if stats["endpoint_stats"]:
            df_endpoints = pd.DataFrame([
                {"Endpoint": k, "Adet": v} for k, v in stats["endpoint_stats"].items()
            ]).sort_values("Adet", ascending=False)
            # Ensure x and y are passed correctly to bar_chart
            df_endpoints = sanitize_numeric_df(df_endpoints)
            st.bar_chart(df_endpoints.set_index("Endpoint"))
        else:
            st.info("Henüz API çağrısı kaydedilmedi.")
        
        st.divider()
        st.subheader(get_text(lang, "minutely_traffic"))
        minutely = monitor.get_minutely_stats(minutes=1)
        if minutely:
            df_minutely = pd.DataFrame([
                {"Zaman": k, "İstek Adet": v} for k, v in minutely.items()
            ]).sort_values("Zaman")
            df_minutely = sanitize_numeric_df(df_minutely)
            st.line_chart(df_minutely.set_index("Zaman"))
        else:
            st.info("Son 1 dakikada trafik yok.")

        st.subheader(get_text(lang, "hourly_traffic_24h"))
        hourly = monitor.get_hourly_stats()
        if hourly:
            df_hourly = pd.DataFrame([
                {"Zaman": k, "İstek Adet": v} for k, v in hourly.items()
            ]).sort_values("Zaman")
            df_hourly = sanitize_numeric_df(df_hourly)
            st.line_chart(df_hourly.set_index("Zaman"))
        else:
            st.info("Son 24 saatte trafik yok.")

        st.divider()
    with tab2:
        st.subheader(get_text(lang, "system_errors"))
        errors = monitor.get_errors()
        if errors:
            for idx, err in enumerate(errors):
                with st.expander(f"❌ {err['timestamp'].strftime('%H:%M:%S')} - {err['module']}: {err['message']}", expanded=idx==0):
                    st.code(err['details'], language="json")
        else:
            st.success("Sistemde kayıtlı hata bulunmuyor.")

    with tab3:
        _start_memory_monitor(sample_interval=10, max_samples=720)
        st.subheader("Uygulama Kontrol")
        st.warning("Bu işlem uygulamayı yeniden başlatır. Aktif kullanıcı oturumları kısa süreli kesilir.")
        reboot_confirm = st.checkbox("Uygulamayı yeniden başlatmayı onaylıyorum", key="admin_reboot_confirm")
        if st.button("🔄 Uygulamayı Reboot Et", type="primary", key="admin_reboot_btn", disabled=not reboot_confirm):
            admin_user = st.session_state.get('app_user', {}).get('username', 'unknown')
            logger.warning(f"[ADMIN REBOOT] Restart requested by {admin_user}")
            _soft_memory_cleanup()
            _silent_restart()

        st.divider()
        st.subheader("Sistem Durumu")
        proc = psutil.Process(os.getpid())
        try:
            rss_mb = proc.memory_info().rss / (1024 * 1024)
        except Exception:
            rss_mb = 0
        try:
            cpu_pct = proc.cpu_percent(interval=0.1)
        except Exception:
            cpu_pct = 0
        thread_count = len(threading.enumerate())
        c1, c2, c3 = st.columns(3)
        c1.metric("RSS Bellek (MB)", f"{rss_mb:.1f}")
        c2.metric("CPU %", f"{cpu_pct:.1f}")
        c3.metric("Thread", thread_count)

        st.divider()
        st.subheader("Notifications Durumu")
        org_code = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
        store = _shared_notif_store()
        with store["lock"]:
            call_nm = store["call"].get(org_code)
            agent_nm = store["agent"].get(org_code)
            global_nm = store["global"].get(org_code)

        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**Queue Notifications**")
            st.write(f"Channels: {len(getattr(call_nm, 'channels', [])) if call_nm else 0}")
            st.write(f"Connected: {getattr(call_nm, 'connected', False) if call_nm else False}")
            st.write(f"Topics: {len(getattr(call_nm, 'subscribed_topics', [])) if call_nm else 0}")
            st.write(f"Truncated: {getattr(call_nm, 'topics_truncated', False) if call_nm else False}")
            st.write(f"Backoff (s): {getattr(call_nm, '_backoff_seconds', 0) if call_nm else 0}")
            st.write(f"Last Error: {getattr(call_nm, 'last_subscribe_error', '') if call_nm else ''}")
            st.write(f"Waiting Calls Cache: {len(getattr(call_nm, 'waiting_calls', {}) or {}) if call_nm else 0}")

        with c2:
            st.markdown("**Agent Notifications**")
            st.write(f"Channels: {len(getattr(agent_nm, 'channels', [])) if agent_nm else 0}")
            st.write(f"Connected: {getattr(agent_nm, 'connected', False) if agent_nm else False}")
            st.write(f"Topics: {len(getattr(agent_nm, 'subscribed_topics', [])) if agent_nm else 0}")
            st.write(f"Active Calls Cache: {len(getattr(agent_nm, 'active_calls', {}) or {}) if agent_nm else 0}")
            st.write(f"Queue Members Cache: {len(getattr(agent_nm, 'queue_members_cache', {}) or {}) if agent_nm else 0}")

        with c3:
            st.markdown("**Global Notifications**")
            st.write(f"Connected: {getattr(global_nm, 'connected', False) if global_nm else False}")
            st.write(f"Topics: {len(getattr(global_nm, 'subscribed_topics', [])) if global_nm else 0}")
            st.write(f"Backoff (s): {getattr(global_nm, '_backoff_seconds', 0) if global_nm else 0}")
            st.write(f"Last Error: {getattr(global_nm, 'last_subscribe_error', '') if global_nm else ''}")
            st.write(f"Active Conversations Cache: {len(getattr(global_nm, 'active_conversations', {}) or {}) if global_nm else 0}")

        st.divider()
        st.subheader("DataManager Cache Durumu")
        dm = st.session_state.get("data_manager")
        if dm:
            c1, c2, c3 = st.columns(3)
            c1.metric("Obs Cache", len(getattr(dm, "obs_data_cache", {}) or {}))
            c2.metric("Daily Cache", len(getattr(dm, "daily_data_cache", {}) or {}))
            c3.metric("Agent Detail Cache", len(getattr(dm, "agent_details_cache", {}) or {}))
            st.write(f"Queue Members Cache: {len(getattr(dm, 'queue_members_cache', {}) or {})}")
            st.write(f"Last Update: {datetime.fromtimestamp(getattr(dm, 'last_update_time', 0)).strftime('%H:%M:%S') if getattr(dm, 'last_update_time', 0) else 'N/A'}")
        else:
            st.info("DataManager bulunamadi.")

        st.divider()
        st.subheader("API Trafik")
        avg_rate = monitor.get_avg_rate_per_minute()
        recent_rate = monitor.get_rate_per_minute(minutes=1)
        st.write(f"Son 1 dk: {recent_rate:.1f} istek/dk")
        st.write(f"Ortalama: {avg_rate:.1f} istek/dk")

        st.divider()
        st.subheader("Bellek Trendi")
        store = _shared_memory_store()
        with store["lock"]:
            samples = list(store.get("samples") or [])
        if samples:
            df_mem = pd.DataFrame(samples)
            df_mem["timestamp"] = pd.to_datetime(df_mem["timestamp"], errors="coerce")
            df_mem = df_mem.dropna(subset=["timestamp"])
            if not df_mem.empty:
                st.line_chart(df_mem.set_index("timestamp")[["rss_mb"]])
        else:
            st.info("Bellek örneği henüz yok.")

        st.divider()
        st.subheader("🔍 Bellek Kaynak Analizi (RSS Şişme Tespiti)")
        
        # Calculate memory usage of each component
        memory_breakdown = []
        
        # 1. Session State
        try:
            session_keys = list(st.session_state.keys())
            session_size_estimate = 0
            session_details = []
            for key in session_keys:
                try:
                    val = st.session_state[key]
                    size_kb = sys.getsizeof(val) / 1024
                    if hasattr(val, '__dict__'):
                        size_kb += sys.getsizeof(val.__dict__) / 1024
                    if isinstance(val, (dict, list)):
                        # Deep size estimate for collections
                        size_kb = len(json.dumps(val, default=str)) / 1024 if size_kb < 1 else size_kb
                    session_size_estimate += size_kb
                    if size_kb > 10:  # Only show items > 10KB
                        session_details.append({"key": key, "size_kb": size_kb, "type": type(val).__name__})
                except:
                    pass
            memory_breakdown.append({"Kaynak": "Session State", "Boyut (KB)": session_size_estimate, "Öğe Sayısı": len(session_keys)})
        except:
            pass
        
        # 2. Notification Managers
        try:
            org_code = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
            store = _shared_notif_store()
            with store["lock"]:
                call_nm = store["call"].get(org_code)
                agent_nm = store["agent"].get(org_code)
                global_nm = store["global"].get(org_code)
            
            # Waiting calls cache
            if call_nm:
                wc = getattr(call_nm, 'waiting_calls', {}) or {}
                wc_size = len(json.dumps(list(wc.values()), default=str)) / 1024 if wc else 0
                memory_breakdown.append({"Kaynak": "Waiting Calls Cache", "Boyut (KB)": wc_size, "Öğe Sayısı": len(wc)})
            
            # User presence/routing cache
            if agent_nm:
                up = getattr(agent_nm, 'user_presence', {}) or {}
                ur = getattr(agent_nm, 'user_routing', {}) or {}
                ac = getattr(agent_nm, 'active_calls', {}) or {}
                qm = getattr(agent_nm, 'queue_members_cache', {}) or {}
                
                up_size = len(json.dumps(up, default=str)) / 1024 if up else 0
                ur_size = len(json.dumps(ur, default=str)) / 1024 if ur else 0
                ac_size = len(json.dumps(list(ac.values()), default=str)) / 1024 if ac else 0
                qm_size = len(json.dumps(qm, default=str)) / 1024 if qm else 0
                
                memory_breakdown.append({"Kaynak": "User Presence Cache", "Boyut (KB)": up_size, "Öğe Sayısı": len(up)})
                memory_breakdown.append({"Kaynak": "User Routing Cache", "Boyut (KB)": ur_size, "Öğe Sayısı": len(ur)})
                memory_breakdown.append({"Kaynak": "Active Calls Cache", "Boyut (KB)": ac_size, "Öğe Sayısı": len(ac)})
                memory_breakdown.append({"Kaynak": "Queue Members Cache", "Boyut (KB)": qm_size, "Öğe Sayısı": len(qm)})
            
            # Global conversations
            if global_nm:
                gc = getattr(global_nm, 'active_conversations', {}) or {}
                gc_size = len(json.dumps(list(gc.values()), default=str)) / 1024 if gc else 0
                memory_breakdown.append({"Kaynak": "Global Conversations Cache", "Boyut (KB)": gc_size, "Öğe Sayısı": len(gc)})
        except:
            pass
        
        # 3. DataManager Caches
        try:
            dm = st.session_state.get("data_manager")
            if dm:
                obs = getattr(dm, 'obs_data_cache', {}) or {}
                daily = getattr(dm, 'daily_data_cache', {}) or {}
                agent = getattr(dm, 'agent_details_cache', {}) or {}
                qmem = getattr(dm, 'queue_members_cache', {}) or {}
                
                obs_size = len(json.dumps(obs, default=str)) / 1024 if obs else 0
                daily_size = len(json.dumps(daily, default=str)) / 1024 if daily else 0
                agent_size = len(json.dumps(agent, default=str)) / 1024 if agent else 0
                qmem_size = len(json.dumps(qmem, default=str)) / 1024 if qmem else 0
                
                memory_breakdown.append({"Kaynak": "DM Obs Cache", "Boyut (KB)": obs_size, "Öğe Sayısı": len(obs)})
                memory_breakdown.append({"Kaynak": "DM Daily Cache", "Boyut (KB)": daily_size, "Öğe Sayısı": len(daily)})
                memory_breakdown.append({"Kaynak": "DM Agent Cache", "Boyut (KB)": agent_size, "Öğe Sayısı": len(agent)})
                memory_breakdown.append({"Kaynak": "DM Queue Members", "Boyut (KB)": qmem_size, "Öğe Sayısı": len(qmem)})
        except:
            pass
        
        # 4. Shared Seed Store
        try:
            seed_store = _shared_seed_store()
            with seed_store["lock"]:
                orgs = seed_store.get("orgs", {})
                for org_key, org_data in orgs.items():
                    call_seed = org_data.get("call_seed_data", [])
                    ivr_calls = org_data.get("ivr_calls_data", [])
                    call_meta = org_data.get("call_meta", {})
                    agent_pres = org_data.get("agent_presence", {})
                    agent_rout = org_data.get("agent_routing", {})
                    
                    cs_size = len(json.dumps(call_seed, default=str)) / 1024 if call_seed else 0
                    ivr_size = len(json.dumps(ivr_calls, default=str)) / 1024 if ivr_calls else 0
                    cm_size = len(json.dumps(call_meta, default=str)) / 1024 if call_meta else 0
                    ap_size = len(json.dumps(agent_pres, default=str)) / 1024 if agent_pres else 0
                    ar_size = len(json.dumps(agent_rout, default=str)) / 1024 if agent_rout else 0
                    
                    memory_breakdown.append({"Kaynak": f"Seed: Call Data ({org_key})", "Boyut (KB)": cs_size, "Öğe Sayısı": len(call_seed)})
                    memory_breakdown.append({"Kaynak": f"Seed: IVR Calls ({org_key})", "Boyut (KB)": ivr_size, "Öğe Sayısı": len(ivr_calls)})
                    memory_breakdown.append({"Kaynak": f"Seed: Call Meta ({org_key})", "Boyut (KB)": cm_size, "Öğe Sayısı": len(call_meta)})
                    memory_breakdown.append({"Kaynak": f"Seed: Agent Presence ({org_key})", "Boyut (KB)": ap_size, "Öğe Sayısı": len(agent_pres)})
                    memory_breakdown.append({"Kaynak": f"Seed: Agent Routing ({org_key})", "Boyut (KB)": ar_size, "Öğe Sayısı": len(agent_rout)})
        except:
            pass
        
        # 5. Monitor Logs
        try:
            api_log = getattr(monitor, 'api_calls_log', []) or []
            error_log = getattr(monitor, 'error_logs', []) or []
            api_size = len(json.dumps(api_log, default=str)) / 1024 if api_log else 0
            err_size = len(json.dumps(error_log, default=str)) / 1024 if error_log else 0
            memory_breakdown.append({"Kaynak": "API Calls Log (Memory)", "Boyut (KB)": api_size, "Öğe Sayısı": len(api_log)})
            memory_breakdown.append({"Kaynak": "Error Log (Memory)", "Boyut (KB)": err_size, "Öğe Sayısı": len(error_log)})
        except:
            pass
        
        # Display breakdown
        if memory_breakdown:
            df_breakdown = pd.DataFrame(memory_breakdown)
            df_breakdown = df_breakdown.sort_values("Boyut (KB)", ascending=False)
            df_breakdown["Boyut (KB)"] = df_breakdown["Boyut (KB)"].round(1)
            
            # Show top memory consumers
            total_kb = df_breakdown["Boyut (KB)"].sum()
            st.metric("Toplam Cache Boyutu", f"{total_kb:.1f} KB ({total_kb/1024:.2f} MB)")
            
            # Find the biggest consumer
            if not df_breakdown.empty:
                top = df_breakdown.iloc[0]
                pct = (top["Boyut (KB)"] / total_kb * 100) if total_kb > 0 else 0
                if pct > 30:
                    st.warning(f"⚠️ En büyük kaynak: **{top['Kaynak']}** - {top['Boyut (KB)']:.1f} KB ({pct:.1f}%)")
                else:
                    st.success(f"✅ En büyük kaynak: **{top['Kaynak']}** - {top['Boyut (KB)']:.1f} KB ({pct:.1f}%)")
            
            st.dataframe(df_breakdown, width='stretch', hide_index=True)
            
            # Show session state details if significant
            if session_details:
                with st.expander("📦 Session State Detayları (>10 KB)"):
                    df_session = pd.DataFrame(session_details).sort_values("size_kb", ascending=False)
                    df_session["size_kb"] = df_session["size_kb"].round(1)
                    st.dataframe(df_session, width='stretch', hide_index=True)
        else:
            st.info("Bellek analizi yapılamadı.")

    with tab4:
        st.subheader(f"🔌 {get_text(lang, 'manual_disconnect')}")
        st.warning(get_text(lang, "disconnect_warning"))
        
        if not st.session_state.get('api_client'):
            st.error(get_text(lang, "disconnect_genesys_required"))
        else:
            import re as _re_disconnect
            _uuid_pattern = _re_disconnect.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
            
            st.info(
                "⚠️ Bu uygulama **Client Credentials** OAuth kullanmaktadır. "
                "Etkileşim sonlandırma işlemi için **kullanıcı bağlamı olan bir token** gereklidir.\n\n"
                "**Token nasıl alınır:**\n"
                "1. [Genesys Cloud Developer Center](https://developer.mypurecloud.ie) → API Explorer'a gidin\n"
                "2. Sağ üstten Genesys Cloud hesabınızla giriş yapın\n"
                "3. Herhangi bir API çağrısında Authorization başlığındaki `Bearer` sonrası token'ı kopyalayın\n"
                "4. Aşağıdaki **User Token** alanına yapıştırın"
            )
            
            user_token = st.text_input(
                "🔑 User Token (Bearer)",
                type="password",
                placeholder="Genesys Cloud user token yapıştırın",
                help="Genesys Cloud Developer Center'dan aldığınız kullanıcı OAuth token'ı",
                key="admin_disconnect_user_token"
            )
            
            dc_col1, dc_col2 = st.columns([3, 1])
            with dc_col1:
                disconnect_id = st.text_input(
                    get_text(lang, "disconnect_interaction_id"),
                    placeholder="e.g. 3fa85f64-5717-4562-b3fc-2c963f66afa6",
                    help=get_text(lang, "disconnect_interaction_id_help"),
                    key="admin_disconnect_id"
                )
            
            with dc_col2:
                st.markdown("<br>", unsafe_allow_html=True)
                disconnect_clicked = st.button(
                    f"🔌 {get_text(lang, 'disconnect_btn')}",
                    type="primary",
                    use_container_width=True,
                    key="admin_disconnect_btn"
                )
            
            if disconnect_clicked:
                disconnect_id_clean = disconnect_id.strip() if disconnect_id else ""
                user_token_clean = user_token.strip() if user_token else ""
                
                if not disconnect_id_clean:
                    st.error(get_text(lang, "disconnect_empty_id"))
                elif not _uuid_pattern.match(disconnect_id_clean):
                    st.error(get_text(lang, "disconnect_invalid_id"))
                elif not user_token_clean:
                    st.error("Lütfen bir User Token girin. Client Credentials ile bu işlem yapılamaz.")
                else:
                    try:
                        # Use user token instead of client credentials
                        user_api_client = {
                            "access_token": user_token_clean,
                            "api_host": st.session_state.api_client.get("api_host", "https://api.mypurecloud.ie"),
                            "region": st.session_state.api_client.get("region", "mypurecloud.ie"),
                        }
                        api = GenesysAPI(user_api_client)
                        
                        with st.spinner("Etkileşim sonlandırılıyor..."):
                            result = api.disconnect_conversation(disconnect_id_clean)
                        
                        admin_user = st.session_state.get('app_user', {}).get('username', 'unknown')
                        media_type = result.get("media_type", "unknown")
                        
                        disconnected = result.get("disconnected", [])
                        skipped = result.get("skipped", [])
                        errors = result.get("errors", [])
                        
                        if disconnected:
                            st.success(f"✅ {get_text(lang, 'disconnect_success')} (ID: {disconnect_id_clean} | Tip: {media_type})")
                            for d in disconnected:
                                action = d.get('action', '')
                                if 'wrapup_submitted' in action:
                                    action_txt = " (Wrap-up kodu gönderildi)"
                                elif 'wrapup_fallback' in action:
                                    action_txt = " (Wrap-up ile kapatıldı)"
                                elif 'wrapup' in action:
                                    action_txt = " (Wrap-up atlandı)"
                                else:
                                    action_txt = ""
                                st.write(f"  ✔️ {d['purpose']}: {d['name']}{action_txt}")
                            logging.info(f"[ADMIN DISCONNECT] Interaction {disconnect_id_clean} — {len(disconnected)} participant(s) disconnected by {admin_user}")
                        
                        if skipped:
                            with st.expander(f"⏭️ Atlanan katılımcılar ({len(skipped)})", expanded=False):
                                for s in skipped:
                                    reason = s.get('reason', '')
                                    reason_txt = "Sistem katılımcısı" if reason == "system" else "Aktif oturum yok"
                                    st.write(f"  ⏭️ {s['purpose']}: {s['name']} — {reason_txt} ({s['state']})")
                        
                        if errors:
                            for er in errors:
                                st.error(f"❌ {er['purpose']}: {er['name']} — {er['error']}")
                            logging.error(f"[ADMIN DISCONNECT] Interaction {disconnect_id_clean} — {len(errors)} error(s) by {admin_user}")
                        
                        if not disconnected and not errors:
                            st.info("Tüm katılımcılar zaten sonlanmış durumda.")
                    except Exception as e:
                        error_msg = str(e)
                        st.error(f"❌ {get_text(lang, 'disconnect_error')}: {error_msg}")
                        logging.error(f"[ADMIN DISCONNECT] Failed to disconnect {disconnect_id_clean}: {error_msg}")

    with tab5:
        st.subheader(f"👥 {get_text(lang, 'group_management')}")
        
        if not st.session_state.get('api_client'):
            st.error(get_text(lang, "disconnect_genesys_required"))
        else:
            try:
                api = GenesysAPI(st.session_state.api_client)
                
                # Fetch groups
                if 'admin_groups_cache' not in st.session_state or st.session_state.get('admin_groups_refresh'):
                    with st.spinner(get_text(lang, "group_loading")):
                        st.session_state.admin_groups_cache = api.get_groups()
                        st.session_state.admin_groups_refresh = False
                
                groups = st.session_state.get('admin_groups_cache', [])
                
                if not groups:
                    st.info(get_text(lang, "group_no_groups"))
                else:
                    _prune_admin_group_member_cache(max_entries=40)
                    # Refresh button
                    if st.button("🔄 Grupları Yenile", key="refresh_groups_btn"):
                        st.session_state.admin_groups_refresh = True
                        st.rerun()
                    
                    group_options = {g['id']: f"{g['name']} ({g['memberCount']} üye)" for g in groups}
                    if True:

                        # --- Bulk Add Users to Multiple Groups ---
                        st.divider()
                        st.markdown("### ⚡ Çoklu Gruba Toplu Üye Ekle")

                        # Fetch all users for bulk selection
                        if 'admin_all_users_cache' not in st.session_state:
                            with st.spinner("Kullanıcılar yükleniyor..."):
                                st.session_state.admin_all_users_cache = api.get_users()
                        all_users = st.session_state.get('admin_all_users_cache', [])

                        multi_group_options = {g['id']: f"{g['name']} ({g.get('memberCount', 0)} üye)" for g in groups}
                        active_user_options = {
                            u['id']: f"{u.get('name', '')} ({u.get('email', '')})"
                            for u in all_users
                            if u.get('id') and u.get('state') == 'active'
                        }

                        multi_group_search = st.text_input(
                            "🔍 Toplu ekleme için kullanıcı ara",
                            placeholder="İsim veya e-posta ile filtrele",
                            key="admin_multi_group_user_search"
                        )
                        if multi_group_search:
                            filtered_multi_user_options = {
                                uid: label for uid, label in active_user_options.items()
                                if multi_group_search.lower() in label.lower()
                            }
                        else:
                            filtered_multi_user_options = active_user_options

                        selected_multi_user_ids = st.multiselect(
                            "Eklenecek kullanıcılar (aktif)",
                            options=list(filtered_multi_user_options.keys()),
                            format_func=lambda x: filtered_multi_user_options.get(x, active_user_options.get(x, x)),
                            key="admin_multi_group_user_ids"
                        )
                        selected_multi_group_ids = st.multiselect(
                            "Eklenecek gruplar (birden fazla seçilebilir)",
                            options=list(multi_group_options.keys()),
                            format_func=lambda x: multi_group_options.get(x, x),
                            key="admin_multi_group_group_ids"
                        )

                        preview_group_options = selected_multi_group_ids if selected_multi_group_ids else list(multi_group_options.keys())
                        preview_group_id = st.selectbox(
                            "Üyeleri listelenecek grup",
                            options=preview_group_options,
                            format_func=lambda x: multi_group_options.get(x, x),
                            key="admin_multi_group_preview_group"
                        )

                        if preview_group_id:
                            preview_cache_key = f"admin_group_members_{preview_group_id}"
                            if preview_cache_key not in st.session_state or st.session_state.get(f'refresh_{preview_cache_key}'):
                                st.session_state[preview_cache_key] = api.get_group_members(preview_group_id)
                                st.session_state[f'refresh_{preview_cache_key}'] = False
                            preview_members = st.session_state.get(preview_cache_key, [])
                            st.caption(f"Seçilen grubun üye sayısı: {len(preview_members)}")
                            if preview_members:
                                preview_df = pd.DataFrame(preview_members)
                                cols = [c for c in ["name", "email", "state"] if c in preview_df.columns]
                                if cols:
                                    preview_df = preview_df[cols].copy()
                                    preview_df.columns = ["Ad", "E-posta", "Durum"][:len(cols)]
                                st.dataframe(preview_df, width='stretch', hide_index=True)
                            else:
                                st.info("Seçilen grupta üye bulunamadı.")

                        if selected_multi_user_ids and selected_multi_group_ids and st.button(
                            "🚀 Seçili Kullanıcıları Seçili Gruplara Ekle",
                            type="primary",
                            key="admin_multi_group_add_submit_btn"
                        ):
                            with st.spinner("Toplu grup üyeliği ekleniyor..."):
                                total_added = 0
                                total_skipped_existing = 0
                                failed_groups = []

                                for gid in selected_multi_group_ids:
                                    g_name = multi_group_options.get(gid, gid)
                                    members_cache_key = f"admin_group_members_{gid}"
                                    try:
                                        group_members = api.get_group_members(gid)
                                        st.session_state[members_cache_key] = group_members
                                        existing_ids = {m.get("id") for m in group_members if m.get("id")}
                                        to_add_ids = [uid for uid in selected_multi_user_ids if uid not in existing_ids]

                                        total_skipped_existing += (len(selected_multi_user_ids) - len(to_add_ids))
                                        if to_add_ids:
                                            api.add_group_members(gid, to_add_ids)
                                            total_added += len(to_add_ids)

                                        st.session_state[f'refresh_{members_cache_key}'] = True
                                    except Exception as e:
                                        failed_groups.append((g_name, str(e)))

                                if total_added:
                                    st.success(f"✅ Toplam {total_added} yeni grup üyeliği eklendi.")
                                if total_skipped_existing:
                                    st.info(f"ℹ️ {total_skipped_existing} üyelik zaten mevcut olduğu için atlandı.")
                                if failed_groups:
                                    st.warning(f"⚠️ {len(failed_groups)} grupta hata oluştu.")
                                    for g_name, err in failed_groups:
                                        st.caption(f"❌ {g_name}: {err}")

                                st.session_state.admin_groups_refresh = True
                                if not failed_groups:
                                    st.rerun()
                        
                        # --- Assign Group to Queues ---
                        st.divider()
                        st.markdown(f"### 📋 {get_text(lang, 'group_to_queue')}")
                        selected_group_for_queue_id = st.selectbox(
                            get_text(lang, "group_select"),
                            options=list(group_options.keys()),
                            format_func=lambda x: group_options.get(x, x),
                            key="admin_group_select_for_queue"
                        )
                        selected_group_for_queue = next((g for g in groups if g['id'] == selected_group_for_queue_id), None)
                        if selected_group_for_queue:
                            st.info(f"**{selected_group_for_queue['name']}** grubu, seçtiğiniz kuyruklara üye grup olarak eklenecek veya çıkarılacaktır.")
                        
                        # Fetch queues
                        if 'admin_queues_cache' not in st.session_state:
                            with st.spinner("Kuyruklar yükleniyor..."):
                                st.session_state.admin_queues_cache = api.get_queues()
                        
                        all_queues = st.session_state.get('admin_queues_cache', [])
                        
                        if all_queues:
                            queue_options = {q['id']: q['name'] for q in all_queues}
                            
                            # Search filter for queues
                            queue_search = st.text_input("🔍 Kuyruk Ara", placeholder="Kuyruk adı ile filtrele", key=f"queue_search_{selected_group_for_queue_id}")
                            
                            if queue_search:
                                filtered_queues = {qid: qname for qid, qname in queue_options.items() if queue_search.lower() in qname.lower()}
                            else:
                                filtered_queues = queue_options
                            
                            selected_queue_ids = st.multiselect(
                                get_text(lang, "group_queue_select"),
                                options=list(filtered_queues.keys()),
                                format_func=lambda x: filtered_queues.get(x, queue_options.get(x, x)),
                                key=f"queue_assign_{selected_group_for_queue_id}"
                            )
                            
                            col_add_q, col_remove_q = st.columns(2)
                            
                            with col_add_q:
                                if selected_queue_ids and st.button(f"➕ {len(selected_queue_ids)} Kuyruğa Ekle", type="primary", key=f"add_to_queue_btn_{selected_group_for_queue_id}"):
                                    with st.spinner("Grup kuyruklara ekleniyor..."):
                                        results = api.add_group_to_queues(selected_group_for_queue_id, selected_queue_ids)
                                        success_count = sum(1 for r in results.values() if r['success'])
                                        fail_count = sum(1 for r in results.values() if not r['success'])
                                        
                                        if fail_count == 0:
                                            st.success(f"✅ {get_text(lang, 'group_queue_success')} ({success_count} kuyruk)")
                                        elif success_count == 0:
                                            st.error(f"❌ {get_text(lang, 'group_queue_error')}")
                                        else:
                                            st.warning(f"⚠️ {get_text(lang, 'group_queue_partial')} ✅ {success_count} / ❌ {fail_count}")
                                        
                                        for qid, result in results.items():
                                            qname = queue_options.get(qid, qid)
                                            if result.get('success') and result.get('already'):
                                                st.caption(f"ℹ️ {qname}: Grup zaten bu kuyruğun üyesi")
                                            elif result.get('success'):
                                                st.caption(f"✅ {qname}")
                                            else:
                                                st.caption(f"❌ {qname}: {result.get('error', '')}")
                                        
                                        logging.info(f"[ADMIN GROUP] Added group '{selected_group_for_queue.get('name', selected_group_for_queue_id)}' to {success_count}/{len(selected_queue_ids)} queues")
                            
                            with col_remove_q:
                                if selected_queue_ids and st.button(f"🗑️ {len(selected_queue_ids)} Kuyruktan Çıkar", type="secondary", key=f"remove_from_queue_btn_{selected_group_for_queue_id}"):
                                    with st.spinner("Grup kuyruklardan çıkarılıyor..."):
                                        results = api.remove_group_from_queues(selected_group_for_queue_id, selected_queue_ids)
                                        success_count = sum(1 for r in results.values() if r['success'])
                                        fail_count = sum(1 for r in results.values() if not r['success'])
                                        
                                        if fail_count == 0:
                                            st.success(f"✅ {get_text(lang, 'group_queue_remove_success')} ({success_count} kuyruk)")
                                        elif success_count == 0:
                                            st.error(f"❌ {get_text(lang, 'group_queue_remove_error')}")
                                        else:
                                            st.warning(f"⚠️ {get_text(lang, 'group_queue_partial')} ✅ {success_count} / ❌ {fail_count}")
                                        
                                        for qid, result in results.items():
                                            qname = queue_options.get(qid, qid)
                                            if result.get('success') and result.get('not_found'):
                                                st.caption(f"ℹ️ {qname}: Grup bu kuyruğun üyesi değildi")
                                            elif result.get('success'):
                                                st.caption(f"✅ {qname}")
                                            else:
                                                st.caption(f"❌ {qname}: {result.get('error', '')}")
                                        
                                        logging.info(f"[ADMIN GROUP] Removed group '{selected_group_for_queue.get('name', selected_group_for_queue_id)}' from {success_count}/{len(selected_queue_ids)} queues")
                        else:
                            st.warning("Kuyruk bulunamadı.")

                        # --- User & Workgroup Inventory + Excel Export ---
                        st.divider()
                        st.markdown("### 📥 Kullanıcı & Workgroup Envanteri")
                        st.caption("Genesys Cloud kullanıcılarını ve bağlı oldukları workgroup (grup) üyeliklerini listeleyip Excel olarak indirebilirsiniz.")

                        export_group_options = {g['id']: g['name'] for g in groups}
                        selected_export_group_ids = st.multiselect(
                            "Envantere dahil edilecek workgroup'lar",
                            options=list(export_group_options.keys()),
                            format_func=lambda x: export_group_options.get(x, x),
                            key="admin_inventory_group_filter"
                        )
                        include_users_without_group = st.checkbox(
                            "Workgroup'u olmayan kullanıcıları da ekle",
                            value=True,
                            key="admin_inventory_include_unassigned"
                        )

                        if st.button("🔄 Envanteri Hazırla", key="admin_inventory_build_btn"):
                            with st.spinner("Kullanıcı/workgroup envanteri hazırlanıyor..."):
                                if 'admin_all_users_cache' not in st.session_state:
                                    st.session_state.admin_all_users_cache = api.get_users()

                                inventory_users = st.session_state.get('admin_all_users_cache', [])
                                users_by_id = {u.get("id"): u for u in inventory_users if u.get("id")}
                                target_group_ids = selected_export_group_ids or list(export_group_options.keys())
                                inventory_rows = []
                                grouped_user_ids = set()

                                for gid in target_group_ids:
                                    group_name = export_group_options.get(gid, gid)
                                    members_cache_key = f"admin_group_members_{gid}"
                                    if members_cache_key not in st.session_state:
                                        st.session_state[members_cache_key] = api.get_group_members(gid)
                                    group_members = st.session_state.get(members_cache_key, [])

                                    for member in group_members:
                                        uid = member.get("id")
                                        if uid:
                                            grouped_user_ids.add(uid)
                                        user_obj = users_by_id.get(uid, {})
                                        inventory_rows.append({
                                            "_user_id": uid or "",
                                            "Username": user_obj.get("name") or member.get("name", ""),
                                            "Email": user_obj.get("email") or member.get("email", ""),
                                            "State": user_obj.get("state") or member.get("state", ""),
                                            "Workgroup Name": group_name
                                        })

                                if include_users_without_group:
                                    for user_obj in inventory_users:
                                        uid = user_obj.get("id")
                                        if uid and uid not in grouped_user_ids:
                                            inventory_rows.append({
                                                "_user_id": uid,
                                                "Username": user_obj.get("name", ""),
                                                "Email": user_obj.get("email", ""),
                                                "State": user_obj.get("state", ""),
                                                "Workgroup Name": ""
                                            })

                                if 'admin_queues_cache' not in st.session_state:
                                    st.session_state.admin_queues_cache = api.get_queues()
                                all_queues_for_inventory = st.session_state.get('admin_queues_cache', [])
                                inventory_user_ids = {
                                    row.get("_user_id")
                                    for row in inventory_rows
                                    if row.get("_user_id")
                                }
                                user_queue_map = api.get_user_queue_map(
                                    user_ids=list(inventory_user_ids),
                                    queues=all_queues_for_inventory
                                )
                                for row in inventory_rows:
                                    uid = row.get("_user_id")
                                    queues = user_queue_map.get(uid, []) if uid else []
                                    row["Queue List"] = ", ".join(queues)

                                inventory_df = pd.DataFrame(inventory_rows)
                                if not inventory_df.empty:
                                    if "_user_id" in inventory_df.columns:
                                        inventory_df = inventory_df.drop(columns=["_user_id"])
                                    sort_cols = [c for c in ["Workgroup Name", "Username"] if c in inventory_df.columns]
                                    if sort_cols:
                                        inventory_df = inventory_df.sort_values(sort_cols).reset_index(drop=True)
                                st.session_state.admin_user_workgroup_inventory_df = inventory_df

                        inventory_df = st.session_state.get("admin_user_workgroup_inventory_df")
                        if isinstance(inventory_df, pd.DataFrame) and not inventory_df.empty:
                            st.dataframe(inventory_df, width='stretch', hide_index=True)
                            st.download_button(
                                "📥 Kullanıcı-Workgroup Excel İndir",
                                data=to_excel(inventory_df),
                                file_name=f"user_workgroup_inventory_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="admin_inventory_download_btn"
                            )
                        elif isinstance(inventory_df, pd.DataFrame):
                            st.info("Seçilen filtrelerle envanter sonucu bulunamadı.")

                        # --- Bulk Assign Users to Queues ---
                        st.divider()
                        st.markdown("### ⚡ Toplu Kuyruk Atama")
                        st.caption("Seçtiğiniz kullanıcıları, seçtiğiniz birden fazla kuyruğa tek seferde atar.")

                        if 'admin_all_users_cache' not in st.session_state:
                            st.session_state.admin_all_users_cache = api.get_users()
                        bulk_all_users = st.session_state.get('admin_all_users_cache', [])
                        bulk_active_users = [u for u in bulk_all_users if u.get("id") and u.get("state") == "active"]

                        bulk_user_options = {u['id']: f"{u.get('name', '')} ({u.get('email', '')})" for u in bulk_active_users}
                        if 'admin_queues_cache' not in st.session_state:
                            st.session_state.admin_queues_cache = api.get_queues()
                        bulk_all_queues = st.session_state.get('admin_queues_cache', [])
                        bulk_queue_options = {q['id']: q.get('name', q['id']) for q in bulk_all_queues}

                        bulk_search = st.text_input(
                            "🔍 Toplu atama için kullanıcı ara",
                            placeholder="İsim veya e-posta ile filtrele",
                            key="admin_bulk_assign_user_search"
                        )
                        if bulk_search:
                            filtered_bulk_user_options = {
                                uid: label for uid, label in bulk_user_options.items()
                                if bulk_search.lower() in label.lower()
                            }
                        else:
                            filtered_bulk_user_options = bulk_user_options

                        selected_bulk_user_ids = st.multiselect(
                            "Atanacak kullanıcılar (aktif)",
                            options=list(filtered_bulk_user_options.keys()),
                            format_func=lambda x: filtered_bulk_user_options.get(x, bulk_user_options.get(x, x)),
                            key="admin_bulk_assign_users"
                        )
                        bulk_group_options = {g['id']: g.get('name', g['id']) for g in groups}
                        selected_source_group_ids = st.multiselect(
                            "Agent çekilecek gruplar",
                            options=list(bulk_group_options.keys()),
                            format_func=lambda x: bulk_group_options.get(x, x),
                            key="admin_bulk_assign_source_groups",
                            help="Seçtiğiniz grupların üyeleri otomatik olarak kullanıcı listesine eklenir."
                        )

                        pulled_user_ids = set()
                        pulled_inactive_count = 0
                        if selected_source_group_ids:
                            active_user_id_set = set(bulk_user_options.keys())
                            for gid in selected_source_group_ids:
                                members_cache_key = f"admin_group_members_{gid}"
                                if members_cache_key not in st.session_state or st.session_state.get(f'refresh_{members_cache_key}'):
                                    st.session_state[members_cache_key] = api.get_group_members(gid)
                                    st.session_state[f'refresh_{members_cache_key}'] = False
                                for member in st.session_state.get(members_cache_key, []) or []:
                                    uid = member.get("id")
                                    if not uid:
                                        continue
                                    if uid in active_user_id_set:
                                        pulled_user_ids.add(uid)
                                    else:
                                        pulled_inactive_count += 1

                        effective_bulk_user_ids = sorted(set(selected_bulk_user_ids) | pulled_user_ids)
                        st.caption(
                            f"Toplam hedef kullanıcı: {len(effective_bulk_user_ids)} "
                            f"(manuel: {len(selected_bulk_user_ids)}, gruptan: {len(pulled_user_ids)})"
                            + (f" | pasif atlanan: {pulled_inactive_count}" if pulled_inactive_count else "")
                        )
                        selected_bulk_queue_ids = st.multiselect(
                            "Atanacak kuyruklar",
                            options=list(bulk_queue_options.keys()),
                            format_func=lambda x: bulk_queue_options.get(x, x),
                            key="admin_bulk_assign_queues"
                        )

                        col_bulk_add, col_bulk_remove = st.columns(2)

                        with col_bulk_add:
                            if effective_bulk_user_ids and selected_bulk_queue_ids and st.button(
                                "🚀 Toplu Kuyruk Ataması Yap",
                                type="primary",
                                key="admin_bulk_assign_submit_btn"
                            ):
                                with st.spinner("Toplu atama yapılıyor..."):
                                    results = api.add_users_to_queues(
                                        user_ids=effective_bulk_user_ids,
                                        queue_ids=selected_bulk_queue_ids
                                    )
                                    success_count = sum(1 for r in results.values() if r.get("success"))
                                    fail_count = sum(1 for r in results.values() if not r.get("success"))
                                    total_added = sum(int(r.get("added", 0)) for r in results.values())
                                    total_skipped = sum(int(r.get("skipped_existing", 0)) for r in results.values())

                                    if fail_count == 0:
                                        st.success(f"✅ Toplu kuyruk ataması tamamlandı. Kuyruk: {success_count}, yeni üyelik: {total_added}")
                                    elif success_count == 0:
                                        st.error("❌ Toplu kuyruk ataması başarısız oldu.")
                                    else:
                                        st.warning(f"⚠️ Kısmi başarı: ✅ {success_count} / ❌ {fail_count} kuyruk")

                                    if total_skipped:
                                        st.info(f"ℹ️ {total_skipped} üyelik zaten mevcut olduğu için atlandı.")

                                    for qid, result in results.items():
                                        qname = bulk_queue_options.get(qid, qid)
                                        if result.get("success"):
                                            st.caption(
                                                f"✅ {qname}: +{result.get('added', 0)} eklendi"
                                                + (f", {result.get('skipped_existing', 0)} zaten üyeydi" if result.get('skipped_existing', 0) else "")
                                            )
                                        else:
                                            st.caption(f"❌ {qname}: {result.get('error', '')}")

                        with col_bulk_remove:
                            if effective_bulk_user_ids and selected_bulk_queue_ids and st.button(
                                "🗑️ Toplu Kuyruk Çıkarma Yap",
                                type="secondary",
                                key="admin_bulk_remove_submit_btn"
                            ):
                                with st.spinner("Toplu kuyruk çıkarma yapılıyor..."):
                                    results = api.remove_users_from_queues(
                                        user_ids=effective_bulk_user_ids,
                                        queue_ids=selected_bulk_queue_ids
                                    )
                                    success_count = sum(1 for r in results.values() if r.get("success"))
                                    fail_count = sum(1 for r in results.values() if not r.get("success"))
                                    total_removed = sum(int(r.get("removed", 0)) for r in results.values())
                                    total_skipped = sum(int(r.get("skipped_missing", 0)) for r in results.values())

                                    if fail_count == 0:
                                        st.success(f"✅ Toplu kuyruk çıkarma tamamlandı. Kuyruk: {success_count}, silinen üyelik: {total_removed}")
                                    elif success_count == 0:
                                        st.error("❌ Toplu kuyruk çıkarma başarısız oldu.")
                                    else:
                                        st.warning(f"⚠️ Kısmi başarı: ✅ {success_count} / ❌ {fail_count} kuyruk")

                                    if total_skipped:
                                        st.info(f"ℹ️ {total_skipped} üyelik kuyruklarda bulunamadığı için atlandı.")

                                    for qid, result in results.items():
                                        qname = bulk_queue_options.get(qid, qid)
                                        if result.get("success"):
                                            st.caption(
                                                f"✅ {qname}: -{result.get('removed', 0)} çıkarıldı"
                                                + (f", {result.get('skipped_missing', 0)} kullanıcı zaten yoktu" if result.get('skipped_missing', 0) else "")
                                            )
                                        else:
                                            st.caption(f"❌ {qname}: {result.get('error', '')}")
            except Exception as e:
                st.error(f"❌ {get_text(lang, 'group_fetch_error')}: {e}")

    with tab6:
        st.subheader("🔍 Kullanıcı Arama (User ID)")
        st.info("Genesys Cloud kullanıcı kimliği (UUID) ile kullanıcı bilgilerini sorgulayabilirsiniz.")
        
        if not st.session_state.get('api_client'):
            st.error("Bu özellik için Genesys Cloud bağlantısı gereklidir.")
        else:
            import re as _re_user_search
            _uuid_pattern_user = _re_user_search.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
            
            search_col1, search_col2 = st.columns([3, 1])
            with search_col1:
                user_id_input = st.text_input(
                    "Kullanıcı ID (UUID)",
                    placeholder="e.g. 24331d74-80bf-4069-a67c-51bc851fdc3e",
                    help="Genesys Cloud kullanıcı kimliğini (UUID formatında) girin",
                    key="admin_user_search_id"
                )
            
            with search_col2:
                st.markdown("<br>", unsafe_allow_html=True)
                search_clicked = st.button("🔍 Ara", type="primary", use_container_width=True, key="admin_user_search_btn")
            
            if search_clicked:
                user_id_clean = user_id_input.strip() if user_id_input else ""
                
                if not user_id_clean:
                    st.error("Lütfen bir kullanıcı ID girin.")
                elif not _uuid_pattern_user.match(user_id_clean):
                    st.error("Geçersiz UUID formatı. Örnek: 24331d74-80bf-4069-a67c-51bc851fdc3e")
                else:
                    try:
                        api = GenesysAPI(st.session_state.api_client)
                        with st.spinner("Kullanıcı bilgileri getiriliyor..."):
                            user_data = api.get_user_by_id(
                                user_id_clean, 
                                expand=['presence', 'routingStatus', 'groups', 'skills', 'languages']
                            )
                        
                        if user_data:
                            st.success(f"✅ Kullanıcı bulundu: **{user_data.get('name', 'N/A')}**")
                            
                            # Basic Info
                            st.markdown("### 👤 Temel Bilgiler")
                            info_col1, info_col2 = st.columns(2)
                            with info_col1:
                                st.markdown(f"**Ad:** {user_data.get('name', 'N/A')}")
                                st.markdown(f"**E-posta:** {user_data.get('email', 'N/A')}")
                                st.markdown(f"**Kullanıcı Adı:** {user_data.get('username', 'N/A')}")
                                st.markdown(f"**Durum:** {user_data.get('state', 'N/A')}")
                            with info_col2:
                                st.markdown(f"**Departman:** {user_data.get('department', 'N/A')}")
                                st.markdown(f"**Ünvan:** {user_data.get('title', 'N/A')}")
                                st.markdown(f"**Yönetici:** {user_data.get('manager', 'N/A')}")
                                st.markdown(f"**Division:** {user_data.get('divisionName', 'N/A')}")
                            
                            # Presence & Routing Status
                            presence = user_data.get('presence', {})
                            routing = user_data.get('routingStatus', {})
                            if presence or routing:
                                st.divider()
                                st.markdown("### 📍 Anlık Durum")
                                status_col1, status_col2 = st.columns(2)
                                with status_col1:
                                    if presence:
                                        pres_def = presence.get('presenceDefinition', {})
                                        st.markdown(f"**Presence:** {pres_def.get('systemPresence', 'N/A')}")
                                        st.markdown(f"**Presence ID:** `{pres_def.get('id', 'N/A')}`")
                                        if presence.get('modifiedDate'):
                                            st.markdown(f"**Son Değişiklik:** {presence.get('modifiedDate', 'N/A')}")
                                with status_col2:
                                    if routing:
                                        st.markdown(f"**Routing Status:** {routing.get('status', 'N/A')}")
                                        if routing.get('startTime'):
                                            st.markdown(f"**Başlangıç:** {routing.get('startTime', 'N/A')}")
                            
                            # Groups
                            groups = user_data.get('groups', [])
                            if groups:
                                st.divider()
                                st.markdown(f"### 👥 Gruplar ({len(groups)})")
                                group_names = [g.get('name', g.get('id', 'N/A')) for g in groups]
                                st.write(", ".join(group_names) if group_names else "Grup yok")
                            
                            # Skills
                            skills = user_data.get('skills', [])
                            if skills:
                                st.divider()
                                st.markdown(f"### 🎯 Yetenekler ({len(skills)})")
                                skill_info = [f"{s.get('name', 'N/A')} (Seviye: {s.get('proficiency', 'N/A')})" for s in skills]
                                for s in skill_info[:10]:  # Show max 10
                                    st.caption(s)
                                if len(skills) > 10:
                                    st.caption(f"... ve {len(skills) - 10} daha")
                            
                            # Languages
                            languages = user_data.get('languages', [])
                            if languages:
                                st.divider()
                                st.markdown(f"### 🌐 Diller ({len(languages)})")
                                lang_info = [f"{l.get('name', 'N/A')} (Seviye: {l.get('proficiency', 'N/A')})" for l in languages]
                                st.write(", ".join(lang_info) if lang_info else "Dil yok")
                            
                            # Raw JSON expander
                            with st.expander("📄 Ham JSON Verisi"):
                                raw = user_data.get('raw', user_data)
                                # Remove 'raw' key to avoid recursion
                                display_raw = {k: v for k, v in raw.items() if k != 'raw'} if isinstance(raw, dict) else raw
                                st.json(display_raw)
                        else:
                            st.warning(f"⚠️ Kullanıcı bulunamadı: `{user_id_clean}`")
                    except Exception as e:
                        st.error(f"❌ Hata: {e}")

    # Logout moved to Organization Settings
    
    # Org DataManager controls moved to Organization Settings

elif st.session_state.page == get_text(lang, "menu_dashboard"):
    # (Config already loaded at top level)
    st.title(get_text(lang, "menu_dashboard"))
    c_c1, c_c2, c_c3 = st.columns([1, 2, 1])
    if c_c1.button(get_text(lang, "add_group"), width='stretch'):
        st.session_state.dashboard_cards.append({"id": max([c['id'] for c in st.session_state.dashboard_cards], default=-1)+1, "title": "", "queues": [], "size": "medium", "live_metrics": ["Waiting", "Interacting", "On Queue"], "daily_metrics": ["Offered", "Answered", "Abandoned", "Answer Rate"]})
        save_dashboard_config(org, st.session_state.dashboard_layout, st.session_state.dashboard_cards)
        refresh_data_manager_queues()
        st.rerun()
    
    with c_c2:
        sc1, sc2 = st.columns([2, 3])
        lo = sc1.radio(get_text(lang, "layout"), [1, 2, 3, 4], format_func=lambda x: f"Grid: {x}", index=min(st.session_state.dashboard_layout-1, 3), horizontal=True, label_visibility="collapsed")
        if lo != st.session_state.dashboard_layout:
            st.session_state.dashboard_layout = lo; save_dashboard_config(org, lo, st.session_state.dashboard_cards); st.rerun()
        m_opts = ["Live", "Yesterday", "Date"]
        if 'dashboard_mode' not in st.session_state: st.session_state.dashboard_mode = "Live"
        st.session_state.dashboard_mode = sc2.radio(get_text(lang, "mode"), m_opts, format_func=lambda x: get_text(lang, f"mode_{x.lower()}"), index=m_opts.index(st.session_state.dashboard_mode), horizontal=True, label_visibility="collapsed")

    if c_c3:
        if st.session_state.dashboard_mode == "Date": st.session_state.dashboard_date = st.date_input(get_text(lang, "mode_date"), datetime.today(), label_visibility="collapsed")
        elif st.session_state.dashboard_mode == "Live": 
            c_auto, c_time, c_spacer, c_agent, c_call = st.columns([1, 1, 1, 1, 1])
            auto_ref = c_auto.toggle(get_text(lang, "auto_refresh"), value=True)
            # Toggle moved to far right
            show_agent_panel = c_agent.toggle(f"👤 {get_text(lang, 'agent_panel')}", value=st.session_state.get('show_agent_panel', False), key='toggle_agent_panel')
            show_call_panel = c_call.toggle(f"📞 {get_text(lang, 'call_panel')}", value=st.session_state.get('show_call_panel', False), key='toggle_call_panel')
            st.session_state.show_agent_panel = show_agent_panel
            st.session_state.show_call_panel = show_call_panel
            if not show_call_panel and st.session_state.get('notifications_manager'):
                st.session_state.notifications_manager.stop()
            if not show_agent_panel:
                try:
                    store = _shared_notif_store()
                    with store["lock"]:
                        nm = store["agent"].get(org)
                    if nm:
                        nm.stop()
                except Exception:
                    pass
            
            # Show Last Update Time
            if data_manager.last_update_time > 0:
                last_upd = datetime.fromtimestamp(data_manager.last_update_time).strftime('%H:%M:%S')
                c_time.markdown(f'<div class="last-update">Last Update: <span>{last_upd}</span></div>', unsafe_allow_html=True)
            
        if st.session_state.dashboard_mode == "Live":
            # DataManager is managed centrally by refresh_data_manager_queues()
            # which is called on login, hot-reload, and config changes.
            ref_int = st.session_state.get('org_config', {}).get('refresh_interval', 15)
            if auto_ref:
                _safe_autorefresh(interval=ref_int * 1000, key="data_refresh")

    # Available metric options
    # Available metric options
    LIVE_METRIC_OPTIONS = ["Waiting", "Interacting", "Idle Agent", "On Queue", "Available", "Busy", "Away", "Break", "Meal", "Meeting", "Training"]
    DAILY_METRIC_OPTIONS = ["Offered", "Answered", "Abandoned", "Answer Rate", "Service Level", "Avg Handle Time", "Avg Wait Time"]

    # Define labels for consistent usage in Settings and Display
    live_labels = {
        "Waiting": get_text(lang, "waiting"), 
        "Interacting": get_text(lang, "interacting"), 
        "Idle Agent": "Boşta (Idle)",
        "On Queue": get_text(lang, "on_queue_agents"), 
        "Available": get_text(lang, "available_agents"), 
        "Busy": "Meşgul", "Away": "Uzakta", "Break": "Mola", 
        "Meal": "Yemek", "Meeting": "Toplantı", "Training": "Eğitim"
    }
    
    daily_labels = {
        "Offered": get_text(lang, "offered"), 
        "Answered": get_text(lang, "answered"), 
        "Abandoned": get_text(lang, "abandoned"), 
        "Answer Rate": get_text(lang, "answer_rate"), 
        "Service Level": get_text(lang, "avg_service_level"), 
        "Avg Handle Time": "Ort. İşlem", 
        "Avg Wait Time": "Ort. Bekleme"
    }

    show_agent = st.session_state.get('show_agent_panel', False)
    show_call = st.session_state.get('show_call_panel', False)
    if show_agent and show_call:
        # 4-column grid: 2 parts dashboard, 1 part agent, 1 part call
        main_c, agent_c, call_c = st.columns([2, 1, 1])
    elif show_agent or show_call:
        main_c, side_c = st.columns([3, 1])
        agent_c = side_c if show_agent else None
        call_c = side_c if show_call else None
    else:
        main_c = st.container()
        agent_c = None
        call_c = None

    grid = main_c.columns(st.session_state.dashboard_layout)
    to_del = []
    for idx, card in enumerate(st.session_state.dashboard_cards):
        with grid[idx % st.session_state.dashboard_layout]:
            # Determine Container Height based on size
            c_size = card.get('size', 'medium')
            # Base heights: xsmall=300, Small=500, Medium=650, Large=800 (Adjusted for content)
            c_height = 300 if c_size == 'xsmall' else (500 if c_size == 'small' else (650 if c_size == 'medium' else 800))
            
            with st.container(height=c_height, border=True):
                card_title = card['title'] if card['title'] else f"Grup #{card['id']+1}"
                st.markdown(f"### {card_title}")
                with st.expander(f"⚙️ Settings", expanded=False):
                    card['title'] = st.text_input("Title", value=card['title'], key=f"t_{card['id']}")
                    size_opts = ["xsmall", "small", "medium", "large"]
                    card['size'] = st.selectbox("Size", size_opts, index=size_opts.index(card.get('size', 'medium')), key=f"sz_{card['id']}")
                    card['visual_metrics'] = st.multiselect("Visuals", ["Service Level", "Answer Rate", "Abandon Rate"], default=card.get('visual_metrics', ["Service Level"]), key=f"vm_{card['id']}")
                    queue_options = list(st.session_state.queues_map.keys())
                    queue_defaults = [q for q in card.get('queues', []) if q in queue_options]
                    card['queues'] = st.multiselect("Queues", queue_options, default=queue_defaults, key=f"q_{card['id']}")
                    card['media_types'] = st.multiselect("Media Types", ["voice", "chat", "email", "callback", "message"], default=card.get('media_types', []), key=f"mt_{card['id']}")
                    
                    st.write("---")
                    st.caption("📡 Canlı Metrikler")
                    card['live_metrics'] = st.multiselect("Live Metrics", LIVE_METRIC_OPTIONS, default=card.get('live_metrics', ["Waiting", "Interacting", "On Queue"]), format_func=lambda x: live_labels.get(x, x), key=f"lm_{card['id']}")
                    
                    st.caption("📊 Günlük Metrikler")
                    card['daily_metrics'] = st.multiselect("Daily Metrics", DAILY_METRIC_OPTIONS, default=card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"]), format_func=lambda x: daily_labels.get(x, x), key=f"dm_{card['id']}")
                    
                    if st.button("Delete", key=f"d_{card['id']}"): to_del.append(idx)
                    save_dashboard_config(org, st.session_state.dashboard_layout, st.session_state.dashboard_cards)
                
                if not card.get('queues'): st.info("Select queues"); continue
                
                # Determine date range based on mode
                if st.session_state.dashboard_mode == "Live":
                    # Use cached live data
                    obs_map, daily_map, _ = st.session_state.data_manager.get_data(card['queues'])
                    items_live = [obs_map.get(q) for q in card['queues'] if obs_map.get(q)]
                    items_daily = [daily_map.get(q) for q in card['queues'] if daily_map.get(q)]
                else:
                    # Fetch historical data via API
                    items_live = []  # No live data for historical
                    
                    if st.session_state.dashboard_mode == "Yesterday":
                        target_date = datetime.today() - timedelta(days=1)
                    else:  # Date mode
                        target_date = datetime.combine(st.session_state.get('dashboard_date', datetime.today()), time(0, 0))
                    
                    # Calculate date range (full day in UTC)
                    start_dt = datetime.combine(target_date, time(0, 0)) - timedelta(hours=saved_creds.get("utc_offset", 3))
                    end_dt = datetime.combine(target_date, time(23, 59, 59)) - timedelta(hours=saved_creds.get("utc_offset", 3))
                    
                    # Fetch aggregate data for selected queues
                    queue_ids = [st.session_state.queues_map.get(q) for q in card['queues'] if st.session_state.queues_map.get(q)]
                    
                    items_daily = []
                    if queue_ids:
                        try:
                            interval = f"{start_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
                            cache_key = f"{interval}|{','.join(sorted(queue_ids))}"
                            daily_data = _get_shared_daily_stats(org, cache_key, max_age_seconds=300)
                            if daily_data is None:
                                api = GenesysAPI(st.session_state.api_client)
                                resp = api.get_queue_daily_stats(queue_ids, interval=interval)
                                daily_data = {}
                                if resp and resp.get('results'):
                                    id_map = {v: k for k, v in st.session_state.queues_map.items()}
                                    from src.processor import process_daily_stats
                                    daily_data = process_daily_stats(resp, id_map) or {}
                                _set_shared_daily_stats(org, cache_key, daily_data, pytime.time(), max_items=60)
                            items_daily = [daily_data.get(q) for q in card['queues'] if daily_data.get(q)]
                        except Exception as e:
                            st.warning(f"Veri çekilemedi: {e}")
                
                # Calculate aggregates
                n_q = len(items_live) or 1
                n_s = len(card['queues']) or 1
                
                # Live Metric Helper: Sum based on selected media types
                selected_media = card.get('media_types', [])
                
                def get_media_sum(item, metric_key):
                    # If metric is NOT dict (old data or non-media metric), return it directly
                    val = item.get(metric_key, 0)
                    if not isinstance(val, dict): return val
                    
                    # If dict, filter by selected media types
                    if not selected_media: return val.get('Total', 0)
                    
                    return sum(val.get(m, 0) for m in selected_media)

                off = sum(get_media_sum(d, 'Offered') for d in items_daily)
                ans = sum(get_media_sum(d, 'Answered') for d in items_daily)
                abn = sum(get_media_sum(d, 'Abandoned') for d in items_daily)
                s_n = sum(d.get('SL_Numerator', 0) for d in items_daily)
                s_d = sum(d.get('SL_Denominator', 0) for d in items_daily)
                sl = (s_n / s_d * 100) if s_d > 0 else 0
                avg_handle = sum(d.get('AvgHandle', 0) for d in items_daily) / len(items_daily) if items_daily else 0
                avg_wait = sum(d.get('AvgWait', 0) for d in items_daily) / len(items_daily) if items_daily else 0
                
                # Live metrics mapping
                # Live metrics mapping
                
                # 1. Fetch Agent Details for selected queues involved in this card
                card_agent_data = st.session_state.data_manager.get_agent_details(card['queues'])
                
                # 2. Flatten and Deduplicate Agents
                unique_agents = {}
                for q_agents in card_agent_data.values():
                    for agent in q_agents:
                        unique_agents[agent['id']] = agent
                        
                # 3. Calculate Counts from Unique Agents
                cnt_interacting = 0
                cnt_idle = 0
                cnt_on_queue = 0
                cnt_available = 0
                cnt_busy = 0
                cnt_away = 0
                cnt_break = 0
                cnt_meal = 0
                cnt_meeting = 0
                cnt_training = 0
                
                for m in unique_agents.values():
                    user_obj = m.get('user', {})
                    presence = user_obj.get('presence', {}).get('presenceDefinition', {}).get('systemPresence', 'OFFLINE').upper()
                    routing = m.get('routingStatus', {}).get('status', 'OFF_QUEUE').upper()
                    
                    # Logic must match the status text logic we fixed earlier
                    if presence == 'AVAILABLE':
                        cnt_available += 1
                    elif presence in ['ON_QUEUE', 'ON QUEUE']:
                        # On Queue Logic
                        if routing in ['INTERACTING', 'COMMUNICATING']:
                            cnt_interacting += 1
                            cnt_on_queue += 1 # Technically on queue
                        elif routing == 'IDLE':
                            cnt_idle += 1 # Ready
                            cnt_on_queue += 1
                        elif routing == 'NOT_RESPONDING':
                            cnt_on_queue += 1 
                        else:
                            cnt_on_queue += 1
                    elif presence == "BUSY": cnt_busy += 1
                    elif presence == "AWAY": cnt_away += 1
                    elif presence == "BREAK": cnt_break += 1
                    elif presence == "MEAL": cnt_meal += 1
                    elif presence == "MEETING": cnt_meeting += 1
                    elif presence == "TRAINING": cnt_training += 1

                live_values = {
                    "Waiting": sum(get_media_sum(d, 'Waiting') for d in items_live) if items_live else 0,
                    "Interacting": cnt_interacting,
                    "Idle Agent": cnt_idle,
                    "On Queue": cnt_on_queue,
                    "Available": cnt_available,
                    "Busy": cnt_busy,
                    "Away": cnt_away,
                    "Break": cnt_break,
                    "Meal": cnt_meal,
                    "Meeting": cnt_meeting,
                    "Training": cnt_training,
                }
                
                # Daily metrics mapping
                daily_values = {
                    "Offered": off,
                    "Answered": ans,
                    "Abandoned": abn,
                    "Answer Rate": f"%{(ans/off*100) if off>0 else 0:.1f}",
                    "Service Level": f"%{sl:.1f}",
                    "Avg Handle Time": f"{avg_handle/60:.1f}m" if avg_handle else "0",
                    "Avg Wait Time": f"{avg_wait:.0f}s" if avg_wait else "0",
                }
                
                if st.session_state.dashboard_mode == "Live":
                    # Show selected live metrics (Responsive Grid)
                    sel_live = card.get('live_metrics', ["Waiting", "Interacting", "On Queue"])
                    if sel_live:
                        # Use 5 columns per row
                        cols_per_row = 5
                        for i in range(0, len(sel_live), cols_per_row):
                            batch = sel_live[i:i+cols_per_row]
                            cols = st.columns(cols_per_row)
                            for j, metric in enumerate(batch):
                                cols[j].metric(live_labels.get(metric, metric), live_values.get(metric, 0))
                    
                    # Show daily summary below live (Today's stats)
                    # Show daily summary below live (Today's stats)
                    st.caption(f"📅 Bugünün Özeti")
                    sel_daily = card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"])
                    if sel_daily:
                        cols_per_row = 5
                        for i in range(0, len(sel_daily), cols_per_row):
                            batch = sel_daily[i:i+cols_per_row]
                            cols = st.columns(cols_per_row)
                            for j, metric in enumerate(batch):
                                cols[j].metric(daily_labels.get(metric, metric), daily_values.get(metric, 0))
                    
                    # Render selected visuals
                    visuals = card.get('visual_metrics', ["Service Level"])
                    # Dynamic Height: Smaller if multiple visuals to fit side-by-side without overflowing vertically if wrapped (though we will use cols)
                    # Actually if side-by-side, we can keep reasonable height but maybe separate row if too many?
                    # For now, put them all in one row.
                    base_h = 100 if card.get('size') == 'xsmall' else (130 if card.get('size') == 'small' else (160 if card.get('size') == 'medium' else 190))
                    # If multiple, potentially reduce slightly or keep same? User said "too big".
                    # Let's trust the reduced base_h.
                    
                    panel_key_suffix = "open" if (st.session_state.get('show_agent_panel', False) or st.session_state.get('show_call_panel', False)) else "closed"

                    if visuals:
                        cols = st.columns(len(visuals))
                        for idx, vis in enumerate(visuals):
                            with cols[idx]:
                                if vis == "Service Level":
                                    st.plotly_chart(create_gauge_chart(sl, get_text(lang, "avg_service_level"), base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_sl_{card['id']}_{panel_key_suffix}")
                                elif vis == "Answer Rate":
                                    ar_val = (ans / off * 100) if off > 0 else 0
                                    st.plotly_chart(create_gauge_chart(ar_val, "Answer Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ar_{card['id']}_{panel_key_suffix}")
                                elif vis == "Abandon Rate":
                                    ab_val = (abn / off * 100) if off > 0 else 0
                                    st.plotly_chart(create_gauge_chart(ab_val, "Abandon Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ab_{card['id']}_{panel_key_suffix}")
                
                else:
                    # Historical mode (Yesterday/Date) - show daily stats with gauge
                    sel_daily = card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"])
                    
                    # Show daily metrics first for ALL sizes
                    st.caption(f"📅 {get_text(lang, 'daily_stat')}")
                    if sel_daily:
                        cols_per_row = 5
                        for i in range(0, len(sel_daily), cols_per_row):
                            batch = sel_daily[i:i+cols_per_row]
                            cols = st.columns(cols_per_row)
                            for j, metric in enumerate(batch):
                                cols[j].metric(daily_labels.get(metric, metric), daily_values.get(metric, 0))
                    
                    # Render selected visuals
                    visuals = card.get('visual_metrics', ["Service Level"])
                    base_h = 100 if card.get('size') == 'xsmall' else (130 if card.get('size') == 'small' else (160 if card.get('size') == 'medium' else 190))
                    panel_key_suffix = "open" if (st.session_state.get('show_agent_panel', False) or st.session_state.get('show_call_panel', False)) else "closed"

                    if visuals:
                        cols = st.columns(len(visuals))
                        for idx, vis in enumerate(visuals):
                            with cols[idx]:
                                if vis == "Service Level":
                                    st.plotly_chart(create_gauge_chart(sl, get_text(lang, "avg_service_level"), base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_sl_{card['id']}_{panel_key_suffix}")
                                elif vis == "Answer Rate":
                                    ar_val = (ans / off * 100) if off > 0 else 0
                                    st.plotly_chart(create_gauge_chart(ar_val, "Answer Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ar_{card['id']}_{panel_key_suffix}")
                                elif vis == "Abandon Rate":
                                    ab_val = (abn / off * 100) if off > 0 else 0
                                    st.plotly_chart(create_gauge_chart(ab_val, "Abandon Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ab_{card['id']}_{panel_key_suffix}")

    if to_del:
        for i in sorted(to_del, reverse=True): del st.session_state.dashboard_cards[i]
        save_dashboard_config(org, st.session_state.dashboard_layout, st.session_state.dashboard_cards)
        refresh_data_manager_queues()
        st.rerun()

    # --- SIDE PANEL LOGIC ---
    if st.session_state.get('show_agent_panel', False) and agent_c:
        with agent_c:
            # --- Compact CSS for Agent List and Filters ---
            st.markdown("""
                <style>
                    /* Target the sidebar vertical blocks to reduce gap */
                    [data-testid="stSidebarUserContent"] .stVerticalBlock {
                        gap: 0.5rem !important;
                    }
                    /* Prevent blur/flicker on agent panel */
                    [data-testid="column"]:has(.agent-card) {
                        backface-visibility: hidden !important;
                        -webkit-backface-visibility: hidden !important;
                        transform: translateZ(0);
                        filter: none !important;
                    }
                    .agent-card {
                        padding: 6px 10px !important;
                        margin-bottom: 6px !important;
                        border-radius: 8px !important;
                        border: 1px solid #f1f5f9 !important;
                        background: #ffffff;
                        display: flex;
                        align-items: center;
                        gap: 10px;
                        animation: none !important;
                        transition: none !important;
                        filter: none !important;
                    }
                    .status-dot {
                        width: 12px;
                        height: 12px;
                        border-radius: 50%;
                        display: inline-block;
                        flex-shrink: 0;
                    }
                    .agent-name {
                        font-size: 1.1rem !important;
                        font-weight: 600 !important;
                        color: #1e293b;
                        margin: 0 !important;
                        line-height: 1.2;
                    }
                    .agent-status {
                        font-size: 1.0rem !important;
                        color: #64748b;
                        margin: 0 !important;
                        line-height: 1.2;
                    }
                    .aktif-sayisi {
                        font-size: 0.85rem;
                        color: #64748b;
                        margin-top: -5px !important;
                        margin-bottom: 5px !important;
                    }
                </style>
            """, unsafe_allow_html=True)

            # Filter Text Input
            search_term = st.text_input("🔍 Agent Ara", "", label_visibility="collapsed", placeholder="Agent Ara...").lower()
            
            # Group Filter (Genesys Groups)
            now_ts = pytime.time()
            groups_cache = st.session_state.get("dashboard_groups_cache", [])
            groups_ts = st.session_state.get("dashboard_groups_ts", 0)
            if (not groups_cache) or ((now_ts - groups_ts) > 600):
                try:
                    api = GenesysAPI(st.session_state.api_client)
                    groups_cache = api.get_groups()
                    st.session_state.dashboard_groups_cache = groups_cache
                    st.session_state.dashboard_groups_ts = now_ts
                except Exception:
                    pass
            group_options = ["Hepsi (All)"] + [g.get('name', '') for g in groups_cache if g.get('name')]
            selected_group = st.selectbox("📌 Grup Filtresi", group_options, index=0)
            
            if st.session_state.dashboard_mode != "Live":
                st.warning("Agent detayları sadece CANLI modda görünür.")
            elif not st.session_state.get('api_client'):
                st.warning(get_text(lang, "genesys_not_connected"))
            elif not st.session_state.get('users_info'):
                st.info("Kullanıcı bilgileri yükleniyor...")
            else:
                agent_notif = ensure_agent_notifications_manager()
                agent_notif.update_client(
                    st.session_state.api_client,
                    st.session_state.queues_map,
                    st.session_state.get('users_info'),
                    st.session_state.get('presence_map')
                )

                # Determine target user ids (group-based)
                if selected_group != "Hepsi (All)":
                    selected_group_obj = next((g for g in groups_cache if g.get('name') == selected_group), None)
                    group_member_ids = set()
                    if selected_group_obj and selected_group_obj.get("id"):
                        group_id = selected_group_obj.get("id")
                        members_cache = st.session_state.get("dashboard_group_members_cache", {})
                        if members_cache:
                            stale_ids = [gid for gid, ent in members_cache.items() if (now_ts - ent.get("ts", 0)) > 1800]
                            for gid in stale_ids:
                                members_cache.pop(gid, None)
                            if len(members_cache) > 40:
                                oldest = sorted(members_cache.items(), key=lambda kv: kv[1].get("ts", 0))[:len(members_cache) - 40]
                                for gid, _ in oldest:
                                    members_cache.pop(gid, None)
                            st.session_state.dashboard_group_members_cache = members_cache
                        entry = members_cache.get(group_id, {})
                        if (not entry) or ((now_ts - entry.get("ts", 0)) > 600):
                            try:
                                api = GenesysAPI(st.session_state.api_client)
                                members = api.get_group_members(group_id)
                                members_cache[group_id] = {"ts": now_ts, "members": members}
                                st.session_state.dashboard_group_members_cache = members_cache
                                entry = members_cache.get(group_id, {})
                            except Exception:
                                pass
                        for m in (entry.get("members") or []):
                            if m.get("id"):
                                group_member_ids.add(m.get("id"))
                    all_user_ids = sorted(group_member_ids)
                else:
                    all_user_ids = sorted(st.session_state.users_info.keys())

                # Seed from API if notifications cache is empty/stale
                shared_ts, shared_presence, shared_routing = _get_shared_agent_seed(org)
                last_msg = getattr(agent_notif, "last_message_ts", 0)
                last_evt = getattr(agent_notif, "last_event_ts", 0)
                notif_stale = (not agent_notif.connected) or (last_msg == 0) or ((now_ts - last_evt) > 60)
                if all_user_ids:
                    if (not getattr(agent_notif, "user_presence", {}) and not getattr(agent_notif, "user_routing", {})) or notif_stale:
                        if _reserve_agent_seed(org, now_ts, min_interval=60):
                            try:
                                api = GenesysAPI(st.session_state.api_client)
                                snap = api.get_users_status_scan(target_user_ids=all_user_ids)
                                pres = snap.get("presence") or {}
                                rout = snap.get("routing") or {}
                                agent_notif.seed_users(pres, rout)
                                _merge_agent_seed(org, pres, rout, now_ts)
                            except Exception:
                                pass
                    else:
                        if shared_presence or shared_routing:
                            pres = {uid: shared_presence.get(uid) for uid in all_user_ids if uid in shared_presence}
                            rout = {uid: shared_routing.get(uid) for uid in all_user_ids if uid in shared_routing}
                            if pres or rout:
                                agent_notif.seed_users_missing(pres, rout)

                # Only keep non-OFFLINE users for websocket and display
                active_user_ids = []
                for uid in all_user_ids:
                    presence = agent_notif.get_user_presence(uid) if agent_notif else {}
                    sys_presence = (presence.get('presenceDefinition', {}).get('systemPresence', '')).upper()
                    if sys_presence and sys_presence != "OFFLINE":
                        active_user_ids.append(uid)

                max_users = (agent_notif.MAX_TOPICS_PER_CHANNEL * agent_notif.MAX_CHANNELS) // 3
                ws_user_ids = active_user_ids[:max_users]
                if len(active_user_ids) > max_users:
                    st.caption(f"⚠️ WebSocket limiti: {max_users}/{len(active_user_ids)} agent anlık takipte")
                if ws_user_ids:
                    agent_notif.start(ws_user_ids)

                # Build agent_data from active users
                agent_data = {"_all": []}
                for uid in active_user_ids:
                    user_info = st.session_state.users_info.get(uid, {})
                    name = user_info.get("name", "Unknown")
                    presence = agent_notif.get_user_presence(uid) if agent_notif else {}
                    routing = agent_notif.get_user_routing(uid) if agent_notif else {}
                    agent_data["_all"].append({
                        "id": uid,
                        "user": {"id": uid, "name": name, "presence": presence},
                        "routingStatus": routing,
                    })

                if not agent_data.get("_all"):
                    st.info("Aktif agent bulunamadı.")
                else:
                    # Flatten, Deduplicate and Filter Offline
                    unique_members = {}
                    for q_name, members in agent_data.items():
                        for m in members:
                            mid = m['id']
                            user_obj = m.get('user', {})
                            presence = user_obj.get('presence', {}).get('presenceDefinition', {}).get('systemPresence', 'OFFLINE').upper()
                            
                            # Temporarily show everyone for debug
                            # if presence == "OFFLINE": continue
                            
                            if mid not in unique_members:
                                unique_members[mid] = m
                    
                    # Apply Search Filter
                    filtered_mems = []
                    for m in unique_members.values():
                        name = m.get('user', {}).get('name', 'Unknown')
                        if search_term in name.lower():
                            filtered_mems.append(m)
                    
                    # Define Sorting Priority
                    # Custom Order: Break, Meal, On Queue, Available
                    def get_sort_score(m):
                        user_obj = m.get('user', {})
                        presence_obj = user_obj.get('presence', {})
                        p = presence_obj.get('presenceDefinition', {}).get('systemPresence', 'OFFLINE').upper()
                        routing_obj = m.get('routingStatus', {})
                        rs = routing_obj.get('status', 'OFF_QUEUE').upper()
                        
                        # Calculate Duration (Secondary Sort - Descending)
                        # We want longest duration first, so we use negative total seconds
                        start_str = routing_obj.get('startTime') # Routing time has precedence for On Queue
                        if not start_str or rs == 'OFF_QUEUE': # Use presence time if not on queue routing
                            start_str = presence_obj.get('modifiedDate')
                        
                        try:
                            if start_str:
                                # Normalize Z to +00:00 for fromisoformat (Python 3.11+ handles Z, but safe bet)
                                start_str = start_str.replace("Z", "+00:00")
                                start_dt = datetime.fromisoformat(start_str)
                                # Duration in seconds (larger is longer)
                                duration_sec = (datetime.now(timezone.utc) - start_dt).total_seconds()
                                neg_duration = -duration_sec
                            else:
                                neg_duration = 0
                        except:
                            neg_duration = 0

                        # Priority Scores (Lower is higher in list)
                        score = 10 # Default
                        
                        if p == 'OFFLINE': score = 99
                        elif p == 'BREAK': score = 1
                        elif p == 'MEAL': score = 2
                        # On Queue Logic (includes Interacting, Idle, Not Responding)
                        elif p in ['ON_QUEUE', 'ON QUEUE'] or rs in ['INTERACTING', 'COMMUNICATING', 'IDLE', 'NOT_RESPONDING']:
                            score = 3
                        elif p == 'AVAILABLE': score = 4
                        elif p == 'BUSY': score = 5
                        elif p == 'MEETING': score = 6
                        elif p == 'TRAINING': score = 7
                        
                        return (score, neg_duration)

                    # Sort members
                    all_members = list(filtered_mems) # Use filtered_mems here
                    all_members.sort(key=get_sort_score)

                    st.markdown(f'<p class="aktif-sayisi">Aktif: {len(all_members)}</p>', unsafe_allow_html=True)
                    
                    max_display = 200
                    for i, m in enumerate(all_members):
                        if i >= max_display:
                            st.caption(f"+{len(all_members) - max_display} daha fazla kayıt")
                            break
                        user_obj = m.get('user', {})
                        name = user_obj.get('name', 'Unknown')
                        # Parse status
                        presence_obj = user_obj.get('presence', {})
                        presence = presence_obj.get('presenceDefinition', {}).get('systemPresence', 'OFFLINE').upper()
                        routing_obj = m.get('routingStatus', {})
                        routing = routing_obj.get('status', 'OFF_QUEUE').upper()
                        
                        # Duration
                        duration_str = format_status_time(presence_obj.get('modifiedDate'), routing_obj.get('startTime'))
                        
                        # Compact Status Mapping
                        dot_color = "#94a3b8" # gray
                        # Use Label if available, else capitalize system presence
                        label = presence_obj.get('presenceDefinition', {}).get('label')
                        status_text = label if label else presence.replace("_", " ").capitalize()
                        
                        # Genesys Standard Logic (Prioritize Routing)
                        # 1. Routing Status takes precedence (Interacting, Idle)
                        # 2. Presence is fallback (Available, Break, etc.)
                        
                        if routing in ["INTERACTING", "COMMUNICATING"]:
                            dot_color = "#3b82f6" # blue
                            status_text = "Görüşmede"
                        elif routing == "IDLE":
                            dot_color = "#22c55e" # green
                            status_text = "On Queue" # User requested "On Queue"
                        elif routing == "NOT_RESPONDING":
                            dot_color = "#ef4444" # red
                            status_text = "Cevapsız"
                            
                        elif presence == "AVAILABLE":
                            dot_color = "#22c55e" # green
                            status_text = "Müsait"
                        elif presence in ["ON_QUEUE", "ON QUEUE"]:
                            # Fallback if routing didn't catch it (e.g. transitioning)
                            dot_color = "#22c55e" # green
                            status_text = "On Queue"
                            
                        elif presence == "BUSY":
                            dot_color = "#ef4444"
                            if not label: status_text = "Meşgul"
                        elif presence in ["AWAY", "BREAK", "MEAL"]:
                            dot_color = "#f59e0b" # orange
                        elif presence == "MEETING":
                            dot_color = "#ef4444"
                            if not label: status_text = "Toplantı"
                        
                        display_status = f"{status_text} - {duration_str}" if duration_str else status_text
                        
                        # Render compact HTML
                        st.markdown(f"""
                            <div class="agent-card">
                                <span class="status-dot" style="background-color: {dot_color};"></span>
                                <div>
                                        <p class="agent-name">{name}</p>
                                        <p class="agent-status">{display_status}</p>
                                    </div>
                                </div>
                            """, unsafe_allow_html=True)

    # --- CALL PANEL LOGIC ---
    if st.session_state.get('show_call_panel', False) and call_c:
        with call_c:

            st.markdown("""
                <style>
                    .call-card {
                        padding: 6px 10px !important;
                        margin-bottom: 6px !important;
                        border-radius: 8px !important;
                        border: 1px solid #f1f5f9 !important;
                        background: #ffffff;
                        display: flex;
                        align-items: center;
                        justify-content: space-between;
                        gap: 10px;
                        opacity: 1;
                        animation: none !important;
                        transition: none !important;
                    }
                    .call-queue {
                        font-size: 1.0rem !important;
                        font-weight: 600 !important;
                        color: #1e293b;
                        line-height: 1.2;
                    }
                    .call-wait {
                        font-size: 0.95rem !important;
                        color: #334155;
                        font-weight: 600;
                        white-space: nowrap;
                    }
                    .call-meta {
                        font-size: 0.9rem;
                        color: #94a3b8;
                        margin-top: 3px;
                    }
                    .call-queue { line-height: 1.15; }
                    .call-info { min-width: 0; }
                    .panel-count {
                        font-size: 0.85rem;
                        color: #64748b;
                        margin-top: -5px !important;
                        margin-bottom: 5px !important;
                    }
                    .panel-top-spacer {
                        height: 38px;
                    }
                </style>
            """, unsafe_allow_html=True)

            st.markdown("####")
            st.write("")

            # Filter options (group filter is optional - data is queue-independent)
            waiting_calls = []
            group_options = ["Hepsi (All)"] + [card['title'] or f"Grup #{idx+1}" for idx, card in enumerate(st.session_state.dashboard_cards)]
            selected_group = st.selectbox("📌 Grup (Kartlar)", group_options, index=0, key="call_panel_group")
            hide_mevcut = st.checkbox("Mevcut içeren kuyrukları gizle", value=False, key="call_panel_hide_mevcut")
            group_queues = set()
            if selected_group != "Hepsi (All)":
                for idx, card in enumerate(st.session_state.dashboard_cards):
                    label = card['title'] or f"Grup #{idx+1}"
                    if label == selected_group and card.get('queues'):
                        group_queues.update(card['queues'])
                        break

            if st.session_state.dashboard_mode != "Live":
                st.warning(get_text(lang, "call_panel_live_only"))
            elif not st.session_state.get('api_client'):
                st.warning(get_text(lang, "genesys_not_connected"))
            else:
                refresh_s = int(st.session_state.get('org_config', {}).get('refresh_interval', 15) or 15)
                refresh_s = max(3, refresh_s)
                _safe_autorefresh(interval=refresh_s * 1000, key="call_panel_fast_refresh")
                now_ts = pytime.time()
                queue_id_to_name = {v: k for k, v in st.session_state.queues_map.items()}

                # ========================================
                # QUEUE-INDEPENDENT CALL PANEL (org-wide)
                # Hybrid: API seed + WebSocket updates
                # Seed/refresh logic aligned with agent panel
                # ========================================
                global_notif = ensure_global_conversation_manager()
                global_notif.update_client(st.session_state.api_client, st.session_state.queues_map)

                all_queue_ids = list(st.session_state.queues_map.values()) if st.session_state.get("queues_map") else []
                global_topics = [f"v2.routing.queues.{qid}.conversations" for qid in all_queue_ids if qid]
                global_notif.start(global_topics)

                last_msg = getattr(global_notif, "last_message_ts", 0)
                last_evt = getattr(global_notif, "last_event_ts", 0)
                notif_stale = (not global_notif.connected) or (last_msg == 0) or ((now_ts - last_evt) > 30)

                # Basic model: WS is primary, periodic API snapshot fully replaces cache.
                # Snapshot throttling is org-wide (shared), not per session.
                shared_snapshot_ts, shared_snapshot_calls = _get_shared_call_seed(org)
                if shared_snapshot_calls:
                    with global_notif._lock:
                        if not global_notif.active_conversations:
                            seed_map = {}
                            for c in shared_snapshot_calls:
                                cid = c.get("conversation_id")
                                if not cid:
                                    continue
                                item = dict(c)
                                item.setdefault("last_update", shared_snapshot_ts or now_ts)
                                seed_map[cid] = item
                            if seed_map:
                                global_notif.active_conversations = seed_map

                should_snapshot = notif_stale or (now_ts - shared_snapshot_ts >= refresh_s)
                if should_snapshot and _reserve_call_seed(org, now_ts, min_interval=refresh_s):
                    try:
                        snapshot_started_ts = pytime.time()
                        api = GenesysAPI(st.session_state.api_client)
                        end_dt = datetime.now(timezone.utc)
                        start_dt = end_dt - timedelta(minutes=15)
                        convs = api.get_conversation_details_recent(start_dt, end_dt, page_size=50, max_pages=1, order="desc")
                        snapshot_calls = _build_active_calls(
                            [c for c in (convs or []) if not c.get("conversationEnd")],
                            lang,
                            queue_id_to_name=queue_id_to_name,
                            users_info=st.session_state.get("users_info"),
                        )
                        now_update = pytime.time()
                        for c in snapshot_calls:
                            c.setdefault("state", "waiting")
                            c.setdefault("wg", c.get("queue_name"))
                            c["last_update"] = now_update
                            if not c.get("media_type"):
                                c["media_type"] = _extract_media_type(c)
                        _update_call_seed(org, snapshot_calls, now_update, max_items=300)
                        _update_call_meta(org, snapshot_calls, now_update, max_items=300)
                        new_map = {c.get("conversation_id"): c for c in snapshot_calls if c.get("conversation_id")}
                        with global_notif._lock:
                            existing_map = dict(global_notif.active_conversations or {})
                            merged_map = {}

                            # 1) Start from snapshot, but never overwrite fresher WS data.
                            for cid, snap_item in new_map.items():
                                ex = existing_map.get(cid) or {}
                                ex_ts = ex.get("last_update", 0) or 0
                                if ex and ex_ts > snapshot_started_ts:
                                    merged_map[cid] = ex
                                else:
                                    merged_map[cid] = _merge_call(ex, snap_item) if ex else snap_item

                            # 2) Keep very fresh WS-only items that may lag in analytics snapshot.
                            for cid, ex in existing_map.items():
                                if cid in merged_map:
                                    continue
                                ex_ts = ex.get("last_update", 0) or 0
                                if ex_ts and (snapshot_started_ts - ex_ts) <= 30:
                                    merged_map[cid] = ex

                            global_notif.active_conversations = merged_map
                    except Exception:
                        pass

                active_conversations = global_notif.get_active_conversations(max_age_seconds=300)
                for c in active_conversations:
                    if "state" not in c:
                        c["state"] = "waiting"
                    if "wg" not in c or not c.get("wg"):
                        c["wg"] = c.get("queue_name")

                combined = {}
                for c in active_conversations:
                    cid = c.get("conversation_id")
                    if cid:
                        combined[cid] = dict(c)

                # Enrich from shared meta cache first (org-wide).
                shared_meta = _get_shared_call_meta(org, max_age_seconds=600)
                if shared_meta:
                    for cid, item in list(combined.items()):
                        meta = shared_meta.get(cid)
                        if meta:
                            combined[cid] = _merge_call(item, meta)

                # Small enrichment pass for missing queue/phone/direction fields.
                missing_ids = []
                for cid, item in combined.items():
                    has_queue = bool(item.get("queue_name")) and not _is_generic_queue_name(item.get("queue_name"))
                    has_wg = bool(item.get("wg"))
                    has_phone = bool(item.get("phone"))
                    has_direction = bool(item.get("direction_label")) or bool(item.get("direction"))
                    if not (has_queue and has_wg and has_phone and has_direction):
                        missing_ids.append(cid)

                if missing_ids and _reserve_call_meta_poll(org, now_ts, min_interval=refresh_s):
                    try:
                        api = GenesysAPI(st.session_state.api_client)
                        users_info = st.session_state.get("users_info") or {}
                        for cid in missing_ids[:2]:
                            meta = _fetch_conversation_meta(api, cid, queue_id_to_name, users_info=users_info)
                            if meta:
                                if meta.get("ended"):
                                    combined.pop(cid, None)
                                    try:
                                        with global_notif._lock:
                                            global_notif.active_conversations.pop(cid, None)
                                    except Exception:
                                        pass
                                    continue
                                combined[cid] = _merge_call(combined.get(cid), meta)
                    except Exception:
                        pass

                # Final cleanup.
                for cid in list(combined.keys()):
                    item = combined.get(cid) or {}
                    if item.get("ended"):
                        combined.pop(cid, None)
                        continue
                    lu = item.get("last_update", 0) or 0
                    if lu and (now_ts - lu) > 300:
                        combined.pop(cid, None)
                        continue

                waiting_calls = list(combined.values())
                try:
                    _update_call_meta(org, waiting_calls, now_ts, max_items=300)
                except Exception:
                    pass

                if group_queues:
                    waiting_calls = [c for c in waiting_calls if c.get("queue_name") in group_queues]
                if hide_mevcut:
                    waiting_calls = [c for c in waiting_calls if "mevcut" not in (c.get("queue_name") or "").lower()]

                waiting_calls.sort(key=lambda x: x.get("wait_seconds") if x.get("wait_seconds") is not None else -1, reverse=True)

                st.markdown('<div class="panel-top-spacer"></div>', unsafe_allow_html=True)
                count_label = "Aktif"
                st.markdown(f'<p class="panel-count">{count_label}: {len(waiting_calls)}</p>', unsafe_allow_html=True)

                if not waiting_calls:
                    st.info(get_text(lang, "no_waiting_calls"))
                else:
                    max_display = 200
                    for i, item in enumerate(waiting_calls):
                        if i >= max_display:
                            st.caption(f"+{len(waiting_calls) - max_display} daha fazla kayıt")
                            break
                        wait_str = format_duration_seconds(item.get("wait_seconds"))
                        q = item.get("queue_name", "")
                        queue_display = "-" if _is_generic_queue_name(q) else q
                        queue_text = f"{queue_display}"
                        conv_id = item.get("conversation_id")
                        conv_short = conv_id[-6:] if conv_id and len(conv_id) > 6 else conv_id
                        phone = item.get("phone")
                        wg = item.get("wg")
                        direction_label = item.get("direction_label")
                        state_label = item.get("state_label")
                        media_type = item.get("media_type")
                        agent_name = item.get("agent_name")
                        agent_id = item.get("agent_id")
                        ivr_selection = item.get("ivr_selection")
                        state_value = (item.get("state") or "").lower()
                        if not direction_label:
                            d = (item.get("direction") or "").lower()
                            if "inbound" in d:
                                direction_label = "Inbound"
                            elif "outbound" in d:
                                direction_label = "Outbound"
                        if media_type and media_type.lower() == "callback":
                            media_type = "Callback"
                            if not direction_label:
                                direction_label = "Outbound"
                        elif media_type and media_type.lower() == "voice":
                            media_type = "Voice"
                        meta_parts = []
                        is_interacting = bool(agent_name) or bool(agent_id) or state_value == "interacting" or (state_label == get_text(lang, "interacting"))
                        state_label = "Bağlandı" if is_interacting else "Bekleyen"
                        if agent_name and is_interacting:
                            meta_parts.append(f"Agent: {agent_name}")
                        if wg and str(wg).strip() and str(wg).strip().lower() != str(queue_display).strip().lower():
                            meta_parts.append(f"WG: {wg}")
                        if ivr_selection:
                            meta_parts.append(f"🔢 {ivr_selection}")
                        if direction_label:
                            meta_parts.append(str(direction_label))
                        if state_label:
                            meta_parts.append(str(state_label))
                        if media_type:
                            meta_parts.append(str(media_type))
                        if phone:
                            meta_parts.append(str(phone))
                        if conv_short:
                            meta_parts.append(f"#{conv_short}")
                        meta_text = " • ".join(meta_parts)
                        meta_html = f'<div class="call-meta">{meta_text}</div>' if meta_text else ""

                        st.markdown(f"""
                            <div class="call-card">
                                <div class="call-info">
                                    <div class="call-queue">{queue_text}</div>
                                    {meta_html}
                                </div>
                                <div class="call-wait">{wait_str}</div>
                            </div>
                        """, unsafe_allow_html=True)
