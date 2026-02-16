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
import hashlib
import time as pytime
import threading
import uuid
import signal
import traceback
import warnings
import tempfile
import psutil
import re
import html

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
RESTART_EXIT_CODE = int(os.environ.get("GENESYS_RESTART_EXIT_CODE", "42"))
TEMP_CLEANUP_INTERVAL_SEC = int(os.environ.get("GENESYS_TEMP_CLEANUP_INTERVAL_SEC", "900"))
TEMP_FILE_MAX_AGE_HOURS = float(os.environ.get("GENESYS_TEMP_FILE_MAX_AGE_HOURS", "6"))
TEMP_FILE_MANUAL_MAX_AGE_HOURS = float(os.environ.get("GENESYS_TEMP_FILE_MANUAL_MAX_AGE_HOURS", "0"))
TEMP_KEEP_RECENT_SECONDS = int(os.environ.get("GENESYS_TEMP_KEEP_RECENT_SECONDS", "120"))

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
    try:
        logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(logs_dir, exist_ok=True)
        file_handler = logging.FileHandler(os.path.join(logs_dir, "app.log"), encoding="utf-8")
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)
    except Exception:
        pass
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
DEBUG_REMEMBER_ME = os.environ.get("GENESYS_DEBUG_REMEMBER_ME", "0").strip().lower() in ("1", "true", "yes", "on")
ENABLE_DASHBOARD_PROFILING = os.environ.get("GENESYS_ENABLE_DASHBOARD_PROFILING", "0").strip().lower() in ("1", "true", "yes", "on")
ORG_CODE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,49}$")
LOGIN_WINDOW_SECONDS = int(os.environ.get("GENESYS_LOGIN_WINDOW_SECONDS", "900"))
LOGIN_LOCK_SECONDS = int(os.environ.get("GENESYS_LOGIN_LOCK_SECONDS", "900"))
LOGIN_MAX_FAILURES = int(os.environ.get("GENESYS_LOGIN_MAX_FAILURES", "5"))
LOGIN_ATTEMPT_MAX_ENTRIES = int(os.environ.get("GENESYS_LOGIN_ATTEMPT_MAX_ENTRIES", "5000"))

def _rm_debug(msg, *args):
    if DEBUG_REMEMBER_ME:
        logger.info("[remember-me] " + msg, *args)

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


def _safe_org_code(org_code, allow_default=True):
    raw = str(org_code or "").strip()
    if not raw:
        if allow_default:
            return "default"
        raise ValueError("Organization code is required")
    if not ORG_CODE_PATTERN.fullmatch(raw):
        raise ValueError("Organization code must match ^[A-Za-z0-9][A-Za-z0-9_-]{2,49}$")
    return raw


def _escape_html(value):
    return html.escape("" if value is None else str(value), quote=True)

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
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose == "outbound":
            return "Outbound"
        for s in p.get("sessions", []) or []:
            sd = (s.get("direction") or "").lower()
            if sd == "inbound":
                return "Inbound"
            if sd == "outbound":
                return "Outbound"
    # Do not force Inbound on ambiguous payloads (external+agent) because outbound
    # conversations can look similar when direction fields are missing.
    return None

def _normalize_call_direction_token(direction_label=None, direction=None):
    raw = str(direction_label or direction or "").lower()
    if "inbound" in raw:
        return "inbound"
    if "outbound" in raw:
        return "outbound"
    return None

def _normalize_call_media_token(media_type):
    mt = str(media_type or "").strip().lower()
    if not mt:
        return None
    if "callback" in mt:
        return "callback"
    if mt in {"voice", "call", "phone", "telephony"} or "voice" in mt:
        return "voice"
    message_aliases = {
        "message", "messages", "sms", "chat", "webchat", "email",
        "webmessaging", "openmessaging", "whatsapp", "facebook", "twitter", "line", "telegram",
    }
    if mt in message_aliases:
        return "message"
    if any(k in mt for k in ["message", "chat", "email", "sms", "whatsapp", "facebook", "twitter", "line", "telegram"]):
        return "message"
    return mt

def _normalize_call_state_token(item):
    if not isinstance(item, dict):
        return None

    state_raw = str(item.get("state") or "").strip().lower()
    if state_raw in {"interacting", "connected", "communicating", "active"}:
        return "connected"
    if state_raw in {"waiting", "queued", "queue", "alerting", "offering", "dialing", "contacting"}:
        return "waiting"

    state_label_raw = str(item.get("state_label") or "").strip().lower()
    if any(token in state_label_raw for token in ["bağlandı", "baglandi", "interacting", "connected"]):
        return "connected"
    if any(token in state_label_raw for token in ["bekleyen", "waiting", "queued", "queue"]):
        return "waiting"

    if item.get("agent_name") or item.get("agent_id"):
        return "connected"
    return "waiting"

def _call_filter_tokens(item):
    direction_token = _normalize_call_direction_token(item.get("direction_label"), item.get("direction"))
    media_token = _normalize_call_media_token(item.get("media_type"))
    state_token = _normalize_call_state_token(item)
    return direction_token, media_token, state_token

def _call_matches_filters(item, direction_filters=None, media_filters=None, state_filters=None):
    direction_filters = {str(x).lower() for x in (direction_filters or []) if x}
    media_filters = {str(x).lower() for x in (media_filters or []) if x}
    state_filters = {str(x).lower() for x in (state_filters or []) if x}
    direction_token, media_token, state_token = _call_filter_tokens(item)
    if direction_filters and direction_token not in direction_filters:
        return False
    if media_filters and media_token not in media_filters:
        return False
    if state_filters and state_token not in state_filters:
        return False
    return True

def _extract_queue_name_from_conv(conv, queue_id_to_name=None):
    queue_id_to_name = queue_id_to_name or {}
    fallback_name = None
    def _remember_name(name):
        nonlocal fallback_name
        if not name:
            return
        if not fallback_name:
            fallback_name = name
        if not _is_generic_queue_name(name):
            fallback_name = name

    if isinstance(conv, dict):
        qname = conv.get("queueName")
        if qname:
            if not _is_generic_queue_name(qname):
                return qname
            _remember_name(qname)
        qid = conv.get("queueId")
        if qid and qid in queue_id_to_name:
            mapped = queue_id_to_name.get(qid)
            if mapped and not _is_generic_queue_name(mapped):
                return mapped
            _remember_name(mapped)
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["acd", "queue"]:
            q_id = p.get("queueId") or p.get("routingQueueId")
            if q_id and q_id in queue_id_to_name:
                mapped = queue_id_to_name.get(q_id)
                if mapped and not _is_generic_queue_name(mapped):
                    return mapped
                _remember_name(mapped)
            name = p.get("name")
            if name:
                if not _is_generic_queue_name(name):
                    return name
                _remember_name(name)
            qobj = p.get("queue") or {}
            if isinstance(qobj, dict):
                if qobj.get("name"):
                    qname = qobj.get("name")
                    if not _is_generic_queue_name(qname):
                        return qname
                    _remember_name(qname)
                qid = qobj.get("id")
                if qid and qid in queue_id_to_name:
                    mapped = queue_id_to_name.get(qid)
                    if mapped and not _is_generic_queue_name(mapped):
                        return mapped
                    _remember_name(mapped)
            for s in p.get("sessions", []) or []:
                qid = s.get("queueId") or s.get("routingQueueId")
                if qid and qid in queue_id_to_name:
                    mapped = queue_id_to_name.get(qid)
                    if mapped and not _is_generic_queue_name(mapped):
                        return mapped
                    _remember_name(mapped)
                qname = s.get("queueName")
                if qname:
                    if not _is_generic_queue_name(qname):
                        return qname
                    _remember_name(qname)
                # Analytics API: queueId is inside segments
                for seg in s.get("segments", []) or []:
                    qid = seg.get("queueId")
                    if qid and qid in queue_id_to_name:
                        mapped = queue_id_to_name.get(qid)
                        if mapped and not _is_generic_queue_name(mapped):
                            return mapped
                        _remember_name(mapped)
                    qname = seg.get("queueName")
                    if qname:
                        if not _is_generic_queue_name(qname):
                            return qname
                        _remember_name(qname)
                    qobj = seg.get("queue") or {}
                    if isinstance(qobj, dict):
                        qname = qobj.get("name")
                        if qname:
                            if not _is_generic_queue_name(qname):
                                return qname
                            _remember_name(qname)
                        qid = qobj.get("id")
                        if qid and qid in queue_id_to_name:
                            mapped = queue_id_to_name.get(qid)
                            if mapped and not _is_generic_queue_name(mapped):
                                return mapped
                            _remember_name(mapped)
        # Outbound/direct calls may carry queue info on non-acd participants.
        q_id = p.get("queueId") or p.get("routingQueueId")
        if q_id and q_id in queue_id_to_name:
            mapped = queue_id_to_name.get(q_id)
            if mapped and not _is_generic_queue_name(mapped):
                return mapped
            _remember_name(mapped)
        qobj = p.get("queue") or {}
        if isinstance(qobj, dict):
            qname = qobj.get("name")
            if qname:
                if not _is_generic_queue_name(qname):
                    return qname
                _remember_name(qname)
            qid = qobj.get("id")
            if qid and qid in queue_id_to_name:
                mapped = queue_id_to_name.get(qid)
                if mapped and not _is_generic_queue_name(mapped):
                    return mapped
                _remember_name(mapped)
        qname = p.get("queueName")
        if qname:
            if not _is_generic_queue_name(qname):
                return qname
            _remember_name(qname)
        for s in p.get("sessions", []) or []:
            qid = s.get("queueId") or s.get("routingQueueId")
            if qid and qid in queue_id_to_name:
                mapped = queue_id_to_name.get(qid)
                if mapped and not _is_generic_queue_name(mapped):
                    return mapped
                _remember_name(mapped)
            qname = s.get("queueName")
            if qname:
                if not _is_generic_queue_name(qname):
                    return qname
                _remember_name(qname)
            qobj = s.get("queue") or {}
            if isinstance(qobj, dict):
                qname = qobj.get("name")
                if qname:
                    if not _is_generic_queue_name(qname):
                        return qname
                    _remember_name(qname)
                qid = qobj.get("id")
                if qid and qid in queue_id_to_name:
                    mapped = queue_id_to_name.get(qid)
                    if mapped and not _is_generic_queue_name(mapped):
                        return mapped
                    _remember_name(mapped)
            for seg in s.get("segments", []) or []:
                qid = seg.get("queueId")
                if qid and qid in queue_id_to_name:
                    mapped = queue_id_to_name.get(qid)
                    if mapped and not _is_generic_queue_name(mapped):
                        return mapped
                    _remember_name(mapped)
                qname = seg.get("queueName")
                if qname:
                    if not _is_generic_queue_name(qname):
                        return qname
                    _remember_name(qname)
                qobj = seg.get("queue") or {}
                if isinstance(qobj, dict):
                    qname = qobj.get("name")
                    if qname:
                        if not _is_generic_queue_name(qname):
                            return qname
                        _remember_name(qname)
                    qid = qobj.get("id")
                    if qid and qid in queue_id_to_name:
                        mapped = queue_id_to_name.get(qid)
                        if mapped and not _is_generic_queue_name(mapped):
                            return mapped
                        _remember_name(mapped)
    # Analytics segments
    for seg in (conv or {}).get("segments") or []:
        qname = seg.get("queueName")
        if qname:
            if not _is_generic_queue_name(qname):
                return qname
            _remember_name(qname)
        qobj = seg.get("queue") or {}
        if isinstance(qobj, dict):
            if qobj.get("name"):
                qname = qobj.get("name")
                if not _is_generic_queue_name(qname):
                    return qname
                _remember_name(qname)
            qid = qobj.get("id")
            if qid and qid in queue_id_to_name:
                mapped = queue_id_to_name.get(qid)
                if mapped and not _is_generic_queue_name(mapped):
                    return mapped
                _remember_name(mapped)
        qid = seg.get("queueId")
        if qid and qid in queue_id_to_name:
            mapped = queue_id_to_name.get(qid)
            if mapped and not _is_generic_queue_name(mapped):
                return mapped
            _remember_name(mapped)
    return fallback_name

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
                    qobj = seg.get("queue") or {}
                    if isinstance(qobj, dict):
                        qid = qobj.get("id")
                        if qid:
                            return qid
    # Some outbound/direct payloads keep queue data on non-acd participants.
    for p in participants:
        qid = p.get("queueId") or p.get("routingQueueId")
        if qid:
            return qid
        qobj = p.get("queue") or {}
        if isinstance(qobj, dict):
            qid = qobj.get("id")
            if qid:
                return qid
        for s in p.get("sessions", []) or []:
            qid = s.get("queueId") or s.get("routingQueueId")
            if qid:
                return qid
            qobj = s.get("queue") or {}
            if isinstance(qobj, dict):
                qid = qobj.get("id")
                if qid:
                    return qid
            for seg in s.get("segments", []) or []:
                qid = seg.get("queueId")
                if qid:
                    return qid
                qobj = seg.get("queue") or {}
                if isinstance(qobj, dict):
                    qid = qobj.get("id")
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
    def _normalize_phone(v):
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        if "<" in s and ">" in s:
            left = s.find("<")
            right = s.rfind(">")
            if left >= 0 and right > left:
                s = s[left + 1:right].strip()
        s_low = s.lower()
        for prefix in ("tel:", "sip:", "sips:"):
            if s_low.startswith(prefix):
                s = s[len(prefix):].strip()
                s_low = s.lower()
                break
        s = s.split(";", 1)[0].strip()
        local = s.split("@", 1)[0].strip() if "@" in s else s
        candidates = [local, s]
        for c in candidates:
            c = (c or "").strip().strip("\"").strip("'")
            if not c:
                continue
            has_plus = c.startswith("+")
            if any(ch.isalpha() for ch in c):
                continue
            digits = "".join(ch for ch in c if ch.isdigit())
            if len(digits) >= 7:
                return ("+" + digits) if has_plus else digits
        return None

    participants = conv.get("participants") or conv.get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["external", "customer", "outbound"]:
            for k in ["ani", "addressOther", "address", "callerId", "fromAddress", "toAddress", "dnis", "name"]:
                phone = _normalize_phone(p.get(k))
                if phone:
                    return phone
            for s in p.get("sessions", []) or []:
                for k in ["ani", "addressOther", "address", "callerId", "fromAddress", "toAddress", "dnis"]:
                    phone = _normalize_phone(s.get(k))
                    if phone:
                        return phone
    # Fallback to conversation-level fields.
    for k in ["ani", "addressOther", "fromAddress", "toAddress", "callerId", "dnis", "address"]:
        phone = _normalize_phone(conv.get(k))
        if phone:
            return phone
    return None

st.set_page_config(page_title="Genesys Cloud Reporting", layout="wide")

CREDENTIALS_FILE = "credentials.enc"
KEY_FILE = ".secret.key"
CONFIG_FILE = "dashboard_config.json"
PRESETS_FILE = "presets.json"

def _resolve_state_base_dir():
    env_dir = os.environ.get("GENESYS_STATE_DIR")
    if env_dir:
        return os.path.abspath(env_dir)
    if getattr(sys, "frozen", False):
        appdata = os.environ.get("APPDATA")
        if appdata:
            return os.path.join(appdata, "GenesysCloudReporting", "orgs")
        return os.path.join(os.path.expanduser("~"), ".genesys_cloud_reporting", "orgs")
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "orgs")

ORG_BASE_DIR = _resolve_state_base_dir()

def _migrate_legacy_state_dir():
    try:
        os.makedirs(ORG_BASE_DIR, exist_ok=True)
        if os.listdir(ORG_BASE_DIR):
            return
    except Exception:
        return
    candidates = []
    try:
        candidates.append(os.path.join(os.getcwd(), "orgs"))
    except Exception:
        pass
    try:
        if getattr(sys, "frozen", False):
            candidates.append(os.path.join(os.path.dirname(os.path.abspath(sys.executable)), "orgs"))
    except Exception:
        pass
    for source in candidates:
        try:
            if not source or os.path.abspath(source) == os.path.abspath(ORG_BASE_DIR):
                continue
            if os.path.isdir(source) and os.listdir(source):
                shutil.copytree(source, ORG_BASE_DIR, dirs_exist_ok=True)
                return
        except Exception:
            continue

_migrate_legacy_state_dir()


def _app_temp_dir(create=True):
    path = os.path.join(ORG_BASE_DIR, "_tmp")
    if create:
        try:
            os.makedirs(path, exist_ok=True)
        except Exception:
            return None
    return path


_TEMPFILE_DIR_CONFIGURED = False


def _configure_tempfile_dir():
    global _TEMPFILE_DIR_CONFIGURED
    if _TEMPFILE_DIR_CONFIGURED:
        return
    temp_dir = _app_temp_dir(create=True)
    if not temp_dir:
        return
    try:
        tempfile.tempdir = temp_dir
        _TEMPFILE_DIR_CONFIGURED = True
    except Exception:
        pass


def _format_bytes(num_bytes):
    size = float(max(0, num_bytes or 0))
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    return f"{size:.1f} {units[idx]}"


def cleanup_temp_files(max_age_hours=None, aggressive=False):
    if max_age_hours is None:
        max_age_hours = TEMP_FILE_MAX_AGE_HOURS
    try:
        max_age_seconds = max(0, int(float(max_age_hours) * 3600))
    except Exception:
        max_age_seconds = max(0, int(TEMP_FILE_MAX_AGE_HOURS * 3600))

    now = pytime.time()
    cutoff_ts = now - max_age_seconds
    keep_recent_seconds = 0 if aggressive else max(0, TEMP_KEEP_RECENT_SECONDS)
    summary = {
        "temp_dir": "",
        "scanned_files": 0,
        "removed_files": 0,
        "truncated_files": 0,
        "removed_dirs": 0,
        "freed_bytes": 0,
        "errors": [],
    }

    def _record_error(message):
        if len(summary["errors"]) < 20:
            summary["errors"].append(message)

    def _is_old_enough(path):
        try:
            mtime = os.path.getmtime(path)
            if keep_recent_seconds and (now - mtime) < keep_recent_seconds:
                return False
            if max_age_seconds > 0 and mtime > cutoff_ts:
                return False
            return True
        except Exception:
            return False

    def _remove_file(path):
        try:
            size = os.path.getsize(path)
        except Exception:
            size = 0
        try:
            os.remove(path)
            summary["removed_files"] += 1
            summary["freed_bytes"] += max(0, int(size or 0))
        except Exception as exc:
            _record_error(f"Dosya silinemedi: {path} ({exc})")

    def _truncate_file(path):
        try:
            size = os.path.getsize(path)
        except Exception:
            size = 0
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write("")
            summary["truncated_files"] += 1
            summary["freed_bytes"] += max(0, int(size or 0))
        except Exception as exc:
            _record_error(f"Dosya sıfırlanamadı: {path} ({exc})")

    app_tmp = _app_temp_dir(create=True)
    summary["temp_dir"] = app_tmp or ""

    if app_tmp and os.path.isdir(app_tmp):
        for root, dirs, files in os.walk(app_tmp, topdown=False):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                summary["scanned_files"] += 1
                if _is_old_enough(file_path):
                    _remove_file(file_path)

            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    if not os.path.isdir(dir_path):
                        continue
                    if os.listdir(dir_path):
                        continue
                    if not _is_old_enough(dir_path):
                        continue
                    os.rmdir(dir_path)
                    summary["removed_dirs"] += 1
                except Exception:
                    continue

    # Also clean only temp-like leftovers in logs directory.
    log_temp_suffixes = (".tmp", ".temp", ".part", ".partial", ".download", ".crdownload")
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    if os.path.isdir(logs_dir):
        try:
            for file_name in os.listdir(logs_dir):
                lower_name = file_name.lower()
                if not lower_name.endswith(log_temp_suffixes):
                    continue
                file_path = os.path.join(logs_dir, file_name)
                if not os.path.isfile(file_path):
                    continue
                summary["scanned_files"] += 1
                if _is_old_enough(file_path):
                    _remove_file(file_path)
        except Exception as exc:
            _record_error(f"Logs dizini temizlenemedi: {logs_dir} ({exc})")

    force_target_cleanup = max_age_seconds <= 0

    # Include persistent high-growth files in cleanup scope.
    # app.log is actively written, so truncate instead of delete.
    app_log_path = os.path.join(logs_dir, "app.log")
    if os.path.isfile(app_log_path):
        summary["scanned_files"] += 1
        if force_target_cleanup or _is_old_enough(app_log_path):
            _truncate_file(app_log_path)

    monitor_state_paths = []
    monitor_persist_path = getattr(monitor, "_persist_path", None)
    if monitor_persist_path:
        monitor_state_paths.append(monitor_persist_path)
    monitor_state_paths.append(os.path.join(ORG_BASE_DIR, "_monitor", "api_buckets.json"))

    seen_monitor_paths = set()
    for base_path in monitor_state_paths:
        if not base_path:
            continue
        normalized = os.path.abspath(base_path)
        if normalized in seen_monitor_paths:
            continue
        seen_monitor_paths.add(normalized)
        for candidate in (normalized, f"{normalized}.tmp"):
            if not os.path.isfile(candidate):
                continue
            summary["scanned_files"] += 1
            if force_target_cleanup or _is_old_enough(candidate):
                _remove_file(candidate)

    return summary


def _maybe_periodic_temp_cleanup(force=False, aggressive=False, max_age_hours=None):
    store = _shared_memory_store()
    now = pytime.time()
    with store["lock"]:
        last_ts = float(store.get("last_temp_cleanup_ts", 0) or 0)
        if not force and (now - last_ts) < TEMP_CLEANUP_INTERVAL_SEC:
            return None
        store["last_temp_cleanup_ts"] = now

    result = cleanup_temp_files(max_age_hours=max_age_hours, aggressive=aggressive)
    if (result.get("removed_files", 0) > 0) or (result.get("removed_dirs", 0) > 0):
        logger.info(
            "Temp cleanup completed: files=%s dirs=%s freed=%s",
            result.get("removed_files", 0),
            result.get("removed_dirs", 0),
            _format_bytes(result.get("freed_bytes", 0)),
        )
    if result.get("errors"):
        logger.warning("Temp cleanup finished with %s errors", len(result.get("errors", [])))
    return result


_configure_tempfile_dir()

def _org_path(org_code, create=False):
    safe_org = _safe_org_code(org_code)
    base = os.path.abspath(ORG_BASE_DIR)
    path = os.path.abspath(os.path.join(base, safe_org))
    if os.path.commonpath([base, path]) != base:
        raise ValueError(f"Invalid organization code: {org_code}")
    if create:
        os.makedirs(path, exist_ok=True)
    return path


def _org_dir(org_code):
    path = _org_path(org_code, create=True)
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

APP_SESSION_COOKIE = "app_session"
APP_SESSION_TTL = 7 * 24 * 3600
_legacy_app_session_cleaned = False

def _cleanup_legacy_app_session_file():
    global _legacy_app_session_cleaned
    if _legacy_app_session_cleaned:
        return
    try:
        legacy_path = ".session.enc"
        if os.path.exists(legacy_path):
            os.remove(legacy_path)
    except Exception:
        pass
    _legacy_app_session_cleaned = True

def _get_secret_key_path():
    base_dir = os.environ.get("GENESYS_STATE_DIR") or ORG_BASE_DIR
    try:
        os.makedirs(base_dir, exist_ok=True)
    except Exception:
        pass
    return os.path.join(base_dir, ".secret.key")

def _get_or_create_key():
    key_path = _get_secret_key_path()
    legacy_path = KEY_FILE
    if os.path.exists(key_path):
        with open(key_path, "rb") as f:
            return f.read()
    if os.path.exists(legacy_path):
        try:
            with open(legacy_path, "rb") as f:
                key = f.read()
            if key:
                try:
                    with open(key_path, "wb") as wf:
                        wf.write(key)
                    os.chmod(key_path, 0o600)
                except Exception:
                    pass
                return key
        except Exception:
            pass
    key = Fernet.generate_key()
    with open(key_path, "wb") as f:
        f.write(key)
    try:
        os.chmod(key_path, 0o600)
    except: pass
    return key

def _get_cipher():
    return Fernet(_get_or_create_key())

_cookie_manager = None

def _get_cookie_manager():
    global _cookie_manager
    if not Runtime.exists():
        _rm_debug("runtime not ready; cookie manager unavailable")
        return None
    if _cookie_manager is None:
        key = _get_or_create_key()
        try:
            key_str = key.decode("utf-8")
        except Exception:
            key_str = str(key)
        try:
            from streamlit_cookies_manager import EncryptedCookieManager
            _cookie_manager = EncryptedCookieManager(prefix="genesys", password=key_str)
            _rm_debug("cookie manager created")
        except RuntimeError:
            _rm_debug("cookie manager creation failed: runtime error")
            return None
        except Exception as e:
            _rm_debug("cookie manager creation failed: %s", e)
            return None
    try:
        if not _cookie_manager.ready():
            _rm_debug("cookie manager not ready yet")
            return None
    except Exception as e:
        _rm_debug("cookie manager ready check failed: %s", e)
        return None
    _rm_debug("cookie manager ready")
    return _cookie_manager

def _cookie_manager_initializing():
    global _cookie_manager
    if _cookie_manager is None:
        return False
    try:
        return not _cookie_manager.ready()
    except Exception:
        return False

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
    org_path = _org_path(org_code, create=False)
    try:
        if os.path.isdir(org_path):
            shutil.rmtree(org_path, ignore_errors=True)
    except Exception as e:
        logger.warning("Failed to delete org files for %s: %s", org_code, e)

def generate_password(length=12):
    """Generate a secure random password."""
    import secrets
    import string
    alphabet = string.ascii_letters + string.digits + "!@#$%"
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# --- APP SESSION MANAGEMENT (REMEMBER ME) ---
def load_app_session():
    try:
        _cleanup_legacy_app_session_file()
        session_data = _read_app_session_cookie()
        if not session_data:
            return None
        timestamp = session_data.get("timestamp", 0)
        if pytime.time() - timestamp > APP_SESSION_TTL:
            _rm_debug("load session: cookie expired")
            delete_app_session()
            return None
        _rm_debug(
            "load session: valid for user=%s org=%s",
            session_data.get("username"),
            session_data.get("org_code"),
        )
        return session_data
    except Exception as e:
        _rm_debug("load session failed: %s", e)
        return None

def _read_app_session_cookie(with_status=False):
    cookies = _get_cookie_manager()
    if cookies is None:
        _rm_debug("read cookie skipped: cookie manager missing")
        return ("manager_missing", None) if with_status else None
    raw = cookies.get(APP_SESSION_COOKIE)
    if not raw:
        _rm_debug("read cookie: not found")
        return ("not_found", None) if with_status else None
    _rm_debug("read cookie: found (%s chars)", len(str(raw)))
    try:
        data = json.loads(raw)
        return ("found", data) if with_status else data
    except Exception as e:
        _rm_debug("read cookie parse failed: %s", e)
        return ("invalid", None) if with_status else None

def _hydrate_app_user_from_saved_session(session_data):
    try:
        username = (session_data or {}).get("username")
        org_code = _safe_org_code((session_data or {}).get("org_code", "default"))
        if not username:
            _rm_debug("hydrate failed: missing username in saved session")
            return None
        org_users = auth_manager.get_all_users(org_code) or {}
        db_user = org_users.get(username)
        if not db_user:
            _rm_debug("hydrate failed: user not found in users file user=%s org=%s", username, org_code)
            return None
        hydrated = {
            "username": username,
            "org_code": org_code,
            "role": db_user.get("role", session_data.get("role", "Reports User")),
            "metrics": db_user.get("metrics", session_data.get("metrics", [])),
            "must_change_password": bool(db_user.get("must_change_password")),
        }
        _rm_debug("hydrate success for user=%s org=%s", username, org_code)
        return hydrated
    except Exception as e:
        _rm_debug("hydrate failed: %s", e)
        return None

def save_app_session(user_data):
    try:
        _cleanup_legacy_app_session_file()
        cookies = _get_cookie_manager()
        if cookies is None:
            _rm_debug(
                "save session failed: cookie manager missing for user=%s org=%s",
                user_data.get("username"),
                user_data.get("org_code"),
            )
            return False
        payload = {**user_data, "timestamp": pytime.time()}
        payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        _rm_debug("save session payload bytes=%s", len(payload_json.encode("utf-8")))
        cookies[APP_SESSION_COOKIE] = payload_json
        save_result = cookies.save()
        _rm_debug("cookie save() returned: %s", save_result)
        _rm_debug("save session success for user=%s org=%s", user_data.get("username"), user_data.get("org_code"))
        return True
    except Exception as e:
        _rm_debug("save session failed: %s", e)
        return False

def delete_app_session():
    _cleanup_legacy_app_session_file()
    try:
        st.session_state.pop("_remember_me_pending_payload", None)
        st.session_state.pop("_remember_me_pending_retries", None)
    except Exception:
        pass
    
    # Delete cookie
    try:
        cookies = _get_cookie_manager()
        if cookies is not None:
            # streamlit_cookies_manager can fail to remove reliably with only `del`;
            # write an empty tombstone value as fallback.
            try:
                if APP_SESSION_COOKIE in cookies:
                    del cookies[APP_SESSION_COOKIE]
            except Exception:
                pass
            cookies[APP_SESSION_COOKIE] = ""
            cookies.save()
            _rm_debug("delete session cookie removal requested")
        else:
            _rm_debug("delete session cookie skipped: cookie missing")
    except Exception as e:
        _rm_debug("delete session failed: %s", e)
        pass

def _flush_pending_remember_me_delete():
    if not st.session_state.get("_remember_me_pending_delete"):
        return True
    retries = int(st.session_state.get("_remember_me_pending_delete_retries", 0))
    cookie_status, existing = _read_app_session_cookie(with_status=True)
    if cookie_status == "manager_missing":
        retries += 1
        st.session_state["_remember_me_pending_delete_retries"] = retries
        _rm_debug("pending remember-me delete waiting for cookie manager retry=%s", retries)
        if retries <= 20:
            _safe_autorefresh(interval=700, key=f"remember_delete_retry_{retries}")
            st.info("Oturum çıkışı doğrulanıyor, lütfen bekleyin...")
            st.stop()
        _rm_debug("pending remember-me delete exceeded retries=%s while waiting manager", retries)
        return False
    if cookie_status == "not_found":
        st.session_state["_remember_me_pending_delete"] = False
        st.session_state["_remember_me_pending_delete_retries"] = 0
        _rm_debug("pending remember-me delete verified")
        return True
    try:
        cookies = _get_cookie_manager()
        if cookies is not None:
            try:
                if APP_SESSION_COOKIE in cookies:
                    del cookies[APP_SESSION_COOKIE]
            except Exception:
                pass
            cookies[APP_SESSION_COOKIE] = ""
            cookies.save()
            _rm_debug("pending remember-me delete attempt=%s save issued", retries + 1)
    except Exception as e:
        _rm_debug("pending remember-me delete failed: %s", e)
    retries += 1
    st.session_state["_remember_me_pending_delete_retries"] = retries
    if retries <= 20:
        _safe_autorefresh(interval=700, key=f"remember_delete_retry_{retries}")
        st.info("Oturum çıkışı doğrulanıyor, lütfen bekleyin...")
        st.stop()
    _rm_debug("pending remember-me delete exceeded retries=%s", retries)
    return False

def _flush_pending_remember_me():
    payload = st.session_state.get("_remember_me_pending_payload")
    if not payload:
        return True
    username = str(payload.get("username") or "").strip()
    try:
        org_code = _safe_org_code(payload.get("org_code", "default"))
    except ValueError:
        _rm_debug("pending remember-me dropped: invalid org_code in payload")
        st.session_state.pop("_remember_me_pending_payload", None)
        st.session_state.pop("_remember_me_pending_retries", None)
        st.session_state.remember_me_enabled = False
        st.session_state["_remember_me_pending_delete"] = True
        st.session_state["_remember_me_pending_delete_retries"] = 0
        return True

    if username:
        try:
            org_users = auth_manager.get_all_users(org_code) or {}
            db_user = org_users.get(username) or {}
            if db_user.get("must_change_password"):
                _rm_debug(
                    "pending remember-me dropped for user=%s org=%s due to must_change_password",
                    username,
                    org_code,
                )
                st.session_state.pop("_remember_me_pending_payload", None)
                st.session_state.pop("_remember_me_pending_retries", None)
                st.session_state.remember_me_enabled = False
                st.session_state["_remember_me_pending_delete"] = True
                st.session_state["_remember_me_pending_delete_retries"] = 0
                return True
        except Exception as e:
            _rm_debug("pending remember-me user lookup failed: %s", e)
    _rm_debug(
        "pending remember-me flush: retry=%s user=%s org=%s",
        st.session_state.get("_remember_me_pending_retries", 0),
        payload.get("username"),
        payload.get("org_code"),
    )
    existing = _read_app_session_cookie()
    if (
        existing
        and existing.get("username") == payload.get("username")
        and existing.get("org_code", "default") == payload.get("org_code", "default")
    ):
        st.session_state.pop("_remember_me_pending_payload", None)
        st.session_state.pop("_remember_me_pending_retries", None)
        _rm_debug("pending remember-me flush verified")
        return True
    retries = int(st.session_state.get("_remember_me_pending_retries", 0)) + 1
    st.session_state["_remember_me_pending_retries"] = retries
    save_ok = save_app_session(payload)
    _rm_debug("pending remember-me flush attempt=%s save_ok=%s", retries, save_ok)
    # Give cookie component time to initialize and persist.
    if retries <= 20:
        _safe_autorefresh(interval=700, key=f"remember_flush_retry_{retries}")
        st.info("Beni Hatırla kaydı doğrulanıyor, lütfen bekleyin...")
        st.stop()
    _rm_debug("pending remember-me flush failed after retries=%s", retries)
    return False

def _ensure_cookie_component_ready(max_retries=8):
    retries = int(st.session_state.get("_cookie_boot_retries", 0))
    cm = _get_cookie_manager()
    if cm is not None:
        st.session_state["_cookie_boot_retries"] = 0
        _rm_debug("cookie component ready at retry=%s", retries)
        return True
    if retries < max_retries:
        st.session_state["_cookie_boot_retries"] = retries + 1
        _rm_debug("cookie component boot retry=%s/%s", retries + 1, max_retries)
        _safe_autorefresh(interval=700, key=f"cookie_boot_retry_{retries}")
        st.info("Oturum bileşeni hazırlanıyor, lütfen bekleyin...")
        st.stop()
    _rm_debug("cookie component not ready after max retries=%s", max_retries)
    return False


@st.cache_resource(show_spinner=False)
def _login_attempt_store():
    return {"lock": threading.Lock(), "entries": {}}


def _login_failure_key(org_code, username):
    return f"{str(org_code or '').strip().lower()}::{str(username or '').strip().lower()}"


def _prune_login_failures_locked(entries, now):
    stale_keys = []
    for k, v in entries.items():
        lock_until = float((v or {}).get("lock_until", 0) or 0)
        first_ts = float((v or {}).get("first_ts", 0) or 0)
        if now > lock_until and now > first_ts + LOGIN_WINDOW_SECONDS:
            stale_keys.append(k)
    for k in stale_keys:
        entries.pop(k, None)

    max_entries = max(1000, int(LOGIN_ATTEMPT_MAX_ENTRIES))
    if len(entries) <= max_entries:
        return

    ranked_entries = []
    for k, v in entries.items():
        lock_until = float((v or {}).get("lock_until", 0) or 0)
        first_ts = float((v or {}).get("first_ts", 0) or 0)
        ranked_entries.append((max(lock_until, first_ts), k))
    ranked_entries.sort(reverse=True)
    for _, key in ranked_entries[max_entries:]:
        entries.pop(key, None)


def _register_login_failure(org_code, username):
    now = pytime.time()
    key = _login_failure_key(org_code, username)
    store = _login_attempt_store()
    with store["lock"]:
        _prune_login_failures_locked(store["entries"], now)
        entry = store["entries"].get(key, {"count": 0, "first_ts": now, "lock_until": 0})
        if now > float(entry.get("first_ts", 0)) + LOGIN_WINDOW_SECONDS:
            entry = {"count": 0, "first_ts": now, "lock_until": 0}
        entry["count"] = int(entry.get("count", 0)) + 1
        if entry["count"] >= max(1, LOGIN_MAX_FAILURES):
            entry["lock_until"] = now + max(1, LOGIN_LOCK_SECONDS)
        store["entries"][key] = entry
        _prune_login_failures_locked(store["entries"], now)
        return {
            "count": int(entry.get("count", 0)),
            "locked": now < float(entry.get("lock_until", 0)),
            "remaining": max(0, int(entry.get("lock_until", 0) - now)),
        }


def _clear_login_failures(org_code, username):
    key = _login_failure_key(org_code, username)
    store = _login_attempt_store()
    with store["lock"]:
        store["entries"].pop(key, None)


def _get_login_lock_state(org_code, username):
    now = pytime.time()
    key = _login_failure_key(org_code, username)
    store = _login_attempt_store()
    with store["lock"]:
        _prune_login_failures_locked(store["entries"], now)
        entry = store["entries"].get(key)
        if not entry:
            return {"locked": False, "remaining": 0}
        first_ts = float(entry.get("first_ts", 0) or 0)
        if now > first_ts + LOGIN_WINDOW_SECONDS and now >= float(entry.get("lock_until", 0) or 0):
            store["entries"].pop(key, None)
            return {"locked": False, "remaining": 0}
        lock_until = float(entry.get("lock_until", 0) or 0)
        if now < lock_until:
            return {"locked": True, "remaining": int(lock_until - now)}
        return {"locked": False, "remaining": 0}

def _user_dir(org_code, username):
    base = _org_dir(org_code)
    user_safe = (username or "unknown").replace("/", "_").replace("\\", "_")
    path = os.path.join(base, "users", user_safe)
    os.makedirs(path, exist_ok=True)
    return path

def _current_user():
    return st.session_state.app_user if st.session_state.get("app_user") else None


def _config_paths(org_code, username=None):
    if username:
        user_path = _user_dir(org_code, username)
        return (
            os.path.join(user_path, CONFIG_FILE),
            os.path.join(user_path, PRESETS_FILE),
        )
    org_path = _org_dir(org_code)
    return (
        os.path.join(org_path, CONFIG_FILE),
        os.path.join(org_path, PRESETS_FILE),
    )


def _config_mtime(path):
    try:
        return float(os.path.getmtime(path))
    except Exception:
        return 0.0


def _invalidate_configs_export_cache():
    try:
        st.session_state.pop("_configs_export_cache", None)
    except Exception:
        pass


def _resolve_utc_offset_hours(raw_offset, default=3.0):
    try:
        return float(raw_offset)
    except Exception:
        return float(default)


def _resolve_org_code_for_config(org_code=None):
    if org_code:
        return str(org_code)
    user = st.session_state.get("app_user") or {}
    return str(user.get("org_code", "default") or "default")


def _resolve_org_config(org_code=None, force_reload=False):
    target_org = _resolve_org_code_for_config(org_code)
    cfg_owner = st.session_state.get("_org_config_owner")
    cfg = st.session_state.get("org_config") or {}
    if force_reload or cfg_owner != target_org or not cfg:
        try:
            cfg = load_credentials(target_org) or {}
        except Exception:
            cfg = {}
        st.session_state.org_config = cfg
        st.session_state["_org_config_owner"] = target_org
    return cfg


def _resolve_org_utc_offset_hours(org_code=None, default=3.0, force_reload=False):
    cfg = _resolve_org_config(org_code=org_code, force_reload=force_reload)
    return _resolve_utc_offset_hours(cfg.get("utc_offset", default), default=default)


def _dashboard_interval_utc(mode, saved_creds, selected_date=None):
    offset_hours = _resolve_utc_offset_hours((saved_creds or {}).get("utc_offset", 3), default=3.0)
    org_tz = timezone(timedelta(hours=offset_hours))
    now_local = datetime.now(org_tz)
    mode_safe = str(mode or "Live")

    if mode_safe == "Yesterday":
        target_day = now_local.date() - timedelta(days=1)
        end_local = datetime.combine(target_day, time(23, 59, 59), tzinfo=org_tz)
    elif mode_safe == "Date":
        if isinstance(selected_date, datetime):
            target_day = selected_date.date()
        elif hasattr(selected_date, "year") and hasattr(selected_date, "month") and hasattr(selected_date, "day"):
            target_day = selected_date
        else:
            target_day = now_local.date()
        end_local = datetime.combine(target_day, time(23, 59, 59), tzinfo=org_tz)
        if target_day == now_local.date() and end_local > now_local:
            end_local = now_local
    else:
        target_day = now_local.date()
        end_local = now_local

    start_local = datetime.combine(target_day, time(0, 0), tzinfo=org_tz)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _resolve_card_queue_names(card_queues, queues_map):
    resolved = []
    missing = []
    if not isinstance(queues_map, dict):
        return resolved, missing
    normalized_name_map = {str(name).strip().lower(): name for name in queues_map.keys()}

    for raw_name in (card_queues or []):
        clean_name = str(raw_name or "").replace("(not loaded)", "").strip()
        if not clean_name:
            continue
        canonical = clean_name if clean_name in queues_map else normalized_name_map.get(clean_name.lower())
        if canonical:
            if canonical not in resolved:
                resolved.append(canonical)
        elif clean_name not in missing:
            missing.append(clean_name)
    return resolved, missing


def _normalize_dashboard_cards(cards):
    """Normalize dashboard card config to prevent render/key errors."""
    if not isinstance(cards, list):
        cards = []

    normalized = []
    seen_ids = set()

    for raw in cards:
        card = raw if isinstance(raw, dict) else {}

        raw_id = card.get("id", None)
        try:
            card_id = int(raw_id)
        except Exception:
            card_id = None

        if (card_id is None) or (card_id in seen_ids):
            card_id = max(seen_ids) + 1 if seen_ids else 0
        seen_ids.add(card_id)

        queues = card.get("queues", [])
        if not isinstance(queues, list):
            queues = []
        media_types = card.get("media_types", [])
        if not isinstance(media_types, list):
            media_types = []
        live_metrics = card.get("live_metrics", ["Waiting", "Interacting", "On Queue"])
        if not isinstance(live_metrics, list):
            live_metrics = ["Waiting", "Interacting", "On Queue"]
        daily_metrics = card.get("daily_metrics", ["Offered", "Answered", "Abandoned", "Answer Rate"])
        if not isinstance(daily_metrics, list):
            daily_metrics = ["Offered", "Answered", "Abandoned", "Answer Rate"]
        visual_metrics = card.get("visual_metrics", ["Service Level"])
        if not isinstance(visual_metrics, list):
            visual_metrics = ["Service Level"]

        normalized.append({
            "id": card_id,
            "title": str(card.get("title", "") or ""),
            "queues": [str(q) for q in queues if q is not None],
            "size": card.get("size", "medium") if card.get("size") in ["xsmall", "small", "medium", "large"] else "medium",
            "media_types": [str(m) for m in media_types if m is not None],
            "live_metrics": [str(m) for m in live_metrics if m is not None],
            "daily_metrics": [str(m) for m in daily_metrics if m is not None],
            "visual_metrics": [str(m) for m in visual_metrics if m is not None],
        })

    return normalized

def load_dashboard_config(org_code):
    user = _current_user()
    if user:
        filename, _ = _config_paths(org_code, user.get("username"))
    else:
        filename, _ = _config_paths(org_code, None)
    if not os.path.exists(filename): return {"layout": 1, "cards": []}
    try:
        with open(filename, "r", encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"layout": 1, "cards": []}
        try:
            layout = int(data.get("layout", 1) or 1)
        except Exception:
            layout = 1
        layout = max(1, min(4, layout))
        cards = _normalize_dashboard_cards(data.get("cards", []))
        return {"layout": layout, "cards": cards}
    except Exception as e:
        logger.warning("Failed to load dashboard config (%s): %s", filename, e)
        return {"layout": 1, "cards": []}

def save_dashboard_config(org_code, layout, cards):
    user = _current_user()
    if user:
        filename, _ = _config_paths(org_code, user.get("username"))
    else:
        filename, _ = _config_paths(org_code, None)
    try:
        try:
            safe_layout = int(layout or 1)
        except Exception:
            safe_layout = 1
        safe_layout = max(1, min(4, safe_layout))
        safe_cards = _normalize_dashboard_cards(cards)
        with open(filename, "w", encoding='utf-8') as f:
            json.dump({"layout": safe_layout, "cards": safe_cards}, f, ensure_ascii=False)
        _invalidate_configs_export_cache()
    except Exception as e:
        logger.warning("Failed to save dashboard config (%s): %s", filename, e)

def load_presets(org_code):
    user = _current_user()
    if user:
        _, filename = _config_paths(org_code, user.get("username"))
    else:
        _, filename = _config_paths(org_code, None)
    if not os.path.exists(filename): return []
    try:
        with open(filename, "r", encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning("Failed to load presets (%s): %s", filename, e)
        return []

def save_presets(org_code, presets):
    user = _current_user()
    if user:
        _, filename = _config_paths(org_code, user.get("username"))
    else:
        _, filename = _config_paths(org_code, None)
    try:
        with open(filename, "w", encoding='utf-8') as f: json.dump(presets, f, ensure_ascii=False)
        _invalidate_configs_export_cache()
    except Exception as e:
        logger.warning("Failed to save presets (%s): %s", filename, e)

def get_all_configs_json():
    user = _current_user()
    org = user.get("org_code", "default") if user else "default"
    username = user.get("username") if user else None
    config_path, presets_path = _config_paths(org, username)
    config_mtime = _config_mtime(config_path)
    presets_mtime = _config_mtime(presets_path)

    cache = st.session_state.get("_configs_export_cache")
    if (
        isinstance(cache, dict)
        and cache.get("org") == org
        and cache.get("username") == (username or "")
        and cache.get("config_mtime") == config_mtime
        and cache.get("presets_mtime") == presets_mtime
    ):
        return cache.get("payload", "{}")

    payload = json.dumps(
        {"dashboard": load_dashboard_config(org), "report_presets": load_presets(org)},
        indent=2,
    )
    st.session_state["_configs_export_cache"] = {
        "org": org,
        "username": username or "",
        "config_mtime": config_mtime,
        "presets_mtime": presets_mtime,
        "payload": payload,
    }
    return payload

def import_all_configs(json_data):
    try:
        data = json.loads(json_data)
        org = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
        if "dashboard" in data:
            save_dashboard_config(org, data["dashboard"].get("layout", 1), data["dashboard"].get("cards", []))
        if "report_presets" in data:
            save_presets(org, data["report_presets"])
        _invalidate_configs_export_cache()
        return True
    except Exception as e:
        logger.warning("Failed to import all configs: %s", e)
        return False

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
    return {
        "lock": threading.Lock(),
        "samples": [],
        "thread": None,
        "stop_event": threading.Event(),
        "last_cleanup_ts": 0,
        "last_temp_cleanup_ts": 0,
        "restart_in_progress": False,
    }

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
    # Use a dedicated non-zero code for intentional restart requests.
    os._exit(RESTART_EXIT_CODE)

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
                org["call_meta_attempt_ts"] = {}
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
        _maybe_periodic_temp_cleanup()
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
        "call_meta_attempt_ts": {},
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
    org.setdefault("call_meta_attempt_ts", {})
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


def _reserve_call_meta_targets(
    org_code,
    candidate_ids,
    now_ts,
    min_interval=60,
    cooldown_seconds=120,
    max_items=1,
):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        last_poll_ts = float(org.get("call_meta_poll_ts", 0) or 0)
        if (now_ts - last_poll_ts) < max(5, int(min_interval or 60)):
            return []

        attempts = org.setdefault("call_meta_attempt_ts", {})
        cooldown = max(10, int(cooldown_seconds or 120))
        max_targets = max(1, int(max_items or 1))
        prune_before = now_ts - max(cooldown * 10, 1800)
        stale_attempts = [cid for cid, ts in attempts.items() if float(ts or 0) < prune_before]
        for cid in stale_attempts:
            attempts.pop(cid, None)

        selected = []
        for raw_cid in candidate_ids or []:
            cid = str(raw_cid or "").strip()
            if not cid:
                continue
            last_attempt_ts = float(attempts.get(cid, 0) or 0)
            if (now_ts - last_attempt_ts) < cooldown:
                continue
            selected.append(cid)
            if len(selected) >= max_targets:
                break

        if not selected:
            return []

        org["call_meta_poll_ts"] = now_ts
        for cid in selected:
            attempts[cid] = now_ts
        return selected

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

def _rollback_agent_seed(org_code, reserved_ts, fallback_ts=0):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        if org.get("agent_seed_ts", 0) == reserved_ts:
            org["agent_seed_ts"] = fallback_ts or 0

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

def _resolve_refresh_interval_seconds(org_code=None, minimum=3, default=15):
    cfg = _resolve_org_config(org_code=org_code, force_reload=False)
    try:
        refresh_s = int(cfg.get("refresh_interval", default) or default)
    except Exception:
        refresh_s = default
    return max(minimum, refresh_s)


def _dashboard_dm_signature(org_code):
    queues_map = st.session_state.get("queues_map") or {}
    cards = st.session_state.get("dashboard_cards") or []
    card_targets = []
    for card in cards:
        resolved, _ = _resolve_card_queue_names(card.get("queues", []), queues_map)
        card_targets.append(tuple(resolved))
    return (
        str(org_code or "default"),
        tuple(card_targets),
        bool(st.session_state.get("use_agent_notifications", True)),
        bool(st.session_state.get("api_client")),
    )


def _dashboard_profile_state():
    state = st.session_state.get("_dashboard_profile")
    if not isinstance(state, dict):
        state = {
            "enabled": False,
            "started_ts": 0.0,
            "duration_s": 180,
            "runs": 0,
            "samples": {},
        }
        st.session_state["_dashboard_profile"] = state
    state.setdefault("enabled", False)
    state.setdefault("started_ts", 0.0)
    state.setdefault("duration_s", 180)
    state.setdefault("runs", 0)
    if not isinstance(state.get("samples"), dict):
        state["samples"] = {}
    return state


def _dashboard_profile_start(duration_s=180):
    state = _dashboard_profile_state()
    try:
        duration = int(duration_s)
    except Exception:
        duration = 180
    duration = max(60, min(900, duration))
    state["enabled"] = True
    state["started_ts"] = pytime.time()
    state["duration_s"] = duration
    state["runs"] = 0
    state["samples"] = {}


def _dashboard_profile_stop():
    state = _dashboard_profile_state()
    state["enabled"] = False


def _dashboard_profile_clear(duration_s=None):
    state = _dashboard_profile_state()
    if duration_s is not None:
        try:
            state["duration_s"] = max(60, min(900, int(duration_s)))
        except Exception:
            pass
    state["runs"] = 0
    state["samples"] = {}
    state["started_ts"] = 0.0
    state["enabled"] = False


def _dashboard_profile_tick():
    if not ENABLE_DASHBOARD_PROFILING:
        return
    state = _dashboard_profile_state()
    if not state.get("enabled"):
        return
    started_ts = float(state.get("started_ts", 0) or 0)
    duration_s = max(60, int(state.get("duration_s", 180) or 180))
    if started_ts and (pytime.time() - started_ts) >= duration_s:
        state["enabled"] = False


def _dashboard_profile_active():
    if not ENABLE_DASHBOARD_PROFILING:
        return False
    return bool(_dashboard_profile_state().get("enabled"))


def _dashboard_profile_record(block_name, elapsed_seconds):
    if not _dashboard_profile_active():
        return
    try:
        elapsed_ms = max(0.0, float(elapsed_seconds) * 1000.0)
    except Exception:
        return
    state = _dashboard_profile_state()
    samples = state.setdefault("samples", {})
    key = str(block_name or "").strip()
    if not key:
        return
    bucket = samples.setdefault(key, [])
    bucket.append(round(elapsed_ms, 3))
    if len(bucket) > 3000:
        samples[key] = bucket[-2000:]


def _dashboard_profile_commit_run():
    if _dashboard_profile_active():
        state = _dashboard_profile_state()
        state["runs"] = int(state.get("runs", 0) or 0) + 1


def _dashboard_profile_rows(limit=15):
    if not ENABLE_DASHBOARD_PROFILING:
        return []
    state = _dashboard_profile_state()
    rows = []
    all_samples = state.get("samples", {}) or {}
    for block_name, values in all_samples.items():
        vals = [float(v) for v in values if isinstance(v, (int, float))]
        if not vals:
            continue
        total_ms = float(sum(vals))
        calls = len(vals)
        avg_ms = total_ms / calls if calls else 0.0
        p95_ms = float(np.percentile(vals, 95)) if calls > 1 else avg_ms
        max_ms = float(max(vals))
        rows.append(
            {
                "Blok": block_name,
                "Çağrı": calls,
                "Avg (ms)": round(avg_ms, 2),
                "P95 (ms)": round(p95_ms, 2),
                "Max (ms)": round(max_ms, 2),
                "Toplam (ms)": round(total_ms, 2),
            }
        )
    rows.sort(key=lambda r: r.get("Toplam (ms)", 0), reverse=True)
    return rows[:max(1, int(limit))]

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

def _clear_page_transient_state():
    """Clear page-scoped transient UI/data to prevent cross-page bleed."""
    try:
        transient_prefixes = (
            "_report_result_",
            "dl_payload_",
            "dl_fmt_",
            "dl_prepare_",
        )
        for key in list(st.session_state.keys()):
            k = str(key)
            if any(k.startswith(prefix) for prefix in transient_prefixes):
                st.session_state.pop(key, None)
        for key in [
            "_agent_panel_last_by_filter",
            "_call_panel_last_by_filter",
        ]:
            st.session_state.pop(key, None)
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
        for card in st.session_state.dashboard_cards:
            q_list, _ = _resolve_card_queue_names(card.get('queues', []), st.session_state.queues_map)
            if q_list:
                # 1. Total Metrics Queues
                for q_name in q_list:
                    q_id = st.session_state.queues_map.get(q_name)
                    if q_id:
                        all_dashboard_queues[q_name] = q_id
                        # 2. Agent details now track all selected queues (capped)
                        if len(agent_queues_map) < 50 and q_name not in agent_queues_map:
                            agent_queues_map[q_name] = q_id
    
    # Register this session's queues and compute union across active sessions
    union_queues, union_agent_queues, sess_count = _register_org_session_queues(org_code, all_dashboard_queues, agent_queues_map)
    
    # (debug log removed for build)
    
    # We use all_dashboard_queues for overall metrics (efficiency)
    # Pass empty dicts ({}) if empty, do NOT fall back to 'None' or full map
    st.session_state.data_manager.update_api_client(st.session_state.api_client, st.session_state.get('presence_map'))
    utc_offset = _resolve_org_utc_offset_hours(org_code=org_code, default=3.0, force_reload=False)
    refresh_s = _resolve_refresh_interval_seconds(org_code, minimum=1, default=15)
    st.session_state.data_manager.update_settings(utc_offset, refresh_s)
    dm_agent_queues = {} if use_agent_notif else union_agent_queues
    st.session_state.data_manager.start(union_queues, dm_agent_queues)

def recover_org_maps_if_needed(org_code, force=False):
    """Best-effort map recovery for dashboard editors (queues/users/presence)."""
    if not st.session_state.get('api_client'):
        return False
    if not force and st.session_state.get('queues_map'):
        return True
    now_ts = pytime.time()
    last_try = float(st.session_state.get("_maps_recover_ts", 0) or 0)
    if not force and (now_ts - last_try) < 15:
        return bool(st.session_state.get('queues_map'))
    st.session_state["_maps_recover_ts"] = now_ts
    try:
        api = GenesysAPI(st.session_state.api_client)
        maps = get_shared_org_maps(org_code, api, ttl_seconds=300, force_refresh=force)
        st.session_state.users_map = maps.get("users_map", st.session_state.get("users_map", {}))
        st.session_state.users_info = maps.get("users_info", st.session_state.get("users_info", {}))
        if st.session_state.users_info:
            st.session_state._users_info_last = dict(st.session_state.users_info)
        st.session_state.queues_map = maps.get("queues_map", st.session_state.get("queues_map", {}))
        st.session_state.wrapup_map = maps.get("wrapup", st.session_state.get("wrapup_map", {}))
        st.session_state.presence_map = maps.get("presence", st.session_state.get("presence_map", {}))
        return bool(st.session_state.get("queues_map"))
    except Exception:
        return False

def create_gauge_chart(value, title, height=250):
    try:
        if value is None or not np.isfinite(float(value)):
            value = 0
    except Exception:
        value = 0
    value = float(value)
    title_size = min(18, max(11, int(height * 0.12)))
    fig = go.Figure(go.Indicator(
        mode="gauge",
        value=value,
        title={"text": title, "font": {"size": title_size, "color": "#475569"}},
        domain={"x": [0, 1], "y": [0, 0.82]},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": "#00AEC7"},
            "steps": [
                {"range": [0, 50], "color": "#ffebee"},
                {"range": [50, 80], "color": "#fff3e0"},
                {"range": [80, 100], "color": "#e8f5e9"},
            ],
        },
    ))
    # Keep gauge dimensions and alignment stable across cards.
    fig.update_layout(
        height=height,
        margin=dict(l=2, r=2, t=32, b=4),
        autosize=True,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    # Center value: +50% size and bold.
    num_size = min(30, max(16, int(height * 0.15)))
    fig.add_annotation(
        x=0.5,
        y=0.09,
        text=f"<b>{value:.0f}</b>",
        showarrow=False,
        font=dict(size=num_size, color="#334155"),
        align="center",
    )
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

def _format_24h_time_labels(series, include_seconds=False, label_mode="time"):
    ts = pd.to_datetime(series, errors="coerce")
    if ts.isna().all():
        return ts
    mode = str(label_mode or "time").lower()
    if mode == "date":
        fmt = "%Y-%m-%d"
    elif mode == "datetime":
        fmt = "%Y-%m-%d %H:%M:%S" if include_seconds else "%Y-%m-%d %H:%M"
    else:
        fmt = "%H:%M:%S" if include_seconds else "%H:%M"
    return ts.dt.strftime(fmt)

def _dedupe_time_labels_keep_visual(labels):
    """Make duplicate time labels unique using zero-width suffixes (visually unchanged)."""
    try:
        seen = {}
        out = []
        for raw_label in labels:
            label = str(raw_label)
            idx = seen.get(label, 0)
            seen[label] = idx + 1
            if idx <= 0:
                out.append(label)
            else:
                out.append(label + ("\u200b" * idx))
        return out
    except Exception:
        return labels

def render_24h_time_line_chart(
    df,
    time_col,
    value_cols,
    include_seconds=False,
    aggregate_by_label=None,
    label_mode="time",
    x_index_name=None,
):
    try:
        if df is None or df.empty or time_col not in df.columns:
            return
        chart_df = df.copy()
        chart_df[time_col] = pd.to_datetime(chart_df[time_col], errors="coerce")
        chart_df = chart_df.dropna(subset=[time_col])
        if chart_df.empty:
            return
        value_cols = [value_cols] if isinstance(value_cols, str) else list(value_cols or [])
        value_cols = [c for c in value_cols if c in chart_df.columns]
        if not value_cols:
            return
        chart_df = sanitize_numeric_df(chart_df)
        chart_df = chart_df.sort_values(time_col)
        is_multi_day = chart_df[time_col].dt.normalize().nunique() > 1
        label_col = "_x_label"
        chart_df[label_col] = _format_24h_time_labels(
            chart_df[time_col],
            include_seconds=include_seconds,
            label_mode=label_mode,
        )
        chart_df = chart_df.dropna(subset=[label_col])
        if chart_df.empty:
            return
        # For multi-day ranges, grouping by HH:MM collapses all days into one point.
        # Keep per-timestamp series and only dedupe labels invisibly.
        if aggregate_by_label in ("sum", "mean", "last"):
            if is_multi_day:
                if aggregate_by_label == "sum":
                    chart_df = chart_df.groupby(time_col, as_index=False)[value_cols].sum()
                elif aggregate_by_label == "mean":
                    chart_df = chart_df.groupby(time_col, as_index=False)[value_cols].mean()
                elif aggregate_by_label == "last":
                    chart_df = chart_df.groupby(time_col, as_index=False)[value_cols].last()
                chart_df = chart_df.sort_values(time_col)
                chart_df[label_col] = _format_24h_time_labels(
                    chart_df[time_col],
                    include_seconds=include_seconds,
                    label_mode=label_mode,
                )
            else:
                if aggregate_by_label == "sum":
                    chart_df = chart_df.groupby(label_col, as_index=False)[value_cols].sum()
                elif aggregate_by_label == "mean":
                    chart_df = chart_df.groupby(label_col, as_index=False)[value_cols].mean()
                elif aggregate_by_label == "last":
                    chart_df = chart_df.groupby(label_col, as_index=False)[value_cols].last()
        chart_df[label_col] = _dedupe_time_labels_keep_visual(chart_df[label_col].tolist())
        plot_df = chart_df.set_index(label_col)[value_cols]
        if x_index_name:
            plot_df.index.name = str(x_index_name)
        st.line_chart(plot_df)
    except Exception as e:
        monitor.log_error("RENDER", f"Line chart render failed ({time_col})", str(e))

def _apply_report_row_limit(df, label="Rapor"):
    if df is None or df.empty:
        return df
    try:
        max_rows = int(st.session_state.get("rep_auto_row_limit", 50000) or 0)
    except Exception:
        max_rows = 50000
    if max_rows > 0 and len(df) > max_rows:
        st.warning(
            f"{label}: {len(df)} satır bulundu. Otomatik limit nedeniyle ilk {max_rows} satır gösterilip indirilecektir. "
            "Tam rapor için 'Maksimum satır (0=limitsiz)' değerini artırın veya 0 yapın."
        )
        return df.head(max_rows).copy()
    return df

def _safe_state_token(value):
    return "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in str(value or ""))

def _report_result_state_key(report_key):
    return f"_report_result_{_safe_state_token(report_key)}"

def _store_report_result(report_key, df, base_name):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return
    st.session_state[_report_result_state_key(report_key)] = {
        "df": df.copy(),
        "base_name": str(base_name or report_key),
        "rows": int(len(df)),
        "updated_at": pytime.time(),
    }

def _get_report_result(report_key):
    payload = st.session_state.get(_report_result_state_key(report_key))
    if not isinstance(payload, dict):
        return None
    df = payload.get("df")
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    return payload

def _clear_report_result(report_key):
    st.session_state.pop(_report_result_state_key(report_key), None)

def _download_df_signature(df):
    if df is None or not isinstance(df, pd.DataFrame):
        return "none"
    try:
        row_count = int(len(df))
        col_count = int(len(df.columns))
        if row_count == 0:
            return f"rows:0|cols:{col_count}"
        sample_size = min(5, row_count)
        sample_df = pd.concat([df.head(sample_size), df.tail(sample_size)], ignore_index=True)
        sample_json = sample_df.astype(str).to_json(orient="split", date_format="iso")
        digest = hashlib.sha1(sample_json.encode("utf-8", errors="ignore")).hexdigest()[:16]
        return f"rows:{row_count}|cols:{col_count}|sig:{digest}"
    except Exception:
        try:
            return f"rows:{len(df)}|cols:{len(df.columns)}"
        except Exception:
            return "unknown"

def render_downloads(df, base_name, key_base=None):
    fmt_options = {
        "CSV": {"ext": "csv", "mime": "text/csv", "builder": to_csv},
        "Excel": {"ext": "xlsx", "mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "builder": to_excel},
        "Parquet": {"ext": "parquet", "mime": "application/octet-stream", "builder": to_parquet},
        "PDF": {"ext": "pdf", "mime": "application/pdf", "builder": lambda d: to_pdf(d, title=base_name)},
    }
    key_token = _safe_state_token(key_base or base_name)
    safe_key = key_token or "report"
    fmt_key = f"dl_fmt_{safe_key}"
    prep_key = f"dl_prepare_{safe_key}"
    payload_key = f"dl_payload_{safe_key}"
    current_sig = _download_df_signature(df)

    st.caption("İndirme formatı")
    c1, c2, c3 = st.columns([2, 1, 2], gap="small")
    with c1:
        selected_fmt = st.selectbox(
            "İndirme formatı",
            list(fmt_options.keys()),
            key=fmt_key,
            label_visibility="collapsed",
        )
    with c2:
        prepare_clicked = st.button("Hazırla", key=prep_key, width='stretch')
    if prepare_clicked:
        opt = fmt_options[selected_fmt]
        with st.spinner(f"{selected_fmt} hazırlanıyor..."):
            payload = opt["builder"](df)
        st.session_state[payload_key] = {
            "format": selected_fmt,
            "ext": opt["ext"],
            "mime": opt["mime"],
            "data": payload,
            "rows": len(df),
            "sig": current_sig,
            "prepared_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
        }
    with c3:
        payload = st.session_state.get(payload_key) or {}
        is_ready = bool(payload and payload.get("format") == selected_fmt and payload.get("sig") == current_sig)
        prepared_at = (payload.get("prepared_at") if is_ready else datetime.now().strftime("%Y%m%d_%H%M%S"))
        ext = payload.get("ext") if is_ready else fmt_options.get(selected_fmt, {}).get("ext", "bin")
        mime = payload.get("mime") if is_ready else fmt_options.get(selected_fmt, {}).get("mime", "application/octet-stream")
        data = payload.get("data") if is_ready else b""
        st.download_button(
            f"{selected_fmt} indir",
            data=data,
            file_name=f"{base_name}_{prepared_at}.{ext}",
            mime=mime,
            width='stretch',
            disabled=not is_ready,
        )
    if not is_ready:
        st.caption("İndirme için önce format seçip 'Hazırla' butonuna basın.")
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
    if 'dashboard_auto_refresh' not in st.session_state: st.session_state.dashboard_auto_refresh = True
    if '_dashboard_dm_sig' not in st.session_state: st.session_state._dashboard_dm_sig = None
    if 'notifications_manager' not in st.session_state: st.session_state.notifications_manager = None
    if 'agent_notifications_manager' not in st.session_state: st.session_state.agent_notifications_manager = None
    if 'use_agent_notifications' not in st.session_state: st.session_state.use_agent_notifications = True
    if 'logged_in' not in st.session_state: st.session_state.logged_in = False
    if 'last_console_log_count' not in st.session_state: st.session_state.last_console_log_count = 0 # Track logged errors
    if '_remember_me_pending_payload' not in st.session_state: st.session_state._remember_me_pending_payload = None
    if '_remember_me_pending_retries' not in st.session_state: st.session_state._remember_me_pending_retries = 0
    if '_remember_me_pending_delete' not in st.session_state: st.session_state._remember_me_pending_delete = False
    if '_remember_me_pending_delete_retries' not in st.session_state: st.session_state._remember_me_pending_delete_retries = 0
    if '_cookie_boot_retries' not in st.session_state: st.session_state._cookie_boot_retries = 0
    if 'remember_me_enabled' not in st.session_state: st.session_state.remember_me_enabled = False
    if 'rep_auto_row_limit' not in st.session_state: st.session_state.rep_auto_row_limit = 50000

def log_to_console(message, level='error'):
    """Injects JavaScript to log to browser console."""
    safe_level = str(level or "log").lower()
    if safe_level not in {"log", "info", "warn", "error", "debug"}:
        safe_level = "log"
    safe_message = json.dumps(f"☁️ GenesysApp: {str(message)}", ensure_ascii=False)
    js_code = f"""
    <script>
        console.{safe_level}({safe_message});
    </script>
    """
    st.markdown(js_code, unsafe_allow_html=True)

init_session_state()
_maybe_periodic_temp_cleanup()
if not _flush_pending_remember_me_delete():
    st.stop()
if not _flush_pending_remember_me() and _cookie_manager_initializing():
    st.info("Oturum bileşeni hazırlanıyor, lütfen bekleyin...")
    st.stop()

# Ensure shared DataManager is available after login
if st.session_state.app_user and 'data_manager' not in st.session_state:
    ensure_data_manager()
data_manager = st.session_state.get('data_manager')

# Dashboard Config and Credentials will be loaded after login
if st.session_state.app_user and 'dashboard_config_loaded' not in st.session_state:
    st.session_state.dashboard_config_loaded = False

if st.session_state.app_user:
    org = st.session_state.app_user.get('org_code', 'default')
    owner_key = f"{org}:{st.session_state.app_user.get('username', '')}"
    if (not st.session_state.get('dashboard_config_loaded')) or (st.session_state.get('_dashboard_config_owner') != owner_key):
        config = load_dashboard_config(org)
        st.session_state.dashboard_layout = config.get("layout", 1)
        st.session_state.dashboard_cards = _normalize_dashboard_cards(
            config.get("cards", [{"id": 0, "title": "", "queues": [], "size": "medium"}])
        )
        st.session_state.dashboard_config_loaded = True
        st.session_state._dashboard_config_owner = owner_key
    else:
        st.session_state.dashboard_cards = _normalize_dashboard_cards(
            st.session_state.get("dashboard_cards", [{"id": 0, "title": "", "queues": [], "size": "medium"}])
        )

# --- APP LOGIN ---
if not st.session_state.app_user:
    _ensure_cookie_component_ready(max_retries=8)
    # Try Auto-Login from Encrypted Session File
    saved_session = load_app_session()
    if saved_session:
        hydrated_user = _hydrate_app_user_from_saved_session(saved_session)
        if not hydrated_user:
            _rm_debug("auto-login rejected: saved session could not be hydrated, deleting cookie")
            delete_app_session()
        else:
            _rm_debug(
                "auto-login success from cookie for user=%s org=%s",
                hydrated_user.get("username"),
                hydrated_user.get("org_code"),
            )
            st.session_state.app_user = hydrated_user
            st.session_state.remember_me_enabled = True
            ensure_data_manager()
            st.rerun()
    _rm_debug("auto-login did not find valid cookie session")

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
                u_name_clean = str(u_name or "").strip()
                try:
                    u_org_safe = _safe_org_code(u_org)
                except ValueError as exc:
                    st.error(str(exc))
                    u_org_safe = None

                if u_org_safe and u_name_clean:
                    lock_state = _get_login_lock_state(u_org_safe, u_name_clean)
                    if lock_state.get("locked"):
                        remaining = int(lock_state.get("remaining", 0))
                        logger.warning("app login blocked user=%s org=%s remaining=%ss", u_name_clean, u_org_safe, remaining)
                        st.error(f"Çok fazla başarısız deneme. {remaining} saniye sonra tekrar deneyin.")
                    else:
                        user_data = auth_manager.authenticate(u_org_safe, u_name_clean, u_pass)
                        if user_data:
                            _clear_login_failures(u_org_safe, u_name_clean)
                            logger.info("app login success user=%s org=%s", u_name_clean, u_org_safe)
                            # Exclude password hash from session state and file
                            safe_user_data = {k: v for k, v in user_data.items() if k != 'password'}
                            full_user = {"username": u_name_clean, **safe_user_data}
                            
                            # Handle Remember Me
                            if remember_me and full_user.get("must_change_password"):
                                _rm_debug(
                                    "remember-me skipped on login for user=%s org=%s due to must_change_password",
                                    u_name_clean,
                                    u_org_safe,
                                )
                                st.session_state.remember_me_enabled = False
                                st.session_state._remember_me_pending_payload = None
                                st.session_state._remember_me_pending_retries = 0
                                st.session_state._remember_me_pending_delete = True
                                st.session_state._remember_me_pending_delete_retries = 0
                                delete_app_session()
                            elif remember_me:
                                _rm_debug("login submit with remember-me checked for user=%s org=%s", u_name_clean, u_org_safe)
                                remember_payload = {
                                    "username": u_name_clean,
                                    "org_code": full_user.get("org_code", u_org_safe),
                                }
                                st.session_state.remember_me_enabled = True
                                st.session_state._remember_me_pending_payload = remember_payload
                                st.session_state._remember_me_pending_retries = 0
                                _rm_debug("remember-me queued for verified write")
                            else:
                                _rm_debug("login submit with remember-me OFF for user=%s org=%s", u_name_clean, u_org_safe)
                                st.session_state.remember_me_enabled = False
                                st.session_state._remember_me_pending_delete = True
                                st.session_state._remember_me_pending_delete_retries = 0
                                delete_app_session()
                                
                            st.session_state.app_user = full_user
                            st.session_state.genesys_logged_out = False
                            st.session_state.dashboard_config_loaded = False
                            st.session_state.pop("_dashboard_config_owner", None)
                            ensure_data_manager()
                            st.rerun()
                        else:
                            lock_info = _register_login_failure(u_org_safe, u_name_clean)
                            logger.warning(
                                "app login failed user=%s org=%s attempts=%s locked=%s",
                                u_name_clean,
                                u_org_safe,
                                lock_info.get("count"),
                                lock_info.get("locked"),
                            )
                            if lock_info.get("locked"):
                                st.error(f"Çok fazla başarısız deneme. {lock_info.get('remaining', 0)} saniye sonra tekrar deneyin.")
                            else:
                                st.error("Hatalı organizasyon, kullanıcı adı veya şifre!")
                elif u_org_safe:
                    st.error("Kullanıcı adı gereklidir.")
    st.stop()

# --- FORCED PASSWORD CHANGE (BOOTSTRAP) ---
if st.session_state.get("app_user") and st.session_state.app_user.get("must_change_password"):
    org = st.session_state.app_user.get("org_code", "default")
    username = st.session_state.app_user.get("username", "")
    st.title("🔐 İlk Giriş Güvenlik Adımı")
    st.warning("Bu hesap için başlangıç şifresi kullanılıyor. Devam etmek için yeni şifre belirleyin.")
    with st.form("force_password_change_form"):
        new_pw = st.text_input("Yeni Şifre", type="password")
        new_pw2 = st.text_input("Yeni Şifre (Tekrar)", type="password")
        if st.form_submit_button("Şifreyi Güncelle", width='stretch'):
            if not new_pw or not new_pw2:
                st.error("Lütfen iki alanı da doldurun.")
            elif new_pw != new_pw2:
                st.error("Şifreler eşleşmiyor.")
            elif len(new_pw) < 8:
                st.error("Şifre en az 8 karakter olmalıdır.")
            else:
                ok, msg = auth_manager.reset_password(org, username, new_pw)
                if ok:
                    st.session_state.app_user["must_change_password"] = False
                    st.success("Şifre güncellendi. Lütfen tekrar giriş yapın.")
                    st.session_state.app_user = None
                    st.session_state.api_client = None
                    st.session_state.logged_in = False
                    st.session_state.genesys_logged_out = False
                    st.rerun()
                else:
                    st.error(msg)
    st.stop()

# --- AUTO-LOGIN GENESYS ---
if st.session_state.app_user:
    org = st.session_state.app_user.get('org_code', 'default')
    saved_creds = _resolve_org_config(org_code=org, force_reload=True)
    utc_offset_hours = _resolve_org_utc_offset_hours(org_code=org, default=3.0, force_reload=False)
    
    if not st.session_state.api_client and saved_creds and not st.session_state.get('genesys_logged_out'):
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
                if st.session_state.users_info:
                    st.session_state._users_info_last = dict(st.session_state.users_info)
                st.session_state.queues_map = maps.get("queues_map", {})
                st.session_state.wrapup_map = maps.get("wrapup", {})
                st.session_state.presence_map = maps.get("presence", {})
                st.session_state.org_config = saved_creds # Store for later use (refresh interval etc.)
                st.session_state["_org_config_owner"] = org
                refresh_data_manager_queues()

    # Keep DataManager session sync lightweight (throttled)
    if st.session_state.get('api_client') and st.session_state.get('queues_map'):
        now_ts = pytime.time()
        last_sync_ts = float(st.session_state.get("_dm_sync_ts", 0) or 0)
        if (now_ts - last_sync_ts) >= 10:
            refresh_data_manager_queues()
            st.session_state["_dm_sync_ts"] = now_ts

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
        st.session_state._remember_me_pending_delete = True
        st.session_state._remember_me_pending_delete_retries = 0
        delete_app_session()
        st.session_state.remember_me_enabled = False
        # Clear all session state
        st.session_state.app_user = None
        st.session_state.api_client = None
        st.session_state.logged_in = False
        st.session_state.genesys_logged_out = False
        if 'dashboard_config_loaded' in st.session_state:
            del st.session_state.dashboard_config_loaded
        st.session_state.pop("_dashboard_config_owner", None)
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
                # Avoid rendering too many script tags in one rerun.
                start_idx = st.session_state.last_console_log_count
                max_batch = 10
                if (current_log_count - start_idx) > max_batch:
                    start_idx = current_log_count - max_batch
                # Log new errors
                for i in range(start_idx, current_log_count):
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
    _clear_page_transient_state()
    st.session_state["_prev_page"] = page
    # Do not force an extra rerun on page switch; render target page in the same run.

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
        if is_agent and (not st.session_state.get("users_map")):
            recover_org_maps_if_needed(org, force=True)
            if (not st.session_state.get("users_map")) and st.session_state.get("users_info"):
                fallback_users_map = {}
                for uid, uinfo in (st.session_state.get("users_info") or {}).items():
                    base_name = str((uinfo or {}).get("name") or (uinfo or {}).get("username") or uid or "").strip() or str(uid)
                    candidate = base_name
                    suffix = 2
                    while candidate in fallback_users_map and fallback_users_map.get(candidate) != uid:
                        candidate = f"{base_name} ({suffix})"
                        suffix += 1
                    fallback_users_map[candidate] = uid
                if fallback_users_map:
                    st.session_state.users_map = fallback_users_map
            if (not st.session_state.get("users_map")) and st.session_state.get("api_client"):
                try:
                    api = GenesysAPI(st.session_state.api_client)
                    users = api.get_users()
                    if isinstance(users, list) and users:
                        st.session_state.users_map = {
                            str(u.get("name") or u.get("username") or u.get("id")): u.get("id")
                            for u in users if isinstance(u, dict) and u.get("id")
                        }
                except Exception:
                    pass
        if (not is_agent) and (not st.session_state.get("queues_map")):
            recover_org_maps_if_needed(org, force=True)
            if (not st.session_state.get("queues_map")) and st.session_state.get("api_client"):
                try:
                    api = GenesysAPI(st.session_state.api_client)
                    queues = api.get_queues()
                    if isinstance(queues, list) and queues:
                        st.session_state.queues_map = {
                            str(q.get("name") or q.get("id")): q.get("id")
                            for q in queues if isinstance(q, dict) and q.get("id")
                        }
                except Exception:
                    pass
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
        st.session_state.rep_auto_row_limit = st.number_input(
            "Maksimum satır (gösterim/indirme, 0=limitsiz)",
            min_value=0,
            max_value=500000,
            value=int(st.session_state.get("rep_auto_row_limit", 50000)),
            step=5000,
            help="Büyük raporlarda bellek/disk baskısını azaltmak için çıktı satırını sınırlar."
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

        st.radio(
            "Sure Gosterimi",
            ["HH:MM:SS", "Saniye"],
            horizontal=True,
            key="rep_duration_mode",
            help="Raporlardaki sure kolonlarini saat:dakika:saniye veya toplam saniye olarak gosterir.",
        )

    def _apply_selected_duration_view(df_in):
        df_out = df_in.copy()
        if st.session_state.get("rep_duration_mode", "HH:MM:SS") == "HH:MM:SS":
            return apply_duration_formatting(df_out)
        # Seconds mode: keep duration columns numeric and clean integer-like display
        target_cols = [
            c for c in df_out.columns
            if (c.startswith('t') or c.startswith('Avg') or c in ['col_staffed_time', 'Duration', 'col_duration'])
            and pd.api.types.is_numeric_dtype(df_out[c])
        ]
        for col in target_cols:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce").fillna(0).round(0).astype("int64")
        return df_out

    report_rendered_this_run = False

    if r_type == "report_agent_skill_detail":
        st.info(get_text(lang, "skill_report_info"))
    if r_type == "report_agent_dnis_skill_detail":
        st.info(get_text(lang, "dnis_skill_report_info"))

    if r_type == "chat_detail":
            st.info(get_text(lang, "chat_detail_info"))
            if st.button(get_text(lang, "fetch_chat_data"), type="primary", width='stretch'):
             with st.spinner(get_text(lang, "fetching_data")):
                 start_date = datetime.combine(sd, st_) - timedelta(hours=utc_offset_hours)
                 end_date = datetime.combine(ed, et) - timedelta(hours=utc_offset_hours)
                 
                 api = GenesysAPI(st.session_state.api_client)
                 max_records = int(st.session_state.get("rep_max_records", 5000))
                 dfs = []
                 total_rows = 0
                 u_offset = utc_offset_hours
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
                         _clear_report_result("chat_detail")
                         st.warning("Seçilen tarih aralığında hiç 'Chat/Mesaj' kaydı bulunamadı. (Sesli çağrılar hariç tutuldu)")
                     elif not df_chat.empty:
                         df_chat = _apply_report_row_limit(df_chat, label="Chat detay raporu")
                         # Display
                         st.dataframe(df_chat, width='stretch')
                         _store_report_result("chat_detail", df_chat, "chat_detail")
                         render_downloads(df_chat, "chat_detail", key_base="chat_detail")
                         report_rendered_this_run = True
                     else:
                         _clear_report_result("chat_detail")
                         st.warning(get_text(lang, "no_data"))
                 else:
                     _clear_report_result("chat_detail")
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
                 
                 s_dt = datetime.combine(sd, st_) - timedelta(hours=utc_offset_hours)
                 e_dt = datetime.combine(ed, et) - timedelta(hours=utc_offset_hours)
                 
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
                         utc_offset=utc_offset_hours,
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
                         final_df = _apply_report_row_limit(final_df, label="Kaçan etkileşim raporu")
                         
                         st.success(f"{len(final_df)} adet kaçan etkileşim bulundu.")
                         st.dataframe(final_df, width='stretch')
                         _store_report_result("missed_interactions", final_df, "missed_interactions")
                         render_downloads(final_df, "missed_interactions", key_base="missed_interactions")
                         report_rendered_this_run = True
                     else:
                         _clear_report_result("missed_interactions")
                         st.warning("Seçilen kriterlere uygun kaçan çağrı/etkileşim bulunamadı.")
                 else:
                     _clear_report_result("missed_interactions")
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
                 start_date = datetime.combine(sd, st_) - timedelta(hours=utc_offset_hours)
                 end_date = datetime.combine(ed, et) - timedelta(hours=utc_offset_hours)
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
                         utc_offset=utc_offset_hours,
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
                     
                     df_filtered = _apply_selected_duration_view(df_filtered)
                     final_df = df_filtered.rename(columns=rename_final)
                     final_df = _apply_report_row_limit(final_df, label="Etkileşim arama raporu")
                     st.dataframe(final_df, width='stretch')
                     _store_report_result("interaction_search", final_df, "interactions")
                     render_downloads(final_df, "interactions", key_base="interaction_search")
                     report_rendered_this_run = True
                 else:
                     _clear_report_result("interaction_search")
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
            unsupported_aggregate_metrics = {
                "tOrganizationResponse", "tAcdWait", "nConsultConnected", "nConsultAnswered",
                # Runtime-unsupported by conversations/aggregates (confirmed by API 400 responses).
                "nConversations", "tAgentVideoConnected", "tScreenMonitoring", "tSnippetRecord",
            }
            dropped_metrics = [m for m in sel_mets if m in unsupported_aggregate_metrics]
            sel_mets_effective = [m for m in sel_mets if m not in unsupported_aggregate_metrics]
            bad_metric_cache_key = f"_agg_bad_metrics_{r_type}"
            cached_bad_metrics = set(st.session_state.get(bad_metric_cache_key, []) or [])
            queue_all_mode = False
            if r_type == "report_queue":
                # Queue aggregate endpoint does not return agent presence/login-style metrics.
                queue_incompatible_metrics = {
                    "tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue",
                    "col_staffed_time", "col_login", "col_logout",
                }
                dropped_queue_metrics = [m for m in sel_mets_effective if m in queue_incompatible_metrics]
                if dropped_queue_metrics:
                    st.warning(
                        "Kuyruk raporunda desteklenmeyen metrikler çıkarıldı: "
                        + ", ".join(dropped_queue_metrics)
                    )
                sel_mets_effective = [m for m in sel_mets_effective if m not in queue_incompatible_metrics]
            if dropped_metrics:
                st.warning(f"Bu metrikler aggregate endpoint tarafından desteklenmiyor ve çıkarıldı: {', '.join(dropped_metrics)}")
            if cached_bad_metrics:
                cached_removed = [m for m in sel_mets_effective if m in cached_bad_metrics]
                if cached_removed:
                    st.info(
                        "Önceden API 400 veren metrikler otomatik çıkarıldı: "
                        + ", ".join(cached_removed[:20])
                    )
                sel_mets_effective = [m for m in sel_mets_effective if m not in cached_bad_metrics]
            if r_type == "report_queue" and not sel_ids:
                # No filter => all queues. This avoids large predicate payload errors.
                if not st.session_state.get("queues_map"):
                    recover_org_maps_if_needed(org, force=True)
                sel_ids = None
                queue_all_mode = True
                st.info("Kuyruk seçilmediği için tüm kuyruklar baz alındı.")
            if r_type == "report_agent" and not sel_ids:
                sel_ids = None
                st.info("Agent seçilmediği için tüm agentlar baz alındı.")
            if not sel_mets_effective:
                st.warning("Desteklenen bir metrik seçiniz.")
                st.stop()

            # Auto-save last used metrics
            st.session_state.last_metrics = sel_mets
            with st.spinner(get_text(lang, "fetching_data")):
                api = GenesysAPI(st.session_state.api_client)
                s_dt, e_dt = (
                    datetime.combine(sd, st_) - timedelta(hours=utc_offset_hours),
                    datetime.combine(ed, et) - timedelta(hours=utc_offset_hours),
                )
                # Use timezone-aware UTC datetimes to avoid deprecated utcnow().
                s_dt = s_dt.replace(tzinfo=timezone.utc)
                e_dt = e_dt.replace(tzinfo=timezone.utc)
                now_utc = datetime.now(timezone.utc)
                if e_dt > now_utc:
                    e_dt = now_utc
                    st.info("Bitiş zamanı gelecekte olduğu için mevcut zamana çekildi.")
                if s_dt >= e_dt:
                    st.warning("Başlangıç zamanı bitiş zamanından küçük olmalıdır.")
                    st.stop()
                is_skill_detailed = r_type == "report_agent_skill_detail"
                is_dnis_skill_detailed = r_type == "report_agent_dnis_skill_detail"
                is_queue_skill = (r_type == "report_queue") and (not queue_all_mode)
                r_kind = "Agent" if r_type == "report_agent" else ("Workgroup" if r_type == "report_queue" else "Detailed")
                g_by = ['userId'] if r_kind == "Agent" else ((['queueId', 'requestedRoutingSkillId', 'requestedLanguageId'] if is_queue_skill else ['queueId']) if r_kind == "Workgroup" else (['userId', 'dnis', 'requestedRoutingSkillId', 'requestedLanguageId', 'queueId'] if is_dnis_skill_detailed else (['userId', 'requestedRoutingSkillId', 'requestedLanguageId', 'queueId'] if is_skill_detailed else ['userId', 'queueId'])))
                f_type = 'user' if r_kind == "Agent" else 'queue'
                
                resp = api.get_analytics_conversations_aggregate(s_dt, e_dt, granularity=gran_opt[sel_gran], group_by=g_by, filter_type=f_type, filter_ids=sel_ids or None, metrics=sel_mets_effective, media_types=sel_media_types or None)
                agg_errors = resp.get("_errors") if isinstance(resp, dict) else None
                dropped_bad_request_metrics = resp.get("_dropped_metrics") if isinstance(resp, dict) else None
                if dropped_bad_request_metrics:
                    merged_bad = sorted(set(cached_bad_metrics).union(set(dropped_bad_request_metrics)))
                    st.session_state[bad_metric_cache_key] = merged_bad
                    try:
                        current_selected = st.session_state.get("rep_met")
                        if isinstance(current_selected, list):
                            st.session_state.rep_met = [m for m in current_selected if m not in set(dropped_bad_request_metrics)]
                    except Exception:
                        pass
                    st.warning(
                        "API 400 nedeniyle bazı metrikler çıkarıldı: "
                        + ", ".join(str(m) for m in dropped_bad_request_metrics[:20])
                    )
                if agg_errors:
                    filtered_runtime_errors = []
                    for err in agg_errors:
                        err_l = str(err).lower()
                        is_metric_400 = ("metric=" in err_l) and ("400 client error" in err_l or " bad request" in err_l)
                        if not is_metric_400:
                            filtered_runtime_errors.append(err)
                    agg_errors = filtered_runtime_errors
                if agg_errors:
                    err_blob = " | ".join(str(e) for e in agg_errors[:5]).lower()
                    if "429" in err_blob:
                        reason_hint = "Rate limit (429) nedeniyle bazı parçalar alınamadı."
                    elif "401" in err_blob or "403" in err_blob:
                        reason_hint = "Yetki/Oturum hatası nedeniyle bazı parçalar alınamadı."
                    elif "timeout" in err_blob or "timed out" in err_blob:
                        reason_hint = "Zaman aşımı nedeniyle bazı parçalar alınamadı."
                    else:
                        reason_hint = "API bazı parçalarda hata döndü."
                    st.warning(
                        f"Aggregate sorgusunda {len(agg_errors)} parça hatası oluştu. "
                        f"{reason_hint} Kısmi veri gösteriliyor olabilir."
                    )
                    with st.expander("Aggregate hata detayı", expanded=False):
                        for err_line in agg_errors[:8]:
                            st.caption(str(err_line))
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
                    utc_offset=utc_offset_hours,
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
                        u_offset = utc_offset_hours
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
                    final_df = _apply_selected_duration_view(final_df)

                    rename = {"Interval": get_text(lang, "col_interval"), "AgentName": get_text(lang, "col_agent"), "Username": get_text(lang, "col_username"), "WorkgroupName": get_text(lang, "col_workgroup"), "Name": get_text(lang, "col_agent" if is_agent else "col_workgroup"), "AvgHandle": get_text(lang, "col_avg_handle"), "col_staffed_time": get_text(lang, "col_staffed_time"), "col_login": get_text(lang, "col_login"), "col_logout": get_text(lang, "col_logout"), "SkillName": get_text(lang, "col_skill"), "SkillId": get_text(lang, "col_skill_id"), "LanguageName": get_text(lang, "col_language"), "LanguageId": get_text(lang, "col_language_id"), "Dnis": get_text(lang, "col_dnis")}
                    rename.update({m: get_text(lang, m) for m in sel_mets_effective if m not in rename})
                    df_out = final_df.rename(columns=rename)
                    df_out = _apply_report_row_limit(df_out, label="Standart rapor")
                    st.dataframe(df_out, width='stretch')
                    _store_report_result(r_type, df_out, f"report_{r_type}")
                    report_rendered_this_run = True

                    # Queue report chart based on selected interval
                    if r_kind == "Workgroup":
                        try:
                            if "Interval" in df.columns:
                                metric_for_chart = mets_to_show[0] if mets_to_show else None
                                if metric_for_chart and metric_for_chart in df.columns:
                                    chart_df = df[["Interval", metric_for_chart]].copy()
                                    chart_df["Interval"] = pd.to_datetime(chart_df["Interval"], errors="coerce")
                                    chart_df = chart_df.dropna(subset=["Interval"])
                                    if not chart_df.empty:
                                        # Daily statistics: aggregate per day across selected filters.
                                        chart_df["Interval"] = chart_df["Interval"].dt.normalize()
                                        chart_df = chart_df.groupby("Interval", as_index=False)[metric_for_chart].sum()
                                        metric_label = get_text(lang, metric_for_chart)
                                        chart_df = chart_df.rename(columns={metric_for_chart: metric_label})
                                        st.subheader(get_text(lang, "daily_stat"))
                                        render_24h_time_line_chart(
                                            chart_df,
                                            "Interval",
                                            [metric_label],
                                            aggregate_by_label="sum",
                                            label_mode="date",
                                            x_index_name="Tarih",
                                        )
                        except Exception as e:
                            monitor.log_error("REPORT_RENDER", "Queue report chart render failed", str(e))
                    render_downloads(df_out, f"report_{r_type}", key_base=r_type)
                else:
                    _clear_report_result(r_type)
                    st.warning(get_text(lang, "no_data"))

    if not report_rendered_this_run:
        cached_report = _get_report_result(r_type)
        if cached_report:
            cached_df = cached_report.get("df")
            if isinstance(cached_df, pd.DataFrame) and not cached_df.empty:
                cached_base_name = str(cached_report.get("base_name") or f"report_{r_type}")
                st.dataframe(cached_df, width='stretch')
                render_downloads(cached_df, cached_base_name, key_base=r_type)

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
    current_username = st.session_state.app_user.get("username")
    for uname, udata in all_users.items():
        col1, col2, col3, col4 = st.columns([2, 2, 4, 1])
        col1.write(f"**{uname}**")
        col2.write(f"Rol: {udata.get('role', 'User')}")
        col3.write(f"Metrikler: {', '.join(udata.get('metrics', [])) if udata.get('metrics') else 'Hepsi'}")
        
        # Action Buttons Column
        with col4:
            if uname != current_username:
                if st.button("🗑️", key=f"del_user_{uname}", help="Kullanıcıyı Sil"):
                    ok, msg = auth_manager.delete_user(org, uname)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
        
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
    conf = _resolve_org_config(org_code=org, force_reload=True)

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
                        try:
                            new_org_safe = _safe_org_code(new_org, allow_default=False)
                        except ValueError as exc:
                            st.error(str(exc))
                            new_org_safe = None
                        if not new_org_safe:
                            ok, msg = False, "Invalid organization code"
                        else:
                            ok, msg = auth_manager.add_organization(new_org_safe, new_admin, new_admin_pw)
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
            prev_u_off = _resolve_utc_offset_hours(conf.get("utc_offset", 3), default=3.0)
            new_u_off = _resolve_utc_offset_hours(u_off, default=3.0)
            # Update credentials file (preserving sensitive data)
            save_credentials(org, conf.get("client_id"), conf.get("client_secret"), conf.get("region"), utc_offset=u_off, refresh_interval=ref_i)
            # Update session state for immediate effect
            st.session_state.org_config = load_credentials(org)
            st.session_state["_org_config_owner"] = org
            st.session_state.data_manager.update_settings(new_u_off, ref_i)
            if abs(prev_u_off - new_u_off) > 1e-9:
                try:
                    dm = st.session_state.get("data_manager")
                    if dm:
                        with dm._lock:
                            dm.daily_data_cache = {}
                            dm.last_daily_refresh = 0
                    _clear_live_panel_caches(org, clear_shared=True)
                except Exception:
                    pass
            st.success(get_text(lang, "view_saved"))
            st.rerun()

    # --- GENESYS API SETTINGS (ORG-SCOPED) ---
    st.subheader(get_text(lang, "genesys_api_creds"))
    c_id = st.text_input("Client ID", value=conf.get("client_id", ""), type="password")
    c_sec = st.text_input("Client Secret", value=conf.get("client_secret", ""), type="password")
    regions = ["mypurecloud.ie", "mypurecloud.com", "mypurecloud.de"]
    region = st.selectbox("Region", regions, index=regions.index(conf.get("region", "mypurecloud.ie")) if conf.get("region") in regions else 0)
    st.caption("API bilgileri organizasyon için şifreli saklanır ve aynı organizasyondaki tüm kullanıcılar için kullanılır.")
    
    if st.button(get_text(lang, "login_genesys")):
        if c_id and c_sec:
            with st.spinner("Authenticating..."):
                client, err = authenticate(c_id, c_sec, region, org_code=org)
                if client:
                    st.session_state.api_client = client
                    st.session_state.genesys_logged_out = False
                    # Use existing offsets/intervals if available
                    cur_off = conf.get("utc_offset", 3)
                    cur_ref = conf.get("refresh_interval", 10)
                    save_credentials(org, c_id, c_sec, region, utc_offset=cur_off, refresh_interval=cur_ref)
                    
                    api = GenesysAPI(client)
                    maps = get_shared_org_maps(org, api, ttl_seconds=300, force_refresh=True)
                    st.session_state.users_map = maps.get("users_map", {})
                    st.session_state.users_info = maps.get("users_info", {})
                    if st.session_state.users_info:
                        st.session_state._users_info_last = dict(st.session_state.users_info)
                    st.session_state.queues_map = maps.get("queues_map", {})
                    st.session_state.wrapup_map = maps.get("wrapup", {})
                    st.session_state.presence_map = maps.get("presence", {})
                    st.session_state.org_config = load_credentials(org)
                    st.session_state["_org_config_owner"] = org
                    
                    st.session_state.data_manager.update_api_client(client, st.session_state.presence_map)
                    st.session_state.data_manager.update_settings(_resolve_utc_offset_hours(cur_off, default=3.0), cur_ref)
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
                token_cache = os.path.join(_org_dir(org), ".token_cache.json")
                if os.path.exists(token_cache):
                    os.remove(token_cache)
            except Exception:
                pass
            st.session_state.genesys_logged_out = True
            set_dm_enabled(org, False)
            # Keep app profile session intact; this only logs out Genesys API for current browser session.
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
        minutely_window = 60
        minutely = monitor.get_minutely_stats(minutes=minutely_window)
        if minutely:
            now_dt = datetime.now().replace(second=0, microsecond=0)
            start_dt = now_dt - timedelta(minutes=minutely_window - 1)
            timeline = pd.date_range(start=start_dt, end=now_dt, freq="min")
            counts = {}
            for k, v in minutely.items():
                ts = pd.to_datetime(k, errors="coerce")
                if pd.notna(ts):
                    counts[ts.floor("min")] = int(v or 0)
            df_minutely = pd.DataFrame({
                "Zaman": timeline,
                "İstek Adet": [int(counts.get(ts, 0) or 0) for ts in timeline],
            })
            df_minutely = sanitize_numeric_df(df_minutely)
            render_24h_time_line_chart(df_minutely, "Zaman", ["İstek Adet"], aggregate_by_label="sum")
        else:
            st.info("Son 60 dakikada trafik yok.")

        st.subheader(get_text(lang, "hourly_traffic_24h"))
        hourly = monitor.get_hourly_stats()
        if hourly:
            now_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
            start_hour = now_hour - timedelta(hours=23)
            timeline = pd.date_range(start=start_hour, end=now_hour, freq="h")
            counts = {}
            for k, v in hourly.items():
                ts = pd.to_datetime(k, errors="coerce")
                if pd.notna(ts):
                    counts[ts.replace(minute=0, second=0, microsecond=0)] = int(v or 0)
            df_hourly = pd.DataFrame({
                "Zaman": timeline,
                "İstek Adet": [int(counts.get(ts, 0) or 0) for ts in timeline],
            })
            df_hourly = sanitize_numeric_df(df_hourly)
            render_24h_time_line_chart(df_hourly, "Zaman", ["İstek Adet"], aggregate_by_label="sum")
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
        st.subheader("Disk Bakımı")
        if st.button("🧹 Geçici Dosyaları Temizle", key="admin_temp_cleanup_btn", width='stretch'):
            with st.spinner("Geçici dosyalar temizleniyor..."):
                cleanup_result = _maybe_periodic_temp_cleanup(
                    force=True,
                    max_age_hours=TEMP_FILE_MANUAL_MAX_AGE_HOURS,
                ) or {}
            st.session_state["admin_temp_cleanup_result"] = cleanup_result
            st.session_state["admin_temp_cleanup_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        last_temp_cleanup = st.session_state.get("admin_temp_cleanup_result")
        if last_temp_cleanup:
            run_ts = st.session_state.get("admin_temp_cleanup_ts", "-")
            removed_files = int(last_temp_cleanup.get("removed_files", 0) or 0)
            truncated_files = int(last_temp_cleanup.get("truncated_files", 0) or 0)
            removed_dirs = int(last_temp_cleanup.get("removed_dirs", 0) or 0)
            freed_text = _format_bytes(last_temp_cleanup.get("freed_bytes", 0))
            error_count = len(last_temp_cleanup.get("errors", []) or [])
            st.caption(
                f"Son temizlik: {run_ts} | Silinen dosya: {removed_files} | "
                f"Sıfırlanan dosya: {truncated_files} | Silinen klasör: {removed_dirs} | Açılan alan: {freed_text}"
            )
            if error_count > 0:
                st.warning(f"Temizlik sırasında {error_count} dosya/klasör silinemedi.")

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
                render_24h_time_line_chart(df_mem, "timestamp", ["rss_mb"], include_seconds=True, aggregate_by_label="last")
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
    dashboard_profile_total_t0 = pytime.perf_counter()
    _dashboard_profile_tick()
    # (Config already loaded at top level)
    maps_recover_t0 = pytime.perf_counter()
    maps_recovered = recover_org_maps_if_needed(org, force=False)
    _dashboard_profile_record("dashboard.maps_recover", pytime.perf_counter() - maps_recover_t0)
    if maps_recovered:
        dm_sig = _dashboard_dm_signature(org)
        if st.session_state.get("_dashboard_dm_sig") != dm_sig:
            dm_refresh_t0 = pytime.perf_counter()
            refresh_data_manager_queues()
            _dashboard_profile_record("dashboard.dm_refresh", pytime.perf_counter() - dm_refresh_t0)
            st.session_state._dashboard_dm_sig = dm_sig
    controls_t0 = pytime.perf_counter()
    st.title(get_text(lang, "menu_dashboard"))
    if ENABLE_DASHBOARD_PROFILING:
        profile_state = _dashboard_profile_state()
        profile_rows = _dashboard_profile_rows(limit=12)
        with st.expander("⏱️ Dashboard Profiling", expanded=bool(profile_state.get("enabled"))):
            profile_duration = st.number_input(
                "Profil süresi (sn)",
                min_value=60,
                max_value=900,
                step=30,
                value=int(profile_state.get("duration_s", 180) or 180),
                key="dashboard_profile_duration_s",
            )
            p_c1, p_c2, p_c3 = st.columns(3)
            if p_c1.button("Profili Başlat", key="dashboard_profile_start_btn", width='stretch'):
                _dashboard_profile_start(duration_s=profile_duration)
                st.rerun()
            if p_c2.button("Profili Durdur", key="dashboard_profile_stop_btn", width='stretch'):
                _dashboard_profile_stop()
                st.rerun()
            if p_c3.button("Profili Temizle", key="dashboard_profile_clear_btn", width='stretch'):
                _dashboard_profile_clear(duration_s=profile_duration)
                st.rerun()
            if profile_state.get("enabled"):
                elapsed_s = max(0, int(pytime.time() - float(profile_state.get("started_ts", 0) or 0)))
                remaining_s = max(0, int(profile_state.get("duration_s", 180)) - elapsed_s)
                st.caption(f"Profil aktif. Geçen: {elapsed_s}s | Kalan: {remaining_s}s | Rerun: {profile_state.get('runs', 0)}")
            else:
                st.caption(f"Profil pasif. Son kayıt rerun sayısı: {profile_state.get('runs', 0)}")
            if profile_rows:
                st.dataframe(pd.DataFrame(profile_rows), width='stretch', hide_index=True)
            else:
                st.caption("Henüz profil verisi yok.")
    c_c1, c_c2, c_c3 = st.columns([1, 2, 1])
    if c_c1.button(get_text(lang, "add_group"), width='stretch'):
        st.session_state.dashboard_cards.append({"id": max([c['id'] for c in st.session_state.dashboard_cards], default=-1)+1, "title": "", "queues": [], "size": "medium", "live_metrics": ["Waiting", "Interacting", "On Queue"], "daily_metrics": ["Offered", "Answered", "Abandoned", "Answer Rate"]})
        save_dashboard_config(org, st.session_state.dashboard_layout, st.session_state.dashboard_cards)
        refresh_data_manager_queues()
        st.session_state._dashboard_dm_sig = _dashboard_dm_signature(org)
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
            auto_ref = c_auto.toggle(
                get_text(lang, "auto_refresh"),
                value=st.session_state.get("dashboard_auto_refresh", True),
                key="dashboard_auto_refresh",
            )
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
                c_time.markdown(
                    f'<div class="last-update">Last Update: <span>{_escape_html(last_upd)}</span></div>',
                    unsafe_allow_html=True,
                )
            
        if st.session_state.dashboard_mode == "Live":
            # DataManager is managed centrally by refresh_data_manager_queues()
            # which is called on login, hot-reload, and config changes.
            ref_int = _resolve_refresh_interval_seconds(org, minimum=3, default=15)
            if st.session_state.get("dashboard_auto_refresh", True):
                _safe_autorefresh(interval=ref_int * 1000, key="data_refresh")
        if not st.session_state.get("queues_map"):
            st.caption("Queue listesi yuklenemedi.")
            if st.button("Queue Listesini Yenile", key="reload_dashboard_queue_map"):
                recover_org_maps_if_needed(org, force=True)
                refresh_data_manager_queues()
                st.session_state._dashboard_dm_sig = _dashboard_dm_signature(org)
                st.rerun()
    _dashboard_profile_record("dashboard.controls", pytime.perf_counter() - controls_t0)

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
        # Agent panel +10%, call panel +20% relative to current widths.
        main_c, agent_c, call_c = st.columns([6, 1.1, 1.2])
    elif show_agent or show_call:
        side_weight = 1.1 if show_agent else 1.2
        main_c, side_c = st.columns([7, side_weight])
        agent_c = side_c if show_agent else None
        call_c = side_c if show_call else None
    else:
        main_c = st.container()
        agent_c = None
        call_c = None

    grid = main_c.columns(st.session_state.dashboard_layout)
    cards_total_t0 = pytime.perf_counter()
    to_del = []
    for idx, card in enumerate(st.session_state.dashboard_cards):
        card_total_t0 = pytime.perf_counter()
        try:
            with grid[idx % st.session_state.dashboard_layout]:
                # Determine Container Height based on size
                c_size = card.get('size', 'medium')
                visuals_cfg = card.get('visual_metrics', ["Service Level"])
                visual_count = max(1, len(visuals_cfg))
                visuals_per_row = 1 if c_size == 'xsmall' else (2 if c_size in ['small', 'medium'] else 3)
                visual_rows = max(1, (visual_count + visuals_per_row - 1) // visuals_per_row)
                gauge_base_h = 110 if c_size == 'xsmall' else (125 if c_size == 'small' else (140 if c_size == 'medium' else 160))
                gauge_row_h = gauge_base_h + 20
    
                live_sel = card.get('live_metrics', ["Waiting", "Interacting", "On Queue"])
                daily_sel = card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"])
                live_rows = ((len(live_sel) - 1) // 5 + 1) if (st.session_state.dashboard_mode == "Live" and live_sel) else 0
                daily_rows = ((len(daily_sel) - 1) // 5 + 1) if daily_sel else 0
                metric_row_h = 74
    
                header_block_h = 96
                caption_block_h = 28 if daily_rows else 0
                metrics_block_h = (live_rows + daily_rows) * metric_row_h
                visuals_block_h = visual_rows * gauge_row_h
                padding_h = 28
                c_height = header_block_h + caption_block_h + metrics_block_h + visuals_block_h + padding_h
    
                min_height_map = {'xsmall': 380, 'small': 470, 'medium': 560, 'large': 650}
                c_height = max(c_height, min_height_map.get(c_size, 560))
    
                with st.container(height=c_height, border=True):
                    card_title = card['title'] if card['title'] else f"Grup #{card['id']+1}"
                    st.markdown(f"### {card_title}")
                    with st.expander(f"⚙️ Settings", expanded=False):
                        prev_card = dict(card)
                        card['title'] = st.text_input("Title", value=card['title'], key=f"t_{card['id']}")
                        size_opts = ["xsmall", "small", "medium", "large"]
                        card['size'] = st.selectbox("Size", size_opts, index=size_opts.index(card.get('size', 'medium')), key=f"sz_{card['id']}")
                        card['visual_metrics'] = st.multiselect("Visuals", ["Service Level", "Answer Rate", "Abandon Rate"], default=card.get('visual_metrics', ["Service Level"]), key=f"vm_{card['id']}")
                        queue_options = list(st.session_state.queues_map.keys())
                        option_by_lower = {q.strip().lower(): q for q in queue_options}
                        queue_defaults = []
                        for q in card.get('queues', []):
                            if not isinstance(q, str):
                                continue
                            q_clean = q.replace("(not loaded)", "").strip()
                            if q_clean in queue_options:
                                queue_defaults.append(q_clean)
                                continue
                            canonical = option_by_lower.get(q_clean.lower())
                            if canonical:
                                queue_defaults.append(canonical)
                        if queue_options:
                            selected_queues = st.multiselect("Queues", queue_options, default=queue_defaults, key=f"q_{card['id']}")
                            card['queues'] = selected_queues
                        else:
                            fallback_default = ", ".join([q for q in card.get('queues', []) if isinstance(q, str)])
                            manual_queues = st.text_input(
                                "Queues (manual)",
                                value=fallback_default,
                                key=f"q_manual_{card['id']}",
                                placeholder="Queue1, Queue2",
                            )
                            card['queues'] = [q.strip() for q in manual_queues.split(",") if q.strip()]
                        card['media_types'] = st.multiselect("Media Types", ["voice", "chat", "email", "callback", "message"], default=card.get('media_types', []), key=f"mt_{card['id']}")
    
                        st.write("---")
                        st.caption("📡 Canlı Metrikler")
                        card['live_metrics'] = st.multiselect("Live Metrics", LIVE_METRIC_OPTIONS, default=card.get('live_metrics', ["Waiting", "Interacting", "On Queue"]), format_func=lambda x: live_labels.get(x, x), key=f"lm_{card['id']}")
    
                        st.caption("📊 Günlük Metrikler")
                        card['daily_metrics'] = st.multiselect("Daily Metrics", DAILY_METRIC_OPTIONS, default=card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"]), format_func=lambda x: daily_labels.get(x, x), key=f"dm_{card['id']}")
    
                        if st.button("Delete", key=f"d_{card['id']}"):
                            to_del.append(idx)
                        if card != prev_card:
                            save_dashboard_config(org, st.session_state.dashboard_layout, st.session_state.dashboard_cards)
                            if prev_card.get("queues") != card.get("queues"):
                                refresh_data_manager_queues()
                                st.session_state._dashboard_dm_sig = _dashboard_dm_signature(org)
    
                    if not card.get('queues'):
                        st.info("Select queues")
                        continue
                    resolved_card_queues, unresolved_card_queues = _resolve_card_queue_names(
                        card.get('queues', []),
                        st.session_state.get("queues_map", {}),
                    )
                    if unresolved_card_queues:
                        st.caption("⚠️ Eşleşmeyen kuyruk: " + ", ".join(unresolved_card_queues[:3]))
                    if not resolved_card_queues:
                        st.warning("Seçili kuyruklar sistemde bulunamadı. Queue listesini yenileyip tekrar seçin.")
                        continue
                    
                    # Determine date range based on mode
                    data_fetch_t0 = pytime.perf_counter()
                    if st.session_state.dashboard_mode == "Live":
                        # Use cached live data
                        obs_map, daily_map, _ = st.session_state.data_manager.get_data(resolved_card_queues)
                        items_live = [obs_map.get(q) for q in resolved_card_queues if obs_map.get(q)]
                        items_daily = [daily_map.get(q) for q in resolved_card_queues if daily_map.get(q)]
                        # Fallback: if DataManager cache is temporarily empty, fetch directly once.
                        if (not items_live and not items_daily) and st.session_state.get('api_client'):
                            queue_ids = [
                                st.session_state.queues_map.get(q)
                                for q in resolved_card_queues
                                if st.session_state.queues_map.get(q)
                            ]
                            if queue_ids:
                                try:
                                    api = GenesysAPI(st.session_state.api_client)
                                    id_map = {v: k for k, v in st.session_state.queues_map.items()}
                                    obs_resp = api.get_queue_observations(queue_ids)
                                    if obs_resp:
                                        obs_map = process_observations(obs_resp, id_map, st.session_state.get("presence_map") or {})
                                        items_live = [obs_map.get(q) for q in resolved_card_queues if obs_map.get(q)]
                                    start_dt, end_dt = _dashboard_interval_utc("Live", saved_creds)
                                    interval = f"{start_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
                                    daily_resp = api.get_queue_daily_stats(queue_ids, interval=interval)
                                    if daily_resp:
                                        daily_map = process_daily_stats(daily_resp, id_map)
                                        items_daily = [daily_map.get(q) for q in resolved_card_queues if daily_map.get(q)]
                                except Exception:
                                    pass
                    else:
                        # Fetch historical data via API
                        items_live = []  # No live data for historical
                        
                        if st.session_state.dashboard_mode == "Yesterday":
                            start_dt, end_dt = _dashboard_interval_utc("Yesterday", saved_creds)
                        else:  # Date mode
                            start_dt, end_dt = _dashboard_interval_utc(
                                "Date",
                                saved_creds,
                                selected_date=st.session_state.get("dashboard_date", datetime.today()),
                            )
                        
                        # Fetch aggregate data for selected queues
                        queue_ids = [
                            st.session_state.queues_map.get(q)
                            for q in resolved_card_queues
                            if st.session_state.queues_map.get(q)
                        ]
                        
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
                                items_daily = [daily_data.get(q) for q in resolved_card_queues if daily_data.get(q)]
                            except Exception as e:
                                st.warning(f"Veri çekilemedi: {e}")
                    _dashboard_profile_record("cards.data_fetch", pytime.perf_counter() - data_fetch_t0)
                    
                    # Calculate aggregates
                    calc_t0 = pytime.perf_counter()
                    n_q = len(items_live) or 1
                    n_s = len(resolved_card_queues) or 1
                    
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
                    handle_sum = sum(get_media_sum(d, 'Handle_Sum') for d in items_daily)
                    handle_count = sum(get_media_sum(d, 'Handle_Count') for d in items_daily)
                    wait_sum = sum(get_media_sum(d, 'Wait_Sum') for d in items_daily)
                    wait_count = sum(get_media_sum(d, 'Wait_Count') for d in items_daily)
                    avg_handle = (handle_sum / handle_count) if handle_count > 0 else 0
                    avg_wait = (wait_sum / wait_count) if wait_count > 0 else 0
                    
                    # Live metrics mapping
                    # Live metrics mapping
                    
                    # 1. Fetch Agent Details for selected queues involved in this card
                    card_agent_data = st.session_state.data_manager.get_agent_details(resolved_card_queues)
                    
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
                    _dashboard_profile_record("cards.compute", pytime.perf_counter() - calc_t0)
                    
                    render_t0 = pytime.perf_counter()
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
                        
                        # Render selected visuals (wrapped rows to prevent overflow)
                        visuals = card.get('visual_metrics', ["Service Level"])
                        size_now = card.get('size')
                        base_h = 110 if size_now == 'xsmall' else (125 if size_now == 'small' else (140 if size_now == 'medium' else 160))
                        panel_key_suffix = "open" if (st.session_state.get('show_agent_panel', False) or st.session_state.get('show_call_panel', False)) else "closed"
    
                        if visuals:
                            per_row = 1 if size_now == 'xsmall' else (2 if size_now in ['small', 'medium'] else 3)
                            for start in range(0, len(visuals), per_row):
                                row = visuals[start:start + per_row]
                                cols = st.columns(per_row)
                                for idx, vis in enumerate(row):
                                    with cols[idx]:
                                        if vis == "Service Level":
                                            st.plotly_chart(create_gauge_chart(sl, get_text(lang, "avg_service_level"), base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_sl_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                                        elif vis == "Answer Rate":
                                            ar_val = (ans / off * 100) if off > 0 else 0
                                            st.plotly_chart(create_gauge_chart(ar_val, "Answer Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ar_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                                        elif vis == "Abandon Rate":
                                            ab_val = (abn / off * 100) if off > 0 else 0
                                            st.plotly_chart(create_gauge_chart(ab_val, "Abandon Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ab_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                    
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
                        
                        # Render selected visuals (wrapped rows to prevent overflow)
                        visuals = card.get('visual_metrics', ["Service Level"])
                        size_now = card.get('size')
                        base_h = 110 if size_now == 'xsmall' else (125 if size_now == 'small' else (140 if size_now == 'medium' else 160))
                        panel_key_suffix = "open" if (st.session_state.get('show_agent_panel', False) or st.session_state.get('show_call_panel', False)) else "closed"
    
                        if visuals:
                            per_row = 1 if size_now == 'xsmall' else (2 if size_now in ['small', 'medium'] else 3)
                            for start in range(0, len(visuals), per_row):
                                row = visuals[start:start + per_row]
                                cols = st.columns(per_row)
                                for idx, vis in enumerate(row):
                                    with cols[idx]:
                                        if vis == "Service Level":
                                            st.plotly_chart(create_gauge_chart(sl, get_text(lang, "avg_service_level"), base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_sl_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                                        elif vis == "Answer Rate":
                                            ar_val = (ans / off * 100) if off > 0 else 0
                                            st.plotly_chart(create_gauge_chart(ar_val, "Answer Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ar_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                                        elif vis == "Abandon Rate":
                                            ab_val = (abn / off * 100) if off > 0 else 0
                                            st.plotly_chart(create_gauge_chart(ab_val, "Abandon Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ab_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                    _dashboard_profile_record("cards.render", pytime.perf_counter() - render_t0)
        finally:
            _dashboard_profile_record("cards.card_total", pytime.perf_counter() - card_total_t0)

    _dashboard_profile_record("cards.total", pytime.perf_counter() - cards_total_t0)

    if to_del:
        for i in sorted(to_del, reverse=True): del st.session_state.dashboard_cards[i]
        save_dashboard_config(org, st.session_state.dashboard_layout, st.session_state.dashboard_cards)
        refresh_data_manager_queues()
        st.session_state._dashboard_dm_sig = _dashboard_dm_signature(org)
        st.rerun()

    # --- SIDE PANEL LOGIC ---
    if st.session_state.get('show_agent_panel', False) and agent_c:
        agent_panel_t0 = pytime.perf_counter()
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

            # Group Filter (Genesys Groups) + submit-based filters (no rerun on each keypress)
            now_ts = pytime.time()
            groups_api_t0 = pytime.perf_counter()
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
            _dashboard_profile_record("agent_panel.groups_api", pytime.perf_counter() - groups_api_t0)
            group_options = ["Hepsi (All)"] + [g.get('name', '') for g in groups_cache if g.get('name')]
            if "agent_panel_search" not in st.session_state:
                st.session_state.agent_panel_search = ""
            if st.session_state.get("agent_panel_group") not in group_options:
                st.session_state.agent_panel_group = "Hepsi (All)"
            with st.form("agent_panel_filters_form", clear_on_submit=False, border=False):
                search_term = st.text_input(
                    "🔍 Agent Ara",
                    label_visibility="collapsed",
                    placeholder="Agent Ara...",
                    key="agent_panel_search",
                )
                selected_group = st.selectbox("📌 Grup Filtresi", group_options, key="agent_panel_group")
                st.form_submit_button("Filtreyi Uygula", use_container_width=True)
            search_term = str(search_term or "").strip().lower()
            
            if st.session_state.dashboard_mode != "Live":
                st.warning("Agent detayları sadece CANLI modda görünür.")
            elif not st.session_state.get('api_client'):
                st.warning(get_text(lang, "genesys_not_connected"))
            else:
                users_info_map = st.session_state.get('users_info') or {}
                if users_info_map:
                    st.session_state._users_info_last = dict(users_info_map)
                    users_info_refresh_ts = float(st.session_state.get("_users_info_full_refresh_ts", 0) or 0)
                    if (now_ts - users_info_refresh_ts) > 600:
                        users_info_api_t0 = pytime.perf_counter()
                        try:
                            api = GenesysAPI(st.session_state.api_client)
                            refreshed = get_shared_org_maps(org, api, ttl_seconds=300, force_refresh=True)
                            refreshed_users_info = refreshed.get("users_info", {}) or {}
                            if refreshed_users_info:
                                users_info_map = refreshed_users_info
                                st.session_state.users_info = users_info_map
                                st.session_state._users_info_last = dict(users_info_map)
                            st.session_state._users_info_full_refresh_ts = now_ts
                        except Exception:
                            pass
                        _dashboard_profile_record("agent_panel.users_info_api", pytime.perf_counter() - users_info_api_t0)
                else:
                    # Recover from last-known users, lazy org refresh, then users_map inversion.
                    cached_users_info = st.session_state.get('_users_info_last') or {}
                    if cached_users_info:
                        users_info_map = dict(cached_users_info)
                    else:
                        last_try = float(st.session_state.get("_users_info_recover_ts", 0) or 0)
                        if (now_ts - last_try) > 20:
                            st.session_state._users_info_recover_ts = now_ts
                            try:
                                api = GenesysAPI(st.session_state.api_client)
                                refreshed = get_shared_org_maps(org, api, ttl_seconds=300, force_refresh=True)
                                users_info_map = refreshed.get("users_info", {}) or {}
                                st.session_state.users_info = users_info_map
                                if users_info_map:
                                    st.session_state._users_info_last = dict(users_info_map)
                            except Exception:
                                pass
                    if not users_info_map:
                        users_map = st.session_state.get("users_map") or {}
                        if users_map:
                            users_info_map = {uid: {"name": name, "username": "", "email": ""} for name, uid in users_map.items()}
                            st.session_state.users_info = users_info_map
                            st.session_state._users_info_last = dict(users_info_map)
                if not users_info_map:
                    st.info("Kullanıcı bilgileri yükleniyor...")

                agent_notif = ensure_agent_notifications_manager()
                agent_notif.update_client(
                    st.session_state.api_client,
                    st.session_state.queues_map,
                    users_info_map,
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
                            group_members_api_t0 = pytime.perf_counter()
                            try:
                                api = GenesysAPI(st.session_state.api_client)
                                members = api.get_group_members(group_id)
                                members_cache[group_id] = {"ts": now_ts, "members": members}
                                st.session_state.dashboard_group_members_cache = members_cache
                                entry = members_cache.get(group_id, {})
                            except Exception:
                                pass
                            _dashboard_profile_record("agent_panel.group_members_api", pytime.perf_counter() - group_members_api_t0)
                        for m in (entry.get("members") or []):
                            if m.get("id"):
                                group_member_ids.add(m.get("id"))
                    all_user_ids = sorted(group_member_ids)
                else:
                    all_user_ids = sorted(users_info_map.keys())

                # Seed from API if notifications cache is empty/stale
                refresh_s = _resolve_refresh_interval_seconds(org, minimum=3, default=15)
                shared_ts, shared_presence, shared_routing = _get_shared_agent_seed(org)
                last_msg = getattr(agent_notif, "last_message_ts", 0)
                last_evt = getattr(agent_notif, "last_event_ts", 0)
                seed_interval_s = max(180, int(refresh_s) * 12)
                stale_after = seed_interval_s
                notif_stale = (not agent_notif.connected) or (last_msg == 0) or ((now_ts - last_evt) > stale_after)
                periodic_seed_due = (now_ts - float(shared_ts or 0)) >= seed_interval_s
                if all_user_ids:
                    seeded_from_api = False
                    reserved_seed = False
                    needs_seed = (
                        (not getattr(agent_notif, "user_presence", {}) and not getattr(agent_notif, "user_routing", {}))
                        or notif_stale
                        or periodic_seed_due
                    )
                    if needs_seed and _reserve_agent_seed(org, now_ts, min_interval=seed_interval_s):
                        reserved_seed = True
                        seed_api_t0 = pytime.perf_counter()
                        try:
                            api = GenesysAPI(st.session_state.api_client)
                            snap = api.get_users_status_scan(target_user_ids=all_user_ids)
                            pres = snap.get("presence") or {}
                            rout = snap.get("routing") or {}
                            if pres or rout:
                                agent_notif.seed_users(pres, rout)
                                _merge_agent_seed(org, pres, rout, now_ts)
                                seeded_from_api = True
                        except Exception:
                            if reserved_seed:
                                _rollback_agent_seed(org, now_ts, fallback_ts=shared_ts)
                        _dashboard_profile_record("agent_panel.seed_api", pytime.perf_counter() - seed_api_t0)
                    if reserved_seed and not seeded_from_api:
                        _rollback_agent_seed(org, now_ts, fallback_ts=shared_ts)
                    if (not seeded_from_api) and (shared_presence or shared_routing):
                        pres = {uid: shared_presence.get(uid) for uid in all_user_ids if uid in shared_presence}
                        rout = {uid: shared_routing.get(uid) for uid in all_user_ids if uid in shared_routing}
                        if pres or rout:
                            agent_notif.seed_users_missing(pres, rout)

                # Keep WS subscriptions on the full target set so offline->active transitions are captured.
                max_users = (agent_notif.MAX_TOPICS_PER_CHANNEL * agent_notif.MAX_CHANNELS) // 3
                ws_user_ids = all_user_ids[:max_users]
                if len(all_user_ids) > max_users:
                    st.caption(f"⚠️ WebSocket limiti: {max_users}/{len(all_user_ids)} kullanıcı anlık takipte")
                if ws_user_ids:
                    agent_notif.start(ws_user_ids)

                # Only keep non-OFFLINE users for display.
                active_user_ids = []
                for uid in all_user_ids:
                    presence = agent_notif.get_user_presence(uid) if agent_notif else {}
                    sys_presence = (presence.get('presenceDefinition', {}).get('systemPresence', '')).upper()
                    if sys_presence and sys_presence != "OFFLINE":
                        active_user_ids.append(uid)

                # Build agent_data from active users
                agent_data = {"_all": []}
                for uid in active_user_ids:
                    user_info = users_info_map.get(uid, {})
                    name = user_info.get("name", "Unknown")
                    presence = agent_notif.get_user_presence(uid) if agent_notif else {}
                    routing = agent_notif.get_user_routing(uid) if agent_notif else {}
                    agent_data["_all"].append({
                        "id": uid,
                        "user": {"id": uid, "name": name, "presence": presence},
                        "routingStatus": routing,
                    })

                # Flatten, deduplicate and filter.
                filter_sort_t0 = pytime.perf_counter()
                all_members = []
                if agent_data.get("_all"):
                    unique_members = {}
                    for q_name, members in agent_data.items():
                        for m in members:
                            mid = m['id']
                            if mid not in unique_members:
                                unique_members[mid] = m
                    for m in unique_members.values():
                        name = m.get('user', {}).get('name', 'Unknown')
                        if search_term in name.lower():
                            all_members.append(m)

                # Custom order: Break, Meal, On Queue, Available
                def get_sort_score(m):
                    user_obj = m.get('user', {})
                    presence_obj = user_obj.get('presence', {})
                    p = presence_obj.get('presenceDefinition', {}).get('systemPresence', 'OFFLINE').upper()
                    routing_obj = m.get('routingStatus', {})
                    rs = routing_obj.get('status', 'OFF_QUEUE').upper()
                    
                    # Secondary sort: longest duration first.
                    start_str = routing_obj.get('startTime')
                    if not start_str or rs == 'OFF_QUEUE':
                        start_str = presence_obj.get('modifiedDate')
                    try:
                        if start_str:
                            start_str = start_str.replace("Z", "+00:00")
                            start_dt = datetime.fromisoformat(start_str)
                            duration_sec = (datetime.now(timezone.utc) - start_dt).total_seconds()
                            neg_duration = -duration_sec
                        else:
                            neg_duration = 0
                    except Exception:
                        neg_duration = 0

                    score = 10
                    if p == 'OFFLINE':
                        score = 99
                    elif p == 'BREAK':
                        score = 1
                    elif p == 'MEAL':
                        score = 2
                    elif p in ['ON_QUEUE', 'ON QUEUE'] or rs in ['INTERACTING', 'COMMUNICATING', 'IDLE', 'NOT_RESPONDING']:
                        score = 3
                    elif p == 'AVAILABLE':
                        score = 4
                    elif p == 'BUSY':
                        score = 5
                    elif p == 'MEETING':
                        score = 6
                    elif p == 'TRAINING':
                        score = 7
                    return (score, neg_duration)

                all_members.sort(key=get_sort_score)

                # Keep previous agent list visible while waiting for fresh data.
                agent_cache_key = f"{selected_group}|{search_term}"
                agent_cache = st.session_state.get("_agent_panel_last_by_filter", {})
                if not isinstance(agent_cache, dict):
                    agent_cache = {}
                fallback_ttl = max(60, int(refresh_s) * 6)
                if all_members:
                    agent_cache[agent_cache_key] = {"ts": now_ts, "data": list(all_members)}
                    if len(agent_cache) > 20:
                        oldest = sorted(agent_cache.items(), key=lambda kv: kv[1].get("ts", 0))[:len(agent_cache) - 20]
                        for k, _ in oldest:
                            agent_cache.pop(k, None)
                    st.session_state["_agent_panel_last_by_filter"] = agent_cache
                else:
                    cached = agent_cache.get(agent_cache_key) or {}
                    if cached and (now_ts - cached.get("ts", 0)) <= fallback_ttl:
                        all_members = list(cached.get("data") or [])
                _dashboard_profile_record("agent_panel.filter_sort", pytime.perf_counter() - filter_sort_t0)

                render_t0 = pytime.perf_counter()
                if not all_members:
                    st.info("Aktif agent bulunamadı.")
                else:
                    st.markdown(f'<p class="aktif-sayisi">Aktif: {len(all_members)}</p>', unsafe_allow_html=True)
                    max_display = 500
                    render_members = all_members[:max_display]
                    extra_count = max(0, len(all_members) - max_display)
                    agent_cards_html = []
                    for m in render_members:
                        user_obj = m.get('user', {})
                        name = user_obj.get('name', 'Unknown')
                        presence_obj = user_obj.get('presence', {})
                        presence = presence_obj.get('presenceDefinition', {}).get('systemPresence', 'OFFLINE').upper()
                        routing_obj = m.get('routingStatus', {})
                        routing = routing_obj.get('status', 'OFF_QUEUE').upper()
                        
                        duration_str = format_status_time(presence_obj.get('modifiedDate'), routing_obj.get('startTime'))
                        dot_color = "#94a3b8"
                        label = presence_obj.get('presenceDefinition', {}).get('label')
                        status_text = label if label else presence.replace("_", " ").capitalize()

                        if routing in ["INTERACTING", "COMMUNICATING"]:
                            dot_color = "#3b82f6"
                            status_text = "Görüşmede"
                        elif routing == "IDLE":
                            dot_color = "#22c55e"
                            status_text = "On Queue"
                        elif routing == "NOT_RESPONDING":
                            dot_color = "#ef4444"
                            status_text = "Cevapsız"
                        elif presence == "AVAILABLE":
                            dot_color = "#22c55e"
                            status_text = "Müsait"
                        elif presence in ["ON_QUEUE", "ON QUEUE"]:
                            dot_color = "#22c55e"
                            status_text = "On Queue"
                        elif presence == "BUSY":
                            dot_color = "#ef4444"
                            if not label:
                                status_text = "Meşgul"
                        elif presence in ["AWAY", "BREAK", "MEAL"]:
                            dot_color = "#f59e0b"
                        elif presence == "MEETING":
                            dot_color = "#ef4444"
                            if not label:
                                status_text = "Toplantı"

                        display_status = f"{status_text} - {duration_str}" if duration_str else status_text
                        safe_name = _escape_html(name)
                        safe_status = _escape_html(display_status)
                        agent_cards_html.append(f"""
                            <div class="agent-card">
                                <span class="status-dot" style="background-color: {dot_color};"></span>
                                <div>
                                        <p class="agent-name">{safe_name}</p>
                                        <p class="agent-status">{safe_status}</p>
                                    </div>
                                </div>
                            """)
                    if agent_cards_html:
                        st.markdown("".join(agent_cards_html), unsafe_allow_html=True)
                    if extra_count:
                        st.caption(f"+{extra_count} daha fazla kayıt")
                _dashboard_profile_record("agent_panel.render", pytime.perf_counter() - render_t0)
        _dashboard_profile_record("agent_panel.total", pytime.perf_counter() - agent_panel_t0)

    # --- CALL PANEL LOGIC ---
    if st.session_state.get('show_call_panel', False) and call_c:
        call_panel_t0 = pytime.perf_counter()
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
                        margin-top: -4px !important;
                        margin-bottom: 5px !important;
                    }
                    .st-key-call_panel_queue_search,
                    .st-key-call_panel_group,
                    .st-key-call_panel_hide_mevcut,
                    .st-key-call_panel_type_filters {
                        margin-bottom: 0.2rem !important;
                    }
                    .st-key-call_panel_type_filters {
                        margin-bottom: 0.05rem !important;
                    }
                </style>
            """, unsafe_allow_html=True)

            # Filter options (group filter is optional - data is queue-independent)
            waiting_calls = []
            group_options = ["Hepsi (All)"] + [card['title'] or f"Grup #{idx+1}" for idx, card in enumerate(st.session_state.dashboard_cards)]
            call_filter_options = ["inbound", "outbound", "waiting", "connected", "callback", "message", "voice"]
            direction_filter_options = ["inbound", "outbound"]
            state_filter_options = ["waiting", "connected"]
            media_filter_options = ["callback", "message", "voice"]
            full_direction_filter_set = set(direction_filter_options)
            full_state_filter_set = set(state_filter_options)
            full_media_filter_set = set(media_filter_options)
            call_filter_labels = {
                "inbound": "Inbound",
                "outbound": "Outbound",
                "waiting": "Bekleyen",
                "connected": "Bağlandı",
                "callback": "Callback",
                "message": "Message",
                "voice": "Voice",
            }

            if "call_panel_queue_search" not in st.session_state:
                st.session_state.call_panel_queue_search = ""
            if st.session_state.get("call_panel_group") not in group_options:
                st.session_state.call_panel_group = "Hepsi (All)"
            if "call_panel_hide_mevcut" not in st.session_state:
                st.session_state.call_panel_hide_mevcut = False
            raw_type_filters = st.session_state.get("call_panel_type_filters")
            if not isinstance(raw_type_filters, list):
                st.session_state.call_panel_type_filters = list(call_filter_options)
            else:
                st.session_state.call_panel_type_filters = [f for f in raw_type_filters if f in call_filter_options]

            with st.form("call_panel_filters_form", clear_on_submit=False, border=False):
                queue_search_term = st.text_input(
                    "🔍 Kuyruk Ara",
                    label_visibility="collapsed",
                    placeholder="Kuyruk Ara...",
                    key="call_panel_queue_search",
                )
                selected_group = st.selectbox("📌 Grup Filtresi", group_options, key="call_panel_group")
                hide_mevcut = st.checkbox("Mevcut içeren kuyrukları gizle", key="call_panel_hide_mevcut")
                selected_call_filters = st.multiselect(
                    "🎛️ Yön / Kanal / Durum Filtresi",
                    options=call_filter_options,
                    key="call_panel_type_filters",
                    format_func=lambda x: call_filter_labels.get(x, str(x).title()),
                )
                st.form_submit_button("Filtreyi Uygula", use_container_width=True)

            queue_search_term = str(queue_search_term or "").strip().lower()
            selected_filter_set = {str(x).lower() for x in (selected_call_filters or []) if x}
            selected_direction_filters = selected_filter_set.intersection(full_direction_filter_set)
            selected_state_filters = selected_filter_set.intersection(full_state_filter_set)
            selected_media_filters = selected_filter_set.intersection(full_media_filter_set)
            is_filter_none = len(selected_filter_set) == 0
            is_filter_all = (
                selected_direction_filters == full_direction_filter_set
                and selected_state_filters == full_state_filter_set
                and selected_media_filters == full_media_filter_set
            )
            group_queues_lower = set()
            if selected_group != "Hepsi (All)":
                for idx, card in enumerate(st.session_state.dashboard_cards):
                    label = card['title'] or f"Grup #{idx+1}"
                    if label == selected_group and card.get('queues'):
                        resolved_group_queues, _ = _resolve_card_queue_names(
                            card.get("queues", []),
                            st.session_state.get("queues_map", {}),
                        )
                        group_queues_lower = {str(q).strip().lower() for q in resolved_group_queues}
                        break

            if st.session_state.dashboard_mode != "Live":
                st.warning(get_text(lang, "call_panel_live_only"))
            elif not st.session_state.get('api_client'):
                st.warning(get_text(lang, "genesys_not_connected"))
            else:
                refresh_s = _resolve_refresh_interval_seconds(org, minimum=3, default=15)
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
                snapshot_interval_s = max(90, int(refresh_s) * 8)
                notif_stale = (not global_notif.connected) or (last_msg == 0) or ((now_ts - last_evt) > snapshot_interval_s)

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

                should_snapshot = notif_stale or (now_ts - shared_snapshot_ts >= snapshot_interval_s)
                if should_snapshot and _reserve_call_seed(org, now_ts, min_interval=snapshot_interval_s):
                    snapshot_api_t0 = pytime.perf_counter()
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
                    _dashboard_profile_record("call_panel.snapshot_api", pytime.perf_counter() - snapshot_api_t0)

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

                meta_poll_interval_s = max(60, int(refresh_s) * 6)
                meta_target_ids = _reserve_call_meta_targets(
                    org,
                    missing_ids,
                    now_ts,
                    min_interval=meta_poll_interval_s,
                    cooldown_seconds=max(120, int(refresh_s) * 12),
                    max_items=1,
                )
                if meta_target_ids:
                    meta_api_t0 = pytime.perf_counter()
                    try:
                        api = GenesysAPI(st.session_state.api_client)
                        users_info = st.session_state.get("users_info") or {}
                        for cid in meta_target_ids:
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
                    _dashboard_profile_record("call_panel.meta_api", pytime.perf_counter() - meta_api_t0)

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

                filter_sort_t0 = pytime.perf_counter()
                if group_queues_lower:
                    waiting_calls = [
                        c for c in waiting_calls
                        if str(c.get("queue_name") or "").strip().lower() in group_queues_lower
                    ]
                if hide_mevcut:
                    waiting_calls = [c for c in waiting_calls if "mevcut" not in (c.get("queue_name") or "").lower()]
                if is_filter_none:
                    waiting_calls = []
                elif not is_filter_all:
                    waiting_calls = [
                        c for c in waiting_calls
                        if _call_matches_filters(
                            c,
                            direction_filters=selected_direction_filters,
                            media_filters=selected_media_filters,
                            state_filters=selected_state_filters,
                        )
                    ]
                if queue_search_term:
                    waiting_calls = [
                        c for c in waiting_calls
                        if (
                            queue_search_term in str(c.get("queue_name") or "").lower()
                            or queue_search_term in str(c.get("wg") or "").lower()
                        )
                    ]

                waiting_calls.sort(key=lambda x: x.get("wait_seconds") if x.get("wait_seconds") is not None else -1, reverse=True)

                # Short-lived fallback cache to avoid empty flashes between refresh cycles.
                if is_filter_all:
                    filter_sig = "all"
                elif selected_filter_set:
                    filter_sig = ",".join(sorted(selected_filter_set))
                else:
                    filter_sig = "none"
                search_sig = queue_search_term.replace("|", " ").strip() if queue_search_term else ""
                call_cache_key = f"v3|{selected_group}|{int(bool(hide_mevcut))}|{filter_sig}|q:{search_sig}"
                call_cache = st.session_state.get("_call_panel_last_by_filter", {})
                if not isinstance(call_cache, dict):
                    call_cache = {}
                fallback_ttl = max(60, int(refresh_s) * 6)
                if waiting_calls:
                    call_cache[call_cache_key] = {"ts": now_ts, "data": list(waiting_calls)}
                    if len(call_cache) > 20:
                        oldest = sorted(call_cache.items(), key=lambda kv: kv[1].get("ts", 0))[:len(call_cache) - 20]
                        for k, _ in oldest:
                            call_cache.pop(k, None)
                    st.session_state["_call_panel_last_by_filter"] = call_cache
                else:
                    if not is_filter_none:
                        cached = call_cache.get(call_cache_key) or {}
                        if cached and (now_ts - cached.get("ts", 0)) <= fallback_ttl:
                            waiting_calls = list(cached.get("data") or [])
                _dashboard_profile_record("call_panel.filter_sort", pytime.perf_counter() - filter_sort_t0)

                count_label = "Aktif"
                st.markdown(f'<p class="panel-count">{count_label}: {len(waiting_calls)}</p>', unsafe_allow_html=True)

                render_t0 = pytime.perf_counter()
                if not waiting_calls:
                    st.info(get_text(lang, "no_waiting_calls"))
                else:
                    max_display = 200
                    render_calls = waiting_calls[:max_display]
                    extra_count = max(0, len(waiting_calls) - max_display)
                    call_cards_html = []
                    for item in render_calls:
                        wait_str = format_duration_seconds(item.get("wait_seconds"))
                        q = item.get("queue_name", "")
                        wg = item.get("wg")
                        queue_display = q
                        if _is_generic_queue_name(queue_display):
                            if wg and not _is_generic_queue_name(wg):
                                queue_display = wg
                            else:
                                queue_display = "-"
                        queue_text = f"{queue_display}"
                        conv_id = item.get("conversation_id")
                        conv_short = conv_id[-6:] if conv_id and len(conv_id) > 6 else conv_id
                        phone = item.get("phone")
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
                        elif _normalize_call_media_token(media_type) == "voice":
                            media_type = "Voice"
                        elif _normalize_call_media_token(media_type) == "message":
                            media_type = "Message"
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
                        safe_queue_text = _escape_html(queue_text)
                        safe_meta_text = _escape_html(meta_text)
                        safe_wait_str = _escape_html(wait_str)
                        meta_html = f'<div class="call-meta">{safe_meta_text}</div>' if safe_meta_text else ""

                        call_cards_html.append(f"""
                            <div class="call-card">
                                <div class="call-info">
                                    <div class="call-queue">{safe_queue_text}</div>
                                    {meta_html}
                                </div>
                                <div class="call-wait">{safe_wait_str}</div>
                            </div>
                        """)
                    if call_cards_html:
                        st.markdown("".join(call_cards_html), unsafe_allow_html=True)
                    if extra_count:
                        st.caption(f"+{extra_count} daha fazla kayıt")
                _dashboard_profile_record("call_panel.render", pytime.perf_counter() - render_t0)
            _dashboard_profile_record("call_panel.total", pytime.perf_counter() - call_panel_t0)

    _dashboard_profile_record("dashboard.total", pytime.perf_counter() - dashboard_profile_total_t0)
    _dashboard_profile_commit_run()
