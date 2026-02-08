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
warnings.filterwarnings("ignore", message=".*st.cache.*deprecated.*")

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
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "app.log")
MEMORY_LOG_FILE = os.path.join(LOG_DIR, "memory.jsonl")
MEMORY_LIMIT_MB = int(os.environ.get("GENESYS_MEMORY_LIMIT_MB", "1024"))
MEMORY_CLEANUP_COOLDOWN_SEC = int(os.environ.get("GENESYS_MEMORY_CLEANUP_COOLDOWN_SEC", "120"))

def _setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("genesys_app")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(fmt)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger

logger = _setup_logging()

def _log_exception(prefix, exc_type, exc_value, exc_tb):
    details = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    logger.error("%s: %s", prefix, details)

def _sys_excepthook(exc_type, exc_value, exc_tb):
    _log_exception("Unhandled exception", exc_type, exc_value, exc_tb)

def _thread_excepthook(args):
    _log_exception(f"Thread exception in {args.thread.name}", args.exc_type, args.exc_value, args.exc_traceback)

sys.excepthook = _sys_excepthook
if hasattr(threading, "excepthook"):
    threading.excepthook = _thread_excepthook

atexit.register(lambda: logger.info("App process exiting"))

# --- IMPORTS & PATHS ---
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from src.lang import get_text, STRINGS, DEFAULT_METRICS, ALL_METRICS
from src.monitor import monitor
from src.auth import authenticate
from src.api import GenesysAPI
from src.processor import process_analytics_response, to_excel, to_csv, to_parquet, to_pdf, fill_interval_gaps, process_observations, process_daily_stats, process_user_aggregates, process_user_details, process_conversation_details, apply_duration_formatting

# --- CONFIGURATION ---

API_CALLS_LOG_PATH = os.path.join("logs", "api_calls.jsonl")
API_CALLS_LOG_TAIL_LINES = 50000
API_CALLS_LOG_TAIL_BYTES = 10 * 1024 * 1024
SESSION_TTL_SECONDS = 120

def _read_tail_lines(path, max_lines=API_CALLS_LOG_TAIL_LINES, max_bytes=API_CALLS_LOG_TAIL_BYTES):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            end_pos = f.tell()
            if end_pos == 0:
                return []
            data = b""
            block_size = 8192
            while end_pos > 0 and data.count(b"\n") <= max_lines and len(data) < max_bytes:
                step = block_size if end_pos >= block_size else end_pos
                end_pos -= step
                f.seek(end_pos)
                data = f.read(step) + data
            lines = data.splitlines()[-max_lines:]
            return [line.decode("utf-8", "ignore") for line in lines]
    except Exception:
        return []

def _load_api_calls_log(path, max_lines=API_CALLS_LOG_TAIL_LINES):
    if not os.path.exists(path):
        return pd.DataFrame()
    entries = []
    try:
        lines = _read_tail_lines(path, max_lines=max_lines)
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except Exception:
                continue
    except Exception:
        return pd.DataFrame()
    if not entries:
        return pd.DataFrame()
    df = pd.DataFrame(entries)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "duration_ms" in df.columns:
        df["duration_ms"] = pd.to_numeric(df["duration_ms"], errors="coerce")
    if "status_code" in df.columns:
        df["status_code"] = pd.to_numeric(df["status_code"], errors="coerce")
    return df

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
    if conv.get("mediaType"):
        return conv.get("mediaType")
    participants = conv.get("participants") or conv.get("participantsDetails") or []
    for p in participants:
        for s in p.get("sessions", []) or []:
            mt = s.get("mediaType")
            if mt:
                return mt
    return None

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
    if not direction:
        return None
    direction = str(direction).lower()
    if "inbound" in direction:
        return "Inbound"
    if "outbound" in direction:
        return "Outbound"
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
            q_id = p.get("queueId") or p.get("routingQueueId") or p.get("participantId")
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
            q_id = p.get("queueId") or p.get("routingQueueId") or p.get("participantId")
            if q_id:
                return q_id
            qobj = p.get("queue") or {}
            if isinstance(qobj, dict):
                qid = qobj.get("id")
                if qid:
                    return qid
            for s in p.get("sessions", []) or []:
                qid = s.get("queueId") or s.get("routingQueueId")
                if qid:
                    return qid
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
    queue_id = _extract_queue_id_from_conv(conv)
    queue_name = _extract_queue_name_from_conv(conv, queue_id_to_name)
    agent_id = _extract_agent_id_from_conv(conv)
    agent_name = _extract_agent_name_from_conv(conv)
    if not agent_name and agent_id and users_info:
        agent_name = users_info.get(agent_id, {}).get("name")
    return {
        "conversation_id": conv_id,
        "queue_id": queue_id,
        "queue_name": queue_name,
        "agent_id": agent_id,
        "agent_name": agent_name,
    }

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
    
    /* Prevent flicker/blur during Streamlit rerun */
    [data-testid="stAppViewContainer"] {
        backface-visibility: hidden;
        -webkit-backface-visibility: hidden;
        transform: translateZ(0);
        -webkit-transform: translateZ(0);
    }
    /* Hide Streamlit running indicator that causes blur effect */
    div[data-testid="stStatusWidget"] {
        visibility: hidden !important;
        opacity: 0 !important;
    }
    /* Disable skeleton loading animation */
    .stMarkdown, .stDataFrame, [data-testid="column"] {
        animation: none !important;
        transition: none !important;
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
    return {"lock": threading.Lock(), "samples": [], "thread": None, "stop_event": threading.Event(), "last_cleanup_ts": 0}

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
                org["call_seed_ts"] = 0
                org["ivr_calls_ts"] = 0
                org["agent_seed_ts"] = 0
    except Exception:
        pass
    
    # Force garbage collection
    try:
        gc.collect()
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
            os.makedirs(LOG_DIR, exist_ok=True)
            proc = psutil.Process(os.getpid())
            gc_counter = 0
            log_write_counter = 0
            MEMORY_LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB max
            
            def _rotate_memory_log():
                try:
                    if os.path.exists(MEMORY_LOG_FILE) and os.path.getsize(MEMORY_LOG_FILE) > MEMORY_LOG_MAX_BYTES:
                        backup = MEMORY_LOG_FILE + ".1"
                        if os.path.exists(backup):
                            os.remove(backup)
                        os.rename(MEMORY_LOG_FILE, backup)
                except Exception:
                    pass
            
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
                sample = {"timestamp": ts, "rss_mb": rss_mb, "cpu_pct": cpu_pct}
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
                
                if rss_mb >= MEMORY_LIMIT_MB and (pytime.time() - last_cleanup) > MEMORY_CLEANUP_COOLDOWN_SEC:
                    _soft_memory_cleanup()
                    with store["lock"]:
                        store["last_cleanup_ts"] = pytime.time()
                try:
                    # Rotate log every 100 writes (~16 min)
                    log_write_counter += 1
                    if log_write_counter >= 100:
                        _rotate_memory_log()
                        log_write_counter = 0
                    with open(MEMORY_LOG_FILE, "a", encoding="utf-8") as f:
                        f.write(json.dumps(sample, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                for _ in range(max(1, int(sample_interval * 5))):
                    if store["stop_event"].is_set():
                        break
                    pytime.sleep(0.2)

        t = threading.Thread(target=run, daemon=True)
        store["thread"] = t
        t.start()
    return store

def _ensure_seed_org(store, org_code):
    org = store["orgs"].setdefault(org_code, {
        "call_seed_ts": 0,
        "call_seed_data": [],
        "ivr_calls_ts": 0,
        "ivr_calls_data": [],
        "agent_seed_ts": 0,
        "agent_presence": {},
        "agent_routing": {},
        "call_meta": {},
    })
    # Ensure backward-compatible keys
    org.setdefault("call_seed_ts", 0)
    org.setdefault("call_seed_data", [])
    org.setdefault("ivr_calls_ts", 0)
    org.setdefault("ivr_calls_data", [])
    org.setdefault("agent_seed_ts", 0)
    org.setdefault("agent_presence", {})
    org.setdefault("agent_routing", {})
    org.setdefault("call_meta", {})
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
            agent_id = c.get("agent_id")
            agent_name = c.get("agent_name")
            if not qname and not qid and not agent_id and not agent_name:
                continue
            entry = meta.get(cid, {})
            if qname and not _is_generic_queue_name(qname):
                entry["queue_name"] = qname
            if qid:
                entry["queue_id"] = qid
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

def _merge_agent_seed(org_code, presence_map, routing_map, now_ts):
    store = _shared_seed_store()
    with store["lock"]:
        org = _ensure_seed_org(store, org_code)
        if presence_map:
            org["agent_presence"].update(presence_map)
        if routing_map:
            org["agent_routing"].update(routing_map)
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

def _register_org_session_queues(org_code, queues_map, agent_queues_map):
    store = _shared_org_session_store()
    now = pytime.time()
    session_id = _get_session_id()
    with store["lock"]:
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

def get_shared_data_manager(org_code):
    store = _get_dm_store()
    with store["lock"]:
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

def ensure_notifications_manager():
    org_code = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
    store = _shared_notif_store()
    with store["lock"]:
        nm = store["call"].get(org_code)
        if nm is None:
            nm = NotificationManager()
            store["call"][org_code] = nm
    return nm

def ensure_agent_notifications_manager():
    org_code = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
    store = _shared_notif_store()
    with store["lock"]:
        nm = store["agent"].get(org_code)
        if nm is None or not hasattr(nm, "seed_users_missing") or not hasattr(nm, "get_active_calls"):
            nm = AgentNotificationManager()
            store["agent"][org_code] = nm
    return nm

def ensure_global_conversation_manager():
    org_code = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
    store = _shared_notif_store()
    with store["lock"]:
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
    users_info = {u['id']: {'name': u['name'], 'username': u.get('username', '')} for u in users}
    queues_map = {q['name']: q['id'] for q in queues}
    return {
        "users": users,
        "queues": queues,
        "wrapup": wrapup,
        "presence": presence,
        "users_map": users_map,
        "users_info": users_info,
        "queues_map": queues_map
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
    entry = {"ts": now, **maps}
    with _org_maps_lock:
        _org_maps_cache[org_code] = entry
    return entry

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
if page == get_text(lang, "menu_reports"):
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
            rep_types = ["report_agent", "report_queue", "report_detailed", "interaction_search", "missed_interactions"]
        else: # Admin, Manager
            rep_types = ["report_agent", "report_queue", "report_detailed", "interaction_search", "chat_detail", "missed_interactions"]
        
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


    if r_type == "chat_detail":
            st.info(get_text(lang, "chat_detail_info"))
            if st.button(get_text(lang, "fetch_chat_data"), type="primary", width='stretch'):
             with st.spinner(get_text(lang, "fetching_data")):
                 start_date = datetime.combine(sd, st_) - timedelta(hours=saved_creds.get("utc_offset", 3))
                 end_date = datetime.combine(ed, et) - timedelta(hours=saved_creds.get("utc_offset", 3))
                 
                 api = GenesysAPI(st.session_state.api_client)
                 # Fetch all, but we will filter for chats ideally or just process all with attributes
                 # Interaction detail endpoint returns all types.
                 raw_convs = api.get_conversation_details(start_date, end_date)
                 u_offset = saved_creds.get("utc_offset", 3)
                 # Process with attributes=True (Base structure)
                 df = process_conversation_details(
                     raw_convs, 
                     st.session_state.users_info, 
                     st.session_state.queues_map, 
                     st.session_state.wrapup_map,
                     include_attributes=True,
                     utc_offset=u_offset
                 )
                 
                 if not df.empty:
                     # Filter for Chat/Message types FIRST to reduce API calls
                     chat_types = ['chat', 'message', 'webchat', 'whatsapp', 'facebook', 'twitter', 'line', 'telegram']
                     df_chat = df[df['MediaType'].isin(chat_types)].copy()

                     if not df_chat.empty:
                         st.info(get_text(lang, "fetching_details_info").format(len(df_chat)))
                         
                         # Create a progress bar
                         progress_bar = st.progress(0)
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
                 raw_data = api.get_conversation_details(s_dt, e_dt)
                 
                 # (debug dump removed for build)
                 
                 df = process_conversation_details(
                     raw_data, 
                     user_map=st.session_state.users_info, 
                     queue_map=st.session_state.queues_map, 
                     wrapup_map=st.session_state.wrapup_map,
                     include_attributes=True,
                     utc_offset=saved_creds.get("utc_offset", 3)
                 )
                 
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
                             "Queue": "col_workgroup"
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
                 raw_convs = api.get_conversation_details(start_date, end_date)
                 
                 # Process with Wrapup Map
                 df = process_conversation_details(raw_convs, st.session_state.users_info, st.session_state.queues_map, st.session_state.wrapup_map, utc_offset=saved_creds.get("utc_offset", 3))
                 
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
                         "Queue": "col_workgroup"
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

    # --- STANDARD REPORTS ---
    elif r_type not in ["chat_detail", "missed_interactions"] and st.button(get_text(lang, "fetch_report"), type="primary", width='stretch'):
        if not sel_mets: st.warning("Lütfen metrik seçiniz.")
        else:
            # Auto-save last used metrics
            st.session_state.last_metrics = sel_mets
            with st.spinner(get_text(lang, "fetching_data")):
                api = GenesysAPI(st.session_state.api_client)
                s_dt, e_dt = datetime.combine(sd, st_) - timedelta(hours=saved_creds.get("utc_offset", 3)), datetime.combine(ed, et) - timedelta(hours=saved_creds.get("utc_offset", 3))
                r_kind = "Agent" if r_type == "report_agent" else ("Workgroup" if r_type == "report_queue" else "Detailed")
                g_by = ['userId'] if r_kind == "Agent" else (['queueId'] if r_kind == "Workgroup" else ['userId', 'queueId'])
                f_type = 'user' if r_kind == "Agent" else 'queue'
                
                resp = api.get_analytics_conversations_aggregate(s_dt, e_dt, granularity=gran_opt[sel_gran], group_by=g_by, filter_type=f_type, filter_ids=sel_ids or None, metrics=sel_mets, media_types=sel_media_types or None)
                q_lookup = {v: k for k, v in st.session_state.queues_map.items()}
                
                # For detailed report, we still need users_info for userId lookup, even though filter is queue
                lookup_map = st.session_state.users_info if r_kind in ["Agent", "Detailed"] else q_lookup
                df = process_analytics_response(resp, lookup_map, r_kind.lower(), queue_map=q_lookup, utc_offset=saved_creds.get("utc_offset", 3))
                
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
                    if any(m in sel_mets for m in p_keys) and is_agent:
                        p_map = process_user_aggregates(api.get_user_aggregates(s_dt, e_dt, sel_ids or list(st.session_state.users_info.keys())), st.session_state.get('presence_map'))
                        for pk in ["tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue", "StaffedTime", "nNotResponding"]:
                            df[pk if pk != "StaffedTime" and pk != "nNotResponding" else ("col_staffed_time" if pk == "StaffedTime" else "nNotResponding")] = df["Id"].apply(lambda x: p_map.get(x.split('|')[0] if '|' in x else x, {}).get(pk, 0))
                    
                    if any(m in sel_mets for m in ["col_login", "col_logout"]) and is_agent:
                        u_offset = saved_creds.get("utc_offset", 3)
                        d_map = process_user_details(api.get_user_status_details(s_dt, e_dt, sel_ids or list(st.session_state.users_info.keys())), utc_offset=u_offset)
                        if "col_login" in sel_mets: df["col_login"] = df["Id"].apply(lambda x: d_map.get(x.split('|')[0] if '|' in x else x, {}).get("Login", "N/A"))
                        if "col_logout" in sel_mets: df["col_logout"] = df["Id"].apply(lambda x: d_map.get(x.split('|')[0] if '|' in x else x, {}).get("Logout", "N/A"))

                    if do_fill and gran_opt[sel_gran] != "P1D": df = fill_interval_gaps(df, datetime.combine(sd, st_), datetime.combine(ed, et), gran_opt[sel_gran])
                    
                    base = (["AgentName", "Username", "WorkgroupName"] if r_kind == "Detailed" else (["Name", "Username"] if is_agent else ["Name"]))
                    if "Interval" in df.columns: base = ["Interval"] + base
                    for sm in sel_mets:
                        if sm not in df.columns: df[sm] = 0
                    # Avoid duplicates if AvgHandle is already in sel_mets
                    mets_to_show = [m for m in sel_mets if m in df.columns]
                    if "AvgHandle" in df.columns and "AvgHandle" not in mets_to_show:
                        mets_to_show.append("AvgHandle")
                    final_df = df[[c for c in base if c in df.columns] + mets_to_show]
                    
                    # Apply duration formatting
                    final_df = apply_duration_formatting(final_df)

                    rename = {"Interval": get_text(lang, "col_interval"), "AgentName": get_text(lang, "col_agent"), "Username": get_text(lang, "col_username"), "WorkgroupName": get_text(lang, "col_workgroup"), "Name": get_text(lang, "col_agent" if is_agent else "col_workgroup"), "AvgHandle": get_text(lang, "col_avg_handle"), "col_staffed_time": get_text(lang, "col_staffed_time"), "col_login": get_text(lang, "col_login"), "col_logout": get_text(lang, "col_logout")}
                    rename.update({m: get_text(lang, m) for m in sel_mets if m not in rename})
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
            st.rerun()

elif st.session_state.page == get_text(lang, "admin_panel") and role == "Admin":
    st.title(f"🛡️ {get_text(lang, 'admin_panel')}")
    
    tab1, tab2, tab3 = st.tabs([f"📊 {get_text(lang, 'api_usage')}", f"📋 {get_text(lang, 'error_logs')}", "🧪 Diagnostics"])
    
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
        st.subheader(get_text(lang, "hourly_traffic"))
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
        st.subheader("API Log Raporu")
        df_calls = _load_api_calls_log(API_CALLS_LOG_PATH)
        if df_calls.empty:
            st.info("API logu bulunamadi. Loglama yeni etkinlestirildiyse veri olusmasi icin biraz zaman tanimlayin.")
        else:
            st.caption(f"Not: Rapor son {API_CALLS_LOG_TAIL_LINES} kaydi baz alir (tail okuma).")
            ts_min = df_calls["timestamp"].min()
            ts_max = df_calls["timestamp"].max()
            st.caption(f"Kapsam: {ts_min} - {ts_max} | Toplam Kayit: {len(df_calls)}")

            df_ts = df_calls.dropna(subset=["timestamp"]).copy()
            if not df_ts.empty:
                df_ts["minute"] = df_ts["timestamp"].dt.floor("min")
                df_min = df_ts.groupby("minute").size().reset_index(name="Istek Adet")
                st.line_chart(df_min.set_index("minute"))

            def _pctl(series, p):
                s = series.dropna()
                if s.empty:
                    return np.nan
                return float(np.nanpercentile(s, p))

            def _error_count(series):
                s = pd.to_numeric(series, errors="coerce")
                return int((s >= 400).sum())

            agg = df_calls.groupby("endpoint").agg(
                Adet=("endpoint", "size"),
                Ortalama_ms=("duration_ms", "mean"),
                p50_ms=("duration_ms", lambda s: _pctl(s, 50)),
                p95_ms=("duration_ms", lambda s: _pctl(s, 95)),
                Max_ms=("duration_ms", "max"),
                Hata_Adet=("status_code", _error_count)
            ).reset_index()
            agg["Hata_Orani"] = (agg["Hata_Adet"] / agg["Adet"]).fillna(0.0)
            agg["Neden"] = agg["endpoint"].apply(_endpoint_reason)
            agg["Kaynak"] = agg["endpoint"].apply(_endpoint_source)
            agg = agg.sort_values("Adet", ascending=False)

            top_endpoint = agg.iloc[0] if not agg.empty else None
            if top_endpoint is not None:
                st.info(f"En cok cagrilan endpoint: {top_endpoint['endpoint']} | Adet: {int(top_endpoint['Adet'])} | Kaynak: {top_endpoint['Kaynak']} | Neden: {top_endpoint['Neden']}")

            src_agg = agg.groupby("Kaynak").agg(
                Adet=("Adet", "sum"),
                Ortalama_ms=("Ortalama_ms", "mean"),
                p50_ms=("p50_ms", "mean"),
                p95_ms=("p95_ms", "mean"),
                Max_ms=("Max_ms", "max"),
                Hata_Adet=("Hata_Adet", "sum")
            ).reset_index().sort_values("Adet", ascending=False)

            if not src_agg.empty:
                st.subheader("En Cok Neresi Kullaniliyor (Kaynak Bazli)")
                st.dataframe(src_agg, width='stretch')

            st.dataframe(agg, width='stretch')

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
            
            st.dataframe(df_breakdown, use_container_width=True, hide_index=True)
            
            # Show session state details if significant
            if session_details:
                with st.expander("📦 Session State Detayları (>10 KB)"):
                    df_session = pd.DataFrame(session_details).sort_values("size_kb", ascending=False)
                    df_session["size_kb"] = df_session["size_kb"].round(1)
                    st.dataframe(df_session, use_container_width=True, hide_index=True)
        else:
            st.info("Bellek analizi yapılamadı.")

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
            # Get dynamic interval (default 10s)
            ref_int = st.session_state.get('org_config', {}).get('refresh_interval', 15)
            if auto_ref: _safe_autorefresh(interval=ref_int * 1000, key="data_refresh")

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
                            api = GenesysAPI(st.session_state.api_client)
                            # Use Genesys standard aggregate query
                            interval = f"{start_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
                            resp = api.get_queue_daily_stats(queue_ids, interval=interval)
                            
                            if resp and resp.get('results'):
                                id_map = {v: k for k, v in st.session_state.queues_map.items()}
                                from src.processor import process_daily_stats
                                daily_data = process_daily_stats(resp, id_map)
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
                                    ar_val = float(daily_values.get("Answer Rate", "0").replace('%', ''))
                                    st.plotly_chart(create_gauge_chart(ar_val, "Answer Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ar_{card['id']}_{panel_key_suffix}")
                                elif vis == "Abandon Rate":
                                    ab_val = float(daily_values.get("Abandon Rate", "0").replace('%', ''))
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
                                    ar_val = float(daily_values.get("Answer Rate", "0").replace('%', ''))
                                    st.plotly_chart(create_gauge_chart(ar_val, "Answer Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ar_{card['id']}_{panel_key_suffix}")
                                elif vis == "Abandon Rate":
                                    ab_val = float(daily_values.get("Abandon Rate", "0").replace('%', ''))
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
                    .agent-card {
                        padding: 6px 10px !important;
                        margin-bottom: 6px !important;
                        border-radius: 8px !important;
                        border: 1px solid #f1f5f9 !important;
                        background: #ffffff;
                        display: flex;
                        align-items: center;
                        gap: 10px;
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
            
            # Card/Group Filter
            group_options = ["Hepsi (All)"] + [card['title'] or f"Grup #{idx+1}" for idx, card in enumerate(st.session_state.dashboard_cards)]
            selected_group = st.selectbox("📌 Grup Filtresi", group_options, index=0)
            
            # Collect queues from selected group or all active cards
            # OPTIMIZATION: Only take the FIRST queue of each card for agent list
            all_queues = set()
            if selected_group == "Hepsi (All)":
                for card in st.session_state.dashboard_cards:
                    if card.get('queues'): 
                        all_queues.add(card['queues'][0]) # Primary only
            else:
                for card in st.session_state.dashboard_cards:
                    if (card['title'] or f"Grup #{st.session_state.dashboard_cards.index(card)+1}") == selected_group:
                        if card.get('queues'): 
                            all_queues.add(card['queues'][0]) # Primary only
                        break
            
            if not all_queues:
                st.info("Kart seçilmedi.")
            elif st.session_state.dashboard_mode != "Live":
                st.warning("Agent detayları sadece CANLI modda görünür.")
            else:
                use_agent_notif = st.session_state.get('use_agent_notifications', True)
                agent_notif = None
                agent_data = {}
                members_map = {}
                tracked_ids = set()

                if use_agent_notif:
                    if not st.session_state.get('api_client'):
                        st.warning(get_text(lang, "genesys_not_connected"))
                    else:
                        agent_notif = ensure_agent_notifications_manager()
                        agent_notif.update_client(
                            st.session_state.api_client,
                            st.session_state.queues_map,
                            st.session_state.get('users_info'),
                            st.session_state.get('presence_map')
                        )
                        queue_ids = [st.session_state.queues_map.get(q) for q in all_queues if st.session_state.queues_map.get(q)]
                        union_queues_map, union_agent_map = _get_union_session_maps(org)
                        notif_queue_ids = list(union_agent_map.values()) if union_agent_map else queue_ids
                        members_map = agent_notif.ensure_members(notif_queue_ids)
                        user_ids = sorted({m.get("id") for mems in members_map.values() for m in mems if m.get("id")})
                        max_users = (agent_notif.MAX_TOPICS_PER_CHANNEL * agent_notif.MAX_CHANNELS) // 3
                        if len(user_ids) > max_users:
                            st.warning(get_text(lang, "agent_panel_topic_limit"))
                            user_ids = user_ids[:max_users]
                        tracked_ids = set(user_ids)
                        if user_ids:
                            agent_notif.start(user_ids)
                            if not agent_notif.connected:
                                st.info(get_text(lang, "agent_panel_notif_connecting"))

                        # Seed from API if notifications cache is empty/stale
                        now_ts = pytime.time()
                        shared_ts, shared_presence, shared_routing = _get_shared_agent_seed(org)
                        last_msg = getattr(agent_notif, "last_message_ts", 0)
                        last_evt = getattr(agent_notif, "last_event_ts", 0)
                        notif_stale = (not agent_notif.connected) or (last_msg == 0) or ((now_ts - last_evt) > 60)
                        if user_ids:
                            if (not getattr(agent_notif, "user_presence", {}) and not getattr(agent_notif, "user_routing", {})) or notif_stale:
                                if _reserve_agent_seed(org, now_ts, min_interval=60):
                                    try:
                                        api = GenesysAPI(st.session_state.api_client)
                                        snap = api.get_users_status_scan(target_user_ids=user_ids)
                                        pres = snap.get("presence") or {}
                                        rout = snap.get("routing") or {}
                                        agent_notif.seed_users(pres, rout)
                                        _merge_agent_seed(org, pres, rout, now_ts)
                                    except Exception:
                                        pass
                            else:
                                if shared_presence or shared_routing:
                                    pres = {uid: shared_presence.get(uid) for uid in user_ids if uid in shared_presence}
                                    rout = {uid: shared_routing.get(uid) for uid in user_ids if uid in shared_routing}
                                    if pres or rout:
                                        agent_notif.seed_users_missing(pres, rout)
                        for q_name in all_queues:
                            q_id = st.session_state.queues_map.get(q_name)
                            mems = members_map.get(q_id, [])
                            items = []
                            for m in mems:
                                uid = m.get("id")
                                if tracked_ids and uid not in tracked_ids:
                                    continue
                                name = m.get("name") or st.session_state.users_info.get(uid, {}).get("name", "Unknown")
                                presence = agent_notif.get_user_presence(uid) if agent_notif else {}
                                routing = agent_notif.get_user_routing(uid) if agent_notif else {}
                                items.append({
                                    "id": uid,
                                    "user": {"id": uid, "name": name, "presence": presence},
                                    "routingStatus": routing,
                                })
                            agent_data[q_name] = items
                else:
                    # Get cached details from DataManager
                    agent_data = st.session_state.data_manager.get_agent_details(all_queues)

                if not agent_data:
                    st.info("Veri bekleniyor...")
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
                    /* Prevent panel flicker during refresh */
                    [data-testid="column"]:has(.call-card) {
                        content-visibility: auto;
                        contain-intrinsic-size: auto 500px;
                    }
                    .call-panel-container {
                        min-height: 100px;
                        will-change: contents;
                    }
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
                        transition: opacity 0.15s ease-in-out;
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

            # No filter: show all active calls (waiting + interacting)
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

            # Always use full org queues for call panel; group filter is optional
            all_queues = set(st.session_state.get('queues_map', {}).keys())

            if st.session_state.dashboard_mode != "Live":
                st.warning(get_text(lang, "call_panel_live_only"))
            elif not st.session_state.get('api_client'):
                st.warning(get_text(lang, "genesys_not_connected"))
            else:
                if not all_queues:
                    st.info(get_text(lang, "no_queue_selected"))
                else:
                    now_ts = pytime.time()
                    union_queues_map, union_agent_map = _get_union_session_maps(org)
                    queue_id_to_name = {v: k for k, v in st.session_state.queues_map.items()}
                    # Global conversation topics are invalid in some orgs (HTTP 400).
                    # Use queue + agent notifications for active/waiting calls.
                    active_calls = []

                    agent_active = []
                    agent_notif = ensure_agent_notifications_manager()
                    agent_notif.update_client(
                        st.session_state.api_client,
                        st.session_state.queues_map,
                        st.session_state.get('users_info'),
                        st.session_state.get('presence_map')
                    )
                    notif_queue_ids = list(union_agent_map.values()) if union_agent_map else list(st.session_state.queues_map.values())
                    members_map = agent_notif.ensure_members(notif_queue_ids)
                    user_ids = sorted({m.get("id") for mems in members_map.values() for m in mems if m.get("id")})
                    max_users = (agent_notif.MAX_TOPICS_PER_CHANNEL * agent_notif.MAX_CHANNELS) // 3
                    if len(user_ids) > max_users:
                        st.warning(get_text(lang, "agent_panel_topic_limit"))
                        user_ids = user_ids[:max_users]
                    if user_ids:
                        agent_notif.start(user_ids)
                        if not agent_notif.connected:
                            st.info(get_text(lang, "agent_panel_notif_connecting"))
                    if agent_notif.is_running() or agent_notif.connected:
                        try:
                            agent_active = agent_notif.get_active_calls()
                        except Exception:
                            agent_active = []

                    notif = ensure_notifications_manager()
                    notif.update_client(st.session_state.api_client, st.session_state.queues_map)
                    all_queue_ids = list(st.session_state.queues_map.values())
                    if selected_group == "Hepsi (All)":
                        sub_queue_ids = all_queue_ids
                    else:
                        sub_queue_ids = list(union_queues_map.values()) if union_queues_map else all_queue_ids
                    notif_started = notif.start(sub_queue_ids)
                    if not notif_started:
                        st.warning("Kuyruk bildirimleri baslatilamadi. Polling moduna dusuluyor.")
                    if getattr(notif, "topics_truncated", False):
                        st.warning("Kuyruk bildirimleri limit nedeniyle kisaltildi; bazı kuyruklar anlik gorunmeyebilir.")
                    queue_waiting = notif.get_waiting_calls()

                    notif_stale = not notif.connected or getattr(notif, "last_message_ts", 0) == 0
                    shared_seed_ts, shared_seed_calls = _get_shared_call_seed(org)
                    if (not queue_waiting or notif_stale) and (now_ts - shared_seed_ts) <= 10 and shared_seed_calls:
                        queue_waiting = shared_seed_calls

                    poll_min_interval = 10
                    if not notif_started:
                        poll_min_interval = 60
                    if (not queue_waiting or notif_stale) and _reserve_call_seed(org, now_ts, min_interval=poll_min_interval):
                        api = GenesysAPI(st.session_state.api_client)
                        id_map = {v: k for k, v in st.session_state.queues_map.items()}
                        obs_resp = api.get_queue_observations(list(id_map.keys()))
                        from src.processor import process_observations
                        obs_list = process_observations(obs_resp, id_map)
                        obs_map = {row["Queue"]: row for row in obs_list}

                        waiting_queues = []
                        for q in all_queues:
                            obs = obs_map.get(q)
                            if obs and obs.get("Waiting", {}).get("Total", 0) > 0:
                                waiting_queues.append(q)
                        if waiting_queues:
                            seed_calls = []
                            for q in waiting_queues:
                                q_id = st.session_state.queues_map.get(q)
                                if not q_id:
                                    continue
                                convs = api.get_queue_conversations(q_id)
                                for conv in convs or []:
                                    wait_s = _extract_wait_seconds(conv)
                                    conv_id = None
                                    if isinstance(conv, dict):
                                        conv_id = conv.get("conversationId") or conv.get("id")
                                        if not conv_id and isinstance(conv.get("conversation"), dict):
                                            conv_id = conv.get("conversation", {}).get("id")
                                    phone = _extract_phone_from_conv(conv)
                                    if conv_id:
                                        seed_calls.append({
                                            "conversation_id": conv_id,
                                            "queue_id": q_id,
                                            "queue_name": q,
                                            "wait_seconds": wait_s,
                                            "phone": phone,
                                        })
                            if seed_calls:
                                notif.upsert_waiting_calls(seed_calls)
                                queue_waiting = notif.get_waiting_calls()
                            _update_call_seed(org, seed_calls, now_ts)
                        else:
                            _update_call_seed(org, [], now_ts)

                for c in queue_waiting:
                    if "state" not in c:
                        c["state"] = "waiting"
                    if "media_type" not in c:
                        c["media_type"] = _extract_media_type(c) or "voice"

                combined = {}
                for c in queue_waiting:
                    cid = c.get("conversation_id")
                    if cid:
                        combined[cid] = c
                for c in active_calls:
                    cid = c.get("conversation_id")
                    if cid:
                        combined[cid] = _merge_call(combined.get(cid), c)
                for c in agent_active:
                    cid = c.get("conversation_id")
                    if cid:
                        combined[cid] = _merge_call(combined.get(cid), c)

                active_calls = list(combined.values())

                # Merge recent analytics to capture IVR-only calls (only if live signals are stale)
                shared_ts, shared_calls = _get_shared_ivr_calls(org)
                analytics_calls = shared_calls if (now_ts - shared_ts) <= 60 and shared_calls else []
                notif_stale = (not notif.connected) or (getattr(notif, "last_message_ts", 0) == 0) or ((now_ts - getattr(notif, "last_message_ts", 0)) > 60)
                should_refresh_ivr = notif_stale or (not queue_waiting and not agent_active)
                if should_refresh_ivr and _reserve_ivr_calls(org, now_ts, min_interval=60):
                    api = GenesysAPI(st.session_state.api_client)
                    end_dt = datetime.now(timezone.utc)
                    start_dt = end_dt - timedelta(minutes=20)
                    convs = api.get_conversation_details_recent(start_dt, end_dt, page_size=100, max_pages=4, order="desc")
                    analytics_calls = _build_active_calls(convs, lang, queue_id_to_name, st.session_state.get('users_info'))
                    _update_ivr_calls(org, analytics_calls, now_ts)

                if analytics_calls:
                    for c in analytics_calls:
                        cid = c.get("conversation_id")
                        if cid:
                            combined[cid] = _merge_call(combined.get(cid), c)
                    active_calls = list(combined.values())

                # Fill queue/agent from cached metadata and ids
                call_meta = _get_shared_call_meta(org)
                # Fill missing queue/agent via direct conversation lookup (throttled)
                missing_meta_ids = []
                for c in active_calls:
                    cid = c.get("conversation_id")
                    if not cid:
                        continue
                    meta_entry = call_meta.get(cid)
                    meta_ts = meta_entry.get("ts", 0) if meta_entry else 0
                    if (now_ts - meta_ts) < 60:
                        continue
                    missing_queue = _is_generic_queue_name(c.get("queue_name")) and not c.get("queue_id")
                    missing_agent = not c.get("agent_name") and not c.get("agent_id")
                    if missing_queue or missing_agent:
                        missing_meta_ids.append(cid)
                if missing_meta_ids:
                    api = GenesysAPI(st.session_state.api_client)
                    lookups = 0
                    for cid in missing_meta_ids:
                        if lookups >= 3:
                            break
                        meta = _fetch_conversation_meta(api, cid, queue_id_to_name, st.session_state.get("users_info"))
                        lookups += 1
                        if not meta:
                            continue
                        _update_call_meta(org, [meta], now_ts)
                        meta_copy = dict(meta)
                        meta_copy["ts"] = now_ts
                        call_meta[cid] = meta_copy
                        for c in active_calls:
                            if c.get("conversation_id") == cid:
                                c.update({k: v for k, v in meta.items() if v})
                for c in active_calls:
                    cid = c.get("conversation_id")
                    meta = call_meta.get(cid) if cid else None
                    if meta:
                        if _is_generic_queue_name(c.get("queue_name")) and meta.get("queue_name"):
                            c["queue_name"] = meta.get("queue_name")
                        if not c.get("queue_id") and meta.get("queue_id"):
                            c["queue_id"] = meta.get("queue_id")
                        if not c.get("agent_name") and meta.get("agent_name"):
                            c["agent_name"] = meta.get("agent_name")
                        if not c.get("agent_id") and meta.get("agent_id"):
                            c["agent_id"] = meta.get("agent_id")
                    if _is_generic_queue_name(c.get("queue_name")):
                        qid = c.get("queue_id")
                        if qid and qid in queue_id_to_name:
                            c["queue_name"] = queue_id_to_name.get(qid) or c.get("queue_name")
                    if not c.get("agent_name"):
                        aid = c.get("agent_id")
                        if aid and st.session_state.get("users_info"):
                            c["agent_name"] = st.session_state.users_info.get(aid, {}).get("name")

                _update_call_meta(org, active_calls + (queue_waiting or []), now_ts)

                # Last resort: map Workgroup from agent's primary queue (first dashboard queue)
                agent_primary_queue = {}
                if agent_notif and st.session_state.get("dashboard_cards"):
                    for card in st.session_state.dashboard_cards:
                        if card.get("queues"):
                            qname = card["queues"][0]
                            qid = st.session_state.queues_map.get(qname)
                            if qid:
                                for qid_key, mems in (agent_notif.ensure_members([qid]) or {}).items():
                                    for m in mems:
                                        uid = m.get("id")
                                        if uid and uid not in agent_primary_queue:
                                            agent_primary_queue[uid] = qname
                    for c in active_calls:
                        if not _is_generic_queue_name(c.get("queue_name")):
                            continue
                        aid = c.get("agent_id")
                        if aid and aid in agent_primary_queue:
                            c["queue_name"] = agent_primary_queue.get(aid)

                waiting_calls = active_calls

                # (debug expander removed for build)

                # Smooth UI: keep last non-empty list for a short grace window to avoid flicker
                cache_key = "call_panel_last_active"
                ts_key = "call_panel_last_ts_active"
                prev_calls = st.session_state.get(cache_key, [])
                prev_ts = st.session_state.get(ts_key, 0)
                now_ts = pytime.time()
                if waiting_calls:
                    st.session_state[cache_key] = waiting_calls
                    st.session_state[ts_key] = now_ts
                else:
                    if prev_calls and (now_ts - prev_ts) < 15:
                        waiting_calls = prev_calls

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
                        direction_label = item.get("direction_label")
                        state_label = item.get("state_label")
                        media_type = item.get("media_type")
                        agent_name = item.get("agent_name")
                        agent_id = item.get("agent_id")
                        state_value = (item.get("state") or "").lower()
                        if not direction_label:
                            d = (item.get("direction") or "").lower()
                            if "inbound" in d:
                                direction_label = "Inbound"
                            elif "outbound" in d:
                                direction_label = "Outbound"
                        meta_parts = []
                        is_interacting = bool(agent_name) or bool(agent_id) or state_value == "interacting" or (state_label == get_text(lang, "interacting"))
                        state_label = "Bağlandı" if is_interacting else "Bekleyen"
                        if agent_name and is_interacting:
                            meta_parts.append(f"Agent: {agent_name}")
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
