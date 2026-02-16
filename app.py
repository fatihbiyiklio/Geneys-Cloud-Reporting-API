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
from src.app.router import render_page
from src.app.utils import (
    _build_active_calls,
    _build_status_audit_rows,
    _call_filter_tokens,
    _call_matches_filters,
    _classify_conversation_state,
    _escape_html,
    _extract_agent_id_from_conv,
    _extract_agent_name_from_conv,
    _extract_direction_label,
    _extract_ivr_attributes,
    _extract_media_type,
    _extract_phone_from_conv,
    _extract_queue_id_from_conv,
    _extract_queue_name_from_conv,
    _extract_wait_seconds,
    _extract_workgroup_from_attrs,
    _fetch_conversation_meta,
    _format_iso_with_utc_offset,
    _format_ivr_display,
    _format_status_values,
    _has_ivr_participant,
    _is_callback_conversation,
    _is_generic_queue_name,
    _merge_call,
    _normalize_call_direction_token,
    _normalize_call_media_token,
    _normalize_call_state_token,
    _normalize_status_value,
    _parse_wait_seconds,
    _apply_report_row_limit,
    _clear_report_result,
    _dedupe_time_labels_keep_visual,
    _download_df_signature,
    _format_24h_time_labels,
    _get_report_result,
    _report_result_state_key,
    _safe_state_token,
    _resolve_user_label,
    _seconds_since,
    _session_is_active,
    _store_report_result,
    create_donut_chart,
    create_gauge_chart,
    format_duration_seconds,
    format_status_time,
    render_table_with_export_view,
    render_24h_time_line_chart,
    render_downloads,
    sanitize_numeric_df,
)

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

def _iter_conversation_pages(
    api,
    start_date,
    end_date,
    max_records=5000,
    chunk_days=3,
    page_size=100,
    conversation_filters=None,
    segment_filters=None,
):
    """Yield conversation pages with an upper bound on total records to avoid OOM."""
    total = 0
    for page in api.iter_conversation_details(
        start_date,
        end_date,
        chunk_days=chunk_days,
        page_size=page_size,
        max_pages=200,
        order="asc",
        conversation_filters=conversation_filters,
        segment_filters=segment_filters,
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
        if "refresh_interval" not in creds: creds["refresh_interval"] = 10
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
        "refresh_interval": kwargs.get("refresh_interval", 10)
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

def _resolve_refresh_interval_seconds(org_code=None, minimum=3, default=10):
    # Live metric polling is intentionally fixed at 10 seconds for stability.
    return max(minimum, 10)


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
    refresh_s = _resolve_refresh_interval_seconds(org_code, minimum=10, default=10)
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

def _is_bootstrap_admin_setup_required():
    try:
        default_users = auth_manager.get_all_users("default") or {}
        admin_user = default_users.get("admin") or {}
        return bool(admin_user and admin_user.get("must_change_password"))
    except Exception:
        return False

# --- APP LOGIN ---
if not st.session_state.app_user:
    if _is_bootstrap_admin_setup_required():
        st.markdown("<h1 style='text-align: center;'>Genesys Reporting API</h1>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            with st.form("bootstrap_admin_password_setup_form"):
                st.subheader("İlk Kurulum: Admin Şifresi Belirleyin")
                st.info("Uygulama ilk açılışta güvenlik için admin şifresi oluşturulmasını zorunlu tutar.")
                st.caption("Organizasyon: default | Kullanıcı: admin")
                new_pw = st.text_input("Yeni Admin Şifresi", type="password")
                new_pw2 = st.text_input("Yeni Admin Şifresi (Tekrar)", type="password")
                if st.form_submit_button("Kurulumu Tamamla", width='stretch'):
                    if not new_pw or not new_pw2:
                        st.error("Lütfen iki alanı da doldurun.")
                    elif new_pw != new_pw2:
                        st.error("Şifreler eşleşmiyor.")
                    elif len(new_pw) < 8:
                        st.error("Şifre en az 8 karakter olmalıdır.")
                    else:
                        ok, msg = auth_manager.reset_password("default", "admin", new_pw)
                        if ok:
                            _clear_login_failures("default", "admin")
                            st.success("Admin şifresi oluşturuldu. Giriş ekranına yönlendiriliyorsunuz.")
                            st.rerun()
                        else:
                            st.error(msg)
        st.stop()

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

render_page(globals())
