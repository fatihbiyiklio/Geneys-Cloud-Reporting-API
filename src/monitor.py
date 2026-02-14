import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta

class AppMonitor:
    """
    Central monitoring class for API usage statistics and error logging.
    Designed as a singleton to be accessible from both DataManager and GenesysAPI.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(AppMonitor, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
            
        self.api_stats = {} # endpoint -> count
        self.api_calls_log = [] # list of dicts: {timestamp, endpoint, method, status_code, duration_ms}
        self.error_logs = [] # list of {timestamp, module, message, details}
        self.start_time = datetime.now()
        self.total_api_calls = 0
        self._lock = threading.Lock()
        self._last_stats_prune = time.time()
        self._last_log_prune = time.time()
        self._last_bucket_prune = time.time()
        self.MAX_ENDPOINT_STATS = 200  # Max unique endpoints to track
        self.STATS_PRUNE_INTERVAL = 3600  # Prune every hour
        self.MAX_API_CALL_LOG_ENTRIES = 20000  # Keep enough data for admin traffic charts.
        self.API_CALL_LOG_RETENTION_HOURS = 48
        self.LOG_PRUNE_INTERVAL_SECONDS = 30
        self.BUCKET_PRUNE_INTERVAL_SECONDS = 30
        self.MINUTE_BUCKET_RETENTION_HOURS = max(48, self.API_CALL_LOG_RETENTION_HOURS)
        self.HOUR_BUCKET_RETENTION_HOURS = max(24 * 14, self.API_CALL_LOG_RETENTION_HOURS)
        self.MAX_MINUTE_BUCKETS = (self.MINUTE_BUCKET_RETENTION_HOURS * 60) + 120
        self.MAX_HOUR_BUCKETS = self.HOUR_BUCKET_RETENTION_HOURS + 48
        self.minute_buckets = {}  # minute datetime -> count
        self.hour_buckets = {}    # hour datetime -> count
        self.PERSIST_INTERVAL_SECONDS = 30
        self._last_persist_ts = 0
        self._persist_path = self._resolve_persist_path()
        self._load_persisted_state()
        self._initialized = True

    def _resolve_state_base_dir(self):
        env_dir = os.environ.get("GENESYS_STATE_DIR")
        if env_dir:
            return os.path.abspath(env_dir)
        if getattr(sys, "frozen", False):
            appdata = os.environ.get("APPDATA")
            if appdata:
                return os.path.join(appdata, "GenesysCloudReporting", "orgs")
            return os.path.join(os.path.expanduser("~"), ".genesys_cloud_reporting", "orgs")
        return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "orgs")

    def _resolve_persist_path(self):
        try:
            base = self._resolve_state_base_dir()
            monitor_dir = os.path.join(base, "_monitor")
            os.makedirs(monitor_dir, exist_ok=True)
            return os.path.join(monitor_dir, "api_buckets.json")
        except Exception:
            return None

    def _bucket_dict_to_payload(self, bucket_dict):
        payload = []
        for dt_key, value in sorted((bucket_dict or {}).items(), key=lambda kv: kv[0]):
            if not isinstance(dt_key, datetime):
                continue
            try:
                payload.append([dt_key.isoformat(timespec="seconds"), int(value or 0)])
            except Exception:
                continue
        return payload

    def _payload_to_bucket_dict(self, payload):
        out = {}
        for row in payload or []:
            try:
                if not isinstance(row, (list, tuple)) or len(row) != 2:
                    continue
                ts = datetime.fromisoformat(str(row[0]))
                out[ts] = int(row[1] or 0)
            except Exception:
                continue
        return out

    def _load_persisted_state(self):
        path = self._persist_path
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            persisted_minutes = self._payload_to_bucket_dict(data.get("minute_buckets"))
            persisted_hours = self._payload_to_bucket_dict(data.get("hour_buckets"))
            if persisted_minutes:
                self.minute_buckets.update(persisted_minutes)
            if persisted_hours:
                self.hour_buckets.update(persisted_hours)
            self.total_api_calls = max(
                int(data.get("total_api_calls", 0) or 0),
                int(self.total_api_calls or 0),
            )
            start_raw = data.get("start_time")
            if start_raw:
                try:
                    persisted_start = datetime.fromisoformat(str(start_raw))
                    if persisted_start < self.start_time:
                        self.start_time = persisted_start
                except Exception:
                    pass
            self._prune_time_buckets(datetime.now())
        except Exception:
            return

    def _persist_state(self, force=False):
        path = self._persist_path
        if not path:
            return
        now_ts = time.time()
        if (not force) and ((now_ts - self._last_persist_ts) < self.PERSIST_INTERVAL_SECONDS):
            return
        payload = {
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "start_time": self.start_time.isoformat(timespec="seconds"),
            "total_api_calls": int(self.total_api_calls or 0),
            "minute_buckets": self._bucket_dict_to_payload(self.minute_buckets),
            "hour_buckets": self._bucket_dict_to_payload(self.hour_buckets),
        }
        tmp_path = f"{path}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            os.replace(tmp_path, path)
            self._last_persist_ts = now_ts
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    def _entry_datetime(self, entry):
        ts_dt = entry.get("timestamp_dt")
        if isinstance(ts_dt, datetime):
            return ts_dt
        raw_ts = entry.get("timestamp")
        if not raw_ts:
            return None
        try:
            return datetime.fromisoformat(raw_ts)
        except Exception:
            return None

    def _prune_time_buckets(self, now_dt):
        minute_cutoff = now_dt - timedelta(hours=self.MINUTE_BUCKET_RETENTION_HOURS)
        stale_minutes = [k for k in self.minute_buckets.keys() if k < minute_cutoff]
        for key in stale_minutes:
            self.minute_buckets.pop(key, None)
        if len(self.minute_buckets) > self.MAX_MINUTE_BUCKETS:
            extra = len(self.minute_buckets) - self.MAX_MINUTE_BUCKETS
            for key in sorted(self.minute_buckets.keys())[:extra]:
                self.minute_buckets.pop(key, None)

        hour_cutoff = now_dt - timedelta(hours=self.HOUR_BUCKET_RETENTION_HOURS)
        stale_hours = [k for k in self.hour_buckets.keys() if k < hour_cutoff]
        for key in stale_hours:
            self.hour_buckets.pop(key, None)
        if len(self.hour_buckets) > self.MAX_HOUR_BUCKETS:
            extra = len(self.hour_buckets) - self.MAX_HOUR_BUCKETS
            for key in sorted(self.hour_buckets.keys())[:extra]:
                self.hour_buckets.pop(key, None)

    def _record_time_buckets(self, now_dt):
        minute_key = now_dt.replace(second=0, microsecond=0)
        hour_key = now_dt.replace(minute=0, second=0, microsecond=0)
        self.minute_buckets[minute_key] = self.minute_buckets.get(minute_key, 0) + 1
        self.hour_buckets[hour_key] = self.hour_buckets.get(hour_key, 0) + 1

        now_ts = time.time()
        needs_prune = (
            (now_ts - self._last_bucket_prune) > self.BUCKET_PRUNE_INTERVAL_SECONDS
            or len(self.minute_buckets) > self.MAX_MINUTE_BUCKETS
            or len(self.hour_buckets) > self.MAX_HOUR_BUCKETS
        )
        if needs_prune:
            self._prune_time_buckets(now_dt)
            self._last_bucket_prune = now_ts

    def log_api_call(self, endpoint, method=None, status_code=None, duration_ms=None):
        """Records an API call with timestamp, endpoint path, and optional timing metadata."""
        with self._lock:
            clean_endpoint = endpoint.split('?')[0] # Remove query params
            # Normalize UUIDs in path to prevent unbounded key growth
            # e.g., /api/v2/routing/queues/abc-123/users -> /api/v2/routing/queues/{id}/users
            import re
            clean_endpoint = re.sub(
                r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
                '{id}', clean_endpoint
            )
            self.api_stats[clean_endpoint] = self.api_stats.get(clean_endpoint, 0) + 1
            self.total_api_calls += 1
            
            # Periodically prune api_stats to prevent unbounded growth
            now = time.time()
            if (now - self._last_stats_prune) > self.STATS_PRUNE_INTERVAL:
                if len(self.api_stats) > self.MAX_ENDPOINT_STATS:
                    # Keep only top N by call count
                    sorted_stats = sorted(self.api_stats.items(), key=lambda x: x[1], reverse=True)
                    self.api_stats = dict(sorted_stats[:self.MAX_ENDPOINT_STATS])
                self._last_stats_prune = now
            
            now_dt = datetime.now()
            self._record_time_buckets(now_dt)
            entry = {
                "timestamp": now_dt.isoformat(timespec="seconds"),
                "timestamp_dt": now_dt,
                "endpoint": clean_endpoint,
                "method": method,
                "status_code": status_code,
                "duration_ms": duration_ms
            }
            self.api_calls_log.append(entry)
            
            # Keep enough history for charts while preventing unbounded growth.
            needs_prune = (
                (now - self._last_log_prune) > self.LOG_PRUNE_INTERVAL_SECONDS
                or len(self.api_calls_log) > self.MAX_API_CALL_LOG_ENTRIES
            )
            if needs_prune:
                cutoff_dt = now_dt - timedelta(hours=self.API_CALL_LOG_RETENTION_HOURS)
                pruned = []
                for log_entry in self.api_calls_log:
                    ts = self._entry_datetime(log_entry)
                    if ts and ts >= cutoff_dt:
                        pruned.append(log_entry)
                if len(pruned) > self.MAX_API_CALL_LOG_ENTRIES:
                    pruned = pruned[-self.MAX_API_CALL_LOG_ENTRIES:]
                self.api_calls_log = pruned
                self._last_log_prune = now
            self._persist_state()

    def log_error(self, module, message, details=None):
        """Records an application error."""
        with self._lock:
            error_entry = {
                "timestamp": datetime.now(),
                "module": module,
                "message": message,
                "details": str(details) if details else ""
            }
            self.error_logs.append(error_entry)
            
            # Keep log manageable (last 100 errors)
            if len(self.error_logs) > 100:
                self.error_logs.pop(0)
            self._persist_state()

    def get_stats(self):
        """Returns current API statistics."""
        with self._lock:
            return {
                "total_calls": self.total_api_calls,
                "endpoint_stats": self.api_stats.copy(),
                "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
                "error_count": len(self.error_logs)
            }

    def get_rate_per_minute(self, minutes=1):
        """Returns average API calls per minute over the last N minutes."""
        if minutes <= 0:
            return 0
        with self._lock:
            cutoff = datetime.now() - timedelta(minutes=minutes)
            count = 0
            for entry in self.api_calls_log:
                ts = self._entry_datetime(entry)
                if not ts:
                    continue
                if ts > cutoff:
                    count += 1
            return count / minutes

    def get_avg_rate_per_minute(self):
        """Returns average API calls per minute since app start."""
        with self._lock:
            uptime_minutes = (datetime.now() - self.start_time).total_seconds() / 60
            if uptime_minutes <= 0:
                return 0
            total_calls = self.total_api_calls
            return total_calls / uptime_minutes

    def get_hourly_stats(self):
        """Returns API calls grouped by hour for the last 24h."""
        with self._lock:
            now_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
            start_hour = now_hour - timedelta(hours=23)

            # Primary source: stable hour buckets (independent from raw log cap).
            hourly = dict(self.hour_buckets) if self.hour_buckets else {}

            # Fallback for backward compatibility if bucket data is empty.
            if not hourly:
                cutoff = start_hour
                for entry in self.api_calls_log:
                    ts = self._entry_datetime(entry)
                    if not ts:
                        continue
                    if ts > cutoff:
                        hour_dt = ts.replace(minute=0, second=0, microsecond=0)
                        hourly[hour_dt] = hourly.get(hour_dt, 0) + 1

            result = {}
            for i in range(24):
                curr = start_hour + timedelta(hours=i)
                result[curr.strftime("%Y-%m-%d %H:00")] = int(hourly.get(curr, 0))
            return result

    def get_minutely_stats(self, minutes=60):
        """Returns API calls grouped by minute for the last N minutes."""
        if minutes <= 0:
            return {}
        with self._lock:
            now_minute = datetime.now().replace(second=0, microsecond=0)
            start_minute = now_minute - timedelta(minutes=minutes - 1)

            # Primary source: stable minute buckets.
            minute_data = dict(self.minute_buckets) if self.minute_buckets else {}

            # Fallback for backward compatibility if bucket data is empty.
            if not minute_data:
                cutoff = now_minute - timedelta(minutes=minutes)
                for entry in self.api_calls_log:
                    ts = self._entry_datetime(entry)
                    if not ts:
                        continue
                    if ts > cutoff:
                        minute_dt = ts.replace(second=0, microsecond=0)
                        minute_data[minute_dt] = minute_data.get(minute_dt, 0) + 1

            result = {}
            for i in range(minutes):
                curr = start_minute + timedelta(minutes=i)
                result[curr.strftime("%Y-%m-%d %H:%M")] = int(minute_data.get(curr, 0))
            return result

    def get_errors(self, limit=50):
        """Returns recent error logs."""
        with self._lock:
            return sorted(self.error_logs, key=lambda x: x['timestamp'], reverse=True)[:limit]

# Global instance for easy access
monitor = AppMonitor()
