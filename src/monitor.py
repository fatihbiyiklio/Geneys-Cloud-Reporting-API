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
        self._lock = threading.Lock()
        self._last_stats_prune = time.time()
        self.MAX_ENDPOINT_STATS = 200  # Max unique endpoints to track
        self.STATS_PRUNE_INTERVAL = 3600  # Prune every hour
        self._initialized = True

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
            
            # Periodically prune api_stats to prevent unbounded growth
            now = time.time()
            if (now - self._last_stats_prune) > self.STATS_PRUNE_INTERVAL:
                if len(self.api_stats) > self.MAX_ENDPOINT_STATS:
                    # Keep only top N by call count
                    sorted_stats = sorted(self.api_stats.items(), key=lambda x: x[1], reverse=True)
                    self.api_stats = dict(sorted_stats[:self.MAX_ENDPOINT_STATS])
                self._last_stats_prune = now
            
            entry = {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "endpoint": clean_endpoint,
                "method": method,
                "status_code": status_code,
                "duration_ms": duration_ms
            }
            self.api_calls_log.append(entry)
            
            # Keep log manageable (last 200 calls instead of 1000)
            if len(self.api_calls_log) > 200:
                self.api_calls_log = self.api_calls_log[-200:]

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

    def get_stats(self):
        """Returns current API statistics."""
        with self._lock:
            return {
                "total_calls": sum(self.api_stats.values()),
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
                try:
                    ts = datetime.fromisoformat(entry.get("timestamp"))
                except Exception:
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
            total_calls = sum(self.api_stats.values())
            return total_calls / uptime_minutes

    def get_hourly_stats(self):
        """Returns API calls grouped by hour for the last 24h."""
        with self._lock:
            cutoff = datetime.now() - timedelta(hours=24)
            hourly = {}
            for entry in self.api_calls_log:
                try:
                    ts = datetime.fromisoformat(entry.get("timestamp"))
                except Exception:
                    continue
                if ts > cutoff:
                    hour_key = ts.strftime("%Y-%m-%d %H:00")
                    hourly[hour_key] = hourly.get(hour_key, 0) + 1
            return hourly

    def get_minutely_stats(self, minutes=60):
        """Returns API calls grouped by minute for the last N minutes."""
        with self._lock:
            cutoff = datetime.now() - timedelta(minutes=minutes)
            per_minute = {}
            for entry in self.api_calls_log:
                try:
                    ts = datetime.fromisoformat(entry.get("timestamp"))
                except Exception:
                    continue
                if ts > cutoff:
                    minute_key = ts.strftime("%Y-%m-%d %H:%M")
                    per_minute[minute_key] = per_minute.get(minute_key, 0) + 1
            return per_minute

    def get_errors(self, limit=50):
        """Returns recent error logs."""
        with self._lock:
            return sorted(self.error_logs, key=lambda x: x['timestamp'], reverse=True)[:limit]

# Global instance for easy access
monitor = AppMonitor()
