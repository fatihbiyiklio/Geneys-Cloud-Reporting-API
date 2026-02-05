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
        self.api_calls_log = [] # list of (timestamp, endpoint)
        self.error_logs = [] # list of {timestamp, module, message, details}
        self.start_time = datetime.now()
        self._lock = threading.Lock()
        self._initialized = True

    def log_api_call(self, endpoint):
        """Records an API call with timestamp and endpoint path."""
        with self._lock:
            clean_endpoint = endpoint.split('?')[0] # Remove query params
            self.api_stats[clean_endpoint] = self.api_stats.get(clean_endpoint, 0) + 1
            self.api_calls_log.append((datetime.now(), clean_endpoint))
            
            # Keep log manageable (last 1000 calls)
            if len(self.api_calls_log) > 1000:
                self.api_calls_log.pop(0)

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
            for ts, _ in self.api_calls_log:
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
            for ts, _ in self.api_calls_log:
                if ts > cutoff:
                    hour_key = ts.strftime("%Y-%m-%d %H:00")
                    hourly[hour_key] = hourly.get(hour_key, 0) + 1
            return hourly

    def get_errors(self, limit=50):
        """Returns recent error logs."""
        with self._lock:
            return sorted(self.error_logs, key=lambda x: x['timestamp'], reverse=True)[:limit]

# Global instance for easy access
monitor = AppMonitor()
