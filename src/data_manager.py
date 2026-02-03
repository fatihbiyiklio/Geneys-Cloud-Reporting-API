import threading
import time
from datetime import datetime, timedelta, timezone
from src.api import GenesysAPI
from src.processor import process_observations, process_daily_stats

class DataManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DataManager, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, api_client=None, presence_map=None):
        if self._initialized:
            if api_client and not self.api:
                self.api = GenesysAPI(api_client)
            if presence_map:
                self.presence_map = presence_map
            return

        self.api = GenesysAPI(api_client) if api_client else None
        self.presence_map = presence_map or {}
        self.queues_map = {}
        
        # Data storage
        self.obs_data_cache = {}
        self.daily_data_cache = {}
        self.last_update_time = 0
        
        # Threading
        self.stop_event = threading.Event()
        self.thread = None
        self._initialized = True

    def start(self, queues_map):
        self.queues_map = queues_map
        if self.thread and self.thread.is_alive():
            return
        
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._update_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join()

    def update_api_client(self, api_client, presence_map=None):
        self.api = GenesysAPI(api_client)
        if presence_map:
            self.presence_map = presence_map

    def _update_loop(self):
        while not self.stop_event.is_set():
            if self.api and self.queues_map:
                try:
                    self._fetch_all_data()
                except Exception as e:
                    print(f"DataManager Error: {e}")
            
            # Wait for 10 seconds before next update
            time.sleep(10)

    def _fetch_all_data(self):
        # print(f"DataManager: Fetching data for {len(self.queues_map)} queues...")
        q_ids = list(self.queues_map.values())
        id_map = {v: k for k, v in self.queues_map.items()}
        
        # 1. Observations (Live)
        obs_response = self.api.get_queue_observations(q_ids)
        obs_data_list = process_observations(obs_response, id_map, presence_map=self.presence_map)
        self.obs_data_cache = {item['Queue']: item for item in obs_data_list}
        
        # 2. Daily Stats (Live - Today)
        now_local = datetime.now()
        start_local = datetime.combine(now_local.date(), datetime.min.time())
        start_utc = start_local - timedelta(hours=3)
        end_utc = datetime.now(timezone.utc)
        query_interval = f"{start_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
        
        daily_response = self.api.get_queue_daily_stats(q_ids, interval=query_interval)
        self.daily_data_cache = process_daily_stats(daily_response, id_map)
        
        self.last_update_time = time.time()
        # print(f"DataManager: Update complete at {datetime.now().strftime('%H:%M:%S')}")

    def get_data(self, requested_queues):
        """Returns filtered data for specific queues."""
        obs = {q: self.obs_data_cache.get(q) for q in requested_queues if q in self.obs_data_cache}
        daily = {q: self.daily_data_cache.get(q) for q in requested_queues if q in self.daily_data_cache}
        return obs, daily, self.last_update_time
