import threading
import time
from datetime import datetime, timedelta, timezone
from src.api import GenesysAPI
from src.monitor import monitor
from src.processor import process_observations, process_daily_stats

class DataManager:
    """
    Manages background data fetching from Genesys API.
    Designed to work with Streamlit's @st.cache_resource.
    """
    MAX_QUEUE_MEMBERS_CACHE = 100  # Max queues to cache members for
    MAX_AGENT_DETAILS_CACHE = 100  # Max queues for agent details
    CACHE_CLEANUP_INTERVAL = 300   # 5 minutes
    
    def __init__(self, api_client=None, presence_map=None):
        self.api = GenesysAPI(api_client) if api_client else None
        self.presence_map = presence_map or {}
        self.queues_map = {}
        self.agent_queues_map = {}
        self.utc_offset = 3
        self.refresh_interval = 10
        self.enabled = True
        
        # Data storage
        self.obs_data_cache = {}
        self.daily_data_cache = {}
        self.agent_details_cache = {}
        self.queue_members_cache = {} 
        self.last_member_refresh = 0
        self.last_daily_refresh = 0
        self.last_update_time = 0
        self.last_cache_cleanup = 0
        self.error_log = [] # For console sync in app.py
        
        # Threading
        self.stop_event = threading.Event()
        self.thread = None
        self._lock = threading.Lock()

    def is_running(self):
        return self.thread and self.thread.is_alive()

    def start(self, queues_map, agent_queues_map=None):
        """Updates monitoring targets and ensures the background thread is running."""
        with self._lock:
            # Update monitored maps
            self.queues_map = queues_map
            
            if agent_queues_map is not None:
                # Check if targets changed significantly to clear cache
                new_keys = set(agent_queues_map.keys())
                old_keys = set(self.agent_queues_map.keys())
                if new_keys != old_keys:
                    self.queue_members_cache = {}
                self.agent_queues_map = agent_queues_map
            
            # Respect disabled state
            if not self.enabled:
                return

            # Ensure thread is running
            if self.thread and self.thread.is_alive():
                # Already running, maps updated above will be picked up
                return
            
            self.stop_event.clear()
            self.thread = threading.Thread(target=self._update_loop, daemon=True)
            self.thread.start()

    def stop(self):
        self.enabled = False
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=2)
            self.thread = None

    def resume(self):
        self.enabled = True

    def force_stop(self):
        """Hard stop: disable, clear maps, and drop API client."""
        self.stop()
        with self._lock:
            self.api = None
            self.queues_map = {}
            self.agent_queues_map = {}

    def update_api_client(self, api_client, presence_map=None):
        with self._lock:
            self.api = GenesysAPI(api_client) if api_client else None
            if presence_map:
                self.presence_map = presence_map

    def update_settings(self, utc_offset, refresh_interval=None):
        with self._lock:
            self.utc_offset = utc_offset
            if refresh_interval is not None:
                try:
                    refresh_interval = int(refresh_interval)
                except Exception:
                    refresh_interval = 10
                self.refresh_interval = max(1, refresh_interval)

    def _log_error(self, message):
        with self._lock:
            self.error_log.append(message)
            if len(self.error_log) > 100: self.error_log.pop(0)
        monitor.log_error("DataManager", message)

    def _update_loop(self):
        while not self.stop_event.is_set():
            if not self.enabled:
                time.sleep(0.2)
                continue
            if self.api and (self.queues_map or self.agent_queues_map):
                try:
                    self._fetch_all_data()
                except Exception as e:
                    self._log_error(f"Global Update Error: {str(e)}")
                    self._log_error(f"DataManager Loop Error: {e}")
                    time.sleep(10)
            
            # Normal sleep interval
            for _ in range(max(1, int(self.refresh_interval * 5))):
                if self.stop_event.is_set() or not self.enabled:
                    break
                time.sleep(0.2)

    def _cleanup_old_caches(self):
        """Periodically trim caches to prevent memory bloat."""
        current_time = time.time()
        if (current_time - self.last_cache_cleanup) < self.CACHE_CLEANUP_INTERVAL:
            return
        
        # Trim queue_members_cache to max size
        if len(self.queue_members_cache) > self.MAX_QUEUE_MEMBERS_CACHE:
            # Keep only the most recently used
            keys_to_remove = list(self.queue_members_cache.keys())[:-self.MAX_QUEUE_MEMBERS_CACHE]
            for k in keys_to_remove:
                self.queue_members_cache.pop(k, None)
        
        # Trim agent_details_cache
        if len(self.agent_details_cache) > self.MAX_AGENT_DETAILS_CACHE:
            keys_to_remove = list(self.agent_details_cache.keys())[:-self.MAX_AGENT_DETAILS_CACHE]
            for k in keys_to_remove:
                self.agent_details_cache.pop(k, None)
        
        self.last_cache_cleanup = current_time
    
    def _fetch_all_data(self):
        # Periodic cache cleanup
        self._cleanup_old_caches()
        
        q_ids = list(self.queues_map.values())
        id_map = {v: k for k, v in self.queues_map.items()}
        
        # Debug Log to confirm optimization
        # (debug log removed for build)
        
        agent_q_ids = list(self.agent_queues_map.values())
        agent_id_map = {v: k for k, v in self.agent_queues_map.items()}
        
        # 1. Observations (Live Metrics)
        if q_ids:
            obs_response = self.api.get_queue_observations(q_ids)
            from src.processor import process_observations
            obs_data_list = process_observations(obs_response, id_map, presence_map=self.presence_map)
            self.obs_data_cache = {item['Queue']: item for item in obs_data_list}
        else:
            self.obs_data_cache = {}
        
        # 2. Daily Stats (fetch less frequently to reduce API load)
        current_time = time.time()
        if q_ids and (current_time - self.last_daily_refresh >= 60):
            now_local = datetime.now()
            start_local = datetime.combine(now_local.date(), datetime.min.time())
            start_utc = start_local - timedelta(hours=self.utc_offset)
            end_utc = datetime.now(timezone.utc)
            query_interval = f"{start_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
            
            daily_response = self.api.get_queue_daily_stats(q_ids, interval=query_interval)
            from src.processor import process_daily_stats
            self.daily_data_cache = process_daily_stats(daily_response, id_map)
            self.last_daily_refresh = current_time
        elif not q_ids:
            self.daily_data_cache = {}
        
        # 3. Agent Details
        # Refresh membership every 30 mins
        missing_some = any(q_id not in self.queue_members_cache for q_id in agent_q_ids)
        refresh_threshold = 60 if missing_some else 3600
        
        if (current_time - self.last_member_refresh > refresh_threshold) and agent_q_ids:
            new_cache = self.queue_members_cache.copy()
            for q_id in agent_q_ids:
                try:
                    mems = self.api.get_queue_members(q_id)
                    processed = []
                    for m in mems:
                        u = m.get('user', {})
                        u_id = u.get('id') or m.get('id')
                        u_name = u.get('name') or m.get('name', 'Unknown')
                        if u_id:
                            processed.append({'id': u_id, 'name': u_name})
                    new_cache[q_id] = processed
                except Exception as e:
                    self._log_error(f"Error fetching members for {q_id}: {str(e)}")
                    self._log_error(f"Error fetching members for {q_id}: {e}")
            self.queue_members_cache = new_cache
            self.last_member_refresh = current_time

        # User Status Scan
        unique_user_ids = set()
        for q_id in agent_q_ids:
            for m in self.queue_members_cache.get(q_id, []):
                unique_user_ids.add(m['id'])
        
        status_map = {}
        if agent_q_ids and unique_user_ids:
            try:
                status_data = self.api.get_users_status_scan(target_user_ids=unique_user_ids)
                pres_map = status_data.get('presence', {})
                rout_map = status_data.get('routing', {})

                for u_id in unique_user_ids:
                    pres_obj = pres_map.get(u_id, {})
                    pid = pres_obj.get('presenceDefinition', {}).get('id')
                    pi = self.presence_map.get(pid, {})
                    sysp = pres_obj.get('presenceDefinition', {}).get('systemPresence', 'OFFLINE')
                    label = pi.get('label', sysp)
                    
                    final_pres = {
                        "presenceDefinition": {"id": pid, "systemPresence": sysp, "label": label},
                        "modifiedDate": pres_obj.get('modifiedDate')
                    }
                    rout_obj = rout_map.get(u_id, {})
                    final_rout = {"status": rout_obj.get('status', 'OFF_QUEUE'), "startTime": rout_obj.get('startTime')}
                    status_map[u_id] = {'presence': final_pres, 'routingStatus': final_rout}
            except Exception as e:
                self._log_error(f"User Scan Error: {str(e)}")
                self._log_error(f"Error updating users: {e}")
            
        # Detail Cache reconstruction
        temp_cache = {}
        for q_id in agent_q_ids:
            q_name = agent_id_map.get(q_id)
            mems = self.queue_members_cache.get(q_id, [])
            items = []
            for m in mems:
                u_id = m['id']
                st = status_map.get(u_id, {})
                items.append({
                    'id': u_id,
                    'user': {'id': u_id, 'name': m['name'], 'presence': st.get('presence', {})},
                    'routingStatus': st.get('routingStatus', {})
                })
            temp_cache[q_name] = items
        
        self.agent_details_cache = temp_cache
        self.last_update_time = time.time()

    def get_data(self, requested_queues):
        obs = {q: self.obs_data_cache.get(q) for q in requested_queues if q in self.obs_data_cache}
        daily = {q: self.daily_data_cache.get(q) for q in requested_queues if q in self.daily_data_cache}
        return obs, daily, self.last_update_time

    def get_agent_details(self, requested_queues):
        agents = {}
        for q in requested_queues:
            if q in self.agent_details_cache:
                agents[q] = self.agent_details_cache[q]
        return agents
