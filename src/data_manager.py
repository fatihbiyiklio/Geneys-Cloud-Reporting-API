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
        self.agent_details_cache = {}
        self.queue_members_cache = {} # Map q_id -> list of member dicts {id, name}
        self.last_member_refresh = 0
        self.last_update_time = 0
        
        # Threading
        self.stop_event = threading.Event()
        self.thread = None
        self._initialized = True

    def start(self, queues_map, agent_queues_map=None):
        self.queues_map = queues_map
        
        # Priority: Use provided map, or initialize to empty if never set
        if agent_queues_map is not None:
            self.agent_queues_map = agent_queues_map
        elif not hasattr(self, 'agent_queues_map'):
            self.agent_queues_map = {}
        
        if self.thread and self.thread.is_alive():
            return
        
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._update_loop, daemon=True)
        self.thread.start()
        # Immediate fetch will be triggered by the first iteration of _update_loop

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join()
        
    def _log_error(self, message):
        with self.lock:
            self.error_log.append(f"{datetime.now().strftime('%H:%M:%S')} - {message}")
            # Keep log size manageable
            if len(self.error_log) > 50:
                self.error_log.pop(0)

    def update_api_client(self, api_client, presence_map=None):
        try:
            self.api = GenesysAPI(api_client)
            if presence_map:
                self.presence_map = presence_map
        except Exception as e:
            self._log_error(f"API Client Update Error: {str(e)}")
            print(f"Error updating API client: {e}")

    def _update_loop(self):
        while not self.stop_event.is_set():
            if self.api and self.queues_map:
                try:
                    self._fetch_all_data()
                except Exception as e:
                    self._log_error(f"Global Update Error: {str(e)}")
                    print(f"Global Update Error: {e}")
                    time.sleep(30) # Wait bit longer on error
            
            # Wait for 10 seconds before next update
            time.sleep(10)

    def _fetch_all_data(self):
        q_ids = list(self.queues_map.values())
        id_map = {v: k for k, v in self.queues_map.items()}
        
        # Queues for Agent monitoring (Limited for performance)
        agent_q_ids = list(self.agent_queues_map.values())
        agent_id_map = {v: k for k, v in self.agent_queues_map.items()}
        
        # 1. Observations (Live Metrics) - ALL QUEUES (Bulk is efficient)
        obs_response = self.api.get_queue_observations(q_ids)
        obs_data_list = process_observations(obs_response, id_map, presence_map=self.presence_map)
        self.obs_data_cache = {item['Queue']: item for item in obs_data_list}
        
        # 2. Daily Stats (Live - Today) - ALL QUEUES
        now_local = datetime.now()
        start_local = datetime.combine(now_local.date(), datetime.min.time())
        start_utc = start_local - timedelta(hours=3)
        end_utc = datetime.now(timezone.utc)
        query_interval = f"{start_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
        
        daily_response = self.api.get_queue_daily_stats(q_ids, interval=query_interval)
        self.daily_data_cache = process_daily_stats(daily_response, id_map)
        
        # 3. Agent Details (Live Status) - SELECTIVE OPTIMIZATION
        current_time = time.time()
        
        # Refresh membership cache every 30 mins (1800s)
        # Only retry quickly (60s) if we are strictly missing data for a queue
        missing_some = any(q_id not in self.queue_members_cache for q_id in agent_q_ids)
        refresh_threshold = 60 if missing_some else 1800
        
        if (current_time - self.last_member_refresh > refresh_threshold) and agent_q_ids:
            new_cache = self.queue_members_cache.copy()
            for q_id in agent_q_ids:
                try:
                    # Removed artificial sleep to prevent stalling
                    mems = self.api.get_queue_members(q_id)
                    # Removed verbose logging
                    
                    processed = []
                    for m in mems:
                        u = m.get('user', {})
                        u_id = u.get('id') or m.get('id') # Resilient lookup
                        u_name = u.get('name') or m.get('name', 'Unknown')
                        if u_id:
                            processed.append({'id': u_id, 'name': u_name})
                    # Record even if empty to prevent infinite retry
                    new_cache[q_id] = processed
                except Exception as e:
                    self._log_error(f"Error fetching members for {q_id}: {str(e)}")
                    print(f"DataManager: Error fetching members for {q_id}: {e}")
            self.queue_members_cache = new_cache
            self.last_member_refresh = current_time

        unique_user_ids = set()
        for q_id in agent_q_ids:
            for m in self.queue_members_cache.get(q_id, []):
                unique_user_ids.add(m['id'])
        
        status_map = {}
        # Scan for active statuses (Push model) instead of iterating IDs (Pull model)
        if agent_q_ids:
            # print(f"DataManager: Scanning active status for {len(unique_user_ids)} potential users")
            try:
                # get_users_status_scan finds everyone active in the last 12h
                status_data = self.api.get_users_status_scan() 
                pres_map = status_data.get('presence', {})
                rout_map = status_data.get('routing', {})

                for u_id in unique_user_ids:
                    # 1. Presence
                    pres_obj = pres_map.get(u_id, {})
                    pid = pres_obj.get('presenceDefinition', {}).get('id')
                    pi = self.presence_map.get(pid, {})
                    sysp = pres_obj.get('presenceDefinition', {}).get('systemPresence', 'OFFLINE')
                    label = pi.get('label', sysp) # Use our cached label map if possible
                    
                    final_pres = {
                        "presenceDefinition": {"id": pid, "systemPresence": sysp, "label": label},
                        "modifiedDate": pres_obj.get('modifiedDate')
                    }

                    # 2. Routing
                    rout_obj = rout_map.get(u_id, {})
                    final_rout = {"status": rout_obj.get('status', 'OFF_QUEUE'), "startTime": rout_obj.get('startTime')}

                    status_map[u_id] = {'presence': final_pres, 'routingStatus': final_rout}
            except Exception as e:
                self._log_error(f"User Scan Error: {str(e)}")
                print(f"Error updating users: {e}")
            
        # Reconstruct detail cache
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
        # print(f"DataManager: Update complete at {datetime.now().strftime('%H:%M:%S')}")

    def get_data(self, requested_queues):
        """Returns filtered data for specific queues."""
        obs = {q: self.obs_data_cache.get(q) for q in requested_queues if q in self.obs_data_cache}
        daily = {q: self.daily_data_cache.get(q) for q in requested_queues if q in self.daily_data_cache}
        return obs, daily, self.last_update_time

    def get_agent_details(self, requested_queues):
        """Returns agent details for specific queues."""
        agents = {}
        for q in requested_queues:
            if q in self.agent_details_cache:
                for agent in self.agent_details_cache[q]:
                    # Deduplication strategy: handled by UI usually, but sending raw lists per queue here
                    pass
                agents[q] = self.agent_details_cache[q]
        return agents
