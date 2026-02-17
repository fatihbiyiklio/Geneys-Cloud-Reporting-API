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
    MAX_QUEUE_MEMBERS_CACHE = 50   # Max queues to cache members for (reduced from 100)
    MAX_AGENT_DETAILS_CACHE = 50   # Max queues for agent details (reduced from 100)
    MAX_OBS_DATA_CACHE = 100       # Max queues for observations cache (reduced from 200)
    MAX_DAILY_DATA_CACHE = 100     # Max queues for daily stats cache (reduced from 200)
    MAX_ROUTING_ACTIVITY_CACHE = 100  # Max queues for routing activity cache
    CACHE_CLEANUP_INTERVAL = 120   # 2 minutes (reduced from 5 minutes)
    
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
        self.routing_activity_cache = {}
        self.agent_details_cache = {}
        self.queue_members_cache = {} 
        self.last_member_refresh = 0
        self.last_daily_refresh = 0
        self.last_daily_interval_key = None
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
            self.thread.join(timeout=5)
            self.thread = None
        # Clear caches on stop to free memory
        self.obs_data_cache = {}
        self.daily_data_cache = {}
        self.routing_activity_cache = {}
        self.agent_details_cache = {}
        self.queue_members_cache = {}
        self.error_log = []

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

    def _local_today_utc_interval(self):
        try:
            offset_hours = float(self.utc_offset)
        except Exception:
            offset_hours = 0.0
        org_tz = timezone(timedelta(hours=offset_hours))
        now_local = datetime.now(org_tz)
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_local.astimezone(timezone.utc), now_local.astimezone(timezone.utc)

    @staticmethod
    def _parse_iso_ts(value):
        if not value:
            return 0.0
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

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
        with self._lock:
            if (current_time - self.last_cache_cleanup) < self.CACHE_CLEANUP_INTERVAL:
                return

            # Get currently active queue names for filtering
            active_queue_names = set(self.queues_map.keys())
            active_agent_queue_names = set(self.agent_queues_map.keys())

            # Trim obs_data_cache - remove entries for queues no longer monitored
            if self.obs_data_cache:
                stale_obs_keys = [k for k in self.obs_data_cache.keys() if k not in active_queue_names]
                for k in stale_obs_keys:
                    self.obs_data_cache.pop(k, None)
                # Also enforce max size
                if len(self.obs_data_cache) > self.MAX_OBS_DATA_CACHE:
                    keys_to_remove = list(self.obs_data_cache.keys())[:-self.MAX_OBS_DATA_CACHE]
                    for k in keys_to_remove:
                        self.obs_data_cache.pop(k, None)

            # Trim daily_data_cache - remove entries for queues no longer monitored
            if self.daily_data_cache:
                stale_daily_keys = [k for k in self.daily_data_cache.keys() if k not in active_queue_names]
                for k in stale_daily_keys:
                    self.daily_data_cache.pop(k, None)
                # Also enforce max size
                if len(self.daily_data_cache) > self.MAX_DAILY_DATA_CACHE:
                    keys_to_remove = list(self.daily_data_cache.keys())[:-self.MAX_DAILY_DATA_CACHE]
                    for k in keys_to_remove:
                        self.daily_data_cache.pop(k, None)

            # Trim routing_activity_cache - remove entries for queues no longer monitored
            if self.routing_activity_cache:
                stale_routing_keys = [k for k in self.routing_activity_cache.keys() if k not in active_queue_names]
                for k in stale_routing_keys:
                    self.routing_activity_cache.pop(k, None)
                if len(self.routing_activity_cache) > self.MAX_ROUTING_ACTIVITY_CACHE:
                    keys_to_remove = list(self.routing_activity_cache.keys())[:-self.MAX_ROUTING_ACTIVITY_CACHE]
                    for k in keys_to_remove:
                        self.routing_activity_cache.pop(k, None)

            # Trim queue_members_cache to max size
            if len(self.queue_members_cache) > self.MAX_QUEUE_MEMBERS_CACHE:
                # Keep only the most recently used
                keys_to_remove = list(self.queue_members_cache.keys())[:-self.MAX_QUEUE_MEMBERS_CACHE]
                for k in keys_to_remove:
                    self.queue_members_cache.pop(k, None)

            # Trim agent_details_cache - remove entries for queues no longer monitored
            if self.agent_details_cache:
                stale_agent_keys = [k for k in self.agent_details_cache.keys() if k not in active_agent_queue_names]
                for k in stale_agent_keys:
                    self.agent_details_cache.pop(k, None)
                # Also enforce max size
                if len(self.agent_details_cache) > self.MAX_AGENT_DETAILS_CACHE:
                    keys_to_remove = list(self.agent_details_cache.keys())[:-self.MAX_AGENT_DETAILS_CACHE]
                    for k in keys_to_remove:
                        self.agent_details_cache.pop(k, None)

            self.last_cache_cleanup = current_time
    
    def _fetch_all_data(self):
        # Periodic cache cleanup
        self._cleanup_old_caches()

        with self._lock:
            q_ids = list(self.queues_map.values())
            id_map = {v: k for k, v in self.queues_map.items()}
            monitored_queue_names = set(id_map.values())
            agent_q_ids = list(self.agent_queues_map.values())
            agent_id_map = {v: k for k, v in self.agent_queues_map.items()}
            current_time = time.time()
            last_daily_refresh = self.last_daily_refresh
            last_member_refresh = self.last_member_refresh
            member_cache_snapshot = self.queue_members_cache.copy()

        # 1. Observations (Live Metrics) - direct overwrite, no fallback retention
        if q_ids:
            try:
                obs_response = self.api.get_queue_observations(q_ids)
                from src.processor import process_observations
                obs_data_list = process_observations(obs_response, id_map, presence_map=self.presence_map) or []
                new_obs = {}
                for item in obs_data_list:
                    q_name = item.get("Queue")
                    if q_name:
                        new_obs[q_name] = item
                with self._lock:
                    merged_obs = {q: v for q, v in self.obs_data_cache.items() if q not in monitored_queue_names}
                    merged_obs.update(new_obs)
                    self.obs_data_cache = merged_obs
            except Exception as e:
                self._log_error(f"Observation refresh error: {e}")
                with self._lock:
                    self.obs_data_cache = {
                        q: v for q, v in self.obs_data_cache.items() if q not in monitored_queue_names
                    }
        else:
            with self._lock:
                self.obs_data_cache = {}

        # 1.5 Routing activity - direct overwrite, no grace/fallback retention
        if q_ids:
            try:
                routing_response = self.api.get_routing_activity(q_ids)
                routing_results = routing_response.get("results") if isinstance(routing_response, dict) else []
                rebuilt = {}
                for result in (routing_results or []):
                    group = result.get("group") or {}
                    q_id = group.get("queueId") or group.get("queue_id")
                    q_name = id_map.get(q_id)
                    if not q_name:
                        continue

                    entities = result.get("entities") or []
                    q_users = {}
                    for ent in entities:
                        uid = str(ent.get("userId") or ent.get("user_id") or "").strip()
                        if not uid:
                            continue

                        normalized = {
                            "user_id": uid,
                            "queue_id": q_id,
                            "routing_status": ent.get("routingStatus") or ent.get("routing_status"),
                            "system_presence": ent.get("systemPresence") or ent.get("system_presence"),
                            "organization_presence_id": ent.get("organizationPresenceId") or ent.get("organization_presence_id"),
                            "activity_date": ent.get("activityDate") or ent.get("activity_date"),
                        }

                        prev = q_users.get(uid) or {}
                        prev_ts = self._parse_iso_ts(prev.get("activity_date"))
                        curr_ts = self._parse_iso_ts(normalized.get("activity_date"))
                        if prev and (curr_ts < prev_ts):
                            continue
                        q_users[uid] = normalized
                    rebuilt[q_name] = q_users

                with self._lock:
                    merged = {q: v for q, v in self.routing_activity_cache.items() if q not in monitored_queue_names}
                    for q_name in monitored_queue_names:
                        merged[q_name] = dict(rebuilt.get(q_name) or {})
                    self.routing_activity_cache = merged
            except Exception as e:
                self._log_error(f"Routing activity refresh error: {e}")
                with self._lock:
                    merged = {q: v for q, v in self.routing_activity_cache.items() if q not in monitored_queue_names}
                    for q_name in monitored_queue_names:
                        merged[q_name] = {}
                    self.routing_activity_cache = merged
        else:
            with self._lock:
                self.routing_activity_cache = {}

        # 2. Daily Stats
        # Keep daily metrics in sync with live refresh cadence (minimum 10s).
        try:
            daily_refresh_s = max(10, int(self.refresh_interval))
        except Exception:
            daily_refresh_s = 10
        if q_ids and (current_time - last_daily_refresh >= daily_refresh_s):
            try:
                start_utc, end_utc = self._local_today_utc_interval()
                query_interval = f"{start_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
                daily_interval_key = start_utc.strftime("%Y-%m-%d")

                daily_response = self.api.get_queue_daily_stats(q_ids, interval=query_interval)
                from src.processor import process_daily_stats
                new_daily = process_daily_stats(daily_response, id_map) if daily_response else {}
                new_daily = new_daily or {}
                with self._lock:
                    preserved = {q: v for q, v in self.daily_data_cache.items() if q not in monitored_queue_names}
                    preserved.update(new_daily)
                    self.daily_data_cache = preserved
                    self.last_daily_interval_key = daily_interval_key
                    self.last_daily_refresh = current_time
            except Exception as e:
                self._log_error(f"Daily stats refresh error: {e}")
                with self._lock:
                    self.daily_data_cache = {
                        q: v for q, v in self.daily_data_cache.items() if q not in monitored_queue_names
                    }
        elif not q_ids:
            with self._lock:
                self.daily_data_cache = {}

        # 3. Agent Details
        # Refresh membership every 30 mins
        missing_some = any(q_id not in member_cache_snapshot for q_id in agent_q_ids)
        refresh_threshold = 60 if missing_some else 3600

        if (current_time - last_member_refresh > refresh_threshold) and agent_q_ids:
            new_cache = member_cache_snapshot.copy()
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
            with self._lock:
                self.queue_members_cache = new_cache
                self.last_member_refresh = current_time

        with self._lock:
            queue_members_snapshot = {q_id: list(self.queue_members_cache.get(q_id, [])) for q_id in agent_q_ids}

        # User Status Scan
        unique_user_ids = set()
        for q_id in agent_q_ids:
            for m in queue_members_snapshot.get(q_id, []):
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
            mems = queue_members_snapshot.get(q_id, [])
            items = []
            for m in mems:
                u_id = m['id']
                st = status_map.get(u_id) or {}
                items.append({
                    'id': u_id,
                    'user': {'id': u_id, 'name': m['name'], 'presence': st.get('presence', {})},
                    'routingStatus': st.get('routingStatus', {})
                })
            temp_cache[q_name] = items

        with self._lock:
            if temp_cache:
                self.agent_details_cache = temp_cache
            elif not agent_q_ids:
                self.agent_details_cache = {}
            self.last_update_time = time.time()

    def get_data(self, requested_queues):
        with self._lock:
            obs = {q: self.obs_data_cache.get(q) for q in requested_queues if q in self.obs_data_cache}
            daily = {q: self.daily_data_cache.get(q) for q in requested_queues if q in self.daily_data_cache}
            last_update = self.last_update_time
        return obs, daily, last_update

    def get_routing_activity(self, requested_queues):
        with self._lock:
            routing = {}
            for q in requested_queues:
                if q not in self.routing_activity_cache:
                    continue
                q_rows = self.routing_activity_cache.get(q) or {}
                routing[q] = {uid: dict(row or {}) for uid, row in q_rows.items()}
        return routing

    def get_agent_details(self, requested_queues):
        with self._lock:
            agents = {}
            for q in requested_queues:
                if q in self.agent_details_cache:
                    agents[q] = list(self.agent_details_cache[q])
        return agents
