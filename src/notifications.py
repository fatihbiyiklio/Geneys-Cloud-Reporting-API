import json
import threading
import time
from datetime import datetime, timezone

import websocket

from src.api import GenesysAPI


class NotificationManager:
    """Manages a single Genesys Notifications channel and caches waiting calls."""
    def __init__(self):
        self.api = None
        self.queues_map = {}
        self.queue_id_to_name = {}
        self.channel_id = None
        self.connect_uri = None
        self.subscribed_topics = []
        self._ws = None
        self._thread = None
        self._resub_thread = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self.connected = False
        self.last_event_ts = 0
        self.last_message_ts = 0
        self.last_topic = ""
        self.last_event_preview = ""
        self.channel_created_ts = 0
        # key: (conversation_id, queue_id)
        self.waiting_calls = {}

    def update_client(self, api_client, queues_map):
        self.api = GenesysAPI(api_client) if api_client else None
        self.queues_map = queues_map or {}
        self.queue_id_to_name = {v: k for k, v in self.queues_map.items()}

    def is_running(self):
        return self._thread and self._thread.is_alive()

    def start(self, queue_ids):
        if not self.api:
            return False
        topics = [f"v2.routing.queues.{qid}.conversations" for qid in queue_ids if qid]
        topics = sorted(set(topics))

        with self._lock:
            if self.subscribed_topics == topics and self.is_running():
                return True
            self.subscribed_topics = topics

        self._stop_event.clear()
        if not self._ensure_channel_and_subscribe(topics):
            return False
        self._start_ws()
        return True

    def stop(self):
        self._stop_event.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass
        self.connected = False

    def get_waiting_calls(self, max_age_seconds=600):
        now = time.time()
        with self._lock:
            # Prune stale entries
            stale_keys = [k for k, v in self.waiting_calls.items() if (now - v.get("last_update", 0)) > max_age_seconds]
            for k in stale_keys:
                self.waiting_calls.pop(k, None)
            return list(self.waiting_calls.values())

    def upsert_waiting_calls(self, calls):
        """Adds or updates waiting calls cache. Expects list of dicts with conversation_id and queue_id."""
        now = time.time()
        with self._lock:
            for c in calls:
                conv_id = c.get("conversation_id")
                queue_id = c.get("queue_id")
                if not conv_id or not queue_id:
                    continue
                key = (conv_id, queue_id)
                self.waiting_calls[key] = {
                    "conversation_id": conv_id,
                    "queue_id": queue_id,
                    "queue_name": c.get("queue_name") or self.queue_id_to_name.get(queue_id, queue_id),
                    "wait_seconds": c.get("wait_seconds"),
                    "phone": c.get("phone"),
                    "last_update": now,
                }

    def _ensure_channel_and_subscribe(self, topics):
        try:
            channel = self.api.create_notification_channel()
            self.channel_id = channel.get("id")
            self.connect_uri = channel.get("connectUri") or channel.get("connectUriSecured")
            if not self.channel_id or not self.connect_uri:
                return False
            self.api.subscribe_notification_channel(self.channel_id, [{"id": t} for t in topics])
            with self._lock:
                self.channel_created_ts = time.time()
            return True
        except Exception:
            return False

    def _start_ws(self):
        if self.is_running():
            return

        def on_open(ws):
            self.connected = True

        def on_close(ws, status_code, msg):
            self.connected = False
            if not self._stop_event.is_set():
                time.sleep(2)
                self._start_ws()

        def on_error(ws, error):
            self.connected = False

        def on_message(ws, message):
            try:
                payload = json.loads(message)
            except Exception:
                return
            self.last_message_ts = time.time()
            # Heartbeat or non-topic messages
            topic = payload.get("topicName")
            self.last_topic = topic or ""
            if not topic or not topic.startswith("v2.routing.queues."):
                return
            event = payload.get("eventBody") or {}
            try:
                self.last_event_preview = json.dumps(event)[:1000]
            except Exception:
                self.last_event_preview = ""
            self._handle_conversation_event(topic, event)

        self._ws = websocket.WebSocketApp(
            self.connect_uri,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            on_message=on_message,
        )

        def run():
            # Keep trying until stop requested
            while not self._stop_event.is_set():
                try:
                    self._ws.run_forever(ping_interval=30, ping_timeout=10)
                except Exception:
                    pass
                if self._stop_event.is_set():
                    break
                time.sleep(2)

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

        # Start re-subscribe timer thread
        if not self._resub_thread or not self._resub_thread.is_alive():
            self._resub_thread = threading.Thread(target=self._resubscribe_loop, daemon=True)
            self._resub_thread.start()

    def _resubscribe_loop(self):
        # Re-subscribe before 24h to avoid expiry
        while not self._stop_event.is_set():
            time.sleep(60)
            with self._lock:
                created = getattr(self, "channel_created_ts", 0)
                topics = list(self.subscribed_topics)
            if not topics or not created:
                continue
            if time.time() - created >= 22 * 3600:
                try:
                    # Recreate channel + subscribe
                    self._ensure_channel_and_subscribe(topics)
                    # Restart WS with new connect URI
                    try:
                        if self._ws:
                            self._ws.close()
                    except Exception:
                        pass
                    self._start_ws()
                except Exception:
                    pass

    def _handle_conversation_event(self, topic, event):
        conversation_id = event.get("id") or event.get("conversationId")
        if not conversation_id:
            return
        self.last_event_ts = time.time()

        participants = event.get("participants", []) or []
        agent_connected = False
        for p in participants:
            purpose = (p.get("purpose") or "").lower()
            if purpose in ["agent", "user"]:
                for s in p.get("sessions", []) or []:
                    state = (s.get("state") or "").lower()
                    if state in ["connected", "alerting"]:
                        agent_connected = True
                        break
            if agent_connected:
                break

        # Extract queue participants
        for p in participants:
            purpose = (p.get("purpose") or "").lower()
            if purpose not in ["acd", "queue"]:
                continue
            queue_id = p.get("queueId") or p.get("participantId")
            if not queue_id:
                continue

            in_queue = False
            wait_seconds = None
            for s in p.get("sessions", []) or []:
                state = (s.get("state") or "").lower()
                if state in ["connected", "alerting", "offering"] and not s.get("disconnectedTime"):
                    in_queue = True
                    wait_seconds = _parse_wait_seconds(s.get("connectedTime") or s.get("startTime"))
                    break

            if agent_connected:
                in_queue = False

            key = (conversation_id, queue_id)
            with self._lock:
                if in_queue:
                    self.waiting_calls[key] = {
                        "conversation_id": conversation_id,
                        "queue_id": queue_id,
                        "queue_name": self.queue_id_to_name.get(queue_id, queue_id),
                        "wait_seconds": wait_seconds,
                        "phone": _extract_phone(event),
                        "last_update": time.time(),
                    }
                else:
                    self.waiting_calls.pop(key, None)


class AgentNotificationManager:
    """Manages user presence/routing notifications and queue membership cache."""
    MAX_TOPICS_PER_CHANNEL = 1000
    MAX_CHANNELS = 20

    def __init__(self):
        self.api = None
        self.queues_map = {}
        self.queue_id_to_name = {}
        self.users_info = {}
        self.presence_map = {}
        self.queue_members_cache = {}
        self.last_member_refresh = {}
        self.user_presence = {}
        self.user_routing = {}
        self.active_calls = {}
        self.subscribed_topics = []
        self.channels = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._resub_thread = None
        self.last_message_ts = 0
        self.last_event_ts = 0
        self.last_topic = ""
        self.last_event_preview = ""

    def update_client(self, api_client, queues_map, users_info=None, presence_map=None):
        self.api = GenesysAPI(api_client) if api_client else None
        self.queues_map = queues_map or {}
        self.queue_id_to_name = {v: k for k, v in self.queues_map.items()}
        self.users_info = users_info or {}
        self.presence_map = presence_map or {}

    def is_running(self):
        return any(ch.get("thread") and ch["thread"].is_alive() for ch in self.channels)

    @property
    def connected(self):
        return any(ch.get("connected") for ch in self.channels)

    def stop(self):
        self._stop_event.set()
        self._stop_channels()

    def _stop_channels(self):
        for ch in self.channels:
            try:
                if ch.get("stop_event"):
                    ch["stop_event"].set()
                if ch.get("ws"):
                    ch["ws"].close()
            except Exception:
                pass
        self.channels = []

    def ensure_members(self, queue_ids):
        """Refreshes queue membership cache (low frequency)."""
        if not self.api:
            return {}
        now = time.time()
        for qid in queue_ids:
            if not qid:
                continue
            last = self.last_member_refresh.get(qid, 0)
            threshold = 60 if qid not in self.queue_members_cache else 1800
            if (now - last) < threshold:
                continue
            try:
                mems = self.api.get_queue_members(qid)
                processed = []
                for m in mems:
                    u = m.get("user", {})
                    u_id = u.get("id") or m.get("id")
                    u_name = u.get("name") or m.get("name")
                    if not u_name and u_id:
                        u_name = self.users_info.get(u_id, {}).get("name")
                    if u_id:
                        processed.append({"id": u_id, "name": u_name or "Unknown"})
                self.queue_members_cache[qid] = processed
                self.last_member_refresh[qid] = now
            except Exception:
                # Keep last cached data on error
                pass
        return {qid: self.queue_members_cache.get(qid, []) for qid in queue_ids if qid}

    def start(self, user_ids):
        if not self.api:
            return False
        topics = []
        for uid in user_ids:
            if not uid:
                continue
            topics.append(f"v2.users.{uid}.presence")
            topics.append(f"v2.users.{uid}.routingStatus")
            topics.append(f"v2.users.{uid}.conversations.calls")
        topics = sorted(set(topics))

        with self._lock:
            if self.subscribed_topics == topics and self.is_running():
                return True
            self.subscribed_topics = topics

        self._stop_channels()
        if self._stop_event.is_set():
            self._stop_event.clear()
        if not topics:
            return True

        max_topics = self.MAX_TOPICS_PER_CHANNEL
        chunks = [topics[i:i + max_topics] for i in range(0, len(topics), max_topics)]
        if len(chunks) > self.MAX_CHANNELS:
            chunks = chunks[:self.MAX_CHANNELS]

        for chunk in chunks:
            ch = self._create_channel(chunk)
            if ch:
                self.channels.append(ch)
                self._start_ws(ch)

        if not self._resub_thread or not self._resub_thread.is_alive():
            self._resub_thread = threading.Thread(target=self._resubscribe_loop, daemon=True)
            self._resub_thread.start()

        return bool(self.channels)

    def get_user_presence(self, user_id):
        if not user_id:
            return {}
        p = self.user_presence.get(user_id) or {}
        return _normalize_presence(p, self.presence_map)

    def get_user_routing(self, user_id):
        if not user_id:
            return {}
        return self.user_routing.get(user_id) or {}

    def get_active_calls(self, max_age_seconds=600):
        now = time.time()
        with self._lock:
            stale_keys = [k for k, v in self.active_calls.items() if (now - v.get("last_update", 0)) > max_age_seconds]
            for k in stale_keys:
                self.active_calls.pop(k, None)
            return list(self.active_calls.values())

    def seed_users(self, presence_map, routing_map):
        """Seed caches from a one-time API snapshot."""
        if presence_map:
            for uid, pres in presence_map.items():
                if pres:
                    self.user_presence[uid] = pres
        if routing_map:
            for uid, rout in routing_map.items():
                if rout:
                    self.user_routing[uid] = rout

    def seed_users_missing(self, presence_map, routing_map):
        """Seed caches only for users not already present."""
        if presence_map:
            for uid, pres in presence_map.items():
                if pres and uid not in self.user_presence:
                    self.user_presence[uid] = pres
        if routing_map:
            for uid, rout in routing_map.items():
                if rout and uid not in self.user_routing:
                    self.user_routing[uid] = rout

    def _create_channel(self, topics):
        try:
            channel = self.api.create_notification_channel()
            channel_id = channel.get("id")
            connect_uri = channel.get("connectUri") or channel.get("connectUriSecured")
            if not channel_id or not connect_uri:
                return None
            self.api.subscribe_notification_channel(channel_id, [{"id": t} for t in topics])
            return {
                "channel_id": channel_id,
                "connect_uri": connect_uri,
                "topics": topics,
                "created_ts": time.time(),
                "ws": None,
                "thread": None,
                "connected": False,
                "stop_event": threading.Event(),
            }
        except Exception:
            return None

    def _start_ws(self, ch):
        def on_open(ws):
            ch["connected"] = True

        def on_close(ws, status_code, msg):
            ch["connected"] = False
            if not self._stop_event.is_set() and not ch.get("stop_event").is_set():
                time.sleep(2)
                self._start_ws(ch)

        def on_error(ws, error):
            ch["connected"] = False

        def on_message(ws, message):
            try:
                payload = json.loads(message)
            except Exception:
                return
            self.last_message_ts = time.time()
            topic = payload.get("topicName")
            self.last_topic = topic or ""
            event = payload.get("eventBody") or {}
            try:
                self.last_event_preview = json.dumps(event)[:1000]
            except Exception:
                self.last_event_preview = ""
            if not topic or not topic.startswith("v2.users."):
                return
            self._handle_user_event(topic, event)

        ws = websocket.WebSocketApp(
            ch["connect_uri"],
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            on_message=on_message,
        )
        ch["ws"] = ws

        def run():
            ch_stop = ch.get("stop_event")
            while not self._stop_event.is_set() and not ch_stop.is_set():
                try:
                    ws.run_forever(ping_interval=30, ping_timeout=10)
                except Exception:
                    pass
                if self._stop_event.is_set() or ch_stop.is_set():
                    break
                time.sleep(2)

        ch["thread"] = threading.Thread(target=run, daemon=True)
        ch["thread"].start()

    def _resubscribe_loop(self):
        while not self._stop_event.is_set():
            time.sleep(60)
            for ch in list(self.channels):
                created = ch.get("created_ts", 0)
                topics = ch.get("topics", [])
                if not topics or not created:
                    continue
                if time.time() - created >= 22 * 3600:
                    try:
                        new_ch = self._create_channel(topics)
                        if new_ch:
                            try:
                                if ch.get("stop_event"):
                                    ch["stop_event"].set()
                                if ch.get("ws"):
                                    ch["ws"].close()
                            except Exception:
                                pass
                            self.channels.remove(ch)
                            self.channels.append(new_ch)
                            self._start_ws(new_ch)
                    except Exception:
                        pass

    def _handle_user_event(self, topic, event):
        parts = topic.split(".")
        if len(parts) < 4:
            return
        user_id = parts[2]
        self.last_event_ts = time.time()

        if topic.endswith(".presence"):
            self.user_presence[user_id] = event or {}
        elif topic.endswith(".routingStatus"):
            self.user_routing[user_id] = event or {}
        elif ".conversations" in topic:
            self._handle_call_event(event)

    def _handle_call_event(self, event):
        if not isinstance(event, dict):
            return
        conv_id = event.get("id") or event.get("conversationId")
        if not conv_id:
            return
        participants = event.get("participants") or []
        active = False
        agent_name = None
        media_type = None
        for p in participants:
            if not agent_name and (p.get("purpose") or "").lower() in ["agent", "user"]:
                agent_name = p.get("name") or p.get("user", {}).get("name")
            for s in p.get("sessions", []) or []:
                if not media_type:
                    mt = s.get("mediaType")
                    if mt:
                        media_type = mt
                if _session_is_active(s):
                    active = True
                    break
            if active:
                break
        if event.get("conversationEnd") or not active:
            with self._lock:
                self.active_calls.pop(conv_id, None)
            return

        queue_name = "Aktif"
        queue_id = None
        wait_seconds = None
        for p in participants:
            purpose = (p.get("purpose") or "").lower()
            if purpose in ["acd", "queue"]:
                q_id = p.get("queueId") or p.get("routingQueueId") or p.get("participantId")
                if q_id:
                    queue_id = q_id
                    queue_name = self.queue_id_to_name.get(q_id, queue_name) or p.get("name") or queue_name
                elif p.get("name"):
                    queue_name = p.get("name") or queue_name
                for s in p.get("sessions", []) or []:
                    if wait_seconds is None:
                        wait_seconds = _parse_wait_seconds(s.get("connectedTime") or s.get("startTime"))
                    if _session_is_active(s):
                        wait_seconds = _parse_wait_seconds(s.get("connectedTime") or s.get("startTime"))
                        break
            if wait_seconds is not None:
                break

        if wait_seconds is None:
            wait_seconds = _parse_wait_seconds(event.get("conversationStart"))

        direction = event.get("originatingDirection") or event.get("direction")
        state = _classify_conversation_state(event)

        with self._lock:
            self.active_calls[conv_id] = {
                "conversation_id": conv_id,
                "queue_id": queue_id,
                "queue_name": queue_name,
                "wait_seconds": wait_seconds,
                "phone": _extract_phone(event),
                "direction": direction,
                "state": state,
                "agent_name": agent_name,
                "media_type": media_type,
                "last_update": time.time(),
            }


class GlobalConversationNotificationManager:
    """Notifications for org-wide conversations (calls/chats/messages/etc)."""
    def __init__(self):
        self.api = None
        self.queues_map = {}
        self.queue_id_to_name = {}
        self.channel_id = None
        self.connect_uri = None
        self.subscribed_topics = []
        self._ws = None
        self._thread = None
        self._resub_thread = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self.connected = False
        self.last_event_ts = 0
        self.last_message_ts = 0
        self.last_topic = ""
        self.last_event_preview = ""
        self.channel_created_ts = 0
        self.active_conversations = {}

    def update_client(self, api_client, queues_map):
        self.api = GenesysAPI(api_client) if api_client else None
        self.queues_map = queues_map or {}
        self.queue_id_to_name = {v: k for k, v in self.queues_map.items()}

    def is_running(self):
        return self._thread and self._thread.is_alive()

    def start(self, topics):
        if not self.api:
            return False
        topics = sorted(set(topics or []))
        with self._lock:
            if self.subscribed_topics == topics and self.is_running():
                return True
            self.subscribed_topics = topics

        self._stop_event.clear()
        if not self._ensure_channel_and_subscribe(topics):
            return False
        self._start_ws()
        return True

    def stop(self):
        self._stop_event.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass
        self.connected = False

    def get_active_conversations(self, max_age_seconds=600):
        now = time.time()
        with self._lock:
            stale = [k for k, v in self.active_conversations.items() if (now - v.get("last_update", 0)) > max_age_seconds]
            for k in stale:
                self.active_conversations.pop(k, None)
            return list(self.active_conversations.values())

    def _ensure_channel_and_subscribe(self, topics):
        try:
            channel = self.api.create_notification_channel()
            self.channel_id = channel.get("id")
            self.connect_uri = channel.get("connectUri") or channel.get("connectUriSecured")
            if not self.channel_id or not self.connect_uri:
                return False
            self.api.subscribe_notification_channel(self.channel_id, [{"id": t} for t in topics])
            with self._lock:
                self.channel_created_ts = time.time()
            return True
        except Exception:
            return False

    def _start_ws(self):
        if self.is_running():
            return

        def on_open(ws):
            self.connected = True

        def on_close(ws, status_code, msg):
            self.connected = False
            if not self._stop_event.is_set():
                time.sleep(2)
                self._start_ws()

        def on_error(ws, error):
            self.connected = False

        def on_message(ws, message):
            try:
                payload = json.loads(message)
            except Exception:
                return
            self.last_message_ts = time.time()
            topic = payload.get("topicName")
            self.last_topic = topic or ""
            event = payload.get("eventBody") or {}
            try:
                self.last_event_preview = json.dumps(event)[:1000]
            except Exception:
                self.last_event_preview = ""
            if not topic or not topic.startswith("v2.conversations."):
                return
            self._handle_conversation_event(event)

        self._ws = websocket.WebSocketApp(
            self.connect_uri,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            on_message=on_message,
        )

        def run():
            while not self._stop_event.is_set():
                try:
                    self._ws.run_forever(ping_interval=30, ping_timeout=10)
                except Exception:
                    pass
                if self._stop_event.is_set():
                    break
                time.sleep(2)

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

        if not self._resub_thread or not self._resub_thread.is_alive():
            self._resub_thread = threading.Thread(target=self._resubscribe_loop, daemon=True)
            self._resub_thread.start()

    def _resubscribe_loop(self):
        while not self._stop_event.is_set():
            time.sleep(60)
            with self._lock:
                created = getattr(self, "channel_created_ts", 0)
                topics = list(self.subscribed_topics)
            if not topics or not created:
                continue
            if time.time() - created >= 22 * 3600:
                try:
                    self._ensure_channel_and_subscribe(topics)
                    try:
                        if self._ws:
                            self._ws.close()
                    except Exception:
                        pass
                    self._start_ws()
                except Exception:
                    pass

    def _handle_conversation_event(self, event):
        if not isinstance(event, dict):
            return
        conv_id = event.get("id") or event.get("conversationId")
        if not conv_id:
            return
        self.last_event_ts = time.time()

        active = False
        for p in event.get("participants", []) or []:
            for s in p.get("sessions", []) or []:
                if _session_is_active(s):
                    active = True
                    break
            if active:
                break

        if event.get("conversationEnd") or not active:
            with self._lock:
                self.active_conversations.pop(conv_id, None)
            return

        queue_name = "Aktif"
        queue_id = None
        wait_seconds = None
        for p in event.get("participants", []) or []:
            purpose = (p.get("purpose") or "").lower()
            if purpose in ["acd", "queue"]:
                q_id = p.get("queueId") or p.get("routingQueueId") or p.get("participantId")
                if q_id:
                    queue_id = q_id
                    queue_name = self.queue_id_to_name.get(q_id, queue_name) or p.get("name") or queue_name
                elif p.get("name"):
                    queue_name = p.get("name") or queue_name
                for s in p.get("sessions", []) or []:
                    if wait_seconds is None:
                        wait_seconds = _parse_wait_seconds(s.get("connectedTime") or s.get("startTime"))
                    if _session_is_active(s):
                        wait_seconds = _parse_wait_seconds(s.get("connectedTime") or s.get("startTime"))
                        break
            if wait_seconds is not None:
                break

        if wait_seconds is None:
            wait_seconds = _parse_wait_seconds(event.get("conversationStart"))

        media_type = _extract_media_type(event)
        agent_id, agent_name = _extract_agent_identity(event)
        direction = event.get("originatingDirection") or event.get("direction")
        state = _classify_conversation_state(event)

        with self._lock:
            self.active_conversations[conv_id] = {
                "conversation_id": conv_id,
                "queue_id": queue_id,
                "queue_name": queue_name,
                "wait_seconds": wait_seconds,
                "phone": _extract_phone(event),
                "direction": direction,
                "state": state,
                "agent_id": agent_id,
                "agent_name": agent_name,
                "media_type": media_type,
                "last_update": time.time(),
            }


def _parse_wait_seconds(val):
    if not val:
        return None
    now = datetime.now(timezone.utc)
    try:
        if isinstance(val, str):
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            return max((now - dt).total_seconds(), 0)
        if isinstance(val, (int, float)):
            v = float(val)
            if v > 1e12:
                return max((now.timestamp() * 1000 - v) / 1000, 0)
            if v > 1e9:
                return max(now.timestamp() - v, 0)
            return max(v, 0)
    except Exception:
        return None
    return None

def _extract_phone(event):
    try:
        participants = event.get("participants", []) or []
        for p in participants:
            purpose = (p.get("purpose") or "").lower()
            if purpose in ["external", "customer", "outbound"]:
                for k in ["ani", "dnis", "address", "addressOther", "name"]:
                    v = p.get(k)
                    if v:
                        return str(v).replace("tel:", "").replace("sip:", "")
                for s in p.get("sessions", []) or []:
                    for k in ["ani", "dnis", "address", "addressOther"]:
                        v = s.get(k)
                        if v:
                            return str(v).replace("tel:", "").replace("sip:", "")
    except Exception:
        return None
    return None

def _extract_media_type(event):
    if not isinstance(event, dict):
        return None
    if event.get("mediaType"):
        return event.get("mediaType")
    participants = event.get("participants", []) or []
    for p in participants:
        for s in p.get("sessions", []) or []:
            mt = s.get("mediaType")
            if mt:
                return mt
    return None

def _extract_agent_name(event):
    participants = event.get("participants", []) or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["agent", "user"]:
            return p.get("name") or p.get("user", {}).get("name")
    return None

def _extract_agent_identity(event):
    participants = event.get("participants", []) or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["agent", "user"]:
            agent_id = p.get("userId") or (p.get("user") or {}).get("id") or p.get("participantId")
            agent_name = p.get("name") or (p.get("user") or {}).get("name")
            return agent_id, agent_name
    return None, None

def _session_is_active(session):
    state = (session.get("state") or "").lower()
    if session.get("disconnectedTime"):
        return False
    return state in ["alerting", "connected", "offering", "dialing", "communicating", "contacting"]

def _classify_conversation_state(event):
    participants = event.get("participants", []) or []
    has_agent = False
    has_queue = False
    has_ivr = False
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        for s in p.get("sessions", []) or []:
            if not _session_is_active(s):
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

def _normalize_presence(presence, presence_map=None):
    if not presence:
        return {}
    presence_map = presence_map or {}
    p = dict(presence)
    pd = p.get("presenceDefinition")
    if not pd and p.get("systemPresence"):
        pd = {"systemPresence": p.get("systemPresence")}
    if pd and isinstance(pd, dict):
        pid = pd.get("id")
        if pid and presence_map:
            label = presence_map.get(pid, {}).get("label")
            if label and not pd.get("label"):
                pd = dict(pd)
                pd["label"] = label
        p["presenceDefinition"] = pd
    return p
