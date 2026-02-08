import time
from datetime import datetime, timezone, timedelta
from src.monitor import monitor

class GenesysAPI:
    def __init__(self, auth_data):
        self.access_token = auth_data['access_token']
        self.api_host = auth_data['api_host']
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def _get(self, path, params=None):
        import requests
        start = time.monotonic()
        try:
            response = requests.get(f"{self.api_host}{path}", headers=self.headers, params=params, timeout=10)
            duration_ms = int((time.monotonic() - start) * 1000)
            monitor.log_api_call(path, method="GET", status_code=response.status_code, duration_ms=duration_ms)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            try:
                status_code = response.status_code
            except Exception:
                status_code = None
            monitor.log_api_call(path, method="GET", status_code=status_code, duration_ms=duration_ms)
            monitor.log_error("API_GET", f"HTTP {response.status_code} on {path}", str(e))
            if response.status_code == 401:
                monitor.log_error("API_GET", "Token expired (401). Should trigger re-auth here.")
                raise e 
            raise e
        except Exception as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            monitor.log_api_call(path, method="GET", status_code=None, duration_ms=duration_ms)
            monitor.log_error("API_GET", f"System Error on {path}", str(e))
            raise e

    def _post(self, path, data):
        import requests
        start = time.monotonic()
        try:
            response = requests.post(f"{self.api_host}{path}", headers=self.headers, json=data, timeout=10)
            duration_ms = int((time.monotonic() - start) * 1000)
            monitor.log_api_call(path, method="POST", status_code=response.status_code, duration_ms=duration_ms)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            try:
                status_code = response.status_code
            except Exception:
                status_code = None
            monitor.log_api_call(path, method="POST", status_code=status_code, duration_ms=duration_ms)
            monitor.log_error("API_POST", f"HTTP {response.status_code} on {path}", str(e))
            if response.status_code == 401:
                 monitor.log_error("API_POST", "Token expired (401) on POST.")
                 raise e
            raise e
        except Exception as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            monitor.log_api_call(path, method="POST", status_code=None, duration_ms=duration_ms)
            monitor.log_error("API_POST", f"System Error on {path}", str(e))
            raise e

    def _put(self, path, data):
        import requests
        start = time.monotonic()
        try:
            response = requests.put(f"{self.api_host}{path}", headers=self.headers, json=data, timeout=10)
            duration_ms = int((time.monotonic() - start) * 1000)
            monitor.log_api_call(path, method="PUT", status_code=response.status_code, duration_ms=duration_ms)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            try:
                status_code = response.status_code
            except Exception:
                status_code = None
            detail = None
            try:
                detail = response.text
                if detail and len(detail) > 2000:
                    detail = detail[:2000] + "...(truncated)"
            except Exception:
                detail = None
            monitor.log_api_call(path, method="PUT", status_code=status_code, duration_ms=duration_ms)
            monitor.log_error("API_PUT", f"HTTP {response.status_code} on {path}", detail or str(e))
            if response.status_code == 401:
                 monitor.log_error("API_PUT", "Token expired (401) on PUT.")
                 raise e
            raise e
        except Exception as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            monitor.log_api_call(path, method="PUT", status_code=None, duration_ms=duration_ms)
            monitor.log_error("API_PUT", f"System Error on {path}", str(e))
            raise e
    
    # ... (other methods remain unchanged) ...

    def get_conversation_details(self, start_date, end_date):
        """Fetches detailed conversation records within date range, chunking if necessary."""
        all_conversations = []
        
        # Genesys Analytics Details limit is typically 31 days.
        # We chunk by 14 days to be safe and manage payload size.
        chunk_days = 14
        current_start = start_date
        
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=chunk_days), end_date)
            interval = f"{current_start.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{current_end.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
            
            try:
                page_number = 1
                while True:
                    query = {
                        "interval": interval,
                        "paging": {"pageSize": 100, "pageNumber": page_number},
                        "order": "asc",
                        "orderBy": "conversationStart"
                    }
                    
                    data = self._post("/api/v2/analytics/conversations/details/query", query)
                    
                    if 'conversations' in data:
                        all_conversations.extend(data['conversations'])
                        if not data.get('conversations') or len(data['conversations']) < 100:
                            break
                        page_number += 1
                    else:
                        break
                        
                    # Safe break to prevent infinite loops or OOM
                    if page_number > 200: break 
            except Exception as e:
                monitor.log_error("API_POST", f"Error fetching conversation details for chunk {interval}: {e}")
                # We continue to next chunk instead of failing completely, usually.
                
            current_start = current_end
            
        return {"conversations": all_conversations}

    def get_conversation_details_recent(self, start_date, end_date, page_size=100, max_pages=5, order="desc"):
        """Fetches recent conversation detail records for a short interval."""
        conversations = []
        interval = f"{start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
        try:
            page_number = 1
            while True:
                query = {
                    "interval": interval,
                    "paging": {"pageSize": page_size, "pageNumber": page_number},
                    "order": order,
                    "orderBy": "conversationStart"
                }
                data = self._post("/api/v2/analytics/conversations/details/query", query)
                page = data.get("conversations") or []
                if page:
                    conversations.extend(page)
                if not page or len(page) < page_size or page_number >= max_pages:
                    break
                page_number += 1
        except Exception as e:
            monitor.log_error("API_POST", f"Error fetching recent conversation details: {e}")
        return conversations

    def get_users(self):
        """Fetches all users using direct API with paging."""
        users = []
        try:
            page_number = 1
            while True:
                data = self._get("/api/v2/users", params={"pageNumber": page_number, "pageSize": 100})
                if 'entities' in data:
                    for user in data['entities']:
                        users.append({
                            'id': user['id'], 
                            'name': user['name'],
                            'username': user.get('username', '')
                        })
                    if not data.get('nextUri'):
                        break
                    page_number += 1
                else:
                    break
        except Exception as e:
            monitor.log_error("API_GET", f"Error: {e}")
            return {"error": str(e)}
        return users

    def get_queues(self):
        """Fetches all queues using direct API with paging."""
        queues = []
        try:
            page_number = 1
            while True:
                data = self._get("/api/v2/routing/queues", params={"pageNumber": page_number, "pageSize": 100})
                if 'entities' in data:
                    for queue in data['entities']:
                        queues.append({'id': queue['id'], 'name': queue['name']})
                    if not data.get('nextUri'):
                        break
                    page_number += 1
                else:
                    break
        except Exception:
            monitor.log_error("API_GET", "Error: Could not fetch queues from Genesys Cloud.")
        return queues

    def get_wrapup_codes(self):
        """Fetches all wrap-up codes for mapping."""
        codes = {}
        try:
            page_number = 1
            while True:
                data = self._get("/api/v2/routing/wrapupcodes", params={"pageNumber": page_number, "pageSize": 100})
                if 'entities' in data:
                    for code in data['entities']:
                        codes[code['id']] = code['name']
                    if not data.get('nextUri'):
                        break
                    page_number += 1
                else:
                    break
        except Exception as e:
            monitor.log_error("API_GET", f"Error fetching wrap-up codes: {e}")
        return codes

    def get_analytics_conversations_aggregate(self, start_date, end_date, granularity="P1D", group_by=None, filter_type=None, filter_ids=None, metrics=None, media_types=None):
        dimension = "userId" if filter_type == 'user' else "queueId"
        operator = "matches"
        predicates = [{"type": "dimension", "dimension": dimension, "operator": operator, "value": fid} for fid in (filter_ids or [])]
        
        # Build filter with entity dimension (user or queue)
        entity_filter = {"type": "or", "predicates": predicates} if predicates else None
        
        # Build media type filter if provided
        media_filter = None
        if media_types:
            media_predicates = [{"type": "dimension", "dimension": "mediaType", "operator": "matches", "value": mt} for mt in media_types]
            media_filter = {"type": "or", "predicates": media_predicates}
        
        # Combine filters with AND if both exist
        if entity_filter and media_filter:
            filter_clause = {"type": "and", "clauses": [entity_filter, media_filter]}
        elif entity_filter:
            filter_clause = entity_filter
        elif media_filter:
            filter_clause = media_filter
        else:
            filter_clause = None

        # Helper to convert UI metrics to API metrics
        def convert_metrics(input_mets, is_queue):
            new_mets = []
            for m in input_mets:
                if m == "nAnswered": new_mets.append("tAnswered")
                elif m == "nAbandon": new_mets.append("tAbandon")
                elif m == "nOffered" and not is_queue: new_mets.append("tAlert") # For users, Offered=Alert
                elif m == "nOffered" and is_queue: new_mets.append("nOffered")
                elif m == "nWrapup": new_mets.append("tAcw") 
                elif m == "nHandled": new_mets.append("tHandle")
                elif m == "nOutbound": new_mets.extend(["nOutbound", "tTalk"])
                elif m == "nNotResponding": new_mets.append("tNotResponding")
                elif m == "nAlert": new_mets.append("tAlert")
                elif m == "nConsultTransferred": new_mets.append("nTransferred")
                elif m == "AvgHandle": new_mets.append("tHandle")
                else: new_mets.append(m)
            return list(set(new_mets))

        if not metrics:
            metrics = ["nOffered", "tAnswered", "tAbandon", "tTalk", "tHandle"]
        else:
            # Convert UI metrics to API metrics
            metrics = convert_metrics(metrics, dimension == "queueId")
            
            if dimension == "queueId":
                queue_valid = {
                    "nOffered", "nAbandon", "tAnswered", "tAbandon", "tTalk", "tHeld", "tAcw", 
                    "tHandle", "tAlert", "tWait", "oServiceLevel", "nTransferred", "nConnected", 
                    "nOutbound", "nBlindTransferred", "tDialing", "tContacting", "nError",
                    "tFlowOut", "tVoicemail", "nOverSla", "tNotResponding"
                }
                metrics = [m for m in metrics if m in queue_valid]
            else:
                userId_safe = {
                    "tTalk", "tHeld", "tAcw", "tHandle", "tAlert", "tAnswered", "tAbandon", "tWait", 
                    "nTransferred", "nConnected", "nOutbound", "nBlindTransferred", "tDialing", "tContacting",
                    "tNotResponding", "nOverSla", "tFlowOut", "tVoicemail", "tAcd", "tOrganizationResponse", "nError"
                }
                metrics = [m for m in metrics if m in userId_safe]

        combined_results = []
        chunk_days = 14
        curr = start_date
        
        while curr < end_date:
            curr_end = curr + timedelta(days=chunk_days)
            if curr_end > end_date:
                curr_end = end_date
            
            # Ensure we don't query 0 duration if loop logic is weird
            if curr_end <= curr: break

            interval = f"{curr.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{curr_end.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
            
            query = {
                "interval": interval,
                "granularity": granularity,
                "metrics": metrics
            }
            if group_by: query["groupBy"] = group_by
            if filter_clause: query["filter"] = filter_clause

            try:
                data = self._post("/api/v2/analytics/conversations/aggregates/query", query)
            except Exception as e:
                monitor.log_error("API_POST", f"Error fetching aggregate chunk {interval}: {e}")
                data = {}

            if 'results' in data:
                combined_results.extend(data['results'])
            
            curr = curr_end
            
        return {"results": combined_results}

    def get_queue_observations(self, queue_ids):
        CHUNK_SIZE = 100
        all_results = []
        chunks = [queue_ids[i:i + CHUNK_SIZE] for i in range(0, len(queue_ids), CHUNK_SIZE)]
        
        for chunk in chunks:
            predicates = [{"type": "dimension", "dimension": "queueId", "value": qid} for qid in chunk]
            query = {
                "filter": {"type": "or", "predicates": predicates},
                "metrics": ["oWaiting", "oInteracting", "oUserPresences", "oMemberUsers", "oOnQueueUsers", "oActiveUsers"]
            }
            try:
                data = self._post("/api/v2/analytics/queues/observations/query", query)
                if 'results' in data:
                    all_results.extend(data['results'])
            except Exception:
                monitor.log_error("API_POST", "Error: Observations query failed.")
        
        # Return a structure that matches the SDK response for consistency
        return {"results": all_results} if all_results else None

    def get_queue_daily_stats(self, queue_ids, interval=None):
        if not interval:
            now_utc = datetime.now(timezone.utc)
            start_of_day = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            interval = f"{start_of_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{now_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"

        CHUNK_SIZE = 50
        all_results = []
        chunks = [queue_ids[i:i + CHUNK_SIZE] for i in range(0, len(queue_ids), CHUNK_SIZE)]

        for chunk in chunks:
            predicates = [{"type": "dimension", "dimension": "queueId", "value": qid} for qid in chunk]
            query = {
                "interval": interval,
                "groupBy": ["queueId", "mediaType"],
                "metrics": ["nOffered", "tAnswered", "tAbandon", "oServiceLevel"],
                "filter": {"type": "or", "predicates": predicates}
            }
            try:
                data = self._post("/api/v2/analytics/conversations/aggregates/query", query)
                if 'results' in data:
                    all_results.extend(data['results'])
            except Exception:
                monitor.log_error("API_POST", "Error: Daily stats query failed.")
        
        return {"results": all_results} if all_results else None

    def get_queue_stats_from_details(self, queue_ids, interval):
        """Get accurate queue stats by analyzing conversation details - counts first queue segment only."""
        # Build filter for queue IDs
        predicates = [{"dimension": "queueId", "value": qid} for qid in queue_ids]
        
        query = {
            "interval": interval,
            "order": "asc",
            "orderBy": "conversationStart",
            "paging": {"pageSize": 100, "pageNumber": 1},
            "segmentFilters": [{
                "type": "or",
                "predicates": predicates
            }]
        }
        
        stats = {qid: {"Offered": 0, "Answered": 0, "Abandoned": 0} for qid in queue_ids}
        counted_conversations = set()
        
        try:
            page = 1
            while True:
                query["paging"]["pageNumber"] = page
                data = self._post("/api/v2/analytics/conversations/details/query", query)
                
                conversations = data.get('conversations', [])
                if not conversations:
                    break
                
                for conv in conversations:
                    conv_id = conv.get('conversationId')
                    if conv_id in counted_conversations:
                        continue
                    counted_conversations.add(conv_id)
                    
                    # Collect ALL queue segments with their start times
                    all_queue_segments = []
                    
                    for participant in conv.get('participants', []):
                        if participant.get('purpose') == 'acd':
                            for session in participant.get('sessions', []):
                                for segment in session.get('segments', []):
                                    queue_id = segment.get('queueId')
                                    seg_start = segment.get('segmentStart')
                                    seg_type = segment.get('segmentType')
                                    
                                    if queue_id and seg_start:
                                        all_queue_segments.append({
                                            'queue_id': queue_id,
                                            'start': seg_start,
                                            'type': seg_type
                                        })
                    
                    if not all_queue_segments:
                        continue
                    
                    # Sort by start time to find the FIRST queue
                    all_queue_segments.sort(key=lambda x: x['start'])
                    first_queue_id = all_queue_segments[0]['queue_id']
                    
                    # Only count if first queue is in our monitored queues
                    if first_queue_id not in queue_ids:
                        continue
                    
                    # Check if conversation was answered (has 'interact' segment) or abandoned
                    was_answered = any(s['type'] == 'interact' for s in all_queue_segments if s['queue_id'] == first_queue_id)
                    
                    stats[first_queue_id]["Offered"] += 1
                    if was_answered:
                        stats[first_queue_id]["Answered"] += 1
                    else:
                        stats[first_queue_id]["Abandoned"] += 1
                
                if len(conversations) < 100:
                    break
                page += 1
                if page > 100:  # Safety limit
                    break
                    
        except Exception as e:
            monitor.log_error("API_GET", f"Error getting queue stats from details: {e}")
        
        return stats

    def get_presence_definitions(self):
        """Fetches presence definitions from API to map UUIDs."""
        definitions = {}
        try:
            page_number = 1
            while True:
                # Correct endpoint is singular 'presence'
                data = self._get("/api/v2/presence/definitions", params={"pageNumber": page_number, "pageSize": 100})
                if 'entities' in data:
                    for p in data['entities']:
                        # Try to get the best label
                        labels = p.get('languageLabels', {})
                        label = labels.get('en_US') or labels.get('tr_TR')
                        if not label and labels:
                            # If no en_US or tr_TR, take any
                            label = list(labels.values())[0]
                        
                        if not label:
                            label = p.get('systemPresence', '')
                            
                        definitions[p['id']] = {
                            'label': label,
                            'systemPresence': p.get('systemPresence', 'OFFLINE')
                        }
                    if not data.get('nextUri'):
                        break
                    page_number += 1
                else:
                    break
        except Exception:
            monitor.log_error("API_GET", "Error: Presence definitions fetch failed.")
        return definitions

    def get_user_aggregates(self, start_date, end_date, user_ids):
        """Fetches user status aggregates (durations) for a list of users, batching requests if needed."""
        if not user_ids: return {"results": []}
        
        BATCH_SIZE = 100
        combined_results = []
        
        # Chunk by 14 days to be safe (Aggregates can handle more but reliable is better)
        chunk_days = 14
        curr = start_date
        while curr < end_date:
            curr_end = min(curr + timedelta(days=chunk_days), end_date)
            interval = f"{curr.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{curr_end.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
            
            for i in range(0, len(user_ids), BATCH_SIZE):
                batch = user_ids[i:i + BATCH_SIZE]
                predicates = [
                    {"type": "dimension", "dimension": "userId", "operator": "matches", "value": uid}
                    for uid in batch
                ]
                query = {
                    "interval": interval,
                    "groupBy": ["userId"],
                    "filter": {"type": "or", "predicates": predicates},
                    "metrics": ["tSystemPresence", "tOrganizationPresence"]
                }
                try:
                    data = self._post("/api/v2/analytics/users/aggregates/query", query)
                except Exception as e:
                    monitor.log_error("API_POST", f"Error fetching user aggregates batch {i} for {interval}: {e}")
                    data = {}

                if data and 'results' in data:
                    combined_results.extend(data['results'])
            
            curr = curr_end
            
        return {"results": combined_results}

    def get_user_status_details(self, start_date, end_date, user_ids):
        """Fetches historical user status details for login/logout calculation, batching requests if needed."""
        if not user_ids: return {}
        
        BATCH_SIZE = 50 
        combined_details = []
        
        # Chunk by 7 days for Details queries
        chunk_days = 7
        curr = start_date
        while curr < end_date:
            curr_end = min(curr + timedelta(days=chunk_days), end_date)
            interval = f"{curr.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{curr_end.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
            
            for i in range(0, len(user_ids), BATCH_SIZE):
                batch = user_ids[i:i + BATCH_SIZE]
                
                try:
                    page = 1
                    while True:
                        query = {
                            "interval": interval,
                            "userFilters": [
                                {"type": "or", "predicates": [{"type": "dimension", "dimension": "userId", "operator": "matches", "value": uid} for uid in batch]}
                            ],
                            "paging": {"pageSize": 100, "pageNumber": page}
                        }
                        
                        data = self._post("/api/v2/analytics/users/details/query", query)
                        
                        if data and 'userDetails' in data:
                            combined_details.extend(data['userDetails'])
                            if len(data['userDetails']) < 100: break
                            page += 1
                            if page > 50: break # Safety limit
                        else:
                            break
                except Exception as e:
                    monitor.log_error("API_POST", f"Error details batch {i} interval {interval}: {e}")
            
            curr = curr_end
            
        return {"userDetails": combined_details}

    def get_queue_members(self, queue_id):
        """Fetches members of a queue with their presence and routing status."""
        members = []
        try:
            page_number = 1
            while True:
                # Expansion is not needed here as we use Bulk Analytics for status
                params = {
                    "pageNumber": page_number, 
                    "pageSize": 100
                }
                data = self._get(f"/api/v2/routing/queues/{queue_id}/users", params=params)
                
                if 'entities' in data:
                    members.extend(data['entities'])
                    if not data.get('nextUri'):
                        break
                    page_number += 1
                else:
                    break
        except Exception as e:
            monitor.log_error("API_GET", f"Error fetching queue members for {queue_id}: {e}")
        return members

    def create_notification_channel(self):
        """Creates a notifications channel for websocket events."""
        return self._post("/api/v2/notifications/channels", {})

    def subscribe_notification_channel(self, channel_id, topics):
        """Subscribes a channel to the given list of topics."""
        return self._put(f"/api/v2/notifications/channels/{channel_id}/subscriptions", topics)

    def get_queue_conversations(self, queue_id, page_size=100, max_pages=3):
        """Fetches active conversations for a queue (best-effort for waiting calls)."""
        conversations = []
        try:
            page_number = 1
            while True:
                params = {"pageNumber": page_number, "pageSize": page_size}
                data = self._get(f"/api/v2/routing/queues/{queue_id}/conversations", params=params)

                entities = data.get("entities") or data.get("conversations") or data.get("results") or []
                if entities:
                    conversations.extend(entities)

                if not data.get("nextUri"):
                    break
                page_number += 1
                if page_number > max_pages:
                    break
        except Exception as e:
            monitor.log_error("API_GET", f"Error fetching queue conversations for {queue_id}: {e}")
        return conversations

    def get_conversation(self, conversation_id):
        """Fetch a single conversation by id."""
        try:
            return self._get(f"/api/v2/conversations/{conversation_id}")
        except Exception as e:
            monitor.log_error("API_GET", f"Error fetching conversation {conversation_id}: {e}")
            return {}
    def get_users_status_scan(self, target_user_ids=None, ignored_user_ids=None):
        """
        Scans for ALL users and their statuses using standard User List API.
        Verified: GET /api/v2/users?expand=presence,routingStatus provides REAL-TIME data.
        Analytics Details Query was found to be stale/latent.
        """
        presence_map = {}
        routing_map = {}
        target_user_ids = set(target_user_ids or [])
        remaining = set(target_user_ids)
        
        page_number = 1
        while True:
            try:
                # Expand presence and routingStatus in the list request
                data = self._get(f"/api/v2/users?pageSize=100&pageNumber={page_number}&expand=presence,routingStatus")
                
                if 'entities' in data:
                    for user in data['entities']:
                        uid = user.get('id')
                        if target_user_ids and uid not in target_user_ids:
                            continue
                        # Extract expanded data directly
                        if 'presence' in user:
                            presence_map[uid] = user['presence']
                        if 'routingStatus' in user:
                            routing_map[uid] = user['routingStatus']
                        if target_user_ids and uid in remaining:
                            remaining.discard(uid)
                            
                    if not data['entities'] or not data.get('nextUri'):
                        break
                    if target_user_ids and not remaining:
                        break
                    page_number += 1
                else:
                    break
                    
                if page_number > 20: break # Safety break
            except Exception as e:
                monitor.log_error("API_GET", f"API: Error in user scan: {e}")
                break
                
        return {"presence": presence_map, "routing": routing_map}
