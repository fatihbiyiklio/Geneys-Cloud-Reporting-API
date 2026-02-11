import time
import requests
from datetime import datetime, timezone, timedelta
from src.monitor import monitor

# Shared session for connection pooling (reduces Windows socket/handle overhead)
_session = requests.Session()
_session.headers.update({"Content-Type": "application/json"})

class GenesysAPI:
    ASSIGNMENT_BATCH_SIZE = 50

    def __init__(self, auth_data):
        self.access_token = auth_data['access_token']
        self.api_host = auth_data['api_host']
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def _get(self, path, params=None):
        start = time.monotonic()
        headers = self.headers
        try:
            response = _session.get(f"{self.api_host}{path}", headers=headers, params=params, timeout=10)
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

    def _post(self, path, data, timeout=10, retries=0, retry_sleep=0.4, params=None):
        start = time.monotonic()
        headers = self.headers
        attempts = max(0, int(retries)) + 1
        for attempt in range(attempts):
            try:
                response = _session.post(
                    f"{self.api_host}{path}",
                    headers=headers,
                    json=data,
                    params=params,
                    timeout=timeout
                )
                duration_ms = int((time.monotonic() - start) * 1000)
                monitor.log_api_call(path, method="POST", status_code=response.status_code, duration_ms=duration_ms)
                response.raise_for_status()
                if response.status_code == 204 or not response.content:
                    return {"status": response.status_code}
                return response.json()
            except requests.exceptions.ReadTimeout as e:
                if attempt < (attempts - 1):
                    time.sleep(retry_sleep * (attempt + 1))
                    continue
                duration_ms = int((time.monotonic() - start) * 1000)
                monitor.log_api_call(path, method="POST", status_code=None, duration_ms=duration_ms)
                monitor.log_error("API_POST", f"Read timeout on {path}", str(e))
                raise e
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
        start = time.monotonic()
        headers = self.headers
        try:
            response = _session.put(f"{self.api_host}{path}", headers=headers, json=data, timeout=10)
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

    def _patch(self, path, data=None):
        start = time.monotonic()
        headers = self.headers
        try:
            response = _session.patch(f"{self.api_host}{path}", headers=headers, json=data or {}, timeout=15)
            duration_ms = int((time.monotonic() - start) * 1000)
            monitor.log_api_call(path, method="PATCH", status_code=response.status_code, duration_ms=duration_ms)
            response.raise_for_status()
            # Some PATCH endpoints return 202/204 with no body
            if response.status_code in (202, 204) or not response.content:
                return {"status": response.status_code}
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
            monitor.log_api_call(path, method="PATCH", status_code=status_code, duration_ms=duration_ms)
            monitor.log_error("API_PATCH", f"HTTP {response.status_code} on {path}", detail or str(e))
            raise e
        except Exception as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            monitor.log_api_call(path, method="PATCH", status_code=None, duration_ms=duration_ms)
            monitor.log_error("API_PATCH", f"System Error on {path}", str(e))
            raise e

    def disconnect_conversation(self, conversation_id):
        """Disconnect/close a conversation.
        
        Per Genesys Cloud docs (https://developer.genesys.cloud/routing/conversations/call-handling-guide):
        - Disconnect: PATCH /api/v2/conversations/calls/{id}/participants/{pid} {"state":"disconnected"}
        - Wrapup:     PATCH /api/v2/conversations/calls/{id}/participants/{pid} {"wrapup":{"code":"...","notes":"..."}}
        
        NOTE: These endpoints require user context (Authorization Code / Implicit grant).
              Client Credentials grant will get 403 "requires user context".
              The caller must supply a user-context token.
        
        The permission 'conversation:communication:disconnect' allows updating any participant,
        not just the authenticated user's own leg.
        """
        # 1. Get conversation details
        conv = self._get(f"/api/v2/conversations/{conversation_id}")
        participants = conv.get('participants', [])
        
        MEDIA_KEYS = ["calls", "callbacks", "chats", "emails", "messages",
                      "cobrowsesessions", "videos", "screenshares", "socialExpressions"]
        ACTIVE_COMM_STATES = {"connected", "alerting", "offering", "contacting", "dialing", "transmitting"}
        SYSTEM_PURPOSES = {"ivr", "acd", "workflow", "system", "dialer"}
        
        # Detect primary media type
        media_type = None
        for p in participants:
            for mk in MEDIA_KEYS:
                comms = p.get(mk)
                if comms and isinstance(comms, list) and len(comms) > 0:
                    media_type = mk
                    break
            if media_type:
                break
        
        # Build media-specific base path for PATCH
        if media_type:
            base_path = f"/api/v2/conversations/{media_type}/{conversation_id}/participants"
        else:
            base_path = f"/api/v2/conversations/{conversation_id}/participants"
        
        results = {"disconnected": [], "skipped": [], "errors": [], "media_type": media_type}
        
        # Get a default wrapup code for wrapup-pending participants
        default_wrapup_code = None
        try:
            wrapup_codes = self.get_wrapup_codes()
            if wrapup_codes:
                for wid, wname in wrapup_codes.items():
                    if 'default' in wname.lower():
                        default_wrapup_code = wid
                        break
                if not default_wrapup_code:
                    default_wrapup_code = next(iter(wrapup_codes))
        except Exception:
            pass
        
        for p in participants:
            pid = p.get('id')
            p_purpose = (p.get('purpose') or '').lower()
            p_name = p.get('name', p.get('address', 'Unknown'))
            p_state = (p.get('state') or '').lower()
            
            # Skip system participants — can't be disconnected
            if p_purpose in SYSTEM_PURPOSES:
                results["skipped"].append({
                    "id": pid, "name": p_name, "purpose": p_purpose,
                    "state": p_state, "reason": "system"
                })
                continue
            
            # Analyze communication sessions
            has_active_comm = False
            needs_wrapup = False
            
            for mk in MEDIA_KEYS:
                comms = p.get(mk)
                if not comms or not isinstance(comms, list):
                    continue
                for comm in comms:
                    comm_state = (comm.get('state') or '').lower()
                    if comm_state in ACTIVE_COMM_STATES:
                        has_active_comm = True
                        break
                    if comm_state == 'disconnected':
                        wrapup = comm.get('wrapup')
                        if not wrapup or not wrapup.get('code'):
                            needs_wrapup = True
                if has_active_comm:
                    break
            
            # Check top-level wrapupRequired
            if not has_active_comm and not needs_wrapup:
                if p.get('wrapupRequired', False):
                    p_wrapup = p.get('wrapup')
                    if not p_wrapup or not p_wrapup.get('code'):
                        needs_wrapup = True
            
            # Fully ended — skip
            if p_state in ('disconnected', 'terminated') and not needs_wrapup and not has_active_comm:
                results["skipped"].append({
                    "id": pid, "name": p_name, "purpose": p_purpose,
                    "state": p_state, "reason": "ended"
                })
                continue
            
            # Customer in disconnected state with no wrapup — skip
            if p_purpose == 'customer' and p_state == 'disconnected' and not needs_wrapup:
                results["skipped"].append({
                    "id": pid, "name": p_name, "purpose": p_purpose,
                    "state": p_state, "reason": "customer_ended"
                })
                continue
            
            # --- Try to close this participant ---
            action_taken = None
            last_error = None
            
            # Strategy 1: Wrapup pending → PATCH with wrapup code (per Genesys docs, wrapup is sent via PATCH)
            if needs_wrapup and default_wrapup_code:
                try:
                    self._patch(
                        f"{base_path}/{pid}",
                        data={
                            "wrapup": {
                                "code": default_wrapup_code,
                                "notes": "Admin disconnect"
                            }
                        }
                    )
                    action_taken = "wrapup_submitted"
                except Exception as e:
                    last_error = self._extract_error_detail(e)
            
            # Strategy 2: Active comm or wrapup failed → PATCH state disconnected
            if not action_taken:
                try:
                    self._patch(f"{base_path}/{pid}", data={"state": "disconnected"})
                    action_taken = "disconnected"
                except Exception as e:
                    last_error = self._extract_error_detail(e)
            
            # Strategy 3: If PATCH disconnect failed, try with wrapupSkipped
            if not action_taken:
                try:
                    self._patch(
                        f"{base_path}/{pid}",
                        data={"state": "disconnected", "wrapupSkipped": True}
                    )
                    action_taken = "disconnected+wrapupSkipped"
                except Exception as e:
                    last_error = self._extract_error_detail(e)
            
            if action_taken:
                results["disconnected"].append({
                    "id": pid, "name": p_name, "purpose": p_purpose, "action": action_taken
                })
            else:
                results["errors"].append({
                    "id": pid, "name": p_name, "purpose": p_purpose, "error": last_error or "Unknown error"
                })
        
        return results
    
    def _extract_error_detail(self, e):
        """Extract meaningful error message from an exception."""
        error_detail = str(e)
        try:
            if hasattr(e, 'response') and e.response is not None:
                try:
                    body = e.response.json()
                    error_detail = body.get('message', body.get('error', error_detail))
                except Exception:
                    error_detail = e.response.text[:500] if e.response.text else error_detail
        except Exception:
            pass
        return error_detail

    def _chunk_list(self, values, chunk_size=None):
        """Yield list chunks for safer bulk operations."""
        items = values or []
        size = int(chunk_size or self.ASSIGNMENT_BATCH_SIZE)
        if size <= 0:
            size = self.ASSIGNMENT_BATCH_SIZE
        for i in range(0, len(items), size):
            yield items[i:i + size]

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

    def iter_conversation_details(self, start_date, end_date, chunk_days=3, page_size=100, max_pages=200, order="asc"):
        """Yields conversation detail pages to reduce memory usage.

        Args:
            start_date: datetime (UTC)
            end_date: datetime (UTC)
            chunk_days: days per interval chunk
            page_size: items per page
            max_pages: max pages per chunk
            order: "asc" or "desc"
        """
        current_start = start_date
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=chunk_days), end_date)
            interval = f"{current_start.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{current_end.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
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
                        yield page
                    if not page or len(page) < page_size or page_number >= max_pages:
                        break
                    page_number += 1
            except Exception as e:
                monitor.log_error("API_POST", f"Error streaming conversation details for chunk {interval}: {e}")
            current_start = current_end

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
                data = self._post("/api/v2/analytics/conversations/details/query", query, timeout=20, retries=1)
                page = data.get("conversations") or []
                if page:
                    conversations.extend(page)
                if not page or len(page) < page_size or page_number >= max_pages:
                    break
                page_number += 1
        except Exception as e:
            monitor.log_error("API_POST", f"Error fetching recent conversation details: {e}")
        return conversations

    def get_conversation(self, conversation_id):
        """Fetch single conversation with full participant attributes."""
        try:
            data = self._get(f"/api/v2/conversations/{conversation_id}")
            return data
        except Exception as e:
            monitor.log_error("API_GET", f"Error fetching conversation {conversation_id}: {e}")
            return None

    def get_conversation_call(self, conversation_id):
        """Fetch call conversation with participant attributes."""
        try:
            data = self._get(f"/api/v2/conversations/calls/{conversation_id}")
            return data
        except Exception as e:
            # Fallback to generic conversation endpoint
            return self.get_conversation(conversation_id)

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

    def get_routing_skills(self):
        """Fetches all routing skills for id->name mapping."""
        skills = {}
        try:
            page_number = 1
            while True:
                data = self._get("/api/v2/routing/skills", params={"pageNumber": page_number, "pageSize": 100})
                entities = data.get('entities', []) if isinstance(data, dict) else []
                for item in entities:
                    sid = item.get('id')
                    if sid:
                        skills[sid] = item.get('name', sid)
                if not data.get('nextUri'):
                    break
                page_number += 1
        except Exception as e:
            monitor.log_error("API_GET", f"Error fetching routing skills: {e}")
        return skills

    def get_languages(self):
        """Fetches all languages for id->name mapping."""
        languages = {}
        try:
            page_number = 1
            while True:
                data = self._get("/api/v2/routing/languages", params={"pageNumber": page_number, "pageSize": 100, "sortOrder": "ascending"})
                entities = data.get('entities', []) if isinstance(data, dict) else []
                for item in entities:
                    lid = item.get('id')
                    if lid:
                        languages[lid] = item.get('name', lid)
                if not data.get('nextUri'):
                    break
                page_number += 1
        except Exception as e:
            monitor.log_error("API_GET", f"Error fetching languages: {e}")
        return languages

    def get_groups(self, page_size=100):
        """Fetches all groups from Genesys Cloud."""
        groups = []
        try:
            page_number = 1
            while True:
                data = self._get("/api/v2/groups", params={"pageNumber": page_number, "pageSize": page_size, "sortOrder": "ASC"})
                if 'entities' in data:
                    for g in data['entities']:
                        groups.append({
                            'id': g['id'],
                            'name': g.get('name', ''),
                            'description': g.get('description', ''),
                            'memberCount': g.get('memberCount', 0),
                            'type': g.get('type', ''),
                            'state': g.get('state', '')
                        })
                    if not data.get('nextUri'):
                        break
                    page_number += 1
                else:
                    break
        except Exception as e:
            monitor.log_error("API_GET", f"Error fetching groups: {e}")
        return groups

    def get_group_members(self, group_id, page_size=100):
        """Fetches members of a specific group."""
        members = []
        try:
            page_number = 1
            while True:
                data = self._get(f"/api/v2/groups/{group_id}/members", params={"pageNumber": page_number, "pageSize": page_size})
                if 'entities' in data:
                    for m in data['entities']:
                        members.append({
                            'id': m['id'],
                            'name': m.get('name', ''),
                            'email': m.get('email', ''),
                            'state': m.get('state', '')
                        })
                    if not data.get('nextUri'):
                        break
                    page_number += 1
                else:
                    break
        except Exception as e:
            monitor.log_error("API_GET", f"Error fetching group members for {group_id}: {e}")
        return members

    def add_group_members(self, group_id, member_ids):
        """Add members to a group. member_ids is a list of user IDs.
        POST /api/v2/groups/{groupId}/members
        Body: {"memberIds": ["id1", "id2", ...], "version": 1}
        """
        last_response = None
        success_count = 0
        failed_batches = []
        for batch in self._chunk_list(member_ids, self.ASSIGNMENT_BATCH_SIZE):
            try:
                last_response = self._post(
                    f"/api/v2/groups/{group_id}/members",
                    data={"memberIds": batch, "version": 1}
                )
                success_count += len(batch)
            except Exception as e:
                failed_batches.append({
                    "batch_size": len(batch),
                    "error": self._extract_error_detail(e)
                })
        if failed_batches:
            raise Exception(
                f"Some member batches failed for group {group_id}. "
                f"success={success_count}, failed_batches={failed_batches}"
            )
        return last_response or {"status": 204}

    def add_group_to_queues(self, group_id, queue_ids):
        """Add a group to one or more queues via PUT /api/v2/routing/queues/{queueId}.
        Uses the memberGroups field (list[MemberGroup]) with type='GROUP'.
        1) GET queue to retrieve current memberGroups
        2) Append {"id": groupId, "type": "GROUP"} if not already present
        3) PUT queue with updated memberGroups
        """
        results = {}
        for queue_batch in self._chunk_list(queue_ids, self.ASSIGNMENT_BATCH_SIZE):
            for qid in queue_batch:
                try:
                    queue_data = self._get(f"/api/v2/routing/queues/{qid}")
                    member_groups = queue_data.get("memberGroups") or []
                    existing_ids = {mg.get("id") for mg in member_groups}
                    if group_id in existing_ids:
                        results[qid] = {"success": True, "already": True}
                        continue
                    member_groups.append({"id": group_id, "type": "GROUP"})
                    queue_data["memberGroups"] = member_groups
                    # Remove read-only fields that cannot be sent in PUT
                    for ro_field in ["id", "selfUri", "dateCreated", "dateModified", "modifiedBy", "createdBy",
                                     "memberCount", "userMemberCount", "joinedMemberCount"]:
                        queue_data.pop(ro_field, None)
                    self._put(f"/api/v2/routing/queues/{qid}", data=queue_data)
                    results[qid] = {"success": True}
                except Exception as e:
                    results[qid] = {"success": False, "error": self._extract_error_detail(e)}
                    monitor.log_error("API_PUT", f"Error adding group {group_id} to queue {qid}: {e}")
            # Keep assignment bursts controlled.
            time.sleep(0.2)
        return results

    def remove_group_from_queues(self, group_id, queue_ids):
        """Remove a group from one or more queues via PUT /api/v2/routing/queues/{queueId}.
        Uses the memberGroups field — removes the group from the list.
        1) GET queue to retrieve current memberGroups
        2) Filter out the group_id
        3) PUT queue with updated memberGroups
        """
        results = {}
        for queue_batch in self._chunk_list(queue_ids, self.ASSIGNMENT_BATCH_SIZE):
            for qid in queue_batch:
                try:
                    queue_data = self._get(f"/api/v2/routing/queues/{qid}")
                    member_groups = queue_data.get("memberGroups") or []
                    new_member_groups = [mg for mg in member_groups if mg.get("id") != group_id]
                    if len(new_member_groups) == len(member_groups):
                        results[qid] = {"success": True, "not_found": True}
                        continue
                    queue_data["memberGroups"] = new_member_groups
                    # Remove read-only fields that cannot be sent in PUT
                    for ro_field in ["id", "selfUri", "dateCreated", "dateModified", "modifiedBy", "createdBy",
                                     "memberCount", "userMemberCount", "joinedMemberCount"]:
                        queue_data.pop(ro_field, None)
                    self._put(f"/api/v2/routing/queues/{qid}", data=queue_data)
                    results[qid] = {"success": True}
                except Exception as e:
                    results[qid] = {"success": False, "error": self._extract_error_detail(e)}
                    monitor.log_error("API_PUT", f"Error removing group {group_id} from queue {qid}: {e}")
            # Keep assignment bursts controlled.
            time.sleep(0.2)
        return results

    def add_users_to_queues(self, user_ids, queue_ids):
        """Add users to one or more queues in batches.
        Uses POST /api/v2/routing/queues/{queueId}/members with body: [{"id": "userId"}, ...]
        """
        results = {}
        normalized_user_ids = [uid for uid in (user_ids or []) if uid]
        normalized_queue_ids = [qid for qid in (queue_ids or []) if qid]

        for queue_batch in self._chunk_list(normalized_queue_ids, self.ASSIGNMENT_BATCH_SIZE):
            for qid in queue_batch:
                try:
                    existing_members = self.get_queue_members(qid) or []
                    existing_ids = {
                        m.get("id")
                        for m in existing_members
                        if isinstance(m, dict) and m.get("id")
                    }
                    to_add = [uid for uid in normalized_user_ids if uid not in existing_ids]
                    skipped_existing = len(normalized_user_ids) - len(to_add)

                    added = 0
                    failed_batches = []
                    for user_batch in self._chunk_list(to_add, self.ASSIGNMENT_BATCH_SIZE):
                        try:
                            body = [{"id": uid} for uid in user_batch]
                            self._post(f"/api/v2/routing/queues/{qid}/members", data=body)
                            added += len(user_batch)
                        except Exception as e:
                            failed_batches.append({
                                "batch_size": len(user_batch),
                                "error": self._extract_error_detail(e)
                            })

                    if failed_batches:
                        results[qid] = {
                            "success": False,
                            "added": added,
                            "skipped_existing": skipped_existing,
                            "error": failed_batches
                        }
                    else:
                        results[qid] = {
                            "success": True,
                            "added": added,
                            "skipped_existing": skipped_existing
                        }
                except Exception as e:
                    results[qid] = {"success": False, "error": self._extract_error_detail(e)}
                    monitor.log_error("API_POST", f"Error adding users to queue {qid}: {e}")
            # Keep assignment bursts controlled.
            time.sleep(0.2)
        return results

    def remove_users_from_queues(self, user_ids, queue_ids):
        """Remove users from one or more queues in batches.
        Uses POST /api/v2/routing/queues/{queueId}/members?delete=true with body: [{"id": "userId"}, ...]
        """
        results = {}
        normalized_user_ids = [uid for uid in (user_ids or []) if uid]
        normalized_queue_ids = [qid for qid in (queue_ids or []) if qid]

        for queue_batch in self._chunk_list(normalized_queue_ids, self.ASSIGNMENT_BATCH_SIZE):
            for qid in queue_batch:
                try:
                    existing_members = self.get_queue_members(qid) or []
                    existing_ids = {
                        m.get("id")
                        for m in existing_members
                        if isinstance(m, dict) and m.get("id")
                    }
                    to_remove = [uid for uid in normalized_user_ids if uid in existing_ids]
                    skipped_missing = len(normalized_user_ids) - len(to_remove)

                    removed = 0
                    failed_batches = []
                    for user_batch in self._chunk_list(to_remove, self.ASSIGNMENT_BATCH_SIZE):
                        try:
                            body = [{"id": uid} for uid in user_batch]
                            self._post(
                                f"/api/v2/routing/queues/{qid}/members",
                                data=body,
                                params={"delete": "true"}
                            )
                            removed += len(user_batch)
                        except Exception as e:
                            failed_batches.append({
                                "batch_size": len(user_batch),
                                "error": self._extract_error_detail(e)
                            })

                    if failed_batches:
                        results[qid] = {
                            "success": False,
                            "removed": removed,
                            "skipped_missing": skipped_missing,
                            "error": failed_batches
                        }
                    else:
                        results[qid] = {
                            "success": True,
                            "removed": removed,
                            "skipped_missing": skipped_missing
                        }
                except Exception as e:
                    results[qid] = {"success": False, "error": self._extract_error_detail(e)}
                    monitor.log_error("API_POST", f"Error removing users from queue {qid}: {e}")
            # Keep assignment bursts controlled.
            time.sleep(0.2)
        return results

    def remove_group_members(self, group_id, member_ids):
        """Remove members from a group.
        DELETE /api/v2/groups/{groupId}/members with ids query param.
        """
        ids_str = ",".join(member_ids)
        start = time.monotonic()
        try:
            response = _session.delete(
                f"{self.api_host}/api/v2/groups/{group_id}/members",
                headers=self.headers,
                params={"ids": ids_str},
                timeout=10
            )
            duration_ms = int((time.monotonic() - start) * 1000)
            monitor.log_api_call(f"/api/v2/groups/{group_id}/members", method="DELETE", status_code=response.status_code, duration_ms=duration_ms)
            response.raise_for_status()
            return {"status": response.status_code}
        except Exception as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            monitor.log_api_call(f"/api/v2/groups/{group_id}/members", method="DELETE", status_code=None, duration_ms=duration_ms)
            monitor.log_error("API_DELETE", f"Error removing group members: {e}")
            raise e

    def get_user_by_id(self, user_id, expand=None):
        """Fetches a single user by ID from Genesys Cloud.
        
        Args:
            user_id: The user ID (UUID) to fetch
            expand: Optional list of expansions (e.g., ['presence', 'routingStatus', 'groups'])
        
        Returns:
            User dict with details or None if not found
        """
        try:
            params = {}
            if expand:
                params["expand"] = ",".join(expand) if isinstance(expand, list) else expand
            
            data = self._get(f"/api/v2/users/{user_id}", params=params if params else None)
            if data and 'id' in data:
                return {
                    'id': data.get('id'),
                    'name': data.get('name', ''),
                    'email': data.get('email', ''),
                    'username': data.get('username', ''),
                    'state': data.get('state', ''),
                    'department': data.get('department', ''),
                    'title': data.get('title', ''),
                    'manager': data.get('manager', {}).get('name') if data.get('manager') else None,
                    'presence': data.get('presence', {}),
                    'routingStatus': data.get('routingStatus', {}),
                    'groups': data.get('groups', []),
                    'skills': data.get('skills', []),
                    'languages': data.get('languages', []),
                    'primaryContactInfo': data.get('primaryContactInfo', []),
                    'addresses': data.get('addresses', []),
                    'divisionId': data.get('division', {}).get('id') if data.get('division') else None,
                    'divisionName': data.get('division', {}).get('name') if data.get('division') else None,
                    'version': data.get('version'),
                    'dateModified': data.get('dateModified'),
                    'raw': data  # Full raw response for advanced use
                }
            return None
        except Exception as e:
            error_str = str(e)
            if "404" in error_str or "not found" in error_str.lower():
                return None  # User not found
            monitor.log_error("API_GET", f"Error fetching user by ID {user_id}: {e}")
            raise e

    def get_users(self, page_size=100, max_pages=50):
        """Fetches all users from Genesys Cloud."""
        users = []
        try:
            page_number = 1
            while page_number <= max_pages:
                data = self._get("/api/v2/users", params={"pageNumber": page_number, "pageSize": page_size, "sortOrder": "ASC"})
                if 'entities' in data:
                    for u in data['entities']:
                        users.append({
                            'id': u['id'],
                            'name': u.get('name', ''),
                            'username': u.get('username', ''),
                            'email': u.get('email', ''),
                            'state': u.get('state', '')
                        })
                    if not data.get('nextUri'):
                        break
                    page_number += 1
                else:
                    break
        except Exception as e:
            monitor.log_error("API_GET", f"Error fetching users: {e}")
        return users

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
                elif m == "tOutbound": new_mets.append("tTalk")
                elif m == "nNotResponding": new_mets.append("tNotResponding")
                elif m == "nAlert": new_mets.append("tAlert")
                elif m == "nConsultTransferred": new_mets.append("nConsultTransferred")
                elif m == "AvgHandle": new_mets.append("tHandle")
                else: new_mets.append(m)
            return list(set(new_mets))

        # Official Genesys ConversationAggregationQuery metric enum
        supported_aggregate_metrics = {
            "nBlindTransferred", "nBotInteractions", "nCobrowseSessions", "nConnected", "nConsult",
            "nConsultTransferred", "nConversations", "nError", "nOffered", "nOutbound",
            "nOutboundAbandoned", "nOutboundAttempted", "nOutboundConnected", "nOverSla",
            "nStateTransitionError", "nTransferred", "oAudioMessageCount", "oExternalAudioMessageCount",
            "oExternalMediaCount", "oMediaCount", "oMessageCount", "oMessageSegmentCount",
            "oMessageTurn", "oServiceLevel", "oServiceTarget", "tAbandon", "tAcd",
            "tActiveCallback", "tActiveCallbackComplete", "tAcw", "tAgentResponseTime",
            "tAgentVideoConnected", "tAlert", "tAnswered", "tAverageAgentResponseTime",
            "tAverageCustomerResponseTime", "tBarging", "tCoaching", "tCoachingComplete",
            "tConnected", "tContacting", "tDialing", "tFirstConnect", "tFirstDial",
            "tFirstEngagement", "tFirstResponse", "tFlowOut", "tHandle", "tHeld",
            "tHeldComplete", "tIvr", "tMonitoring", "tMonitoringComplete", "tNotResponding",
            "tPark", "tParkComplete", "tScreenMonitoring", "tShortAbandon", "tSnippetRecord",
            "tTalk", "tTalkComplete", "tUserResponseTime", "tVoicemail", "tWait"
        }

        if not metrics:
            metrics = ["nOffered", "tAnswered", "tAbandon", "tTalk", "tHandle"]
        else:
            # Convert UI metrics to API metrics
            metrics = convert_metrics(metrics, dimension == "queueId")
            metrics = [m for m in metrics if m in supported_aggregate_metrics]

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

    def get_user_queue_map(self, user_ids=None, queues=None):
        """Build user->queue names map by scanning queue memberships."""
        user_queue_map = {}
        target_user_ids = {uid for uid in (user_ids or []) if uid}
        queue_list = queues if queues is not None else self.get_queues()

        for queue in queue_list:
            qid = queue.get("id")
            qname = queue.get("name", "")
            if not qid:
                continue
            members = self.get_queue_members(qid) or []
            for member in members:
                uid = member.get("id")
                if not uid:
                    continue
                if target_user_ids and uid not in target_user_ids:
                    continue
                if uid not in user_queue_map:
                    user_queue_map[uid] = set()
                if qname:
                    user_queue_map[uid].add(qname)

        # Convert sets to sorted lists for stable output
        return {uid: sorted(list(names)) for uid, names in user_queue_map.items()}

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
