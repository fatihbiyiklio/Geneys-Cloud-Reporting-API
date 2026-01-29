import requests
from datetime import datetime, UTC

class GenesysAPI:
    def __init__(self, auth_data):
        self.access_token = auth_data['access_token']
        self.api_host = auth_data['api_host']
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def _get(self, path, params=None):
        response = requests.get(f"{self.api_host}{path}", headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def _post(self, path, data):
        response = requests.post(f"{self.api_host}{path}", headers=self.headers, json=data)
        response.raise_for_status()
        return response.json()

    def get_users(self):
        """Fetches all users using direct API with paging."""
        users = []
        try:
            page_number = 1
            while True:
                data = self._get("/api/v2/users", params={"pageNumber": page_number, "pageSize": 100})
                if 'entities' in data:
                    for user in data['entities']:
                        users.append({'id': user['id'], 'name': user['name']})
                    if not data.get('nextUri'):
                        break
                    page_number += 1
                else:
                    break
        except Exception:
            print("Error: Could not fetch users from Genesys Cloud.")
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
            print("Error: Could not fetch queues from Genesys Cloud.")
        return queues

    def get_analytics_conversations_aggregate(self, start_date, end_date, granularity="P1D", group_by=None, filter_type=None, filter_ids=None, metrics=None):
        interval = f"{start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
        
        dimension = "userId" if filter_type == 'user' else "queueId"
        predicates = [{"type": "dimension", "dimension": dimension, "value": fid} for fid in (filter_ids or [])]
        
        filter_clause = {"type": "or", "predicates": predicates} if predicates else None

        if not metrics:
            metrics = ["nOffered", "tAnswered", "tAbandon", "tTalk", "tHandle"]
        else:
            new_metrics = []
            for m in metrics:
                if m == "nAnswered": new_metrics.append("tAnswered")
                elif m == "nAbandon": new_metrics.append("tAbandon")
                elif m == "nOffered": new_metrics.extend(["nOffered", "tAlert"])
                else: new_metrics.append(m)
            metrics = list(set(new_metrics))

        query = {
            "interval": interval,
            "granularity": granularity,
            "metrics": metrics
        }
        if group_by: query["groupBy"] = group_by
        if filter_clause: query["filter"] = filter_clause

        try:
            return self._post("/api/v2/analytics/conversations/aggregates/query", query)
        except Exception:
            print("Error: Analytics query failed.")
            return None

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
                print("Error: Observations query failed.")
        
        # Return a structure that matches the SDK response for consistency
        return {"results": all_results} if all_results else None

    def get_queue_daily_stats(self, queue_ids, interval=None):
        if not interval:
            now_utc = datetime.now(UTC)
            start_of_day = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            interval = f"{start_of_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{now_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"

        CHUNK_SIZE = 50
        all_results = []
        chunks = [queue_ids[i:i + CHUNK_SIZE] for i in range(0, len(queue_ids), CHUNK_SIZE)]

        for chunk in chunks:
            predicates = [{"type": "dimension", "dimension": "queueId", "value": qid} for qid in chunk]
            query = {
                "interval": interval,
                "groupBy": ["queueId"],
                "metrics": ["nOffered", "tAnswered", "tAbandon", "oServiceLevel"],
                "filter": {"type": "or", "predicates": predicates}
            }
            try:
                data = self._post("/api/v2/analytics/conversations/aggregates/query", query)
                if 'results' in data:
                    all_results.extend(data['results'])
            except Exception:
                print("Error: Daily stats query failed.")
        
        return {"results": all_results} if all_results else None

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
                        definitions[p['id']] = p['systemPresence']
                    if not data.get('nextUri'):
                        break
                    page_number += 1
                else:
                    break
        except Exception:
            print("Error: Presence definitions fetch failed.")
        return definitions
