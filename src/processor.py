import pandas as pd
from datetime import datetime, timedelta

def _get_val(obj, path, default=0):
    """Safely get a value from a nested dict."""
    try:
        parts = path.split('.')
        curr = obj
        for p in parts:
            if isinstance(curr, dict):
                curr = curr.get(p)
            else:
                curr = getattr(curr, p, None)
            if curr is None: return default
        return curr
    except:
        return default

def process_daily_stats(response, lookup_map):
    """Processes dictionary-based response from direct API."""
    stats = {}
    if not response or 'results' not in response:
        return stats

    for result in response['results']:
        group = result.get('group', {})
        queue_id = group.get('queueId')
        q_name = lookup_map.get(queue_id, queue_id)
        
        if q_name not in stats:
            stats[q_name] = {"Offered": 0, "Answered": 0, "Abandoned": 0, "SL_Numerator": 0, "SL_Denominator": 0}
            
        data_buckets = result.get('data', [])
        for d in data_buckets:
            metrics = d.get('metrics', [])
            for m_obj in metrics:
                m = m_obj.get('metric')
                s = m_obj.get('stats', {})
                
                if m == "nOffered":
                    stats[q_name]["Offered"] += s.get('count', 0)
                elif m == "tAnswered":
                    stats[q_name]["Answered"] += s.get('count', 0)
                elif m == "tAbandon":
                    stats[q_name]["Abandoned"] += s.get('count', 0)
                elif m == "oServiceLevel":
                    stats[q_name]["SL_Numerator"] += s.get('numerator', 0)
                    stats[q_name]["SL_Denominator"] += s.get('denominator', 0)
    return stats

def process_analytics_response(response, lookup_map, report_type):
    """Processes dictionary-based analytics response into DataFrame."""
    data = []
    if not response or 'results' not in response:
        return pd.DataFrame()

    for result_row in response['results']:
        group = result_row.get('group', {})
        user_id = group.get("userId")
        queue_id = group.get("queueId")
        
        row_base = {}
        if report_type in ['user', 'agent']:
            name = lookup_map.get(user_id, user_id) if user_id else "Unknown"
            row_base = {"Name": name, "Id": user_id}
        elif report_type in ['queue', 'workgroup']:
            name = lookup_map.get(queue_id, queue_id) if queue_id else "Unknown"
            row_base = {"Name": name, "Id": queue_id}
        elif report_type == 'detailed':
            agent_name = lookup_map.get(user_id, user_id) if user_id else "Unknown"
            queue_name = lookup_map.get(queue_id, queue_id) if queue_id else "Unknown"
            row_base = {"AgentName": agent_name, "WorkgroupName": queue_name, "Id": f"{user_id}|{queue_id}"}

        data_list = result_row.get('data', [])
        for interval_data in data_list:
            row = row_base.copy()
            raw_interval = interval_data.get('interval')
            if raw_interval:
                try:
                    start_str = raw_interval.split('/')[0].replace('Z', '')
                    dt_utc = datetime.fromisoformat(start_str)
                    dt_local = dt_utc + timedelta(hours=3)
                    row["Interval"] = dt_local.strftime("%Y-%m-%d %H:%M")
                except:
                    row["Interval"] = raw_interval

            metrics = interval_data.get('metrics', [])
            for metric in metrics:
                m_name = metric.get('metric')
                stats = metric.get('stats', {})
                val = 0
                
                if m_name.startswith("t"):
                    val = stats.get('sum', 0) / 1000
                    count_metric_name = "n" + m_name[1:]
                    row[count_metric_name] = stats.get('count', 0)
                    if m_name == "tHandle": row["CountHandle"] = stats.get('count', 0)
                    if m_name == "tAlert": row["nAlert"] = stats.get('count', 0)
                elif m_name.startswith("n") or m_name.startswith("o"):
                    val = stats.get('count', 0)
                
                row[m_name] = val
            data.append(row)

    df = pd.DataFrame(data)
    if not df.empty:
        if report_type == 'detailed':
            group_cols = ["AgentName", "WorkgroupName", "Id"]
        else:
            group_cols = ["Name", "Id"]
            
        if "Interval" in df.columns:
            group_cols.insert(0, "Interval")
            
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        df = df.groupby(group_cols)[numeric_cols].sum().reset_index()
        
        if report_type in ['user', 'agent', 'detailed']:
            if "nOffered" in df.columns and "nAlert" in df.columns:
                df["nOffered"] = df.apply(lambda x: x["nAlert"] if x["nOffered"] == 0 else x["nOffered"], axis=1)
        
        if "tHandle" in df.columns and "CountHandle" in df.columns:
            df["AvgHandle"] = df.apply(lambda x: x["tHandle"] / x["CountHandle"] if x["CountHandle"] > 0 else 0, axis=1).round(2)

        helper_cols = ["nAlert", "ntTalk", "ntAnswered", "ntAbandon", "ntHandle", "ntWait", "ntAcd", "ntAcw", "ntHeld", "CountHandle"]
        cols_to_drop = [c for c in helper_cols if c in df.columns]
        if cols_to_drop: df = df.drop(columns=cols_to_drop)

        for col in df.select_dtypes(include=['float']).columns:
            df[col] = df[col].round(2)
            
    return df

def fill_interval_gaps(df, start_dt_local, end_dt_local, granularity):
    if df.empty or "Interval" not in df.columns: return df
    delta = timedelta(days=1)
    if granularity == "PT30M": delta = timedelta(minutes=30)
    elif granularity == "PT1H": delta = timedelta(hours=1)
    
    expected_intervals = []
    curr = start_dt_local
    while curr < end_dt_local:
        expected_intervals.append(curr.strftime("%Y-%m-%d %H:%M"))
        curr += delta
    
    group_cols = [c for c in df.columns if c not in df.select_dtypes(include=['number']).columns and c != "Interval"]
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    zero_template = {c: 0 for c in numeric_cols}

    unique_groups = df[group_cols].drop_duplicates()
    new_rows = []
    
    for _, group_row in unique_groups.iterrows():
        existing_intervals = set(df.merge(group_row.to_frame().T)["Interval"].tolist())
        for interval in expected_intervals:
            if interval not in existing_intervals:
                new_row = group_row.to_dict()
                new_row["Interval"] = interval
                new_row.update(zero_template)
                new_rows.append(new_row)
    
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        df = df.sort_values(by=["Interval"] + group_cols).reset_index(drop=True)
    return df

def process_observations(resp, id_map, presence_map=None):
    data_map = {k: {"Queue": v, "Waiting": 0, "Interacting": 0, "ServiceLevel": 0, "Members": 0, "OnQueue": 0, 
                    "OnQueueIdle": 0, "OnQueueInteracting": 0, "ActiveUsers": 0,
                    "Presences": {"Available": 0, "Busy": 0, "Away": 0, "Offline": 0, "On Queue": 0}} 
                for k, v in id_map.items()}

    if not resp or 'results' not in resp:
        return list(data_map.values())

    for result in resp['results']:
        queue_id = result.get('group', {}).get("queueId")
        if queue_id in data_map:
            row = data_map[queue_id]
            data_points = result.get('data', [])
            sl_values = []
            for dp in data_points:
                m = dp.get('metric')
                val = dp.get('stats', {}).get('count', 0)
                qualifier = dp.get('qualifier', "")
                
                if m == "oWaiting": row["Waiting"] += val
                elif m == "oInteracting": row["Interacting"] += val
                elif m == "oServiceLevel": sl_values.append(val)
                elif m == "oMemberUsers": row["Members"] = max(row["Members"], val)
                elif m == "oActiveUsers": row["ActiveUsers"] = max(row["ActiveUsers"], val)
                elif m == "oOnQueueUsers":
                    if qualifier.upper() == "IDLE": row["OnQueueIdle"] += val
                    elif qualifier.upper() == "INTERACTING": row["OnQueueInteracting"] += val
                    else: row["OnQueue"] = max(row["OnQueue"], val)
                elif m == "oUserPresences":
                    # Map UUID to System Presence if map provided
                    if presence_map and qualifier in presence_map:
                        mapped_qualifier = presence_map[qualifier]
                    else:
                        mapped_qualifier = qualifier
                        
                    q_lower = mapped_qualifier.lower()
                    if "available" in q_lower: row["Presences"]["Available"] += val
                    elif "busy" in q_lower or "meeting" in q_lower or "do not disturb" in q_lower: row["Presences"]["Busy"] += val
                    elif "away" in q_lower or "break" in q_lower or "meal" in q_lower: row["Presences"]["Away"] += val
                    elif "offline" in q_lower: row["Presences"]["Offline"] += val
                    elif "on queue" in q_lower or "onqueue" in q_lower: row["Presences"]["On Queue"] += val
            
            if sl_values:
                row["ServiceLevel"] = round((sum(sl_values) / len(sl_values)) * 100, 1)

    return list(data_map.values())

def to_excel(df):
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    return output.getvalue()
