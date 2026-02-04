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
                
                # Use tAnswered count for answered calls
                if m == "tAnswered":
                    stats[q_name]["Answered"] += s.get('count', 0)
                # Use tAbandon count for abandoned calls
                elif m == "tAbandon":
                    stats[q_name]["Abandoned"] += s.get('count', 0)
                elif m == "oServiceLevel":
                    stats[q_name]["SL_Numerator"] += s.get('numerator', 0)
                    stats[q_name]["SL_Denominator"] += s.get('denominator', 0)
        
        # Calculate Offered as Answered + Abandoned (matches Genesys workgroup view)
        stats[q_name]["Offered"] = stats[q_name]["Answered"] + stats[q_name]["Abandoned"]
    
    return stats

def process_analytics_response(response, lookup_map, report_type, queue_map=None):
    """Processes dictionary-based analytics response into DataFrame."""
    data = []
    if not response or 'results' not in response:
        return pd.DataFrame()

    for result_row in response['results']:
        group = result_row.get('group', {})
        user_id = group.get("userId")
        queue_id = group.get("queueId")
        
        row_base = {}
        if report_type in ['user', 'agent', 'productivity']:
            user_info = lookup_map.get(user_id, {}) if user_id else {}
            name = user_info.get('name', user_id if user_id else "Unknown")
            raw_username = user_info.get('username', "")
            username = raw_username.split('@')[0] if raw_username else ""
            row_base = {"Name": name, "Username": username, "Id": user_id}
        elif report_type in ['queue', 'workgroup']:
            name = lookup_map.get(queue_id, queue_id) if queue_id else "Unknown"
            row_base = {"Name": name, "Id": queue_id}
        elif report_type == 'detailed':
            user_info = lookup_map.get(user_id, {}) if user_id else {}
            agent_name = user_info.get('name', user_id if user_id else "Unknown")
            raw_username = user_info.get('username', "")
            username = raw_username.split('@')[0] if raw_username else ""
            queue_name = queue_map.get(queue_id, queue_id) if queue_map and queue_id else (lookup_map.get(queue_id, queue_id) if queue_id else "Unknown")
            row_base = {"AgentName": agent_name, "Username": username, "WorkgroupName": queue_name, "Id": f"{user_id}|{queue_id}"}

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
                
                # Manual aliases for re-mapped metrics
                if m_name == "tAcw": row["nWrapup"] = stats.get('count', 0)
                if m_name == "tNotResponding": row["nNotResponding"] = stats.get('count', 0)
                if m_name == "nOutbound": row["nOutbound"] = val
                if m_name == "nOffered": row["nAlert"] = val  # For agents, Offered = Alerted
                if m_name == "tAlert": row["nAlert"] = stats.get('count', 0)
                if m_name == "tHandle": row["nHandled"] = stats.get('count', 0)
                
                # Aliases for missing metrics that map to tTalk in aggregate view
                if m_name == "tTalk":
                    row["tOutbound"] = val
                    row["tTalkComplete"] = val
            data.append(row)

    df = pd.DataFrame(data)
    if not df.empty:
        if report_type == 'detailed':
            group_cols = ["AgentName", "Username", "WorkgroupName", "Id"]
        elif report_type in ['user', 'agent', 'productivity']:
            group_cols = ["Name", "Username", "Id"]
        else:
            group_cols = ["Name", "Id"]
            
        if "Interval" in df.columns:
            group_cols.insert(0, "Interval")
            
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        df = df.groupby(group_cols)[numeric_cols].sum().reset_index()
        
        if report_type in ['user', 'agent', 'detailed', 'productivity']:
            # nOffered fix: Use nAlert as fallback, and ensure it's at least nAnswered
            if "nOffered" not in df.columns: df["nOffered"] = 0
            

        
        if "tHandle" in df.columns and "CountHandle" in df.columns:
            df["AvgHandle"] = df.apply(lambda x: x["tHandle"] / x["CountHandle"] if x["CountHandle"] > 0 else 0, axis=1).round(2)

        helper_cols = ["ntTalk", "ntAnswered", "ntAbandon", "ntHandle", "ntWait", "ntAcd", "ntAcw", "ntHeld", "CountHandle"]
        cols_to_drop = [c for c in helper_cols if c in df.columns]
        if cols_to_drop: df = df.drop(columns=cols_to_drop)

        
        for col in df.select_dtypes(include=['float']).columns:
            df[col] = df[col].round(2)
            
    return df

def format_seconds_to_hms(val):
    if pd.isna(val) or val == 0: return "00:00:00"
    seconds = int(round(val))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def apply_duration_formatting(df):
    target_cols = [c for c in df.columns if (c.startswith('t') or c.startswith('Avg') or c in ['col_staffed_time', 'Duration', 'col_duration']) and pd.api.types.is_numeric_dtype(df[c])]
    for col in target_cols:
        df[col] = df[col].apply(format_seconds_to_hms)
    return df

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

def process_user_aggregates(resp, presence_map=None):
    """Processes user status aggregates into a dictionary of metric durations."""
    results = {}
    if not resp or 'results' not in resp:
        return results

    for result in resp['results']:
        user_id = result.get('group', {}).get('userId')
        user_data = {
            "tMeal": 0, "tMeeting": 0, "tAvailable": 0, "tBusy": 0, 
            "tAway": 0, "tTraining": 0, "tOnQueue": 0, "StaffedTime": 0,
            "nNotResponding": 0
        }
        
        data_list = result.get('data', [])
        for d in data_list:
            metrics = d.get('metrics', [])
            for m_obj in metrics:
                m_name = m_obj.get('metric')
                stats = m_obj.get('stats', {})
                duration = stats.get('sum', 0) / 1000 # Convert to seconds
                
                qualifier = m_obj.get('qualifier', '')
                
                # Map qualifier to standard name
                if presence_map and qualifier in presence_map:
                    mapped = presence_map[qualifier].lower()
                else:
                    mapped = qualifier.lower()
                
                # Map to our columns
                # Handle English, Turkish, and System Enum formats
                
                # On Queue matching (often system presence "ON_QUEUE")
                if "on_queue" in mapped or "on queue" in mapped or "onqueue" in mapped: 
                    user_data["tOnQueue"] += duration
                
                # Meal matching
                elif "meal" in mapped or "yemek" in mapped: 
                    user_data["tMeal"] += duration
                
                # Meeting matching
                elif "meeting" in mapped or "toplantı" in mapped: 
                    user_data["tMeeting"] += duration
                
                # Training matching
                elif "training" in mapped or "eğitim" in mapped: 
                    user_data["tTraining"] += duration
                
                # Available/Ready matching
                elif "available" in mapped or "hazır" in mapped: 
                    user_data["tAvailable"] += duration
                
                # Busy matching
                elif "busy" in mapped or "meşgul" in mapped: 
                    user_data["tBusy"] += duration
                
                # Away matching
                elif "away" in mapped or "uzakta" in mapped: 
                    user_data["tAway"] += duration
                
                if m_name == "tNotResponding":
                    user_data["nNotResponding"] += stats.get('count', 0)

                # Staffed time is generally sum of all except Offline.
                # In User Aggregates, presence metrics represent active presence.
                if m_name in ["tSystemPresence", "tOrganizationPresence"] and "offline" not in mapped:
                    user_data["StaffedTime"] += duration
        
        # Avoid double counting if both system and org presence are returned
        # Usually tOrganizationPresence is more specific.
        results[user_id] = user_data
    return results

def process_user_details(resp):
    """Processes user details to find first login and last logout of the period."""
    results = {}
    if not resp or 'userDetails' not in resp:
        return results

    for user in resp['userDetails']:
        user_id = user.get('userId')
        primary_presences = user.get('primaryPresence', [])
        
        # Filter out offline status
        active_presences = [p for p in primary_presences if p.get('systemPresence', '').lower() != 'offline']
        
        if active_presences:
            # Sort by start time
            active_presences.sort(key=lambda x: x.get('startTime', ''))
            
            first_login_utc = active_presences[0].get('startTime')
            last_logout_utc = active_presences[-1].get('endTime')
            
            # Format and adjust to UTC+3
            from datetime import timedelta
            def format_utc_to_local(utc_str):
                if not utc_str: return "N/A"
                try:
                    dt = datetime.fromisoformat(utc_str.replace('Z', ''))
                    dt_local = dt + timedelta(hours=3)
                    return dt_local.strftime("%H:%M:%S")
                except:
                    return "N/A"
            
            results[user_id] = {
                "Login": format_utc_to_local(first_login_utc),
                "Logout": format_utc_to_local(last_logout_utc)
            }
        else:
            results[user_id] = {"Login": "N/A", "Logout": "N/A"}
            
    return results

def process_conversation_details(response, user_map=None, queue_map=None, wrapup_map=None, include_attributes=False):
    """Flattens conversation detail JSON into a DataFrame."""
    rows = []
    if not response or 'conversations' not in response:
        return pd.DataFrame()

    def fmt_time(iso_str):
        if not iso_str: return ""
        try:
            dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
            return (dt + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
        except: return iso_str

    def sec_to_hms(seconds):
        if not seconds: return "00:00:00"
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    for conv in response['conversations']:
        row = {
            "Id": conv.get("conversationId"),
            "Start": fmt_time(conv.get("conversationStart")),
            "End": fmt_time(conv.get("conversationEnd")),
            "Direction": conv.get("originatingDirection", "N/A"),
            "Ani": "",
            "Dnis": "",
            "Queue": "",
            "Agent": "",
            "Username": "",
            "Wrapup": "",
            "DisconnectType": "",
            "Duration": 0,
            "Talk": 0,
            "Hold": 0,
            "Acw": 0,
            "Wait": 0,
            "Alert": 0,
            "HoldCount": 0,
            "MediaType": ""
        }
        
        if include_attributes:
            # Explicitly initialize requested attributes so they appear as columns
            wanted_attrs = [
                "Guest", "callbackNumber", "queueId", "agentId", "isChat", "ptype", "pid", 
                "callbackNote", "Screen Pop URL", "sid", "customerPhone", "callbackTime", 
                "customerEmail", "triggerSource", "chatType", "callbackCustomerName", "pname", 
                "pdatein", "padult", "pchild", "customerName", "scriptId", "commercialConsent", 
                "sbj", "pageUrl", "pdateout"
            ]
            for wa in wanted_attrs:
                row[wa] = ""

        # Determine metrics from participants
        for p in conv.get("participants", []):
            purpose = p.get("purpose")
            
            # Extract Attributes (Participant Data) if requested
            if include_attributes and p.get("attributes"):
                for k, v in p.get("attributes").items():
                    # Update row with attribute value
                    row[k] = v
                    
                    # Also try to match with wanted_attrs case-insensitively to fill the specific columns
                    for wa in wanted_attrs:
                        if k.lower() == wa.lower():
                             row[wa] = v
            # Also check if participantName holds queue name for purpose=acd

            # Media Type (take first non-empty)
            if not row["MediaType"]:
                for s in p.get("sessions", []):
                    if s.get("mediaType"):
                        row["MediaType"] = s.get("mediaType")
                        break
            
             # Disconnect Type (from peer/agent/external usually)
            if not row["DisconnectType"]:
                for s in p.get("sessions", []):
                    if s.get("disconnectType"):
                         raw_disc = s.get("disconnectType")
                         # Prefer specific ones
                         if raw_disc in ["client", "system", "transfer", "endpoint"]:
                             row["DisconnectType"] = raw_disc
                             break
                         row["DisconnectType"] = raw_disc
            
             # Disconnect Type (from peer/agent/external usually)
            if not row["DisconnectType"]:
                for s in p.get("sessions", []):
                    if s.get("disconnectType"):
                         raw_disc = s.get("disconnectType")
                         # Prefer specific ones
                         if raw_disc in ["client", "system", "transfer", "endpoint"]:
                             row["DisconnectType"] = raw_disc
                             break
                         row["DisconnectType"] = raw_disc
            if purpose in ["external", "customer", "outbound"]: # outbound sometimes used for external
                # Try standard fields first
                if not row["Ani"] and p.get("ani"): row["Ani"] = p.get("ani")
                if not row["Dnis"] and p.get("dnis"): row["Dnis"] = p.get("dnis")
                
                # Check sessions for address info
                for s in p.get("sessions", []):
                    if not row["Ani"] and s.get("ani"): row["Ani"] = s.get("ani")
                    if not row["Dnis"] and s.get("dnis"): row["Dnis"] = s.get("dnis")
                    # addressOther is often the remote party
                    if not row["Ani"] and row["Direction"] == "inbound" and s.get("addressOther"): 
                        row["Ani"] = s.get("addressOther")
                    if not row["Dnis"] and row["Direction"] == "outbound" and s.get("addressOther"):
                         row["Dnis"] = s.get("addressOther")

                # Fallback to participant name if look like phone number
                if not row["Ani"] and p.get("name") and p.get("name").replace("+","").isdigit():
                    row["Ani"] = p.get("name")

            # Queue Info
            if purpose == "acd":
                if not row["Queue"] and p.get("participantName"): row["Queue"] = p.get("participantName")
                if not row["Queue"]:
                    q_id = p.get("participantId") 
                    if queue_map:
                         qs_rev = {v: k for k, v in queue_map.items()}
                         if q_id in qs_rev: row["Queue"] = qs_rev[q_id]
            
            # Agent Info - Check for Outbound Queue here too
            if purpose == "agent" or purpose == "user":
                if not row["Agent"] and p.get("participantName"): 
                     row["Agent"] = p.get("participantName")
                
                # Try to get better name + username
                if p.get("userId"):
                    uid = p.get("userId")
                    if user_map and uid in user_map:
                         u_obj = user_map[uid]
                         if isinstance(u_obj, dict):
                             row["Agent"] = u_obj.get("name", row["Agent"] or uid)
                             if u_obj.get("username"):
                                 row["Username"] = u_obj.get("username").split("@")[0]
                         else:
                             row["Agent"] = str(u_obj)
                
                # Outbound calls on behalf of queue often have queueId in segments
                for s in p.get("sessions", []):
                    # Disconnect type fallback if not set
                    if not row["DisconnectType"] and s.get("disconnectType"):
                        row["DisconnectType"] = s.get("disconnectType")
                        
                    for segment in s.get("segments", []):
                        if not row["Queue"] and segment.get("queueId"):
                            q_id = segment.get("queueId")
                            if queue_map:
                                qs_rev = {v: k for k, v in queue_map.items()} 
                                if q_id in qs_rev: row["Queue"] = qs_rev[q_id]
                                
                        if segment.get("disconnectType") and not row["DisconnectType"]:
                             row["DisconnectType"] = segment.get("disconnectType")

                        if segment.get("segmentEnd") and segment.get("segmentStart"):
                            start = datetime.fromisoformat(segment["segmentStart"].replace('Z', '+00:00'))
                            end = datetime.fromisoformat(segment["segmentEnd"].replace('Z', '+00:00'))
                            dur = (end - start).total_seconds()
                            
                            stype = segment.get("segmentType")
                            if stype == "interact": row["Talk"] += dur
                            elif stype == "hold": 
                                row["Hold"] += dur
                                row["HoldCount"] += 1
                            elif stype == "alert": row["Alert"] += dur
                            elif stype == "wrapup": 
                                row["Acw"] += dur
                                w_code = segment.get("wrapUpCode")
                                if w_code:
                                    if wrapup_map and w_code in wrapup_map:
                                        row["Wrapup"] = wrapup_map[w_code]
                                    else:
                                        row["Wrapup"] = w_code
        
        # Connection/Answer Status Logic
        is_connected = False
        if row["Talk"] > 0: is_connected = True
        else:
            # Check if any customer/external participant had an interact segment that wasn't just alerting
            # Or check if any agent had interact
            pass
            
        if row["Direction"] == "inbound":
             row["ConnectionStatus"] = "Cevaplandı" if is_connected else "Kaçan/Cevapsız"
        elif row["Direction"] == "outbound":
             # For outbound, if not connected:
             # If disconnect type is proper system/error -> Unreachable
             # If just no answer/timeout -> Missed (Cevapsız)
             # But user wants differentiation. "Ulaşılamadı" generally means failure to reach. 
             # "Cevapsız" means it rang but no answer.
             # Genesys often gives 'system' disconnect for unreachable.
             
             if is_connected: row["ConnectionStatus"] = "Ulaşıldı"
             else:
                 d_type = str(row["DisconnectType"]).lower()
                 if d_type in ["system", "error", "uncallable", "timeout"]:
                     row["ConnectionStatus"] = "Ulaşılamadı"
                 else:
                     row["ConnectionStatus"] = "Kaçan/Cevapsız"
        else:
             row["ConnectionStatus"] = "Bağlandı" if is_connected else "Bağlanamadı"

        # Final Disconnect Reason Mapping
        disc_map = {
            "client": "Müşteri",
            "system": "Sistem",
            "transfer": "Transfer",
            "endpoint": "Uç Nokta/Agent",
            "peer": "Agent",
            "error": "Hata",
            "timeout": "Zaman Aşımı",
            "spam": "Spam",
            "uncallable": "Aranamaz"
        }
        raw_disc_lower = str(row["DisconnectType"]).lower()
        if raw_disc_lower in disc_map:
            row["DisconnectType"] = disc_map[raw_disc_lower]
        
        # Clean up ANI/DNIS prefixes
        if row["Ani"]: row["Ani"] = row["Ani"].replace("tel:", "").replace("sip:", "")

        # Clean up ANI/DNIS prefixes
        if row["Ani"]: row["Ani"] = row["Ani"].replace("tel:", "").replace("sip:", "")
        if row["Dnis"]: row["Dnis"] = row["Dnis"].replace("tel:", "").replace("sip:", "")

        # Total duration
        if conv.get("conversationEnd") and conv.get("conversationStart"):
            try:
                s = datetime.fromisoformat(conv["conversationStart"].replace('Z', '+00:00'))
                e = datetime.fromisoformat(conv["conversationEnd"].replace('Z', '+00:00'))
                row["Duration"] = int((e - s).total_seconds())
            except: pass
            
        # Format durations to HH:MM:SS
        row["Talk"] = sec_to_hms(row["Talk"])
        row["Hold"] = sec_to_hms(row["Hold"])
        row["Acw"] = sec_to_hms(row["Acw"])
        row["Wait"] = sec_to_hms(row["Wait"])
        row["Alert"] = sec_to_hms(row["Alert"])
        row["Duration"] = sec_to_hms(row["Duration"])
        
        rows.append(row)

    return pd.DataFrame(rows)

def to_excel(df):
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    return output.getvalue()
