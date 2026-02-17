from datetime import datetime, timezone
from src.lang import get_text

def _parse_wait_seconds(val):
    """Parses wait duration from timestamps or numeric durations."""
    if val is None:
        return None
    now = datetime.now(timezone.utc)
    try:
        if isinstance(val, str):
            s = val.strip()
            if not s:
                return None
            # Numeric string
            if s.replace(".", "", 1).isdigit():
                val = float(s)
            else:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                return max((now - dt).total_seconds(), 0)
        if isinstance(val, (int, float)):
            v = float(val)
            # Epoch milliseconds
            if v > 1e12:
                return max((now.timestamp() * 1000 - v) / 1000, 0)
            # Epoch seconds
            if v > 1e9:
                return max(now.timestamp() - v, 0)
            # Assume duration in seconds
            return max(v, 0)
    except Exception:
        return None
    return None

def _extract_wait_seconds(conv):
    """Extracts best-effort wait duration in seconds from a queue conversation payload."""
    candidates = []
    def add_candidate(v):
        s = _parse_wait_seconds(v)
        if s is not None:
            candidates.append(s)

    for key in ["queueTime", "queueStartTime", "enqueueTime", "startTime", "conversationStart",
                "waitTime", "timeInQueue", "queueDuration", "waitingTime", "waitSeconds"]:
        if isinstance(conv, dict) and conv.get(key) is not None:
            add_candidate(conv.get(key))

    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        for key in ["queueTime", "queueStartTime", "enqueueTime", "startTime", "connectedTime"]:
            if p.get(key) is not None:
                add_candidate(p.get(key))
        for s in p.get("sessions", []) or []:
            for key in ["queueTime", "queueStartTime", "enqueueTime", "startTime", "connectedTime"]:
                if s.get(key) is not None:
                    add_candidate(s.get(key))
            for seg in s.get("segments", []) or []:
                stype = str(seg.get("segmentType", "")).lower()
                if stype in ["queue", "alert", "acd"] and seg.get("segmentStart") and not seg.get("segmentEnd"):
                    add_candidate(seg.get("segmentStart"))

    if not candidates:
        return None
    return max(candidates)

def _extract_media_type(conv):
    if not isinstance(conv, dict):
        return None
    # Check for callback at conversation level first
    if conv.get("mediaType"):
        mt = conv.get("mediaType").lower()
        if mt == "callback":
            return "callback"
        # Check if this is a callback-originated voice call
        if mt == "voice" and _is_callback_conversation(conv):
            return "callback"
        return conv.get("mediaType")
    participants = conv.get("participants") or conv.get("participantsDetails") or []
    found_media = None
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        # Check for callback purpose
        if purpose == "outbound":
            for s in p.get("sessions", []) or []:
                mt = (s.get("mediaType") or "").lower()
                if mt == "callback":
                    return "callback"
        for s in p.get("sessions", []) or []:
            mt = s.get("mediaType")
            if mt:
                if mt.lower() == "callback":
                    return "callback"
                if not found_media:
                    found_media = mt
    # If voice but callback-originated
    if found_media and found_media.lower() == "voice" and _is_callback_conversation(conv):
        return "callback"
    return found_media

def _is_callback_conversation(conv):
    """Check if conversation originated from a callback request."""
    if not isinstance(conv, dict):
        return False
    # Check participants for callback indicators
    participants = conv.get("participants") or conv.get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose == "outbound":
            # Outbound purpose with agent usually indicates callback
            for s in p.get("sessions", []) or []:
                if s.get("mediaType", "").lower() == "callback":
                    return True
        # Check session-level callback indicators
        for s in p.get("sessions", []) or []:
            if s.get("mediaType", "").lower() == "callback":
                return True
            # Check for callback direction
            direction = s.get("direction", "").lower()
            if direction == "outbound" and (p.get("purpose") or "").lower() in ["agent", "user"]:
                # Agent making outbound call could be callback
                pass
    # Check attributes for callback origin
    attributes = conv.get("attributes") or {}
    for key in attributes:
        if "callback" in key.lower():
            return True
    return False

def _seconds_since(iso_str):
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
        return max((datetime.now(timezone.utc) - dt).total_seconds(), 0)
    except Exception:
        return None

def _has_ivr_participant(conv):
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["ivr", "flow"]:
            return True
    return False

def _session_is_active(session):
    state = (session.get("state") or "").lower()
    if session.get("disconnectedTime"):
        return False
    return state in ["alerting", "connected", "offering", "dialing", "communicating", "contacting"]

def _classify_conversation_state(conv):
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    has_agent = False
    has_queue = False
    has_ivr = False
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        sessions = p.get("sessions", []) or []
        if purpose in ["ivr", "flow"] and not sessions:
            has_ivr = True
        for s in sessions:
            active = _session_is_active(s)
            if not active:
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

def _extract_direction_label(conv):
    direction = (conv or {}).get("originatingDirection") or (conv or {}).get("direction")
    if direction:
        direction = str(direction).lower()
    if direction and "inbound" in direction:
        return "Inbound"
    if direction and "outbound" in direction:
        return "Outbound"
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose == "outbound":
            return "Outbound"
        for s in p.get("sessions", []) or []:
            sd = (s.get("direction") or "").lower()
            if sd == "inbound":
                return "Inbound"
            if sd == "outbound":
                return "Outbound"
    # Do not force Inbound on ambiguous payloads (external+agent) because outbound
    # conversations can look similar when direction fields are missing.
    return None

def _normalize_call_direction_token(direction_label=None, direction=None):
    raw = str(direction_label or direction or "").lower()
    if "inbound" in raw:
        return "inbound"
    if "outbound" in raw:
        return "outbound"
    return None

def _normalize_call_media_token(media_type):
    mt = str(media_type or "").strip().lower()
    if not mt:
        return None
    if "callback" in mt:
        return "callback"
    if mt in {"voice", "call", "phone", "telephony"} or "voice" in mt:
        return "voice"
    message_aliases = {
        "message", "messages", "sms", "chat", "webchat", "email",
        "webmessaging", "openmessaging", "whatsapp", "facebook", "twitter", "line", "telegram",
    }
    if mt in message_aliases:
        return "message"
    if any(k in mt for k in ["message", "chat", "email", "sms", "whatsapp", "facebook", "twitter", "line", "telegram"]):
        return "message"
    return mt

def _normalize_call_state_token(item):
    if not isinstance(item, dict):
        return None

    state_raw = str(item.get("state") or "").strip().lower()
    if state_raw in {"interacting", "connected", "communicating", "active"}:
        return "connected"

    state_label_raw = str(item.get("state_label") or "").strip().lower()
    if any(token in state_label_raw for token in ["bağlandı", "baglandi", "interacting", "connected"]):
        return "connected"

    # Some WS/API payloads can keep "waiting" state briefly while agent fields are already set.
    # Prefer agent evidence so filter behavior matches rendered "Bağlandı/Bekleyen" labels.
    if item.get("agent_name") or item.get("agent_id"):
        return "connected"

    if state_raw in {"waiting", "queued", "queue", "alerting", "offering", "dialing", "contacting"}:
        return "waiting"
    if any(token in state_label_raw for token in ["bekleyen", "waiting", "queued", "queue"]):
        return "waiting"
    return "waiting"

def _call_filter_tokens(item):
    direction_token = _normalize_call_direction_token(item.get("direction_label"), item.get("direction"))
    media_token = _normalize_call_media_token(item.get("media_type"))
    state_token = _normalize_call_state_token(item)
    return direction_token, media_token, state_token

def _call_matches_filters(item, direction_filters=None, media_filters=None, state_filters=None):
    direction_filters = {str(x).lower() for x in (direction_filters or []) if x}
    media_filters = {str(x).lower() for x in (media_filters or []) if x}
    state_filters = {str(x).lower() for x in (state_filters or []) if x}
    direction_token, media_token, state_token = _call_filter_tokens(item)
    if direction_filters and direction_token not in direction_filters:
        return False
    if media_filters and media_token not in media_filters:
        return False
    if state_filters and state_token not in state_filters:
        return False
    return True

def _extract_queue_name_from_conv(conv, queue_id_to_name=None):
    queue_id_to_name = queue_id_to_name or {}
    fallback_name = None
    def _remember_name(name):
        nonlocal fallback_name
        if not name:
            return
        if not fallback_name:
            fallback_name = name
        if not _is_generic_queue_name(name):
            fallback_name = name

    if isinstance(conv, dict):
        qname = conv.get("queueName")
        if qname:
            if not _is_generic_queue_name(qname):
                return qname
            _remember_name(qname)
        qid = conv.get("queueId")
        if qid and qid in queue_id_to_name:
            mapped = queue_id_to_name.get(qid)
            if mapped and not _is_generic_queue_name(mapped):
                return mapped
            _remember_name(mapped)
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["acd", "queue"]:
            q_id = p.get("queueId") or p.get("routingQueueId")
            if q_id and q_id in queue_id_to_name:
                mapped = queue_id_to_name.get(q_id)
                if mapped and not _is_generic_queue_name(mapped):
                    return mapped
                _remember_name(mapped)
            name = p.get("name")
            if name:
                if not _is_generic_queue_name(name):
                    return name
                _remember_name(name)
            qobj = p.get("queue") or {}
            if isinstance(qobj, dict):
                if qobj.get("name"):
                    qname = qobj.get("name")
                    if not _is_generic_queue_name(qname):
                        return qname
                    _remember_name(qname)
                qid = qobj.get("id")
                if qid and qid in queue_id_to_name:
                    mapped = queue_id_to_name.get(qid)
                    if mapped and not _is_generic_queue_name(mapped):
                        return mapped
                    _remember_name(mapped)
            for s in p.get("sessions", []) or []:
                qid = s.get("queueId") or s.get("routingQueueId")
                if qid and qid in queue_id_to_name:
                    mapped = queue_id_to_name.get(qid)
                    if mapped and not _is_generic_queue_name(mapped):
                        return mapped
                    _remember_name(mapped)
                qname = s.get("queueName")
                if qname:
                    if not _is_generic_queue_name(qname):
                        return qname
                    _remember_name(qname)
                # Analytics API: queueId is inside segments
                for seg in s.get("segments", []) or []:
                    qid = seg.get("queueId")
                    if qid and qid in queue_id_to_name:
                        mapped = queue_id_to_name.get(qid)
                        if mapped and not _is_generic_queue_name(mapped):
                            return mapped
                        _remember_name(mapped)
                    qname = seg.get("queueName")
                    if qname:
                        if not _is_generic_queue_name(qname):
                            return qname
                        _remember_name(qname)
                    qobj = seg.get("queue") or {}
                    if isinstance(qobj, dict):
                        qname = qobj.get("name")
                        if qname:
                            if not _is_generic_queue_name(qname):
                                return qname
                            _remember_name(qname)
                        qid = qobj.get("id")
                        if qid and qid in queue_id_to_name:
                            mapped = queue_id_to_name.get(qid)
                            if mapped and not _is_generic_queue_name(mapped):
                                return mapped
                            _remember_name(mapped)
        # Outbound/direct calls may carry queue info on non-acd participants.
        q_id = p.get("queueId") or p.get("routingQueueId")
        if q_id and q_id in queue_id_to_name:
            mapped = queue_id_to_name.get(q_id)
            if mapped and not _is_generic_queue_name(mapped):
                return mapped
            _remember_name(mapped)
        qobj = p.get("queue") or {}
        if isinstance(qobj, dict):
            qname = qobj.get("name")
            if qname:
                if not _is_generic_queue_name(qname):
                    return qname
                _remember_name(qname)
            qid = qobj.get("id")
            if qid and qid in queue_id_to_name:
                mapped = queue_id_to_name.get(qid)
                if mapped and not _is_generic_queue_name(mapped):
                    return mapped
                _remember_name(mapped)
        qname = p.get("queueName")
        if qname:
            if not _is_generic_queue_name(qname):
                return qname
            _remember_name(qname)
        for s in p.get("sessions", []) or []:
            qid = s.get("queueId") or s.get("routingQueueId")
            if qid and qid in queue_id_to_name:
                mapped = queue_id_to_name.get(qid)
                if mapped and not _is_generic_queue_name(mapped):
                    return mapped
                _remember_name(mapped)
            qname = s.get("queueName")
            if qname:
                if not _is_generic_queue_name(qname):
                    return qname
                _remember_name(qname)
            qobj = s.get("queue") or {}
            if isinstance(qobj, dict):
                qname = qobj.get("name")
                if qname:
                    if not _is_generic_queue_name(qname):
                        return qname
                    _remember_name(qname)
                qid = qobj.get("id")
                if qid and qid in queue_id_to_name:
                    mapped = queue_id_to_name.get(qid)
                    if mapped and not _is_generic_queue_name(mapped):
                        return mapped
                    _remember_name(mapped)
            for seg in s.get("segments", []) or []:
                qid = seg.get("queueId")
                if qid and qid in queue_id_to_name:
                    mapped = queue_id_to_name.get(qid)
                    if mapped and not _is_generic_queue_name(mapped):
                        return mapped
                    _remember_name(mapped)
                qname = seg.get("queueName")
                if qname:
                    if not _is_generic_queue_name(qname):
                        return qname
                    _remember_name(qname)
                qobj = seg.get("queue") or {}
                if isinstance(qobj, dict):
                    qname = qobj.get("name")
                    if qname:
                        if not _is_generic_queue_name(qname):
                            return qname
                        _remember_name(qname)
                    qid = qobj.get("id")
                    if qid and qid in queue_id_to_name:
                        mapped = queue_id_to_name.get(qid)
                        if mapped and not _is_generic_queue_name(mapped):
                            return mapped
                        _remember_name(mapped)
    # Analytics segments
    for seg in (conv or {}).get("segments") or []:
        qname = seg.get("queueName")
        if qname:
            if not _is_generic_queue_name(qname):
                return qname
            _remember_name(qname)
        qobj = seg.get("queue") or {}
        if isinstance(qobj, dict):
            if qobj.get("name"):
                qname = qobj.get("name")
                if not _is_generic_queue_name(qname):
                    return qname
                _remember_name(qname)
            qid = qobj.get("id")
            if qid and qid in queue_id_to_name:
                mapped = queue_id_to_name.get(qid)
                if mapped and not _is_generic_queue_name(mapped):
                    return mapped
                _remember_name(mapped)
        qid = seg.get("queueId")
        if qid and qid in queue_id_to_name:
            mapped = queue_id_to_name.get(qid)
            if mapped and not _is_generic_queue_name(mapped):
                return mapped
            _remember_name(mapped)
    return fallback_name

def _extract_queue_id_from_conv(conv):
    if isinstance(conv, dict):
        qid = conv.get("queueId")
        if qid:
            return qid
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["acd", "queue"]:
            q_id = p.get("queueId") or p.get("routingQueueId")
            if q_id:
                return q_id
            qobj = p.get("queue") or {}
            if isinstance(qobj, dict):
                qid = qobj.get("id")
                if qid:
                    return qid
            # Analytics API: queueId is in sessions > segments
            for s in p.get("sessions", []) or []:
                qid = s.get("queueId") or s.get("routingQueueId")
                if qid:
                    return qid
                for seg in s.get("segments", []) or []:
                    qid = seg.get("queueId")
                    if qid:
                        return qid
                    qobj = seg.get("queue") or {}
                    if isinstance(qobj, dict):
                        qid = qobj.get("id")
                        if qid:
                            return qid
    # Some outbound/direct payloads keep queue data on non-acd participants.
    for p in participants:
        qid = p.get("queueId") or p.get("routingQueueId")
        if qid:
            return qid
        qobj = p.get("queue") or {}
        if isinstance(qobj, dict):
            qid = qobj.get("id")
            if qid:
                return qid
        for s in p.get("sessions", []) or []:
            qid = s.get("queueId") or s.get("routingQueueId")
            if qid:
                return qid
            qobj = s.get("queue") or {}
            if isinstance(qobj, dict):
                qid = qobj.get("id")
                if qid:
                    return qid
            for seg in s.get("segments", []) or []:
                qid = seg.get("queueId")
                if qid:
                    return qid
                qobj = seg.get("queue") or {}
                if isinstance(qobj, dict):
                    qid = qobj.get("id")
                    if qid:
                        return qid
    # Conversation-level segments
    for seg in (conv or {}).get("segments") or []:
        qid = seg.get("queueId")
        if qid:
            return qid
        qobj = seg.get("queue") or {}
        if isinstance(qobj, dict):
            qid = qobj.get("id")
            if qid:
                return qid
    return None

def _extract_agent_id_from_conv(conv):
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["agent", "user"]:
            return p.get("userId") or (p.get("user") or {}).get("id") or p.get("participantId")
    return None

def _extract_agent_name_from_conv(conv):
    participants = (conv or {}).get("participants") or (conv or {}).get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["agent", "user"]:
            return p.get("name") or (p.get("user") or {}).get("name")
    return None

def _is_generic_queue_name(name):
    if not name:
        return True
    return str(name).strip().lower() in ["aktif", "active"]

def _merge_call(existing, incoming):
    if not existing:
        return dict(incoming) if incoming else {}
    merged = dict(existing)
    def _state_rank(v):
        v = (v or "").lower()
        if v == "interacting":
            return 3
        if v == "waiting":
            return 2
        if v == "ivr":
            return 1
        return 0
    for k, v in (incoming or {}).items():
        if v is None or v == "":
            continue
        if k == "media_type":
            existing_mt = str(existing.get("media_type") or "").lower()
            incoming_mt = str(v).lower()
            # Never downgrade callback to voice
            if existing_mt == "callback" and incoming_mt == "voice":
                continue
            merged[k] = v
            continue
        if k == "queue_name":
            if _is_generic_queue_name(v) and not _is_generic_queue_name(existing.get("queue_name")):
                continue
            merged[k] = v
            continue
        if k == "state":
            if _state_rank(v) >= _state_rank(existing.get("state")):
                merged[k] = v
            continue
        merged[k] = v
    if (merged.get("agent_name") or merged.get("agent_id")) and _state_rank(merged.get("state")) < _state_rank("interacting"):
        merged["state"] = "interacting"
    return merged

def _fetch_conversation_meta(api, conv_id, queue_id_to_name, users_info=None):
    if not api or not conv_id:
        return None
    try:
        conv = api.get_conversation(conv_id)
    except Exception:
        conv = None
    if not conv:
        return None
    if conv.get("conversationEnd"):
        return {
            "conversation_id": conv_id,
            "ended": True,
        }
    queue_id = _extract_queue_id_from_conv(conv)
    queue_name = _extract_queue_name_from_conv(conv, queue_id_to_name)
    agent_id = _extract_agent_id_from_conv(conv)
    agent_name = _extract_agent_name_from_conv(conv)
    if not agent_name and agent_id and users_info:
        agent_name = users_info.get(agent_id, {}).get("name")
    phone = _extract_phone_from_conv(conv)
    direction_label = _extract_direction_label(conv)
    ivr_attrs = _extract_ivr_attributes(conv)
    wg = _extract_workgroup_from_attrs(ivr_attrs) or queue_name
    return {
        "conversation_id": conv_id,
        "queue_id": queue_id,
        "queue_name": queue_name,
        "phone": phone,
        "direction": conv.get("originatingDirection") or conv.get("direction"),
        "direction_label": direction_label,
        "wg": wg,
        "media_type": _extract_media_type(conv),
        "ended": False,
        "agent_id": agent_id,
        "agent_name": agent_name,
    }

def _extract_ivr_attributes(conv):
    """
    Extract IVR/workgroup DTMF selections and attributes from conversation.
    Genesys stores these in:
    - conversation.attributes (custom flow data)
    - participant.attributes (IVR participant data)
    - segment.wrapUpCode (wrap-up selections)
    - flow outcomes and variables
    """
    if not isinstance(conv, dict):
        return {}
    
    result = {}
    
    # 1. Conversation-level attributes (most common for IVR data)
    conv_attrs = conv.get("attributes") or {}
    if conv_attrs:
        for key, val in conv_attrs.items():
            if val:
                result[key] = val
    
    # 2. Check participants for IVR/flow data
    participants = conv.get("participants") or conv.get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        
        # All participant attributes
        p_attrs = p.get("attributes") or {}
        if p_attrs:
            for key, val in p_attrs.items():
                if val:
                    result[key] = val
        
        # Flow/IVR purpose - extract extra data
        if purpose in ["ivr", "flow", "acd"]:
            # Check sessions for flow outcomes
            for s in p.get("sessions") or []:
                s_attrs = s.get("attributes") or {}
                for key, val in s_attrs.items():
                    if val:
                        result[key] = val
                
                # Check segments for flow outcomes
                for seg in s.get("segments") or []:
                    seg_attrs = seg.get("attributes") or {}
                    for key, val in seg_attrs.items():
                        if val:
                            result[key] = val
                    
                    # Also check flowOutcome and flowOutcomeValue
                    if seg.get("flowOutcome"):
                        result["flowOutcome"] = seg.get("flowOutcome")
                    if seg.get("flowOutcomeValue"):
                        result["flowOutcomeValue"] = seg.get("flowOutcomeValue")
    
    return result

def _format_ivr_display(ivr_attrs):
    """Format IVR attributes for display in UI."""
    if not ivr_attrs:
        return None
    
    # Priority order for display - include more patterns
    priority_keys = ["workgroup", "dtmf", "menu", "selection", "departman", "secim", "choice", "option", "priority", "note", "callback"]
    
    # Find the most relevant value to display
    for pkey in priority_keys:
        for key, val in ivr_attrs.items():
            if pkey in key.lower() and val:
                # Format nicely: "ivr.Priority: 50" -> "Priority: 50"
                display_key = key.split(".")[-1] if "." in key else key
                return f"{display_key}: {val}"
    
    # If no priority match, return first non-empty value
    for key, val in ivr_attrs.items():
        if val:
            display_key = key.split(".")[-1] if "." in key else key
            return f"{display_key}: {val}"
    
    return None

def _extract_workgroup_from_attrs(ivr_attrs):
    if not isinstance(ivr_attrs, dict):
        return None
    priority_keys = ["workgroup", "wg", "departman", "department", "menu", "selection", "secim"]
    for pkey in priority_keys:
        for key, val in ivr_attrs.items():
            if not val:
                continue
            key_l = str(key).lower()
            if pkey in key_l:
                return str(val)
    return None

def _build_active_calls(conversations, lang, queue_id_to_name=None, users_info=None):
    items = []
    for conv in conversations or []:
        if conv.get("conversationEnd"):
            continue
        mt = _extract_media_type(conv)
        state = _classify_conversation_state(conv)
        direction_label = _extract_direction_label(conv)
        queue_id = _extract_queue_id_from_conv(conv)
        queue_name = _extract_queue_name_from_conv(conv, queue_id_to_name) or "Aktif"
        agent_id = _extract_agent_id_from_conv(conv)
        agent_name = _extract_agent_name_from_conv(conv)
        if not agent_name and agent_id and users_info:
            agent_name = users_info.get(agent_id, {}).get("name")
        if state == "interacting":
            state_label = get_text(lang, "interacting")
        elif state == "waiting":
            state_label = get_text(lang, "waiting")
        elif state == "ivr":
            state_label = "IVR"
        else:
            state_label = None

        wait_s = _extract_wait_seconds(conv)
        if wait_s is None:
            wait_s = _seconds_since(conv.get("conversationStart"))

        conv_id = conv.get("conversationId") or conv.get("id")
        items.append({
            "conversation_id": conv_id,
            "queue_id": queue_id,
            "queue_name": queue_name,
            "wait_seconds": wait_s,
            "phone": _extract_phone_from_conv(conv),
            "direction_label": direction_label,
            "state_label": state_label,
            "media_type": mt,
            "agent_id": agent_id,
            "agent_name": agent_name,
        })
    return items

def _extract_phone_from_conv(conv):
    """Best-effort phone extraction from queue conversations payload."""
    if not isinstance(conv, dict):
        return None
    def _normalize_phone(v):
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        if "<" in s and ">" in s:
            left = s.find("<")
            right = s.rfind(">")
            if left >= 0 and right > left:
                s = s[left + 1:right].strip()
        s_low = s.lower()
        for prefix in ("tel:", "sip:", "sips:"):
            if s_low.startswith(prefix):
                s = s[len(prefix):].strip()
                s_low = s.lower()
                break
        s = s.split(";", 1)[0].strip()
        local = s.split("@", 1)[0].strip() if "@" in s else s
        candidates = [local, s]
        for c in candidates:
            c = (c or "").strip().strip("\"").strip("'")
            if not c:
                continue
            has_plus = c.startswith("+")
            if any(ch.isalpha() for ch in c):
                continue
            digits = "".join(ch for ch in c if ch.isdigit())
            if len(digits) >= 7:
                return ("+" + digits) if has_plus else digits
        return None

    participants = conv.get("participants") or conv.get("participantsDetails") or []
    for p in participants:
        purpose = (p.get("purpose") or "").lower()
        if purpose in ["external", "customer", "outbound"]:
            for k in ["ani", "addressOther", "address", "callerId", "fromAddress", "toAddress", "dnis", "name"]:
                phone = _normalize_phone(p.get(k))
                if phone:
                    return phone
            for s in p.get("sessions", []) or []:
                for k in ["ani", "addressOther", "address", "callerId", "fromAddress", "toAddress", "dnis"]:
                    phone = _normalize_phone(s.get(k))
                    if phone:
                        return phone
    # Fallback to conversation-level fields.
    for k in ["ani", "addressOther", "fromAddress", "toAddress", "callerId", "dnis", "address"]:
        phone = _normalize_phone(conv.get(k))
        if phone:
            return phone
    return None
