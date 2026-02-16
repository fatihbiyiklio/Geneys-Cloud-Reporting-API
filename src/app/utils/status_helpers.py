import html
import json
import re
from datetime import datetime, timedelta, timezone

def _escape_html(value):
    return html.escape("" if value is None else str(value), quote=True)

def format_status_time(presence_ts, routing_ts):
    """Calculates duration since the most recent status change in HH:MM:SS format."""
    try:
        times = []
        if presence_ts: times.append(datetime.fromisoformat(presence_ts.replace('Z', '+00:00')))
        if routing_ts: times.append(datetime.fromisoformat(routing_ts.replace('Z', '+00:00')))
        if not times: return "00:00:00"
        start_time = max(times)
        diff = datetime.now(timezone.utc) - start_time
        seconds = int(diff.total_seconds())
        if seconds < 0: seconds = 0
        
        hrs = seconds // 3600
        mins = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hrs:02d}:{mins:02d}:{secs:02d}"
    except: return "00:00:00"

def format_duration_seconds(seconds):
    """Formats seconds into HH:MM:SS, returns '-' if None."""
    try:
        if seconds is None:
            return "-"
        seconds = int(max(0, seconds))
        hrs = seconds // 3600
        mins = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hrs:02d}:{mins:02d}:{secs:02d}"
    except:
        return "-"

def _format_iso_with_utc_offset(iso_str, utc_offset_hours=3.0, out_fmt="%Y-%m-%d %H:%M:%S"):
    if not iso_str:
        return "-"
    try:
        dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
        dt_local = dt + timedelta(hours=float(utc_offset_hours or 0))
        return dt_local.strftime(out_fmt)
    except Exception:
        return str(iso_str)

def _resolve_user_label(user_id=None, users_info=None, fallback_name=None):
    uid = str(user_id or "").strip()
    if uid and isinstance(users_info, dict):
        user_obj = users_info.get(uid) or {}
        if isinstance(user_obj, dict):
            name = str(user_obj.get("name") or "").strip()
            if name:
                return name
            username = str(user_obj.get("username") or "").strip()
            if username:
                return username
    if fallback_name:
        return str(fallback_name)
    return uid or "-"

def _normalize_status_value(raw_value, presence_map=None):
    if isinstance(raw_value, dict):
        pd = raw_value.get("presenceDefinition")
        if isinstance(pd, dict):
            return _normalize_status_value(pd, presence_map=presence_map)
        rs = raw_value.get("routingStatus")
        if isinstance(rs, dict):
            return _normalize_status_value(rs, presence_map=presence_map)
        for key in ["label", "systemPresence", "status", "name", "value", "id"]:
            val = raw_value.get(key)
            if val:
                return _normalize_status_value(val, presence_map=presence_map)
        try:
            return json.dumps(raw_value, ensure_ascii=False)
        except Exception:
            return str(raw_value)

    if isinstance(raw_value, list):
        joined = [_normalize_status_value(v, presence_map=presence_map) for v in raw_value]
        joined = [x for x in joined if x and x != "-"]
        return ", ".join(joined) if joined else "-"

    raw = str(raw_value or "").strip()
    if not raw:
        return "-"
    if raw.startswith("{") or raw.startswith("["):
        try:
            return _normalize_status_value(json.loads(raw), presence_map=presence_map)
        except Exception:
            pass

    p_map = presence_map or {}
    p_info = p_map.get(raw)
    if isinstance(p_info, dict):
        p_label = str(p_info.get("label") or "").strip()
        p_sys = str(p_info.get("systemPresence") or "").strip()
        if p_label:
            return p_label
        if p_sys:
            return p_sys.replace("_", " ").title()

    routing_map = {
        "OFF_QUEUE": "Off Queue",
        "IDLE": "On Queue",
        "INTERACTING": "Görüşmede",
        "NOT_RESPONDING": "Cevapsız",
        "COMMUNICATING": "Görüşmede",
    }
    upper = raw.upper()
    if upper in routing_map:
        return routing_map[upper]

    return raw

def _format_status_values(values, presence_map=None):
    if values is None:
        return "-"
    if not isinstance(values, list):
        values = [values]
    cleaned = []
    seen = set()
    for v in values:
        normalized = _normalize_status_value(v, presence_map=presence_map)
        key = normalized.strip().lower()
        if not key or key == "-":
            continue
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(normalized)
    return ", ".join(cleaned) if cleaned else "-"

def _build_status_audit_rows(audit_entities, target_user_id, users_info=None, presence_map=None, utc_offset_hours=3.0):
    rows = []
    if not isinstance(audit_entities, list):
        return rows

    status_property_hints = (
        "presence",
        "routingstatus",
        "routing",
        "onqueue",
        "organizationpresence",
        "systempresence",
        "primarypresence",
    )
    status_text_hints = (
        "presence",
        "routingstatus",
        "routing status",
        "onqueue",
        "on queue",
        "organizationpresence",
        "systempresence",
        "primarypresence",
    )
    old_ctx_keys = [
        "oldStatus",
        "previousStatus",
        "fromStatus",
        "oldPresence",
        "previousPresence",
        "oldRoutingStatus",
        "oldRouting",
    ]
    new_ctx_keys = [
        "newStatus",
        "currentStatus",
        "toStatus",
        "newPresence",
        "currentPresence",
        "newRoutingStatus",
        "newRouting",
    ]
    target_uid = str(target_user_id or "").strip()

    actor_key_tokens = (
        "actoruserid",
        "actinguserid",
        "modifiedby",
        "changedby",
        "initiatedbyuserid",
        "requestinguserid",
        "performedbyuserid",
        "updatedby",
        "updatedbyuserid",
    )
    target_key_tokens = (
        "userid",
        "targetuserid",
        "affecteduserid",
        "memberuserid",
        "agentid",
        "subjectuserid",
        "entityuserid",
    )
    uuid_pattern = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

    def _looks_like_uuid(value):
        val = str(value or "").strip()
        return bool(val and uuid_pattern.match(val))

    def _is_actor_key(key):
        k = str(key or "").strip().lower()
        if not k:
            return False
        if any(tok in k for tok in actor_key_tokens):
            return True
        return ("actor" in k) and (("user" in k) or ("id" in k))

    def _is_target_key(key):
        k = str(key or "").strip().lower()
        if not k:
            return False
        if any(tok == k or tok in k for tok in target_key_tokens):
            return True
        if ("user" in k) or ("agent" in k):
            return not _is_actor_key(k)
        return False

    def _collect_context_user_ids(obj):
        actor_ids = set()
        target_ids = set()
        if obj is None:
            return actor_ids, target_ids

        def _add_if_uuid(bucket, value):
            val = str(value or "").strip()
            if _looks_like_uuid(val):
                bucket.add(val)

        def _walk(node, parent_key=""):
            if isinstance(node, dict):
                for k, v in node.items():
                    key = str(k or "").strip().lower()
                    if isinstance(v, dict):
                        preferred_keys = ("id", "userId", "userid", "agentId", "agentid")
                        for ik in preferred_keys:
                            vv = v.get(ik)
                            if vv in (None, ""):
                                continue
                            if _is_actor_key(key):
                                _add_if_uuid(actor_ids, vv)
                            elif _is_target_key(key):
                                _add_if_uuid(target_ids, vv)
                            break
                        _walk(v, key)
                        continue
                    if isinstance(v, (list, tuple)):
                        if _is_actor_key(key) or _is_target_key(key):
                            for item in v:
                                if isinstance(item, dict):
                                    for ik in ("id", "userId", "userid", "agentId", "agentid"):
                                        if item.get(ik) not in (None, ""):
                                            if _is_actor_key(key):
                                                _add_if_uuid(actor_ids, item.get(ik))
                                            else:
                                                _add_if_uuid(target_ids, item.get(ik))
                                            break
                                else:
                                    if _is_actor_key(key):
                                        _add_if_uuid(actor_ids, item)
                                    else:
                                        _add_if_uuid(target_ids, item)
                        for item in v:
                            _walk(item, key)
                        continue
                    if v in (None, ""):
                        continue
                    if _is_actor_key(key):
                        _add_if_uuid(actor_ids, v)
                    elif _is_target_key(key):
                        _add_if_uuid(target_ids, v)
            elif isinstance(node, (list, tuple)):
                for item in node:
                    _walk(item, parent_key)

        _walk(obj)
        return actor_ids, target_ids

    for audit in audit_entities:
        if not isinstance(audit, dict):
            continue

        entity = audit.get("entity") or {}
        entity_id = str(entity.get("id") or "").strip()
        entity_type = str(audit.get("entityType") or entity.get("type") or "").strip().lower()

        actor = audit.get("user") or {}
        actor_id = str(actor.get("id") or "").strip()
        context = audit.get("context") or {}
        context_actor_ids, context_target_ids = _collect_context_user_ids(context)

        if not _looks_like_uuid(actor_id):
            actor_id = ""
        if (not actor_id) and context_actor_ids:
            actor_id = sorted(context_actor_ids)[0]

        target_candidates = set(context_target_ids)
        if _looks_like_uuid(entity_id):
            # entityType is usually USER for direct user status changes.
            if (not entity_type) or ("user" in entity_type) or ("agent" in entity_type):
                target_candidates.add(entity_id)

        if target_uid:
            target_match = target_uid in target_candidates
            if (not target_match) and target_candidates:
                continue
            if not target_match:
                payload_blob = ""
                try:
                    payload_blob = json.dumps(
                        {"entity": entity, "context": context, "message": audit.get("message")},
                        ensure_ascii=False,
                    ).lower()
                except Exception:
                    payload_blob = f"{entity} {context} {audit.get('message')}".lower()
                if target_uid.lower() in payload_blob:
                    target_match = True
                elif actor_id and actor_id == target_uid:
                    target_match = True
            if not target_match:
                continue

        actor_name = _resolve_user_label(
            user_id=actor_id,
            users_info=users_info,
            fallback_name=actor.get("name") or actor.get("displayName"),
        )
        affected_user_id = target_uid or (sorted(target_candidates)[0] if target_candidates else "-")

        event_date_raw = audit.get("eventDate")
        event_date_local = _format_iso_with_utc_offset(event_date_raw, utc_offset_hours=utc_offset_hours)
        action = str(audit.get("action") or "").strip()
        service_name = str(audit.get("serviceName") or "").strip()
        application = str(audit.get("application") or "").strip()
        audit_status = str(audit.get("status") or "").strip()
        audit_id = str(audit.get("id") or "").strip()
        message_obj = audit.get("message") or {}
        message_text = str(
            message_obj.get("message")
            or message_obj.get("messageWithParams")
            or ""
        ).strip()

        property_changes = audit.get("propertyChanges") or []
        entity_changes = audit.get("entityChanges") or []
        matched = False
        for ch in property_changes:
            if not isinstance(ch, dict):
                continue
            prop_name = str(ch.get("property") or "").strip()
            prop_lower = prop_name.lower()
            if not prop_lower:
                continue
            if not any(h in prop_lower for h in status_property_hints):
                continue
            matched = True
            old_text = _format_status_values(ch.get("oldValues"), presence_map=presence_map)
            new_text = _format_status_values(ch.get("newValues"), presence_map=presence_map)
            rows.append({
                "Zaman": event_date_local,
                "Servis": service_name or "-",
                "Aksiyon": action or "-",
                "Alan": prop_name,
                "Eski Değer": old_text,
                "Yeni Değer": new_text,
                "Değiştiren": actor_name,
                "Değiştiren ID": actor_id or "-",
                "Etkilenen ID": affected_user_id or "-",
                "Uygulama": application or "-",
                "Audit Durumu": audit_status or "-",
                "Mesaj": message_text or "-",
                "Audit ID": audit_id or "-",
            })

        if not matched and isinstance(entity_changes, list):
            for ech in entity_changes:
                ech_text = str(ech).lower()
                if any(h in ech_text for h in status_text_hints):
                    matched = True
                    rows.append({
                        "Zaman": event_date_local,
                        "Servis": service_name or "-",
                        "Aksiyon": action or "-",
                        "Alan": "entityChanges",
                        "Eski Değer": "-",
                        "Yeni Değer": "-",
                        "Değiştiren": actor_name,
                        "Değiştiren ID": actor_id or "-",
                        "Etkilenen ID": affected_user_id or "-",
                        "Uygulama": application or "-",
                        "Audit Durumu": audit_status or "-",
                        "Mesaj": message_text or "-",
                        "Audit ID": audit_id or "-",
                    })
                    break

        if matched:
            continue

        old_ctx_val = None
        new_ctx_val = None
        old_ctx_key = None
        new_ctx_key = None
        if isinstance(context, dict):
            for k in old_ctx_keys:
                if context.get(k) not in (None, ""):
                    old_ctx_key = k
                    old_ctx_val = context.get(k)
                    break
            for k in new_ctx_keys:
                if context.get(k) not in (None, ""):
                    new_ctx_key = k
                    new_ctx_val = context.get(k)
                    break

        combined_text = f"{service_name} {action} {message_text} {str(context)}".lower()
        if any(h in combined_text for h in status_text_hints) or old_ctx_val is not None or new_ctx_val is not None:
            old_text = _format_status_values(old_ctx_val, presence_map=presence_map)
            new_text = _format_status_values(new_ctx_val, presence_map=presence_map)
            field_name = (old_ctx_key or new_ctx_key or "-")
            rows.append({
                "Zaman": event_date_local,
                "Servis": service_name or "-",
                "Aksiyon": action or "-",
                "Alan": field_name,
                "Eski Değer": old_text,
                "Yeni Değer": new_text,
                "Değiştiren": actor_name,
                "Değiştiren ID": actor_id or "-",
                "Etkilenen ID": affected_user_id or "-",
                "Uygulama": application or "-",
                "Audit Durumu": audit_status or "-",
                "Mesaj": message_text or "-",
                "Audit ID": audit_id or "-",
            })

    return rows
