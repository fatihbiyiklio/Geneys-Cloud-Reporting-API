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

def _resolve_audit_actor_display(audit_item, actor_id, actor_name, target_user_id=None, users_info=None):
    def _coerce_bool(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            if value == 1:
                return True
            if value == 0:
                return False
        txt = str(value or "").strip().lower()
        if txt in {"true", "1", "yes", "y"}:
            return True
        if txt in {"false", "0", "no", "n"}:
            return False
        return None

    def _looks_like_uuid(value):
        txt = str(value or "").strip()
        if len(txt) != 36:
            return False
        parts = txt.split("-")
        if len(parts) != 5:
            return False
        expected = (8, 4, 4, 4, 12)
        hex_chars = "0123456789abcdefABCDEF"
        for part, size in zip(parts, expected):
            if len(part) != size:
                return False
            if any(ch not in hex_chars for ch in part):
                return False
        return True

    def _collect_context_actor_id(context_obj):
        if not isinstance(context_obj, (dict, list, tuple)):
            return ""
        actor_key_hints = (
            "actoruserid",
            "actinguserid",
            "modifiedby",
            "changedby",
            "updatedby",
            "initiatedbyuserid",
            "performedbyuserid",
            "requestinguserid",
        )

        def _walk(node):
            if isinstance(node, dict):
                for key, value in node.items():
                    key_lower = str(key or "").strip().lower()
                    if isinstance(value, (dict, list, tuple)):
                        nested = _walk(value)
                        if nested:
                            return nested
                        continue
                    val = str(value or "").strip()
                    if (not val) or (not _looks_like_uuid(val)):
                        continue
                    if any(hint in key_lower for hint in actor_key_hints) or ("actor" in key_lower):
                        return val
            elif isinstance(node, (list, tuple)):
                for item in node:
                    nested = _walk(item)
                    if nested:
                        return nested
            return ""

        return _walk(context_obj)

    actor_uid = str(actor_id or "").strip()
    target_uid = str(target_user_id or "").strip()
    actor_label = str(actor_name or "").strip()
    level = str((audit_item or {}).get("level") or "").strip().upper()
    transaction_initiator = _coerce_bool((audit_item or {}).get("transactionInitiator"))

    if level in {"SYSTEM", "GENESYS_INTERNAL"}:
        return ("Sistem", "-", "Sistem")

    if actor_uid and target_uid and (actor_uid.lower() == target_uid.lower()):
        if transaction_initiator is False:
            context_actor_id = _collect_context_actor_id((audit_item or {}).get("context"))
            if context_actor_id and context_actor_id.lower() != target_uid.lower():
                return (
                    _resolve_user_label(
                        user_id=context_actor_id,
                        users_info=users_info,
                        fallback_name=None,
                    ),
                    context_actor_id,
                    "Supervisor",
                )
        # Agent changed their own status.
        return (
            _resolve_user_label(
                user_id=target_uid,
                users_info=users_info,
                fallback_name=actor_label if actor_label and actor_label != "-" else None,
            ),
            actor_uid,
            "Agent",
        )
    if actor_uid:
        # Non-self actor (typically supervisor/admin) changed the status.
        return (
            _resolve_user_label(
                user_id=actor_uid,
                users_info=users_info,
                fallback_name=actor_label if actor_label and actor_label != "-" else None,
            ),
            actor_uid,
            "Supervisor",
        )
    if actor_label and actor_label != "-":
        return (actor_label, "-", "Supervisor")
    return ("-", "-", "-")

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

def _build_queue_membership_audit_rows(audit_entities, target_user_id, users_info=None, presence_map=None, utc_offset_hours=3.0, queue_name_map=None):
    rows = []
    if not isinstance(audit_entities, list):
        return rows

    target_uid = str(target_user_id or "").strip()
    target_uid_lower = target_uid.lower()
    uuid_pattern = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
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
        "memberid",
        "agentid",
        "targetid",
        "subjectid",
        "subjectuserid",
        "entityuserid",
    )
    queue_id_keys = (
        "queueid",
        "routingqueueid",
        "routing_queue_id",
        "workgroupid",
        "workgroup_id",
    )
    normalized_queue_name_map = {}
    if isinstance(queue_name_map, dict):
        for qid_raw, qname_raw in queue_name_map.items():
            qid = str(qid_raw or "").strip()
            if not qid:
                continue
            qname = str(qname_raw or qid).strip() or qid
            normalized_queue_name_map[qid] = qname
            normalized_queue_name_map[qid.lower()] = qname

    def _looks_like_uuid(value):
        val = str(value or "").strip()
        return bool(val and uuid_pattern.match(val))

    uuid_scan_pattern = re.compile(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    )

    def _to_blob_lower(value):
        try:
            return json.dumps(value, ensure_ascii=False).lower()
        except Exception:
            return str(value).lower()

    def _extract_uuids(value):
        if value in (None, ""):
            return []
        if isinstance(value, (dict, list, tuple, set)):
            try:
                text = json.dumps(value, ensure_ascii=False)
            except Exception:
                text = str(value)
        else:
            text = str(value)
        found = []
        seen = set()
        for match in uuid_scan_pattern.findall(text):
            uid = str(match or "").strip()
            if not uid:
                continue
            uid_lower = uid.lower()
            if uid_lower in seen:
                continue
            seen.add(uid_lower)
            found.append(uid)
        return found

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

    def _collect_queue_refs(audit_item):
        refs = []
        seen = set()

        def _add_ref(queue_id=None, queue_name=None):
            qid = str(queue_id or "").strip()
            qname = str(queue_name or "").strip()
            if (not qid) and (not qname):
                return
            if qname and qid and qname == qid:
                qname = ""
            key = (qid.lower(), qname.lower())
            if key in seen:
                return
            seen.add(key)
            refs.append({
                "id": qid or "-",
                "name": qname or (qid or "-"),
            })

        def _extract_from_dict(node):
            if not isinstance(node, dict):
                return
            entity_type_hint = str(node.get("entityType") or node.get("type") or "").strip().lower()
            entity_queue_like = ("queue" in entity_type_hint) or ("workgroup" in entity_type_hint)
            _add_ref(
                queue_id=(
                    node.get("id")
                    or node.get("queueId")
                    or node.get("routingQueueId")
                    or node.get("workgroupId")
                    or (node.get("entityId") if entity_queue_like else None)
                ),
                queue_name=node.get("name")
                or node.get("queueName")
                or node.get("displayName")
                or node.get("workgroupName")
                or (node.get("entityName") if entity_queue_like else None),
            )

        def _walk(node, parent_key=""):
            parent = str(parent_key or "").strip().lower()
            if isinstance(node, dict):
                if ("queue" in parent) or ("workgroup" in parent):
                    _extract_from_dict(node)
                for k, v in node.items():
                    key = str(k or "").strip().lower()
                    if key in queue_id_keys:
                        if isinstance(v, dict):
                            _extract_from_dict(v)
                        elif isinstance(v, (list, tuple)):
                            for item in v:
                                if isinstance(item, dict):
                                    _extract_from_dict(item)
                                else:
                                    _add_ref(queue_id=item)
                        else:
                            _add_ref(queue_id=v)
                    elif ("queue" in key) or ("workgroup" in key):
                        if isinstance(v, dict):
                            _extract_from_dict(v)
                        elif isinstance(v, (list, tuple)):
                            for item in v:
                                if isinstance(item, dict):
                                    _extract_from_dict(item)
                                elif _looks_like_uuid(item):
                                    _add_ref(queue_id=item)
                        elif _looks_like_uuid(v):
                            _add_ref(queue_id=v)
                    _walk(v, key)
            elif isinstance(node, (list, tuple)):
                for item in node:
                    _walk(item, parent)
            else:
                if (parent in queue_id_keys) and _looks_like_uuid(node):
                    _add_ref(queue_id=node)

        entity_obj = audit_item.get("entity") or {}
        entity_type = str(audit_item.get("entityType") or entity_obj.get("type") or "").strip().lower()
        if ("queue" in entity_type) or ("workgroup" in entity_type):
            _add_ref(
                queue_id=entity_obj.get("id"),
                queue_name=entity_obj.get("name") or entity_obj.get("displayName"),
            )

        _walk(audit_item.get("context") or {}, "context")
        _walk(audit_item.get("propertyChanges") or [], "propertyChanges")
        _walk(audit_item.get("entityChanges") or [], "entityChanges")
        _walk(audit_item.get("message") or {}, "message")
        return refs

    def _first_queue_ref(queue_refs):
        if not isinstance(queue_refs, list) or not queue_refs:
            return {"id": "-", "name": "-"}
        first = queue_refs[0] or {}
        return {
            "id": str(first.get("id") or "-").strip() or "-",
            "name": str(first.get("name") or first.get("id") or "-").strip() or "-",
        }

    def _bool_from_value(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            if value == 1:
                return True
            if value == 0:
                return False
        raw = str(value or "").strip().lower()
        if raw in {"true", "1", "yes", "on", "joined", "active", "member"}:
            return True
        if raw in {"false", "0", "no", "off", "inactive", "notjoined", "removed"}:
            return False
        return None

    def _format_queue_values(values):
        if values is None:
            return "-"
        candidate = values
        if isinstance(values, list) and len(values) == 1:
            candidate = values[0]
        state = _bool_from_value(candidate)
        if state is True:
            return "Aktif"
        if state is False:
            return "Pasif"
        return _format_status_values(values, presence_map=presence_map)

    def _values_contain_target(values):
        if (not target_uid_lower) or values in (None, ""):
            return False
        return target_uid_lower in _to_blob_lower(values)

    def _infer_queue_operation(action, message_text, prop_name="", old_values=None, new_values=None):
        prop_lower = str(prop_name or "").strip().lower()

        if "joined" in prop_lower:
            old_val = old_values[0] if isinstance(old_values, list) and old_values else old_values
            new_val = new_values[0] if isinstance(new_values, list) and new_values else new_values
            old_state = _bool_from_value(old_val)
            new_state = _bool_from_value(new_val)
            if (old_state is not None) and (new_state is not None):
                if old_state != new_state:
                    return "Aktif Yapıldı" if new_state else "Pasif Yapıldı"
            elif new_state is not None:
                return "Aktif Yapıldı" if new_state else "Pasif Yapıldı"

        old_has_target = _values_contain_target(old_values)
        new_has_target = _values_contain_target(new_values)
        if old_has_target != new_has_target:
            return "Eklendi" if new_has_target else "Çıkarıldı"

        combined = f"{action} {message_text} {prop_name}".lower()
        remove_tokens = (
            "delete",
            "remove",
            "removed",
            "detach",
            "unassign",
            "deactivate",
            "joined=false",
            "memberremoved",
            "exclude",
        )
        add_tokens = (
            "add",
            "added",
            "assign",
            "attach",
            "create",
            "activate",
            "joined=true",
            "memberadded",
            "include",
        )
        has_remove = any(tok in combined for tok in remove_tokens)
        has_add = any(tok in combined for tok in add_tokens)
        if has_remove and (not has_add):
            return "Çıkarıldı"
        if has_add and (not has_remove):
            return "Eklendi"
        if "joined" in combined:
            return "Durum Güncellendi"
        return "Güncellendi"

    global_seen_keys = set()

    for audit in audit_entities:
        if not isinstance(audit, dict):
            continue

        entity = audit.get("entity") or {}
        entity_id = str(entity.get("id") or "").strip()
        entity_type = str(audit.get("entityType") or entity.get("type") or "").strip().lower()
        property_changes = audit.get("propertyChanges") or []
        entity_changes = audit.get("entityChanges") or []
        message_obj = audit.get("message") or {}

        actor = audit.get("user") or {}
        actor_id = str(actor.get("id") or "").strip()
        context = audit.get("context") or {}
        context_actor_ids, context_target_ids = _collect_context_user_ids(context)

        if not _looks_like_uuid(actor_id):
            actor_id = ""
        if (not actor_id) and context_actor_ids:
            actor_id = sorted(context_actor_ids)[0]

        queue_refs = _collect_queue_refs(audit)
        queue_ref = _first_queue_ref(queue_refs)
        queue_id = str(queue_ref.get("id") or "-").strip() or "-"
        queue_name = str(queue_ref.get("name") or queue_id or "-").strip() or "-"
        mapped_queue_name = str(normalized_queue_name_map.get(queue_id) or normalized_queue_name_map.get(queue_id.lower()) or "").strip()
        if mapped_queue_name and (queue_name in {"-", queue_id}):
            queue_name = mapped_queue_name

        queue_uuid_set = set()
        if _looks_like_uuid(queue_id):
            queue_uuid_set.add(queue_id.lower())
        if _looks_like_uuid(entity_id) and (("queue" in entity_type) or ("workgroup" in entity_type)):
            queue_uuid_set.add(entity_id.lower())
        for qref in queue_refs:
            qid_ref = str((qref or {}).get("id") or "").strip()
            if _looks_like_uuid(qid_ref):
                queue_uuid_set.add(qid_ref.lower())

        target_candidates_ordered = []
        target_candidate_scores = {}
        target_candidate_order = {}
        target_candidates_seen = set()

        def _add_target_candidate(value, score=1):
            uid = str(value or "").strip()
            if not _looks_like_uuid(uid):
                return
            uid_lower = uid.lower()
            if actor_id and uid_lower == actor_id.lower():
                return
            if uid_lower in queue_uuid_set:
                return
            try:
                score_value = int(score)
            except Exception:
                score_value = 1
            if score_value < 1:
                score_value = 1
            target_candidate_scores[uid_lower] = int(target_candidate_scores.get(uid_lower, 0) or 0) + score_value
            if uid_lower in target_candidates_seen:
                return
            target_candidates_seen.add(uid_lower)
            target_candidate_order[uid_lower] = len(target_candidates_ordered)
            target_candidates_ordered.append(uid)

        for uid in sorted(context_target_ids):
            _add_target_candidate(uid, score=4)

        if _looks_like_uuid(entity_id):
            if (
                (not entity_type)
                or ("user" in entity_type)
                or ("agent" in entity_type)
                or ("member" in entity_type)
            ):
                _add_target_candidate(entity_id, score=5)

        for ch in property_changes:
            if not isinstance(ch, dict):
                continue
            prop_name = str(ch.get("property") or "").strip()
            prop_lower = prop_name.lower()
            if (
                ("user" not in prop_lower)
                and ("agent" not in prop_lower)
                and ("member" not in prop_lower)
                and ("joined" not in prop_lower)
            ):
                continue
            for uid in _extract_uuids(prop_name):
                _add_target_candidate(uid, score=6)
            for uid in _extract_uuids(ch.get("newValues")):
                _add_target_candidate(uid, score=6)
            for uid in _extract_uuids(ch.get("oldValues")):
                _add_target_candidate(uid, score=6)

        for ec in entity_changes:
            if not isinstance(ec, dict):
                continue
            ec_type = str(ec.get("entityType") or "").strip().lower()
            if ("user" in ec_type) or ("agent" in ec_type) or ("member" in ec_type):
                _add_target_candidate(ec.get("entityId"), score=5)
                for uid in _extract_uuids(ec.get("entityName")):
                    _add_target_candidate(uid, score=4)
                for uid in _extract_uuids(ec.get("newValues")):
                    _add_target_candidate(uid, score=5)
                for uid in _extract_uuids(ec.get("oldValues")):
                    _add_target_candidate(uid, score=5)

        if not target_candidates_ordered:
            combined_for_target = {
                "entity": entity,
                "context": context,
                "message": message_obj,
                "propertyChanges": property_changes,
                "entityChanges": entity_changes,
            }
            for uid in _extract_uuids(combined_for_target):
                _add_target_candidate(uid, score=1)

        target_candidates = set(target_candidates_ordered)

        if target_uid:
            target_match = target_uid in target_candidates
            if (not target_match) and target_candidates:
                continue
            if not target_match:
                payload_blob = _to_blob_lower(
                    {"entity": entity, "context": context, "message": audit.get("message")}
                )
                if target_uid_lower in payload_blob:
                    target_match = True
            if not target_match:
                continue

        actor_name = _resolve_user_label(
            user_id=actor_id,
            users_info=users_info,
            fallback_name=actor.get("name") or actor.get("displayName"),
        )
        actor_display, actor_display_id, actor_kind = _resolve_audit_actor_display(
            audit_item=audit,
            actor_id=actor_id,
            actor_name=actor_name,
            target_user_id=target_uid,
            users_info=users_info,
        )
        if target_candidates_ordered:
            affected_user_id = max(
                target_candidates_ordered,
                key=lambda uid: (
                    int(target_candidate_scores.get(str(uid).strip().lower(), 0) or 0),
                    -int(target_candidate_order.get(str(uid).strip().lower(), 10_000) or 10_000),
                ),
            )
        elif target_uid:
            # Keep user-selected target only when payload has no extractable target id.
            affected_user_id = target_uid
        else:
            affected_user_id = "-"

        event_date_local = _format_iso_with_utc_offset(audit.get("eventDate"), utc_offset_hours=utc_offset_hours)
        action = str(audit.get("action") or "").strip()
        service_name = str(audit.get("serviceName") or "").strip()
        application = str(audit.get("application") or "").strip()
        audit_status = str(audit.get("status") or "").strip()
        audit_id = str(audit.get("id") or "").strip()
        message_text = str(
            message_obj.get("message")
            or message_obj.get("messageWithParams")
            or ""
        ).strip()
        service_name_lower = str(service_name or "-").strip().lower()
        action_lower = str(action or "-").strip().lower()
        message_lower = str(message_text or "-").strip().lower()
        event_local_s = str(event_date_local or "-").strip()
        actor_key = str(actor_id or "-").strip().lower()
        affected_key = str(affected_user_id or "-").strip().lower()
        queue_key = str(queue_id or "-").strip().lower()
        audit_key = str(audit_id or "-").strip().lower()
        per_audit_seen = set()

        def _append_unique_row(row_obj):
            if not isinstance(row_obj, dict):
                return
            operation_key = str(row_obj.get("İşlem") or "-").strip().lower()
            summary_key = str(row_obj.get("Özet") or "-").strip().lower()
            dedupe_key = (
                event_local_s,
                service_name_lower,
                action_lower,
                audit_key,
                queue_key,
                actor_key,
                affected_key,
                operation_key,
                summary_key,
                message_lower,
            )
            if dedupe_key in per_audit_seen:
                return
            if dedupe_key in global_seen_keys:
                return
            per_audit_seen.add(dedupe_key)
            global_seen_keys.add(dedupe_key)
            rows.append(row_obj)

        context_blob_lower = _to_blob_lower(context)
        message_blob_lower = _to_blob_lower(message_obj)
        matched = False

        for ch in property_changes:
            if not isinstance(ch, dict):
                continue
            prop_name = str(ch.get("property") or "").strip()
            prop_lower = prop_name.lower()
            if not prop_lower:
                continue

            has_queue_hint = ("queue" in prop_lower) or ("routingqueue" in prop_lower) or ("workgroup" in prop_lower)
            has_membership_hint = ("member" in prop_lower) or ("membership" in prop_lower) or ("joined" in prop_lower)
            has_user_hint = ("user" in prop_lower) or ("agent" in prop_lower)
            has_queue_context_hint = (
                ("queue" in context_blob_lower)
                or ("workgroup" in context_blob_lower)
                or ("queue" in message_blob_lower)
                or ("workgroup" in message_blob_lower)
                or ("queue" in entity_type)
                or ("workgroup" in entity_type)
            )
            if not (has_queue_hint or has_membership_hint or (has_user_hint and has_queue_context_hint)):
                continue

            if target_uid and ("joined" not in prop_lower):
                ch_blob = _to_blob_lower(ch)
                if target_uid_lower not in ch_blob:
                    if (target_uid_lower not in context_blob_lower) and (target_uid_lower not in message_blob_lower):
                        continue

            matched = True
            old_values = ch.get("oldValues")
            new_values = ch.get("newValues")
            operation = _infer_queue_operation(
                action=action,
                message_text=message_text,
                prop_name=prop_name,
                old_values=old_values,
                new_values=new_values,
            )
            old_text = _format_queue_values(old_values)
            new_text = _format_queue_values(new_values)

            if operation == "Eklendi":
                old_text = "Üye Değil"
                new_text = "Üye"
            elif operation == "Çıkarıldı":
                old_text = "Üye"
                new_text = "Üye Değil"
            elif operation == "Aktif Yapıldı":
                if old_text == "-":
                    old_text = "Pasif"
                new_text = "Aktif"
            elif operation == "Pasif Yapıldı":
                if old_text == "-":
                    old_text = "Aktif"
                new_text = "Pasif"

            _append_unique_row({
                "Zaman": event_date_local,
                "Servis": service_name or "-",
                "Aksiyon": action or "-",
                "Alan": prop_name or "queueMembership",
                "İşlem": operation,
                "Kuyruk": queue_name or "-",
                "Kuyruk ID": queue_id or "-",
                "Özet": f"{(queue_name or queue_id or '-')} {operation}".strip(),
                "Eski Değer": old_text,
                "Yeni Değer": new_text,
                "Değiştiren": actor_display,
                "Değiştiren ID": actor_display_id,
                "Değiştiren Türü": actor_kind,
                "Etkilenen": _resolve_user_label(user_id=affected_user_id, users_info=users_info, fallback_name=None),
                "Etkilenen ID": affected_user_id or "-",
                "Uygulama": application or "-",
                "Audit Durumu": audit_status or "-",
                "Mesaj": message_text or "-",
                "Audit ID": audit_id or "-",
            })

        if matched:
            continue

        combined_text = _to_blob_lower({
            "service": service_name,
            "action": action,
            "entityType": entity_type,
            "entity": entity,
            "message": message_text,
            "context": context,
            "entityChanges": entity_changes,
        })
        has_queue_hint = (
            ("queue" in combined_text)
            or ("routingqueue" in combined_text)
            or ("/queues" in combined_text)
            or ("workgroup" in combined_text)
        )
        has_membership_hint = (
            ("member" in combined_text)
            or ("membership" in combined_text)
            or ("joined" in combined_text)
            or ("/members" in combined_text)
            or ((("user" in combined_text) or ("agent" in combined_text)) and has_queue_hint)
        )
        entity_type_queue_membership = (
            (("queue" in entity_type) or ("workgroup" in entity_type) or ("routingqueue" in entity_type))
            and (
                ("member" in entity_type)
                or ("membership" in entity_type)
                or ("user" in entity_type)
                or ("agent" in entity_type)
                or ("joined" in entity_type)
            )
        )

        if (has_queue_hint and has_membership_hint) or entity_type_queue_membership:
            operation = _infer_queue_operation(
                action=action,
                message_text=message_text,
                prop_name="queueMembership",
                old_values=None,
                new_values=None,
            )
            old_text = "-"
            new_text = "-"
            if operation == "Eklendi":
                old_text = "Üye Değil"
                new_text = "Üye"
            elif operation == "Çıkarıldı":
                old_text = "Üye"
                new_text = "Üye Değil"
            elif operation == "Aktif Yapıldı":
                old_text = "Pasif"
                new_text = "Aktif"
            elif operation == "Pasif Yapıldı":
                old_text = "Aktif"
                new_text = "Pasif"

            _append_unique_row({
                "Zaman": event_date_local,
                "Servis": service_name or "-",
                "Aksiyon": action or "-",
                "Alan": "queueMembership",
                "İşlem": operation,
                "Kuyruk": queue_name or "-",
                "Kuyruk ID": queue_id or "-",
                "Özet": f"{(queue_name or queue_id or '-')} {operation}".strip(),
                "Eski Değer": old_text,
                "Yeni Değer": new_text,
                "Değiştiren": actor_display,
                "Değiştiren ID": actor_display_id,
                "Değiştiren Türü": actor_kind,
                "Etkilenen": _resolve_user_label(user_id=affected_user_id, users_info=users_info, fallback_name=None),
                "Etkilenen ID": affected_user_id or "-",
                "Uygulama": application or "-",
                "Audit Durumu": audit_status or "-",
                "Mesaj": message_text or "-",
                "Audit ID": audit_id or "-",
            })

    return rows

def _build_status_audit_rows(audit_entities, target_user_id, users_info=None, presence_map=None, utc_offset_hours=3.0, audit_mode="status", queue_name_map=None):
    rows = []
    if not isinstance(audit_entities, list):
        return rows

    mode = str(audit_mode or "status").strip().lower()
    if mode in {"queue", "queue_membership", "queue_memberships", "queue-history", "queuehistory"}:
        return _build_queue_membership_audit_rows(
            audit_entities=audit_entities,
            target_user_id=target_user_id,
            users_info=users_info,
            presence_map=presence_map,
            utc_offset_hours=utc_offset_hours,
            queue_name_map=queue_name_map,
        )

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
    target_uid_lower = target_uid.lower()

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
            normalized_targets = {
                str(candidate or "").strip().lower()
                for candidate in target_candidates
                if str(candidate or "").strip()
            }
            target_match = target_uid_lower in normalized_targets
            if not target_match:
                payload_blob = ""
                try:
                    payload_blob = json.dumps(
                        {"entity": entity, "context": context, "message": audit.get("message")},
                        ensure_ascii=False,
                    ).lower()
                except Exception:
                    payload_blob = f"{entity} {context} {audit.get('message')}".lower()
                if target_uid_lower in payload_blob:
                    target_match = True
                elif actor_id and actor_id.lower() == target_uid_lower and (not normalized_targets):
                    target_match = True
            if not target_match:
                continue

        actor_name = _resolve_user_label(
            user_id=actor_id,
            users_info=users_info,
            fallback_name=actor.get("name") or actor.get("displayName"),
        )
        actor_display, actor_display_id, actor_kind = _resolve_audit_actor_display(
            audit_item=audit,
            actor_id=actor_id,
            actor_name=actor_name,
            target_user_id=target_uid,
            users_info=users_info,
        )
        affected_user_id = target_uid or (sorted(target_candidates)[0] if target_candidates else "-")
        affected_user_label = _resolve_user_label(
            user_id=affected_user_id if str(affected_user_id or "").strip() not in {"", "-"} else "",
            users_info=users_info,
            fallback_name=None,
        )

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
        status_hint_payload = f"{service_name} {entity_type} {action} {message_text} {str(context)}".lower()

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
            prop_is_status_like = any(h in prop_lower for h in status_property_hints)
            if (not prop_is_status_like) and (prop_lower in {"status", "state", "routingstatus", "presencestatus"}):
                if any(h in status_hint_payload for h in status_text_hints):
                    prop_is_status_like = True
            if not prop_is_status_like:
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
                "Değiştiren": actor_display,
                "Değiştiren ID": actor_display_id,
                "Değiştiren Türü": actor_kind,
                "Etkilenen": affected_user_label,
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
                        "Değiştiren": actor_display,
                        "Değiştiren ID": actor_display_id,
                        "Değiştiren Türü": actor_kind,
                        "Etkilenen": affected_user_label,
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
                "Değiştiren": actor_display,
                "Değiştiren ID": actor_display_id,
                "Değiştiren Türü": actor_kind,
                "Etkilenen": affected_user_label,
                "Etkilenen ID": affected_user_id or "-",
                "Uygulama": application or "-",
                "Audit Durumu": audit_status or "-",
                "Mesaj": message_text or "-",
                "Audit ID": audit_id or "-",
            })

    return rows


# ---------------------------------------------------------------------------
# Timeline yardımcı fonksiyonları
# (Tam Statü Timeline — Users Details bölümü için)
# ---------------------------------------------------------------------------

def _parse_iso_utc(iso_value):
    """ISO 8601 tarih string'ini UTC datetime objesine çevirir."""
    if not iso_value:
        return None
    try:
        return datetime.fromisoformat(
            str(iso_value).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except Exception:
        return None


def _normalize_timeline_status_token(raw_value):
    """Statü string'ini normalize eder (küçük harf, alias eşleştirme)."""
    text = str(raw_value or "").strip().casefold()
    if not text:
        return "-"
    text = text.replace(" ", "_").replace("-", "_")
    _ALIASES = {
        "on_queue": "on_queue",
        "off_queue": "off_queue",
        "interacting": "interacting",
        "communicating": "interacting",
        "available": "available",
        "away": "away",
        "offline": "offline",
        "break": "break",
    }
    return _ALIASES.get(text, text)


def _timeline_presence_label(seg, presence_map=None):
    """Presence segmentinden okunabilir etiket üretir."""
    if not isinstance(seg, dict):
        return "-"
    p_map = presence_map or {}
    org_presence_id = seg.get("organizationPresenceId")
    if org_presence_id:
        mapped = p_map.get(org_presence_id) or p_map.get(str(org_presence_id))
        if isinstance(mapped, dict):
            label = str(
                mapped.get("label") or mapped.get("systemPresence") or ""
            ).strip()
            if label:
                return label
    system_presence = seg.get("systemPresence")
    if system_presence:
        return str(system_presence).replace("_", " ").title()
    return "-"


def _timeline_routing_label(seg):
    """Routing segmentinden okunabilir etiket üretir."""
    if not isinstance(seg, dict):
        return "-"
    raw = str(seg.get("routingStatus") or seg.get("status") or "").strip().upper()
    _ROUTING_MAP = {
        "OFF_QUEUE": "Off Queue",
        "IDLE": "On Queue",
        "INTERACTING": "Görüşmede",
        "NOT_RESPONDING": "Cevapsız",
        "COMMUNICATING": "Görüşmede",
    }
    if raw in _ROUTING_MAP:
        return _ROUTING_MAP[raw]
    return raw.replace("_", " ").title() if raw else "-"


def _infer_audit_service(audit_item):
    """Audit kaydından servis adını (Presence/Routing) çıkarır."""
    service = str((audit_item or {}).get("serviceName") or "").strip().lower()
    if service in {"presence", "routing"}:
        return service.title()
    etype = str((audit_item or {}).get("entityType") or "").strip().lower()
    if "presence" in etype:
        return "Presence"
    blob = (
        f"{(audit_item or {}).get('propertyChanges')} "
        f"{(audit_item or {}).get('context')}"
    ).lower()
    if any(tok in blob for tok in ("routing", "onqueue", "offqueue", "joined")):
        return "Routing"
    if "presence" in blob:
        return "Presence"
    return "-"


def _extract_audit_transition_tokens(audit_item):
    """PropertyChanges listesinden eski/yeni statü token'larını çıkarır."""
    old_t = "-"
    new_t = "-"
    for change in (audit_item or {}).get("propertyChanges") or []:
        if not isinstance(change, dict):
            continue
        prop = str(change.get("property") or "").strip().lower()
        if not any(k in prop for k in ("presence", "routing", "status", "joined", "queue")):
            continue
        old_values = change.get("oldValues") or []
        new_values = change.get("newValues") or []
        old_raw = old_values[0] if isinstance(old_values, list) and old_values else old_values
        new_raw = new_values[0] if isinstance(new_values, list) and new_values else new_values
        old_t = _normalize_timeline_status_token(old_raw)
        new_t = _normalize_timeline_status_token(new_raw)
        break
    return old_t, new_t


def _collect_timeline_context_actor_id(context_obj):
    """Context nesnesinden actor UUID'sini çıkarır (recursive walk)."""
    if not isinstance(context_obj, (dict, list, tuple)):
        return ""
    _HINTS = (
        "actoruserid", "actinguserid", "modifiedby", "changedby",
        "updatedby", "initiatedbyuserid", "performedbyuserid",
        "requestinguserid",
    )

    def _looks_like_uuid(v):
        txt = str(v or "").strip()
        if len(txt) != 36:
            return False
        parts = txt.split("-")
        if len(parts) != 5:
            return False
        sizes = (8, 4, 4, 4, 12)
        hex_chars = "0123456789abcdefABCDEF"
        for part, size in zip(parts, sizes):
            if len(part) != size:
                return False
            if any(ch not in hex_chars for ch in part):
                return False
        return True

    def _walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                key_l = str(key or "").strip().lower()
                if isinstance(value, (dict, list, tuple)):
                    nested = _walk(value)
                    if nested:
                        return nested
                    continue
                val = str(value or "").strip()
                if (not val) or (not _looks_like_uuid(val)):
                    continue
                if any(h in key_l for h in _HINTS) or "actor" in key_l:
                    return val
        elif isinstance(node, (list, tuple)):
            for item in node:
                nested = _walk(item)
                if nested:
                    return nested
        return ""

    return _walk(context_obj)


def _classify_timeline_actor(audit_item, target_user_id, users_info=None):
    """
    Audit kaydındaki actor'u sınıflandırır.
    Döndürür: (isim, id, tür)  — tür: "Agent" | "Supervisor" | "Sistem"
    """
    item = audit_item or {}
    actor_obj = item.get("user") or {}
    actor_id = str(actor_obj.get("id") or "").strip()
    actor_name = str(
        actor_obj.get("name") or actor_obj.get("displayName") or ""
    ).strip()
    level = str(item.get("level") or "").strip().upper()
    initiator = item.get("transactionInitiator")
    target_uid = str(target_user_id or "").strip().lower()

    if level in {"SYSTEM", "GENESYS_INTERNAL"}:
        return "Sistem", "-", "Sistem"

    if actor_id and actor_id.lower() == target_uid and initiator is False:
        ctx_actor = _collect_timeline_context_actor_id(item.get("context"))
        if ctx_actor and ctx_actor.lower() != actor_id.lower():
            return (
                _resolve_user_label(
                    user_id=ctx_actor, users_info=users_info, fallback_name=None
                ),
                ctx_actor,
                "Supervisor",
            )

    if actor_id:
        role = "Agent" if actor_id.lower() == target_uid else "Supervisor"
        return (
            _resolve_user_label(
                user_id=actor_id,
                users_info=users_info,
                fallback_name=actor_name if actor_name else None,
            ),
            actor_id,
            role,
        )
    if actor_name:
        return actor_name, "-", "Supervisor"
    return "-", "-", "-"


def _timeline_actor_type_rank(changer_type):
    """Actor türünü sıralama için sayıya çevirir (küçük = daha öncelikli)."""
    c = str(changer_type or "").strip().lower()
    if c == "supervisor":
        return 0
    if c == "sistem":
        return 1
    if c == "agent":
        return 2
    return 3


def prepare_actor_events(
    audit_entities, target_user_id, users_info=None,
):
    """
    Audit entity listesinden actor olay listesi üretir.
    Her eleman: {service, time_utc, old_token, new_token, changer, changer_id,
                 changer_type, audit_id}
    """
    events = []
    for item in audit_entities or []:
        if not isinstance(item, dict):
            continue
        event_time = _parse_iso_utc(item.get("eventDate"))
        if not isinstance(event_time, datetime):
            continue
        old_token, new_token = _extract_audit_transition_tokens(item)
        changer_name, changer_id, changer_type = _classify_timeline_actor(
            item, target_user_id, users_info=users_info,
        )
        events.append({
            "service": _infer_audit_service(item),
            "time_utc": event_time,
            "old_token": old_token,
            "new_token": new_token,
            "changer": changer_name,
            "changer_id": changer_id,
            "changer_type": changer_type,
            "audit_id": str(item.get("id") or "-").strip() or "-",
        })
    return events


def find_best_actor_for_transition(
    actor_events, source_name, start_utc, old_value, new_value,
):
    """
    Verilen geçiş için en iyi actor eşleşmesini bulur.
    Skor: (transition_penalty, zaman_farkı, actor_type_rank)
    Döndürür: (isim, id, tür, audit_id) veya None
    """
    if not isinstance(start_utc, datetime):
        return None
    old_token = _normalize_timeline_status_token(old_value)
    new_token = _normalize_timeline_status_token(new_value)
    source_key = str(source_name or "").strip().lower()

    best = None
    best_score = None
    for ev in actor_events or []:
        event_time = ev.get("time_utc")
        if not isinstance(event_time, datetime):
            continue
        delta = abs((event_time - start_utc).total_seconds())
        if delta > 20 * 60:
            continue
        ev_service = str(ev.get("service") or "").strip().lower()
        if source_key and ev_service and ev_service != source_key:
            continue

        transition_penalty = 2
        ev_old = str(ev.get("old_token") or "-")
        ev_new = str(ev.get("new_token") or "-")
        if ev_old == old_token and ev_new == new_token:
            transition_penalty = 0
        elif ev_old == old_token or ev_new == new_token:
            transition_penalty = 1

        if transition_penalty == 2 and delta > 45:
            continue

        score = (transition_penalty, int(delta), _timeline_actor_type_rank(ev.get("changer_type")))
        if (best is None) or (score < best_score):
            best = ev
            best_score = score

    if not best:
        return None
    return (
        str(best.get("changer") or "-").strip() or "-",
        str(best.get("changer_id") or "-").strip() or "-",
        str(best.get("changer_type") or "-").strip() or "-",
        str(best.get("audit_id") or "-").strip() or "-",
    )


def build_timeline_rows_from_segments(
    segments, source_name, value_resolver,
    timeline_start_utc, timeline_end_utc,
    actor_events, utc_offset_hours=3.0,
    api_fn_actor_fallback=None,
    actor_window_cache=None,
    user_id_for_fallback=None,
):
    """
    Users Details segmentlerinden timeline geçiş satırları üretir.

    Parametreler:
        segments: Presence veya Routing segment listesi
        source_name: "Presence" veya "Routing"
        value_resolver: segment → etiket fonksiyonu
        timeline_start_utc, timeline_end_utc: Aralık sınırları
        actor_events: prepare_actor_events() çıktısı
        utc_offset_hours: UTC offset (saat)
        api_fn_actor_fallback: Opsiyonel fallback API çağrısı
        actor_window_cache: Fallback sonuçlarını cache'lemek için dict
        user_id_for_fallback: Fallback sorgusunda kullanılacak user ID
    """
    # Segmentleri zamana göre sırala
    ordered = []
    for seg in segments or []:
        if not isinstance(seg, dict):
            continue
        start_raw = seg.get("startTime")
        start_utc = _parse_iso_utc(start_raw)
        if start_utc is None:
            continue
        end_raw = seg.get("endTime")
        end_utc = _parse_iso_utc(end_raw)
        display_value = str(value_resolver(seg) or "-").strip() or "-"
        ordered.append({
            "start_raw": start_raw,
            "end_raw": end_raw,
            "start_utc": start_utc,
            "end_utc": end_utc,
            "value": display_value,
        })
    ordered.sort(
        key=lambda r: r.get("start_utc") or datetime.min.replace(tzinfo=timezone.utc)
    )

    if actor_window_cache is None:
        actor_window_cache = {}

    rows = []
    for idx, row in enumerate(ordered):
        start_ts = row.get("start_utc")
        if start_ts is None:
            continue
        if (start_ts < timeline_start_utc) or (start_ts >= timeline_end_utc):
            continue

        prev_value = ordered[idx - 1]["value"] if idx > 0 else "-"
        end_ts = row.get("end_utc") or timeline_end_utc
        if end_ts > timeline_end_utc:
            end_ts = timeline_end_utc
        duration_seconds = int(max(0, (end_ts - start_ts).total_seconds()))

        # Actor eşleştirmesi: önce mevcut listeden, sonra fallback
        actor_result = find_best_actor_for_transition(
            actor_events=actor_events,
            source_name=source_name,
            start_utc=start_ts,
            old_value=prev_value,
            new_value=row.get("value"),
        )

        if actor_result is None and api_fn_actor_fallback is not None:
            old_token = _normalize_timeline_status_token(prev_value)
            new_token = _normalize_timeline_status_token(row.get("value"))
            cache_key = (
                str(source_name or "").strip().lower(),
                int(start_ts.timestamp()),
                str(old_token or "-"),
                str(new_token or "-"),
            )
            if cache_key not in actor_window_cache:
                actor_window_cache[cache_key] = _fallback_actor_search(
                    api_fn=api_fn_actor_fallback,
                    user_id=user_id_for_fallback,
                    source_name=source_name,
                    start_utc=start_ts,
                    old_token=old_token,
                    new_token=new_token,
                )
            actor_result = actor_window_cache.get(cache_key)

        if actor_result is None:
            actor_result = ("-", "-", "-", "-")

        changer_name, changer_id, changer_type, actor_audit_id = actor_result

        rows.append({
            "_sort_ts": start_ts,
            "Kayıt Tipi": "Geçiş",
            "Zaman": _format_iso_with_utc_offset(
                row.get("start_raw"), utc_offset_hours=utc_offset_hours
            ),
            "Kaynak": source_name,
            "Eski Değer": prev_value or "-",
            "Yeni Değer": row.get("value") or "-",
            "Değiştiren": changer_name,
            "Değiştiren ID": changer_id,
            "Değiştiren Türü": changer_type,
            "Audit ID": actor_audit_id,
            "Başlangıç": _format_iso_with_utc_offset(
                row.get("start_raw"), utc_offset_hours=utc_offset_hours
            ),
            "Bitiş": (
                _format_iso_with_utc_offset(
                    row.get("end_raw"), utc_offset_hours=utc_offset_hours
                )
                if row.get("end_raw")
                else "-"
            ),
            "Süre (sn)": duration_seconds,
            "Süre": format_duration_seconds(duration_seconds),
        })
    return rows


def _fallback_actor_search(
    api_fn, user_id, source_name, start_utc, old_token, new_token,
):
    """
    Mevcut actor listesinde eşleşme bulunamadığında pencere bazlı audit sorgusu yapar.
    Döndürür: (isim, id, tür, audit_id) veya None
    """
    try:
        window_start = start_utc - timedelta(minutes=20)
        window_end = start_utc + timedelta(minutes=20)
        source_clean = str(source_name or "").strip()
        window_payload = api_fn(
            user_id=user_id,
            start_date=window_start,
            end_date=window_end,
            page_size=100,
            max_pages=6,
            service_name=source_clean or None,
            include_async_query=True,
            collect_all_variants=True,
            strict_user_match=False,
            realtime_first=False,
        ) or {}
        window_entities = window_payload.get("entities") or []
    except Exception:
        return None

    best = None
    best_score = None
    for item in window_entities:
        if not isinstance(item, dict):
            continue
        event_time = _parse_iso_utc(item.get("eventDate"))
        if not isinstance(event_time, datetime):
            continue
        delta = abs((event_time - start_utc).total_seconds())
        if delta > 20 * 60:
            continue
        item_service = str(_infer_audit_service(item) or "").strip().lower()
        source_lower = str(source_name or "").strip().lower()
        if source_lower and item_service and item_service != source_lower:
            continue

        item_old, item_new = _extract_audit_transition_tokens(item)
        transition_penalty = 2
        if str(item_old or "-") == old_token and str(item_new or "-") == new_token:
            transition_penalty = 0
        elif str(item_old or "-") == old_token or str(item_new or "-") == new_token:
            transition_penalty = 1
        if transition_penalty == 2 and delta > 45:
            continue

        changer_name, changer_id, changer_type = _classify_timeline_actor(
            item, user_id,
        )
        if str(changer_name or "-").strip() == "-":
            continue

        score = (transition_penalty, int(delta), _timeline_actor_type_rank(changer_type))
        if (best is None) or (score < best_score):
            best = (
                str(changer_name or "-").strip() or "-",
                str(changer_id or "-").strip() or "-",
                str(changer_type or "-").strip() or "-",
                str(item.get("id") or "-").strip() or "-",
            )
            best_score = score
    return best
