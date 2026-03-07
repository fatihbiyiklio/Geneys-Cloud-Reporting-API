import copy
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.app.context import bind_context
from src.api import GenesysAPI


# Injected by bind_context at runtime.
st: Any = None
get_text: Callable[..., str] = lambda _lang, key: key
lang: str = "TR"


def _audit_user_action(action, detail=None, status="info", metadata=None):
    audit_fn = globals().get("_log_user_action")
    if callable(audit_fn):
        try:
            audit_fn(
                action=action,
                detail=detail,
                status=status,
                metadata=metadata,
                source="dialer-service",
            )
        except Exception:
            pass


def _safe_json_dumps(value):
    try:
        return json.dumps(value, ensure_ascii=False, indent=2)
    except Exception:
        return "{}"


def _safe_json_loads(raw_text):
    try:
        loaded = json.loads(raw_text)
    except Exception as exc:
        return None, str(exc)
    return loaded, None


def _as_list_entities(payload):
    if isinstance(payload, dict):
        entities = payload.get("entities")
        if isinstance(entities, list):
            return entities
    if isinstance(payload, list):
        return payload
    return []


def _summary_value(item, keys, default=0):
    for key in keys:
        value = item.get(key)
        if isinstance(value, (int, float)):
            return value
    return default


def _campaign_row(c):
    return {
        "id": c.get("id"),
        "name": c.get("name"),
        "status": c.get("campaignStatus") or c.get("status"),
        "dialingMode": c.get("dialingMode") or c.get("mode"),
        "contactList": (c.get("contactList") or {}).get("name") or c.get("contactListId"),
        "queue": (c.get("queue") or {}).get("name") or c.get("queueId"),
        "division": (c.get("division") or {}).get("name") or c.get("divisionId"),
        "lastModified": c.get("dateModified") or c.get("modifiedDate") or "",
    }


def _contact_list_row(item, attempt_limit_names: Dict[str, str] = None):
    column_names = item.get("columnNames")
    if isinstance(column_names, list):
        columns = len(column_names)
    else:
        columns = 0
    attempt_limit_names = attempt_limit_names or {}
    attempt_ref = item.get("attemptLimits")
    if not attempt_ref:
        attempt_ref = item.get("attemptLimit")
    attempt_limit_value = ""
    if isinstance(attempt_ref, dict):
        attempt_limit_name = str(attempt_ref.get("name") or "").strip()
        attempt_limit_id = str(attempt_ref.get("id") or "").strip()
        attempt_limit_value = attempt_limit_name or str(attempt_limit_names.get(attempt_limit_id) or attempt_limit_id).strip()
    elif isinstance(attempt_ref, list):
        for ref in attempt_ref:
            if isinstance(ref, dict):
                attempt_limit_name = str(ref.get("name") or "").strip()
                attempt_limit_id = str(ref.get("id") or "").strip()
                attempt_limit_value = attempt_limit_name or str(attempt_limit_names.get(attempt_limit_id) or attempt_limit_id).strip()
                if attempt_limit_value:
                    break
            elif ref:
                ref_id = str(ref).strip()
                attempt_limit_value = str(attempt_limit_names.get(ref_id) or ref_id).strip()
                if attempt_limit_value:
                    break
    elif attempt_ref:
        ref_id = str(attempt_ref).strip()
        attempt_limit_value = str(attempt_limit_names.get(ref_id) or ref_id).strip()
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "columns": columns,
        "attemptLimit": attempt_limit_value,
        "callableTimeSet": (item.get("callableTimeSet") or {}).get("name") or item.get("callableTimeSetId"),
        "lastModified": item.get("dateModified") or item.get("modifiedDate") or "",
    }


def _clean_excel_records(df: pd.DataFrame, max_rows: int = 50000):
    cleaned = df.copy()
    cleaned = cleaned.where(pd.notnull(cleaned), None)
    if len(cleaned) > max_rows:
        cleaned = cleaned.head(max_rows)
    records = []
    for row in cleaned.to_dict(orient="records"):
        out = {}
        for key, value in row.items():
            safe_key = str(key).strip()
            if not safe_key:
                continue
            if value is None:
                continue
            if isinstance(value, float) and pd.isna(value):
                continue
            out[safe_key] = str(value).strip() if not isinstance(value, (int, float, bool)) else value
        if out:
            records.append(out)
    return records


def _chunk_list(values, chunk_size):
    chunk_size = max(1, int(chunk_size))
    for idx in range(0, len(values), chunk_size):
        yield values[idx : idx + chunk_size]


def _is_forbidden_error(exc: Exception) -> bool:
    text = str(exc or "")
    return "403" in text or "forbidden" in text.lower()


def _render_forbidden_hint(scope_label: str):
    st.warning(
        (
            f"{scope_label} endpointine erişim yetkisi yok (HTTP 403). "
            "Bu genellikle Outbound lisansı/özelliği kapalı veya OAuth client role izinleri eksik olduğunda görülür."
        )
    )
    st.caption(
        "Kontrol edin: Outbound Dialer lisansı, ilgili division erişimi, OAuth client için outbound campaign/contact list izinleri."
    )


def _error_detail(exc: Exception) -> str:
    try:
        response = getattr(exc, "response", None)
        if response is not None:
            try:
                payload = response.json()
                message = payload.get("message") or payload.get("error") or payload.get("details")
                if message:
                    return str(message)
            except Exception:
                raw_text = getattr(response, "text", "")
                if raw_text:
                    return str(raw_text)[:500]
    except Exception:
        pass
    return str(exc)


def _normalize_cell_value(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized if normalized else None
    return value


def _build_mapped_records(df: pd.DataFrame, field_map: Dict[str, str], custom_columns: List[str], max_rows: int = 50000):
    records = []
    skipped_missing_phone = 0
    subset = df.head(max(1, int(max_rows))).copy()

    for row in subset.to_dict(orient="records"):
        item = {}
        for target_field, source_column in (field_map or {}).items():
            if not source_column:
                continue
            value = _normalize_cell_value(row.get(source_column))
            if value is None:
                continue
            item[target_field] = value

        for source_column in custom_columns or []:
            value = _normalize_cell_value(row.get(source_column))
            if value is None:
                continue
            item[str(source_column)] = value

        if not item.get("phone"):
            skipped_missing_phone += 1
            continue
        records.append(item)

    return records, skipped_missing_phone


def _find_best_target_column(columns: List[str], aliases: List[str]) -> str:
    lowered = {str(c).strip().lower(): str(c) for c in (columns or []) if str(c).strip()}
    for alias in aliases:
        hit = lowered.get(alias.lower())
        if hit:
            return hit
    for col in columns or []:
        col_text = str(col).strip().lower()
        for alias in aliases:
            if alias.lower() in col_text:
                return str(col)
    return ""


def _has_any_phone_value(record: Dict[str, Any], phone_columns: List[str]) -> bool:
    candidates = [c for c in (phone_columns or []) if c]
    if not candidates:
        candidates = ["phone"]
    for col in candidates:
        value = record.get(col)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return True
    return False


def _adapt_record_to_target_columns(record: Dict[str, Any], target_columns: List[str], phone_columns: List[str]) -> Dict[str, Any]:
    if not isinstance(record, dict):
        return {}
    if not target_columns:
        return dict(record)

    allowed = [str(c).strip() for c in target_columns if str(c).strip()]
    allowed_set = set(allowed)
    out: Dict[str, Any] = {}

    # Keep directly matching keys.
    for key, value in record.items():
        if key in allowed_set:
            out[key] = value

    def put_if_missing(target_key: str, source_key: str):
        if not target_key or target_key not in allowed_set:
            return
        if target_key in out:
            return
        value = record.get(source_key)
        if value is None:
            return
        if isinstance(value, str) and not value.strip():
            return
        out[target_key] = value

    # Map semantic fields to best matching target columns when names differ.
    phone_target = ""
    if phone_columns:
        for col in phone_columns:
            if col in allowed_set:
                phone_target = col
                break
    if not phone_target:
        phone_target = _find_best_target_column(allowed, ["phone", "telefon", "gsm", "mobile", "tel", "cell"])
    put_if_missing(phone_target, "phone")

    name_target = _find_best_target_column(allowed, ["first", "ad", "isim", "name"])
    surname_target = _find_best_target_column(allowed, ["last", "soyad", "surname"])
    email_target = _find_best_target_column(allowed, ["email", "mail", "e-posta"])
    external_target = _find_best_target_column(allowed, ["external", "customer", "musteri", "id"])
    put_if_missing(name_target, "firstName")
    put_if_missing(surname_target, "lastName")
    put_if_missing(email_target, "email")
    put_if_missing(external_target, "externalId")

    return out


def _auto_map_source(columns: List[str], aliases: List[str]) -> str:
    lower_map = {str(c).strip().lower(): str(c) for c in columns}
    for alias in aliases:
        exact = lower_map.get(alias.lower())
        if exact:
            return exact
    for column in columns:
        cl = str(column).strip().lower()
        for alias in aliases:
            if alias.lower() in cl:
                return str(column)
    return ""


def _fetch_paged_entities(
    api: GenesysAPI,
    path: str,
    page_size: int = 100,
    max_pages: int = 20,
    suppress_error_statuses: List[int] = None,
) -> List[Dict[str, Any]]:
    entities: List[Dict[str, Any]] = []
    page_number = 1
    while page_number <= max_pages:
        data = api._get(
            path,
            params={"pageNumber": page_number, "pageSize": page_size},
            suppress_error_statuses=suppress_error_statuses,
        )
        page_entities = data.get("entities") if isinstance(data, dict) else None
        if not isinstance(page_entities, list) or not page_entities:
            break
        entities.extend([x for x in page_entities if isinstance(x, dict)])
        if not data.get("nextUri"):
            break
        page_number += 1
    return entities


def _build_option_map(entities: List[Dict[str, Any]], name_keys: List[str] = None) -> Dict[str, str]:
    name_keys = name_keys or ["name"]
    out: Dict[str, str] = {}
    for item in entities or []:
        eid = str(item.get("id") or "").strip()
        if not eid:
            continue
        label = ""
        for key in name_keys:
            raw = item.get(key)
            if raw:
                label = str(raw).strip()
                if label:
                    break
        if not label:
            label = eid
            
        original_label = label
        while label in out:
            label += " "
            
        out[label] = eid
    return out


def _label_index_for_value(option_map: Dict[str, str], value: str) -> int:
    labels = list(option_map.keys())
    if not labels:
        return 0
    needle = str(value or "").strip()
    if not needle:
        return 0
    for idx, label in enumerate(labels):
        if option_map.get(label) == needle:
            return idx
    return 0


def _parse_column_input(raw_text: str) -> List[str]:
    seen = set()
    out: List[str] = []
    for part in str(raw_text or "").split(","):
        col = str(part).strip()
        if not col:
            continue
        col_key = col.lower()
        if col_key in seen:
            continue
        seen.add(col_key)
        out.append(col)
    return out


def _extract_phone_columns(detail: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    value = (detail or {}).get("phoneColumns")
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                col = item.strip()
            elif isinstance(item, dict):
                col = str(item.get("columnName") or "").strip()
            else:
                col = ""
            if col and col not in out:
                out.append(col)
    return out


def _normalize_campaign_phone_columns(detail: Dict[str, Any]) -> List[Dict[str, str]]:
    """Build a campaign-compatible phoneColumns list from contact list detail."""
    raw = (detail or {}).get("phoneColumns")
    out: List[Dict[str, str]] = []
    if not isinstance(raw, list):
        return out

    for item in raw:
        if isinstance(item, dict):
            col = str(item.get("columnName") or "").strip()
            col_type = str(item.get("type") or "cell").strip() or "cell"
        elif isinstance(item, str):
            col = item.strip()
            col_type = "cell"
        else:
            col = ""
            col_type = "cell"

        if not col:
            continue
        normalized = {"columnName": col, "type": col_type}
        if normalized not in out:
            out.append(normalized)
    return out


def _pick_result_column(columns: List[str]) -> str:
    # Prefer Sistem Kodu (renamed lastResult) first, then custom user columns
    for native in ("Sistem Kodu", "lastResult"):
        if native in (columns or []):
            return native
    return _find_best_target_column(
        columns or [],
        ["sistemkodu", "resultcode", "result_code", "wrapupcode", "wrapup_code", "wrapup", "outcome", "sonuc", "result"],
    )


def _fetch_contact_list_sample(api: GenesysAPI, contact_list_id: str, max_records: int = 300, page_size: int = 100) -> List[Dict[str, Any]]:
    contact_list_id = str(contact_list_id or "").strip()
    if not contact_list_id:
        return []

    entities: List[Dict[str, Any]] = []
    page_number = 1
    safe_page_size = max(25, min(200, int(page_size or 100)))
    target_count = max(1, int(max_records or 300))

    while len(entities) < target_count:
        try:
            payload = api.get_outbound_contact_list_contacts(contact_list_id, page_number=page_number, page_size=safe_page_size)
        except Exception:
            break
        page_entities = payload.get("entities") if isinstance(payload, dict) else None
        if not isinstance(page_entities, list) or not page_entities:
            break
        entities.extend([item for item in page_entities if isinstance(item, dict)])
        if len(page_entities) < safe_page_size:
            break
        if isinstance(payload, dict) and not payload.get("nextUri") and len(page_entities) < safe_page_size:
            break
        page_number += 1

    return entities[:target_count]


def _flatten_contact_entity(entity: Dict[str, Any], preferred_columns: List[str] = None) -> Dict[str, Any]:
    preferred_columns = [str(col).strip() for col in (preferred_columns or []) if str(col).strip()]
    data = dict(entity.get("data") or {}) if isinstance(entity, dict) else {}
    row: Dict[str, Any] = {
        "contactId": str((entity or {}).get("id") or "").strip(),
        "callable": (entity or {}).get("callable"),
    }

    if preferred_columns:
        for column in preferred_columns:
            row[column] = data.get(column)
    else:
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                continue
            row[str(key)] = value

    # Genesys native call results: callRecords[phoneColumn].lastResult / lastAttempt
    call_records = entity.get("callRecords") if isinstance(entity, dict) else None
    if isinstance(call_records, dict) and call_records:
        # Pick the first phone column that has a result
        last_result = None
        last_attempt = None
        wrapup_code = None
        for _phone_col, rec in call_records.items():
            if not isinstance(rec, dict):
                continue
            lr = rec.get("lastResult")
            la = rec.get("lastAttempt")
            wc = rec.get("wrapUpCode") or rec.get("wrapupCode")
            if lr and not last_result:
                last_result = str(lr)
            if la and not last_attempt:
                last_attempt = str(la)
            if wc and not wrapup_code:
                wrapup_code = str(wc)
        if last_result is not None:
            row["Sistem Kodu"] = last_result
        if last_attempt is not None:
            row["lastAttempt"] = last_attempt
        if wrapup_code is not None:
            row["wrapUpCode"] = wrapup_code

    return row


def _build_result_summary(rows: List[Dict[str, Any]], result_column: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["sonuc", "adet"])
    values = []
    target_col = str(result_column or "").strip()
    for row in rows:
        raw = row.get(target_col)
        text = str(raw).strip() if raw is not None else ""
        values.append(text or "(Boş)")
    summary = pd.Series(values, dtype="string").value_counts(dropna=False).reset_index()
    summary.columns = ["sonuc", "adet"]
    return summary


def _section_header(title: str, help_text: str = "", level: int = 5) -> None:
    """Render a simple section heading for grouped form blocks."""
    st.markdown(f"**{title}**")


def _inject_dialer_styles() -> None:
    pass  # Standard Streamlit styling used — no custom overrides needed.


_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)


def _looks_like_uuid(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(_UUID_RE.match(text))


def _extract_wrapup_code_from_conversation(conv: Dict[str, Any]) -> str:
    participants = conv.get("participants") or []
    latest = ""
    for participant in participants:
        wrapup_obj = participant.get("wrapup")
        if isinstance(wrapup_obj, dict):
            code = str(wrapup_obj.get("code") or "").strip()
            if code:
                latest = code
        for session in participant.get("sessions") or []:
            for segment in session.get("segments") or []:
                code = str(segment.get("wrapUpCode") or segment.get("wrapupCode") or "").strip()
                if code:
                    latest = code
    return latest


def _extract_phone_from_conversation(conv: Dict[str, Any]) -> str:
    participants = conv.get("participants") or []
    preferred = []
    fallback = []
    for participant in participants:
        purpose = str(participant.get("purpose") or "").lower()
        values = [
            participant.get("address"),
            participant.get("ani"),
            participant.get("dnis"),
        ]
        for session in participant.get("sessions") or []:
            for segment in session.get("segments") or []:
                values.extend([segment.get("ani"), segment.get("dnis"), segment.get("address")])
        for raw in values:
            text = str(raw or "").strip()
            if not text:
                continue
            if purpose in {"customer", "external"}:
                preferred.append(text)
            else:
                fallback.append(text)
    if preferred:
        return preferred[0]
    if fallback:
        return fallback[0]
    return ""


def _normalize_phone_value(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""

    lower = text.lower()
    if lower.startswith("tel:"):
        text = text[4:]
    elif lower.startswith("sip:"):
        text = text[4:]

    text = text.split("@", 1)[0].split(";", 1)[0].strip()
    if not text:
        return ""

    if re.search(r"[A-Za-z]", text):
        return ""

    if text.startswith("+") and text[1:].isdigit() and len(text) >= 8:
        return text

    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) < 7:
        return ""
    if text.startswith("+"):
        return f"+{digits}"
    return digits


def _phone_match_candidates(phone: str) -> List[str]:
    raw = str(phone or "").strip()
    if not raw:
        return []
    digits = "".join(ch for ch in raw if ch.isdigit())
    out = []
    for item in [raw, digits, f"+{digits}", f"0{digits}"]:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _extract_id_from_attributes(attrs: Dict[str, Any], key_hint: str) -> str:
    if not isinstance(attrs, dict):
        return ""
    hint = str(key_hint or "").lower()
    for key, value in attrs.items():
        key_text = str(key or "").lower()
        if hint in key_text and _looks_like_uuid(value):
            return str(value).strip()
    return ""


def _resolve_contact_list_id_from_conversation(conv: Dict[str, Any], campaign_contact_map: Dict[str, str]) -> str:
    direct = ""
    campaign_id = ""

    conv_attrs = conv.get("attributes") if isinstance(conv.get("attributes"), dict) else {}
    direct = _extract_id_from_attributes(conv_attrs, "contactlist")
    campaign_id = _extract_id_from_attributes(conv_attrs, "campaign")

    for participant in conv.get("participants") or []:
        attrs = participant.get("attributes") if isinstance(participant.get("attributes"), dict) else {}
        if not direct:
            direct = _extract_id_from_attributes(attrs, "contactlist")
        if not campaign_id:
            campaign_id = _extract_id_from_attributes(attrs, "campaign")
        if direct and campaign_id:
            break

    if campaign_id and campaign_id in campaign_contact_map:
        return str(campaign_contact_map.get(campaign_id) or "").strip()
    return str(direct or "").strip()


def _resolve_campaign_id_from_conversation(conv: Dict[str, Any]) -> str:
    if not isinstance(conv, dict):
        return ""
    conv_attrs = conv.get("attributes") if isinstance(conv.get("attributes"), dict) else {}
    campaign_id = _extract_id_from_attributes(conv_attrs, "campaign")
    if campaign_id:
        return campaign_id
    for participant in conv.get("participants") or []:
        attrs = participant.get("attributes") if isinstance(participant.get("attributes"), dict) else {}
        campaign_id = _extract_id_from_attributes(attrs, "campaign")
        if campaign_id:
            return campaign_id
    return ""


def _parse_iso_datetime(value: Any):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _format_local_timestamp(raw_iso: Any, utc_offset_hours: float) -> str:
    dt = _parse_iso_datetime(raw_iso)
    if dt is None:
        return str(raw_iso or "").replace("T", " ")[:19]
    try:
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return (dt + timedelta(hours=float(utc_offset_hours or 0.0))).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(raw_iso or "").replace("T", " ")[:19]


def _collect_ids_from_attrs(attrs: Any, key_hint: str) -> List[str]:
    if not isinstance(attrs, dict):
        return []
    out: List[str] = []
    hint = str(key_hint or "").lower()
    for key, value in attrs.items():
        key_text = str(key or "").lower()
        if hint not in key_text:
            continue
        if _looks_like_uuid(value):
            id_text = str(value).strip()
            if id_text and id_text not in out:
                out.append(id_text)
    return out


def _collect_conversation_ids(conv: Any, key_hint: str) -> List[str]:
    if not isinstance(conv, dict):
        return []
    out: List[str] = []
    for item in _collect_ids_from_attrs(conv.get("attributes"), key_hint):
        if item not in out:
            out.append(item)
    for participant in conv.get("participants") or []:
        for item in _collect_ids_from_attrs(participant.get("attributes"), key_hint):
            if item not in out:
                out.append(item)
    return out


def _conversation_matches_campaign(conv: Dict[str, Any], campaign_id: str, contact_list_id: str, call_conv: Optional[Dict[str, Any]] = None) -> bool:
    campaign_id = str(campaign_id or "").strip()
    contact_list_id = str(contact_list_id or "").strip()
    if not campaign_id and not contact_list_id:
        return True

    candidate_campaign_ids: List[str] = []
    candidate_contact_ids: List[str] = []
    for source in (conv, call_conv):
        if not isinstance(source, dict):
            continue
        resolved = _resolve_campaign_id_from_conversation(source)
        if resolved and resolved not in candidate_campaign_ids:
            candidate_campaign_ids.append(resolved)
        for item in _collect_conversation_ids(source, "campaign"):
            if item not in candidate_campaign_ids:
                candidate_campaign_ids.append(item)
        for item in _collect_conversation_ids(source, "contactlist"):
            if item not in candidate_contact_ids:
                candidate_contact_ids.append(item)

    if campaign_id and campaign_id in candidate_campaign_ids:
        return True
    if contact_list_id and contact_list_id in candidate_contact_ids:
        return True

    # If attributes are unavailable in the payload, keep queue-level data visible
    # instead of showing false-zero campaign results.
    if not candidate_campaign_ids and not candidate_contact_ids:
        return True
    return False


def _build_users_reverse_map() -> Dict[str, str]:
    users_info = st.session_state.get("users_info") or {}
    users_map_rev: Dict[str, str] = {}
    for uid, user_info in users_info.items():
        if not isinstance(user_info, dict):
            continue
        name = str(user_info.get("name") or user_info.get("username") or "").strip()
        if name:
            users_map_rev[str(uid).strip()] = name
    return users_map_rev


def _normalize_wrapup_display(
    api: GenesysAPI,
    raw_value: Any,
    wrapup_id_to_name: Dict[str, str],
    wrapup_id_resolution_cache: Dict[str, str],
) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    if _looks_like_uuid(text):
        mapped = str(wrapup_id_to_name.get(text) or "").strip()
        if mapped:
            return mapped
        if text in wrapup_id_resolution_cache:
            return wrapup_id_resolution_cache[text]
        try:
            detail = api.get_wrapup_code(text) or {}
            resolved = str(detail.get("name") or detail.get("code") or detail.get("id") or text).strip()
        except Exception:
            resolved = text
        wrapup_id_resolution_cache[text] = resolved
        return resolved
    return text


def _extract_wrapup_value(
    api: GenesysAPI,
    raw_wrapup: Any,
    wrapup_id_to_name: Dict[str, str],
    wrapup_id_resolution_cache: Dict[str, str],
) -> str:
    if not isinstance(raw_wrapup, dict):
        return _normalize_wrapup_display(api, raw_wrapup, wrapup_id_to_name, wrapup_id_resolution_cache)
    code_val = str(raw_wrapup.get("code") or "").strip()
    name_val = str(raw_wrapup.get("name") or "").strip()
    id_val = str(raw_wrapup.get("id") or "").strip()
    if code_val and not _looks_like_uuid(code_val):
        return code_val
    if name_val:
        return name_val
    if code_val:
        return _normalize_wrapup_display(api, code_val, wrapup_id_to_name, wrapup_id_resolution_cache)
    if id_val:
        return _normalize_wrapup_display(api, id_val, wrapup_id_to_name, wrapup_id_resolution_cache)
    return ""


def _pick_phone_from_candidates(candidates: List[Any], caller_address: str = "", prefer_non_caller: bool = True) -> str:
    normalized: List[str] = []
    for raw_val in candidates:
        phone = _normalize_phone_value(raw_val)
        if phone and phone not in normalized:
            normalized.append(phone)
    if not normalized:
        return ""
    if prefer_non_caller and caller_address:
        for phone in normalized:
            if phone != caller_address:
                return phone
    return normalized[0]


def _get_call_conversation(api: GenesysAPI, conv_id: str, conv_call_cache: Dict[str, Any]) -> Dict[str, Any]:
    conv_id = str(conv_id or "").strip()
    if not conv_id:
        return {}
    cached = conv_call_cache.get(conv_id)
    if isinstance(cached, dict):
        return cached
    try:
        call_conv = api.get_conversation_call(conv_id) or {}
    except Exception:
        call_conv = {}
    if isinstance(call_conv, dict):
        conv_call_cache[conv_id] = call_conv
    return call_conv if isinstance(call_conv, dict) else {}


def _normalize_interaction_row(
    api: GenesysAPI,
    conv: Dict[str, Any],
    utc_offset_hours: float,
    users_map_rev: Dict[str, str],
    wrapup_id_to_name: Dict[str, str],
    wrapup_id_resolution_cache: Dict[str, str],
    caller_address: str = "",
    call_conv: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source_conv = call_conv if isinstance(call_conv, dict) and isinstance(call_conv.get("participants"), list) else conv
    participants = source_conv.get("participants") or conv.get("participants") or []

    conv_id = str(conv.get("conversationId") or conv.get("id") or source_conv.get("conversationId") or source_conv.get("id") or "").strip()
    start_raw = source_conv.get("conversationStart") or conv.get("conversationStart")
    end_raw = source_conv.get("conversationEnd") or conv.get("conversationEnd")
    start_dt = _parse_iso_datetime(start_raw)
    end_dt = _parse_iso_datetime(end_raw)

    duration_seconds = 0
    if start_dt is not None and end_dt is not None:
        try:
            duration_seconds = max(0, int((end_dt - start_dt).total_seconds()))
        except Exception:
            duration_seconds = 0

    agent_names: List[str] = []
    has_agent_participant = False
    agent_ani_candidates: List[Any] = []
    user_ani_candidates: List[Any] = []
    dnis_candidates: List[Any] = []
    agent_wrapup_last = ""
    user_wrapup_last = ""

    for participant in participants:
        purpose = str(participant.get("purpose") or "").lower()
        is_agent = purpose == "agent"
        is_user = purpose in {"user", "customer", "external"}
        if is_agent:
            has_agent_participant = True

        participant_name = str(participant.get("participantName") or participant.get("name") or "").strip()
        user_id = str(
            ((participant.get("user") or {}).get("id") if isinstance(participant.get("user"), dict) else participant.get("userId"))
            or ""
        ).strip()
        if not participant_name and user_id:
            participant_name = users_map_rev.get(user_id, "")
        if is_agent and participant_name and participant_name not in agent_names:
            agent_names.append(participant_name)

        participant_wrapup = _extract_wrapup_value(
            api,
            participant.get("wrapup"),
            wrapup_id_to_name,
            wrapup_id_resolution_cache,
        )
        if participant_wrapup:
            if is_agent:
                agent_wrapup_last = participant_wrapup
            elif is_user:
                user_wrapup_last = participant_wrapup

        if is_agent:
            agent_ani_candidates.extend([participant.get("ani"), participant.get("address")])
        elif is_user:
            user_ani_candidates.extend([participant.get("ani"), participant.get("address")])
        dnis_candidates.extend([participant.get("dnis"), participant.get("address")])

        for session in participant.get("sessions") or []:
            if is_agent:
                agent_ani_candidates.extend([session.get("ani"), session.get("address")])
            elif is_user:
                user_ani_candidates.extend([session.get("ani"), session.get("address")])
            dnis_candidates.extend([session.get("dnis"), session.get("address")])

            for segment in session.get("segments") or []:
                if is_agent:
                    agent_ani_candidates.extend([segment.get("ani"), segment.get("address")])
                elif is_user:
                    user_ani_candidates.extend([segment.get("ani"), segment.get("address")])
                dnis_candidates.extend([segment.get("dnis"), segment.get("address")])

                segment_wrapup_raw = (
                    segment.get("wrapUpCode")
                    or segment.get("wrapupCode")
                    or segment.get("wrapUp")
                    or segment.get("wrapup")
                )
                segment_wrapup = _extract_wrapup_value(
                    api,
                    segment_wrapup_raw,
                    wrapup_id_to_name,
                    wrapup_id_resolution_cache,
                )
                if segment_wrapup:
                    if is_agent:
                        agent_wrapup_last = segment_wrapup
                    elif is_user:
                        user_wrapup_last = segment_wrapup

    call_type = "Agent" if has_agent_participant else "Sistem"
    if call_type == "Agent":
        phone = (
            _pick_phone_from_candidates(agent_ani_candidates, caller_address=caller_address, prefer_non_caller=True)
            or _pick_phone_from_candidates(user_ani_candidates, caller_address=caller_address, prefer_non_caller=True)
            or _pick_phone_from_candidates(dnis_candidates, caller_address=caller_address, prefer_non_caller=False)
        )
    else:
        phone = (
            _pick_phone_from_candidates(dnis_candidates, caller_address=caller_address, prefer_non_caller=False)
            or _pick_phone_from_candidates(user_ani_candidates, caller_address=caller_address, prefer_non_caller=False)
            or _pick_phone_from_candidates(agent_ani_candidates, caller_address=caller_address, prefer_non_caller=False)
        )

    wrapup_code = agent_wrapup_last or user_wrapup_last
    if not wrapup_code:
        fallback_raw = _extract_wrapup_code_from_conversation(source_conv)
        if not fallback_raw and source_conv is not conv:
            fallback_raw = _extract_wrapup_code_from_conversation(conv)
        wrapup_code = _normalize_wrapup_display(api, fallback_raw, wrapup_id_to_name, wrapup_id_resolution_cache)

    bucket = _classify_outbound_result_bucket(wrapup_code)
    return {
        "Başlangıç": _format_local_timestamp(start_raw, utc_offset_hours),
        "Bitiş": _format_local_timestamp(end_raw, utc_offset_hours),
        "Telefon": phone,
        "Çağrı Tipi": call_type,
        "Agent": ", ".join(agent_names),
        "Wrap-up Kodu": wrapup_code,
        "Durasyon (sn)": duration_seconds,
        "Sonuç Sınıfı": {
            "mesgul": "Meşgul",
            "ulasilamadi": "Ulaşılamadı",
            "tamamlanan": "Tamamlanan",
        }.get(bucket, "Tekrar Aranacak"),
        "Konuşma ID": conv_id,
    }


def _normalize_interaction_rows(
    api: GenesysAPI,
    conversations: List[Dict[str, Any]],
    campaign_id: str,
    contact_list_id: str,
    utc_offset_hours: float,
    users_map_rev: Dict[str, str],
    wrapup_id_to_name: Dict[str, str],
    wrapup_id_resolution_cache: Dict[str, str],
    caller_address: str = "",
    conv_call_cache: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    call_cache = conv_call_cache if isinstance(conv_call_cache, dict) else {}
    for conv in conversations or []:
        if not isinstance(conv, dict):
            continue
        conv_id = str(conv.get("conversationId") or conv.get("id") or "").strip()
        call_conv = _get_call_conversation(api, conv_id, call_cache) if conv_id else {}
        if not _conversation_matches_campaign(conv, campaign_id, contact_list_id, call_conv=call_conv):
            continue
        rows.append(
            _normalize_interaction_row(
                api=api,
                conv=conv,
                call_conv=call_conv,
                utc_offset_hours=utc_offset_hours,
                users_map_rev=users_map_rev,
                wrapup_id_to_name=wrapup_id_to_name,
                wrapup_id_resolution_cache=wrapup_id_resolution_cache,
                caller_address=caller_address,
            )
        )
    return rows


def _classify_outbound_result_bucket(raw_value: Any) -> str:
    text = str(raw_value or "").strip().lower()
    if not text:
        return "tekrar_aranacak"
    if any(tok in text for tok in ("busy", "mesgul")):
        return "mesgul"
    if any(tok in text for tok in ("noanswer", "no_answer", "unreach", "notreach", "ula", "answeringmachine", "voicemail")):
        return "ulasilamadi"
    if any(tok in text for tok in ("complete", "completed", "success", "connected", "contacted", "tamam")):
        return "tamamlanan"
    return "tekrar_aranacak"


def _ensure_contact_list_result_column(api: GenesysAPI, contact_list_detail: Dict[str, Any], result_column: str) -> Dict[str, Any]:
    result_column = str(result_column or "").strip()
    if not result_column:
        return contact_list_detail or {}

    detail = contact_list_detail if isinstance(contact_list_detail, dict) else {}
    columns = list(detail.get("columnNames") or [])
    if result_column in columns:
        return detail

    version = detail.get("version")
    if version is None:
        return detail

    updated_columns = list(columns)
    updated_columns.append(result_column)
    payload = {
        "id": str(detail.get("id") or ""),
        "name": str(detail.get("name") or ""),
        "columnNames": updated_columns,
        "phoneColumns": list(detail.get("phoneColumns") or []),
        "version": version,
    }
    api.update_outbound_contact_list(payload.get("id"), payload)
    return api.get_outbound_contact_list(payload.get("id"))


def _auto_sync_result_codes(api: GenesysAPI, campaigns: List[Dict[str, Any]], result_column: str, lookback_minutes: int = 20) -> Dict[str, int]:
    stats = {
        "scanned": 0,
        "updated": 0,
        "skipped_no_wrapup": 0,
        "skipped_no_contactlist": 0,
        "skipped_no_phone": 0,
        "not_found": 0,
        "errors": 0,
    }

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(minutes=max(1, int(lookback_minutes or 20)))
    conversations = api.get_conversation_details_recent(start_utc, now_utc, page_size=100, max_pages=3, order="desc")

    campaign_contact_map: Dict[str, str] = {}
    for item in campaigns or []:
        if not isinstance(item, dict):
            continue
        cid = str(item.get("id") or "").strip()
        clid = str((item.get("contactList") or {}).get("id") or item.get("contactListId") or "").strip()
        if cid and clid:
            campaign_contact_map[cid] = clid

    processed = st.session_state.get("dialer_auto_result_processed") or {}
    if not isinstance(processed, dict):
        processed = {}

    cl_cache: Dict[str, Dict[str, Any]] = {}
    for conv in conversations or []:
        if not isinstance(conv, dict):
            continue
        stats["scanned"] += 1

        conv_id = str(conv.get("conversationId") or conv.get("id") or "").strip()
        wrapup_code = _extract_wrapup_code_from_conversation(conv)
        if not wrapup_code:
            stats["skipped_no_wrapup"] += 1
            continue

        dedupe_key = f"{conv_id}:{wrapup_code}"
        if dedupe_key in processed:
            continue

        contact_list_id = _resolve_contact_list_id_from_conversation(conv, campaign_contact_map)
        if not contact_list_id:
            stats["skipped_no_contactlist"] += 1
            processed[dedupe_key] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            continue

        phone = _extract_phone_from_conversation(conv)
        phone_candidates = _phone_match_candidates(phone)
        if not phone_candidates:
            stats["skipped_no_phone"] += 1
            processed[dedupe_key] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            continue

        detail = cl_cache.get(contact_list_id)
        if not detail:
            try:
                detail = api.get_outbound_contact_list(contact_list_id)
            except Exception:
                detail = {}
            if isinstance(detail, dict):
                detail = _ensure_contact_list_result_column(api, detail, result_column)
            cl_cache[contact_list_id] = detail if isinstance(detail, dict) else {}

        phone_columns = _extract_phone_columns(detail if isinstance(detail, dict) else {})
        if not phone_columns:
            col_names = list((detail or {}).get("columnNames") or []) if isinstance(detail, dict) else []
            inferred = _find_best_target_column(col_names, ["phone", "telefon", "gsm", "mobile", "tel", "cell"])
            phone_columns = [inferred] if inferred else []

        match_contact_id = ""
        for phone_col in phone_columns:
            if not phone_col:
                continue
            for candidate in phone_candidates:
                try:
                    search_res = api.search_outbound_contact_list_contacts(
                        contact_list_id,
                        column=phone_col,
                        value=candidate,
                        page_number=1,
                        page_size=5,
                    )
                    entities = search_res.get("entities") if isinstance(search_res, dict) else []
                    entities = [x for x in entities if isinstance(x, dict) and x.get("id")]
                    if entities:
                        match_contact_id = str(entities[0].get("id") or "").strip()
                        break
                except Exception:
                    continue
            if match_contact_id:
                break

        if not match_contact_id:
            stats["not_found"] += 1
            continue

        try:
            api.write_result_code_to_contact_data(
                contact_list_id,
                match_contact_id,
                result_column=result_column,
                result_code=wrapup_code,
                extra_fields={
                    "resultConversationId": conv_id,
                    "resultUpdatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                },
            )
            stats["updated"] += 1
            processed[dedupe_key] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        except Exception:
            stats["errors"] += 1

    # Keep de-dup state bounded.
    if len(processed) > 2000:
        keep_items = list(processed.items())[-2000:]
        processed = {k: v for k, v in keep_items}

    st.session_state["dialer_auto_result_processed"] = processed
    return stats


def render_dialer_service(context: Dict[str, Any]) -> None:
    """Render Dialer page for outbound campaign management and tracking."""
    bind_context(globals(), context)
    _inject_dialer_styles()
    st.title("Outbound Operasyon Merkezi")
    st.divider()

    api_client = st.session_state.get("api_client")
    if not api_client:
        st.warning(get_text(lang, "genesys_not_connected"))
        return

    api = GenesysAPI(api_client)
    endpoint_status_cache = st.session_state.get("_dialer_optional_endpoint_status")
    if not isinstance(endpoint_status_cache, dict):
        endpoint_status_cache = {}
        st.session_state["_dialer_optional_endpoint_status"] = endpoint_status_cache
    endpoint_scope = str(st.session_state.get("app_user", {}).get("org_code") or getattr(api, "api_host", "") or "default")

    def _get_optional_entities(path: str, page_size: int = 100, max_pages: int = 20) -> List[Dict[str, Any]]:
        cache_key = f"{endpoint_scope}:{path}"
        cached_status = endpoint_status_cache.get(cache_key)
        if cached_status in {"403", "404"}:
            return []
        try:
            entities = _fetch_paged_entities(
                api,
                path,
                page_size=page_size,
                max_pages=max_pages,
                suppress_error_statuses=[403, 404],
            )
            endpoint_status_cache[cache_key] = "ok"
            return entities
        except Exception as exc:
            try:
                status_code = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0)
            except Exception:
                status_code = 0
            if status_code in (403, 404):
                endpoint_status_cache[cache_key] = str(status_code)
                return []
            raise

    tabs = st.tabs([
        "Kampanya Yönetimi",
        "Eylem Planı / Strateji",
        "Excel / Contact List",
        "Takip & Sonuçlar",
    ])

    with tabs[0]:
        campaigns = []
        campaigns_forbidden = False
        try:
            campaigns = api.get_outbound_campaigns(page_size=100, max_pages=20)
        except Exception as exc:
            campaigns_forbidden = _is_forbidden_error(exc)
            st.error(f"Kampanyalar alınamadı: {exc}")
            if campaigns_forbidden:
                _render_forbidden_hint("Outbound Campaign")

        campaign_rows = [_campaign_row(c) for c in campaigns if isinstance(c, dict)]
        st.session_state["dialer_campaign_ids_cache"] = [
            str(c.get("id") or "").strip()
            for c in campaigns
            if isinstance(c, dict) and str(c.get("id") or "").strip()
        ]
        total_campaigns = len(campaign_rows)
        predictive_count = sum(1 for row in campaign_rows if str(row.get("dialingMode") or "").lower() == "predictive")
        progressive_count = sum(1 for row in campaign_rows if str(row.get("dialingMode") or "").lower() == "progressive")
        connected_lists = sum(1 for row in campaign_rows if str(row.get("contactList") or "").strip())

        metric_cols = st.columns(4)
        metric_cols[0].metric("Toplam Kampanya", total_campaigns)
        metric_cols[1].metric("Predictive", predictive_count)
        metric_cols[2].metric("Progressive", progressive_count)
        metric_cols[3].metric("Contact List Bağlı", connected_lists)

        if campaign_rows:
            with st.expander("📋 Kampanya Listesi", expanded=True):
                campaign_df = pd.DataFrame(campaign_rows)
                if "id" in campaign_df.columns:
                    campaign_df = campaign_df.drop(columns=["id"])
                if "ID" in campaign_df.columns:
                    campaign_df = campaign_df.drop(columns=["ID"])
                st.dataframe(campaign_df, width='stretch', hide_index=True)
        else:
            st.info("Outbound kampanya bulunamadı.")

        campaign_options = {}
        for c in campaigns:
            if not isinstance(c, dict):
                continue
            cid = str(c.get("id") or "").strip()
            if not cid:
                continue
            name = str(c.get("name") or cid)
            status = str(c.get("campaignStatus") or c.get("status") or "-")
            label = f"{name} [{status}]"
            while label in campaign_options:
                 label += " "
            campaign_options[label] = cid

        queue_options = {}
        try:
            queue_map = st.session_state.get("queues_map") or {}
            for q_name, q_id in queue_map.items():
                qid = str(q_id or "").strip()
                if not qid:
                    continue
                label = q_name
                while label in queue_options:
                    label += " "
                queue_options[label] = qid
        except Exception:
            queue_options = {}

        cl_options_tab0 = {}
        try:
            cl_entities = api.get_outbound_contact_lists(page_size=100, max_pages=20)
            for item in cl_entities or []:
                if not isinstance(item, dict):
                    continue
                cid = str(item.get("id") or "").strip()
                if not cid:
                    continue
                label = str(item.get('name') or cid)
                while label in cl_options_tab0:
                    label += " "
                cl_options_tab0[label] = cid
        except Exception:
            cl_options_tab0 = {}

        dialing_modes = ["preview", "predictive", "power", "progressive", "agentless", "external"]

        script_options = {}
        edge_group_options = {}
        division_options = {}
        caller_id_options = {}
        site_options = {}
        call_analysis_set_options = {}
        caller_details_by_id = {}
        try:
            script_entities = _fetch_paged_entities(api, "/api/v2/scripts", page_size=100, max_pages=20)
            script_options = _build_option_map(script_entities, name_keys=["name"]) 
        except Exception:
            script_options = {}
        try:
            edge_entities = _get_optional_entities("/api/v2/telephony/providers/edges/edgegroups", page_size=100, max_pages=20)
            edge_group_options = _build_option_map(edge_entities, name_keys=["name"]) 
        except Exception:
            edge_group_options = {}
        try:
            division_entities = _fetch_paged_entities(api, "/api/v2/authorization/divisions", page_size=100, max_pages=20)
            division_options = _build_option_map(division_entities, name_keys=["name"]) 
        except Exception:
            division_options = {}
        try:
            caller_entities = _get_optional_entities("/api/v2/outbound/callerids", page_size=100, max_pages=20)
            for item in caller_entities:
                cid = str(item.get("id") or "").strip()
                if not cid:
                    continue
                name = str(item.get("name") or "").strip()
                addr = str(item.get("phoneNumber") or item.get("address") or "").strip()
                if not name:
                    name = addr or cid
                label = f"{name} / {addr}" if addr else name
                while label in caller_id_options:
                    label += " "
                caller_id_options[label] = cid
                caller_details_by_id[cid] = {"name": name, "address": addr}
        except Exception:
            caller_id_options = {}
            caller_details_by_id = {}
        try:
            site_entities = _fetch_paged_entities(api, "/api/v2/telephony/providers/edges/sites", page_size=100, max_pages=20)
            site_options = _build_option_map(site_entities, name_keys=["name"])
        except Exception:
            site_options = {}
        try:
            call_analysis_entities = _fetch_paged_entities(api, "/api/v2/outbound/callanalysisresponsesets", page_size=100, max_pages=20)
            call_analysis_set_options = _build_option_map(call_analysis_entities, name_keys=["name"])
        except Exception:
            call_analysis_set_options = {}

        if campaign_options:
            with st.expander("🎛️ Canlı Kampanya Kontrolü", expanded=True):
                st.caption("Seçili kampanya üzerinde başlatma, durdurma ve hızlı yenileme işlemlerini buradan yönetin.")
                selected_label = st.selectbox("Kampanya Seç", list(campaign_options.keys()), key="dialer_campaign_select")
                selected_campaign_id = campaign_options[selected_label]

                c1, c2, c3 = st.columns(3)
                if c1.button("▶ Kampanyayı Başlat", width='stretch'):
                    try:
                        api.start_outbound_campaign(selected_campaign_id)
                        _audit_user_action("dialer_campaign_start", f"Campaign started: {selected_campaign_id}", "success")
                        st.success("Kampanya başlatıldı.")
                    except Exception as exc:
                        st.error(f"Başlatma hatası: {exc}")
                if c2.button("⏹ Kampanyayı Durdur", width='stretch'):
                    try:
                        api.stop_outbound_campaign(selected_campaign_id)
                        _audit_user_action("dialer_campaign_stop", f"Campaign stopped: {selected_campaign_id}", "success")
                        st.success("Kampanya durduruldu.")
                    except Exception as exc:
                        st.error(f"Durdurma hatası: {exc}")
                if c3.button("Yeniden Yükle", width='stretch'):
                    for suffix in ("name", "mode", "contact", "queue", "script", "edge", "division", "site", "ca_set"):
                        st.session_state.pop(f"dialer_campaign_{suffix}_{selected_campaign_id}", None)
                    st.rerun()

            try:
                campaign_detail = api.get_outbound_campaign(selected_campaign_id)
            except Exception:
                campaign_detail = next((c for c in campaigns if c.get("id") == selected_campaign_id), {})

            current_name = str(campaign_detail.get("name") or "")
            current_mode = str(campaign_detail.get("dialingMode") or "preview").lower()
            current_contact = str((campaign_detail.get("contactList") or {}).get("id") or campaign_detail.get("contactListId") or "").strip()
            current_queue = str((campaign_detail.get("queue") or {}).get("id") or campaign_detail.get("queueId") or "").strip()
            current_script = str((campaign_detail.get("script") or {}).get("id") or campaign_detail.get("scriptId") or "").strip()
            current_edge = str((campaign_detail.get("edgeGroup") or {}).get("id") or campaign_detail.get("edgeGroupId") or "").strip()
            current_division = str((campaign_detail.get("division") or {}).get("id") or campaign_detail.get("divisionId") or "").strip()
            current_site = str((campaign_detail.get("site") or {}).get("id") or campaign_detail.get("siteId") or "").strip()
            current_call_analysis_set = str((campaign_detail.get("callAnalysisResponseSet") or {}).get("id") or campaign_detail.get("callAnalysisResponseSetId") or "").strip()
            current_caller_name = str(campaign_detail.get("callerName") or "").strip()
            current_caller_address = str(campaign_detail.get("callerAddress") or "").strip()

            if current_mode not in dialing_modes:
                dialing_modes.append(current_mode)

            mode_default_idx = dialing_modes.index(current_mode) if current_mode in dialing_modes else 0

            with st.expander("⚙️ Kampanya Ayarları", expanded=True):
                st.caption("Seçili kampanyanın canlı ayarlarını düzenleyin ve isterseniz aynı yapıdan yeni kampanya üretin.")

                with st.container(border=True):
                    _section_header(
                        "Temel Bilgiler",
                        help_text="Kampanya Adı, Dialing Mode ve Contact List temel işletim parametreleridir. Contact List telefon kolonları doğru tanımlı olmalı.",
                    )
                    f1, f2, f3, f3_2 = st.columns(4)
                    campaign_name = f1.text_input("Kampanya Adı", value=current_name, key=f"dialer_campaign_name_{selected_campaign_id}")
                    campaign_mode = f2.selectbox("Dialing Mode", dialing_modes, index=mode_default_idx, key=f"dialer_campaign_mode_{selected_campaign_id}")
                    cl_labels = list(cl_options_tab0.keys())
                    cl_default_idx = 0
                    if current_contact:
                        for idx, label in enumerate(cl_labels):
                            if cl_options_tab0.get(label) == current_contact:
                                cl_default_idx = idx
                                break
                    if cl_labels:
                        selected_contact_label = f3.selectbox("Contact List", cl_labels, index=cl_default_idx, key=f"dialer_campaign_contact_{selected_campaign_id}")
                        selected_contact_id = cl_options_tab0.get(selected_contact_label)
                    else:
                        selected_contact_id = st.text_input("Contact List ID", value=current_contact, key=f"dialer_campaign_contact_{selected_campaign_id}")

                    # Arama Limiti (Attempt Limit) fallback
                    try:
                        al_options_local = _build_option_map(api.get_outbound_attempt_limits_list(page_size=100, max_pages=10), ["name"])
                    except Exception:
                        al_options_local = {}
                    
                    current_attempt_limit = ""
                    if current_contact:
                        try:
                            current_attempt_limit = str(
                                api.get_outbound_contact_list_attempt_limit_id(current_contact) or ""
                            ).strip()
                        except Exception:
                            current_attempt_limit = ""
                    if al_options_local:
                        al_labels = list(al_options_local.keys())
                        al_idx = _label_index_for_value(al_options_local, current_attempt_limit)
                        al_label = f3_2.selectbox("Arama Şablonu (Attempt Limit)", ["(Seçim Yok)"] + al_labels, index=0 if not current_attempt_limit else al_idx + 1, key=f"dialer_campaign_al_{selected_campaign_id}")
                        selected_attempt_limit_id = al_options_local.get(al_label) if al_label != "(Seçim Yok)" else None
                    else:
                        selected_attempt_limit_id = f3_2.text_input("Attempt Limit ID", value=current_attempt_limit, key=f"dialer_campaign_al_{selected_campaign_id}")
                    f3_2.caption("Not: Attempt Limit kampanyaya değil Contact List'e atanır.")

                with st.container(border=True):
                    _section_header(
                        "Yönlendirme ve Kaynaklar",
                        help_text="Queue / Script / Edge / Division / Site / Call Analysis seçimleri arama akışını belirler. Predictive/Power/Progressive modlarda Site ve Call Analysis kritik olabilir.",
                    )
                    f4, f5, f6, f7 = st.columns(4)
                    queue_labels = list(queue_options.keys())
                    queue_default_idx = 0
                    if current_queue:
                        for idx, label in enumerate(queue_labels):
                            if queue_options.get(label) == current_queue:
                                queue_default_idx = idx
                                break
                    if queue_labels:
                        selected_queue_label = f4.selectbox("Queue", queue_labels, index=queue_default_idx, key=f"dialer_campaign_queue_{selected_campaign_id}")
                        selected_queue_id = queue_options.get(selected_queue_label)
                    else:
                        selected_queue_id = f4.text_input("Queue ID", value=current_queue, key=f"dialer_campaign_queue_{selected_campaign_id}")

                    if script_options:
                        script_labels = list(script_options.keys())
                        script_idx = _label_index_for_value(script_options, current_script)
                        script_label = f5.selectbox("Script", script_labels, index=script_idx, key=f"dialer_campaign_script_{selected_campaign_id}")
                        script_id = script_options.get(script_label)
                    else:
                        script_id = f5.text_input("Script ID", value=current_script, key=f"dialer_campaign_script_{selected_campaign_id}")

                    if edge_group_options:
                        edge_labels = list(edge_group_options.keys())
                        edge_idx = _label_index_for_value(edge_group_options, current_edge)
                        edge_label = f6.selectbox("Edge Group", edge_labels, index=edge_idx, key=f"dialer_campaign_edge_{selected_campaign_id}")
                        edge_group_id = edge_group_options.get(edge_label)
                    else:
                        edge_group_id = f6.text_input("Edge Group ID", value=current_edge, key=f"dialer_campaign_edge_{selected_campaign_id}")

                    if division_options:
                        div_labels = list(division_options.keys())
                        div_idx = _label_index_for_value(division_options, current_division)
                        div_label = f7.selectbox("Division", div_labels, index=div_idx, key=f"dialer_campaign_division_{selected_campaign_id}")
                        division_id = division_options.get(div_label)
                    else:
                        division_id = f7.text_input("Division ID", value=current_division, key=f"dialer_campaign_division_{selected_campaign_id}")

                    f8, f9 = st.columns(2)
                    if site_options:
                        site_labels = list(site_options.keys())
                        site_idx = _label_index_for_value(site_options, current_site)
                        site_label = f8.selectbox("Site", site_labels, index=site_idx, key=f"dialer_campaign_site_{selected_campaign_id}")
                        site_id = site_options.get(site_label)
                    else:
                        site_id = f8.text_input("Site ID", value=current_site, key=f"dialer_campaign_site_{selected_campaign_id}")

                    if call_analysis_set_options:
                        ca_labels = list(call_analysis_set_options.keys())
                        ca_idx = _label_index_for_value(call_analysis_set_options, current_call_analysis_set)
                        ca_label = f9.selectbox("Call Analysis Response Set", ca_labels, index=ca_idx, key=f"dialer_campaign_ca_set_{selected_campaign_id}")
                        call_analysis_set_id = call_analysis_set_options.get(ca_label)
                    else:
                        call_analysis_set_id = f9.text_input(
                            "Call Analysis Response Set ID",
                            value=current_call_analysis_set,
                            key=f"dialer_campaign_ca_set_{selected_campaign_id}",
                        )

                with st.container(border=True):
                    _section_header(
                        "Arayan Bilgisi",
                        help_text="Caller Name ve Caller Address müşterinin gördüğü arayan kimliğidir. Yerel operatör kurallarına uygun format kullanın.",
                    )
                    cf1, cf2 = st.columns(2)
                    caller_name = current_caller_name
                    caller_address = current_caller_address
                    if caller_id_options:
                        caller_labels = list(caller_id_options.keys())
                        caller_default_idx = 0
                        for idx, label in enumerate(caller_labels):
                            cid = caller_id_options.get(label)
                            details = caller_details_by_id.get(cid, {})
                            if details.get("name") == current_caller_name and details.get("address") == current_caller_address:
                                caller_default_idx = idx
                                break
                        caller_label = cf1.selectbox("Caller ID", caller_labels, index=caller_default_idx, key=f"dialer_campaign_caller_{selected_campaign_id}")
                        selected_caller_id = caller_id_options.get(caller_label)
                        selected_caller = caller_details_by_id.get(selected_caller_id, {})
                        caller_name = selected_caller.get("name") or caller_name
                        caller_address = selected_caller.get("address") or caller_address
                        cf2.caption(f"Caller Address: {caller_address or '-'}")
                    else:
                        caller_name = cf1.text_input("Caller Name", value=current_caller_name, key=f"dialer_campaign_caller_name_{selected_campaign_id}")
                        caller_address = cf2.text_input("Caller Address", value=current_caller_address, key=f"dialer_campaign_caller_addr_{selected_campaign_id}")

                st.divider()
                update_col1, update_col2 = st.columns([2, 1])
                if update_col1.button("Kampanya Ayarlarını Kaydet", key=f"dialer_campaign_update_btn_{selected_campaign_id}", width='stretch', type="primary"):
                    try:
                        payload = copy.deepcopy(campaign_detail if isinstance(campaign_detail, dict) else {})
                        payload["id"] = selected_campaign_id
                        payload["name"] = campaign_name
                        payload["dialingMode"] = campaign_mode
                        if caller_name:
                            payload["callerName"] = caller_name
                        if caller_address:
                            payload["callerAddress"] = caller_address
                        if selected_contact_id:
                            payload["contactList"] = {"id": selected_contact_id}
                        if selected_queue_id:
                            payload["queue"] = {"id": selected_queue_id}
                        if script_id:
                            payload["script"] = {"id": script_id}
                        if edge_group_id:
                            payload["edgeGroup"] = {"id": edge_group_id}
                        if division_id:
                            payload["division"] = {"id": division_id}
                        if site_id:
                            payload["site"] = {"id": site_id}
                        if call_analysis_set_id:
                            payload["callAnalysisResponseSet"] = {"id": call_analysis_set_id}
                        
                        api.update_outbound_campaign(selected_campaign_id, payload)
                        # Attempt limit is applied at contact-list level in Genesys.
                        attempt_target_contact_id = str(selected_contact_id or current_contact or "").strip()
                        if attempt_target_contact_id:
                            api.set_outbound_contact_list_attempt_limit(
                                attempt_target_contact_id,
                                selected_attempt_limit_id,
                            )
                            verified_attempt_limit_id = str(
                                api.get_outbound_contact_list_attempt_limit_id(attempt_target_contact_id) or ""
                            ).strip()
                            expected_attempt_limit_id = str(selected_attempt_limit_id or "").strip()
                            if verified_attempt_limit_id != expected_attempt_limit_id:
                                raise ValueError(
                                    "Attempt Limit atamasi dogrulanamadi. "
                                    f"ContactList={attempt_target_contact_id}, expected={expected_attempt_limit_id or '-'}, "
                                    f"actual={verified_attempt_limit_id or '-'}"
                                )
                        _audit_user_action("dialer_campaign_update", f"Campaign updated: {selected_campaign_id}", "success")
                        st.success("Kampanya ayarları güncellendi.")
                    except Exception as exc:
                        st.error(f"Güncelleme hatası: {_error_detail(exc)}")

                if update_col2.button("Bu ayarlarla yeni kampanya oluştur", key=f"dialer_campaign_clone_btn_{selected_campaign_id}", width='stretch'):
                    try:
                        create_payload = {
                            "name": f"{campaign_name}-copy",
                            "dialingMode": campaign_mode,
                        }
                        if selected_contact_id:
                            create_payload["contactList"] = {"id": selected_contact_id}
                        if selected_queue_id:
                            create_payload["queue"] = {"id": selected_queue_id}
                        if script_id:
                            create_payload["script"] = {"id": script_id}
                        if edge_group_id:
                            create_payload["edgeGroup"] = {"id": edge_group_id}
                        if division_id:
                            create_payload["division"] = {"id": division_id}
                        if site_id:
                            create_payload["site"] = {"id": site_id}
                        if call_analysis_set_id:
                            create_payload["callAnalysisResponseSet"] = {"id": call_analysis_set_id}
                        if caller_name:
                            create_payload["callerName"] = caller_name
                        if caller_address:
                            create_payload["callerAddress"] = caller_address
                        created = api.create_outbound_campaign(create_payload)
                        attempt_target_contact_id = str(selected_contact_id or "").strip()
                        if attempt_target_contact_id:
                            api.set_outbound_contact_list_attempt_limit(
                                attempt_target_contact_id,
                                selected_attempt_limit_id,
                            )
                            verified_attempt_limit_id = str(
                                api.get_outbound_contact_list_attempt_limit_id(attempt_target_contact_id) or ""
                            ).strip()
                            expected_attempt_limit_id = str(selected_attempt_limit_id or "").strip()
                            if verified_attempt_limit_id != expected_attempt_limit_id:
                                raise ValueError(
                                    "Attempt Limit atamasi dogrulanamadi. "
                                    f"ContactList={attempt_target_contact_id}, expected={expected_attempt_limit_id or '-'}, "
                                    f"actual={verified_attempt_limit_id or '-'}"
                                )
                        created_id = str((created or {}).get("id") or "")
                        _audit_user_action("dialer_campaign_create", f"Campaign created: {created_id}", "success")
                        st.success(f"Yeni kampanya oluşturuldu. ID: {created_id or '-'}")
                    except Exception as exc:
                        st.error(f"Kampanya oluşturma hatası: {_error_detail(exc)}")

        with st.expander("➕ Yeni Kampanya Oluştur", expanded=False):
            st.caption("Yeni kampanyayı form üzerinden kademeli olarak oluşturun.")

            with st.container(border=True):
                _section_header(
                    "Temel Bilgiler",
                    help_text="Kampanya Adı, Dialing Mode ve Contact List seçimi zorunlu temel alanlardır.",
                )
                create_c1, create_c2, create_c3, create_c3_2 = st.columns(4)
                create_name = create_c1.text_input(
                    "Kampanya Adı",
                    value=f"Yeni Kampanya {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                    key="dialer_create_campaign_name",
                )
                create_mode = create_c2.selectbox("Dialing Mode", dialing_modes, index=0, key="dialer_create_campaign_mode")
                cl_labels_create = list(cl_options_tab0.keys())
                if cl_labels_create:
                    create_contact_label = create_c3.selectbox("Contact List", cl_labels_create, index=0, key="dialer_create_campaign_contact")
                    create_contact_id = cl_options_tab0.get(create_contact_label)
                else:
                    create_contact_id = create_c3.text_input("Contact List ID", value="", key="dialer_create_campaign_contact")

                try:
                    al_options_create = _build_option_map(api.get_outbound_attempt_limits_list(page_size=100, max_pages=10), ["name"])
                except Exception:
                    al_options_create = {}

                if al_options_create:
                    create_al_labels = list(al_options_create.keys())
                    create_al_label = create_c3_2.selectbox("Arama Şablonu (Attempt Limit)", ["(Seçim Yok)"] + create_al_labels, index=0, key="dialer_create_campaign_al")
                    create_attempt_limit_id = al_options_create.get(create_al_label) if create_al_label != "(Seçim Yok)" else None
                else:
                    create_attempt_limit_id = create_c3_2.text_input("Attempt Limit ID", value="", key="dialer_create_campaign_al")
                create_c3_2.caption("Not: Attempt Limit kampanyaya değil Contact List'e atanır.")

            with st.container(border=True):
                _section_header(
                    "Yönlendirme ve Kaynaklar",
                    help_text="Queue, Script, Edge, Division, Site ve Call Analysis seçimleri outbound davranışını belirler. Predictive modda Script/Site/Call Analysis alanlarını mutlaka seçin.",
                )
                create_c4, create_c5, create_c6, create_c7 = st.columns(4)
                queue_labels_create = list(queue_options.keys())
                if queue_labels_create:
                    create_queue_label = create_c4.selectbox("Queue", queue_labels_create, index=0, key="dialer_create_campaign_queue")
                    create_queue_id = queue_options.get(create_queue_label)
                else:
                    create_queue_id = create_c4.text_input("Queue ID", value="", key="dialer_create_campaign_queue")
                if script_options:
                    script_labels_create = list(script_options.keys())
                    create_script_label = create_c5.selectbox("Script", script_labels_create, index=0, key="dialer_create_campaign_script")
                    create_script_id = script_options.get(create_script_label)
                else:
                    create_script_id = create_c5.text_input("Script ID", value="", key="dialer_create_campaign_script")

                if edge_group_options:
                    edge_labels_create = list(edge_group_options.keys())
                    create_edge_label = create_c6.selectbox("Edge Group", edge_labels_create, index=0, key="dialer_create_campaign_edge")
                    create_edge_group_id = edge_group_options.get(create_edge_label)
                else:
                    create_edge_group_id = create_c6.text_input("Edge Group ID", value="", key="dialer_create_campaign_edge")

                if division_options:
                    div_labels_create = list(division_options.keys())
                    create_div_label = create_c7.selectbox("Division", div_labels_create, index=0, key="dialer_create_campaign_division")
                    create_division_id = division_options.get(create_div_label)
                else:
                    create_division_id = create_c7.text_input("Division ID", value="", key="dialer_create_campaign_division")

                create_c8, create_c9 = st.columns(2)
                if site_options:
                    site_labels_create = list(site_options.keys())
                    create_site_label = create_c8.selectbox("Site", site_labels_create, index=0, key="dialer_create_campaign_site")
                    create_site_id = site_options.get(create_site_label)
                else:
                    create_site_id = create_c8.text_input("Site ID", value=current_site, key="dialer_create_campaign_site")

                if call_analysis_set_options:
                    ca_labels_create = list(call_analysis_set_options.keys())
                    create_ca_label = create_c9.selectbox(
                        "Call Analysis Response Set",
                        ca_labels_create,
                        index=0,
                        key="dialer_create_campaign_ca_set",
                    )
                    create_call_analysis_set_id = call_analysis_set_options.get(create_ca_label)
                else:
                    create_call_analysis_set_id = create_c9.text_input(
                        "Call Analysis Response Set ID",
                        value=current_call_analysis_set,
                        key="dialer_create_campaign_ca_set",
                    )

            with st.container(border=True):
                _section_header(
                    "Arayan Bilgisi",
                    help_text="Caller Name ve Caller Address alanları zorunludur; kampanya oluşumunda doğrulanır.",
                )
                cc1, cc2 = st.columns(2)
                create_caller_name = ""
                create_caller_address = ""
                if caller_id_options:
                    caller_labels_create = list(caller_id_options.keys())
                    create_caller_label = cc1.selectbox("Caller ID", caller_labels_create, index=0, key="dialer_create_campaign_caller")
                    create_caller_id = caller_id_options.get(create_caller_label)
                    create_caller = caller_details_by_id.get(create_caller_id, {})
                    create_caller_name = str(create_caller.get("name") or "").strip()
                    create_caller_address = str(create_caller.get("address") or "").strip()
                    cc2.caption(f"Caller Address: {create_caller_address or '-'}")
                else:
                    create_caller_name = cc1.text_input(
                        "Caller Name",
                        value=current_caller_name,
                        key="dialer_create_campaign_caller_name",
                    )
                    create_caller_address = cc2.text_input(
                        "Caller Address",
                        value=current_caller_address,
                        key="dialer_create_campaign_caller_addr",
                    )

            st.divider()
            if st.button("Kampanya Oluştur", key="dialer_create_campaign_form_btn", width='stretch', type="primary"):
                if not create_name.strip():
                    st.error("Kampanya adı zorunludur.")
                elif not create_contact_id:
                    st.error("Contact List seçimi zorunludur.")
                elif not create_caller_name or not create_caller_address:
                    st.error("Caller ID (caller name/address) zorunludur.")
                elif create_mode in {"preview", "predictive", "power", "progressive"} and not create_script_id:
                    st.error("Seçilen dialing mode için Script seçimi zorunludur.")
                elif create_mode in {"predictive", "power", "progressive"} and not create_site_id:
                    st.error("Seçilen dialing mode için Site seçimi zorunludur.")
                elif create_mode in {"predictive", "power", "progressive"} and not create_call_analysis_set_id:
                    st.error("Seçilen dialing mode için Call Analysis Response Set seçimi zorunludur.")
                else:
                    try:
                        cl_detail = api.get_outbound_contact_list(str(create_contact_id).strip())
                        phone_columns = cl_detail.get("phoneColumns") if isinstance(cl_detail, dict) else None
                        if not phone_columns:
                            st.error("Seçilen contact list'te phoneColumns tanımlı değil. Önce Contact List sekmesinden telefon kolonu tanımlayın.")
                        else:
                            campaign_phone_columns = _normalize_campaign_phone_columns(cl_detail)
                            if not campaign_phone_columns:
                                st.error("Contact list phoneColumns bilgisi kampanya için uygun formatta değil.")
                            else:
                                st.caption(
                                    "Kampanyaya giden phoneColumns: "
                                    + ", ".join([str(x.get("columnName") or "") for x in campaign_phone_columns])
                                )
                                create_payload = {
                                    "name": create_name.strip(),
                                    "dialingMode": create_mode,
                                    "contactList": {"id": str(create_contact_id).strip()},
                                    "callerName": create_caller_name.strip(),
                                    "callerAddress": create_caller_address.strip(),
                                    "phoneColumns": campaign_phone_columns,
                                }
                                if create_queue_id:
                                    create_payload["queue"] = {"id": str(create_queue_id).strip()}
                                if create_script_id:
                                    create_payload["script"] = {"id": create_script_id.strip()}
                                if create_edge_group_id:
                                    create_payload["edgeGroup"] = {"id": create_edge_group_id.strip()}
                                if create_division_id:
                                    create_payload["division"] = {"id": create_division_id.strip()}
                                if create_site_id:
                                    create_payload["site"] = {"id": str(create_site_id).strip()}
                                if create_call_analysis_set_id:
                                    create_payload["callAnalysisResponseSet"] = {"id": str(create_call_analysis_set_id).strip()}
                                created = api.create_outbound_campaign(create_payload)
                                if create_contact_id:
                                    api.set_outbound_contact_list_attempt_limit(
                                        str(create_contact_id).strip(),
                                        str(create_attempt_limit_id or "").strip(),
                                    )
                                created_id = str((created or {}).get("id") or "")
                                _audit_user_action("dialer_campaign_create_template", f"Campaign created via form: {created_id}", "success")
                                st.success(f"Kampanya oluşturuldu. ID: {created_id or '-'}")
                    except Exception as exc:
                        st.error(f"Kampanya oluşturma hatası: {_error_detail(exc)}")

        if campaigns_forbidden:
            st.stop()

    with tabs[1]:
        st.subheader("Kampanya Stratejileri (Attempt Limits & CARS)")
        st.caption("Arama limitleri, otomatik yeniden aramalar (Meşgul, Cevapsız) ve Telesekreter (Answering Machine) davranışlarını ayarlayın.")

        try:
            attempt_limits = api.get_outbound_attempt_limits_list(page_size=100, max_pages=10)
            call_analysis_sets = api.get_outbound_call_analysis_response_sets(page_size=100, max_pages=10)
        except Exception as exc:
            st.error(f"Strateji verileri alınamadı: {exc}")
            attempt_limits = []
            call_analysis_sets = []

        al_options = {}
        for al in attempt_limits:
            if isinstance(al, dict) and al.get("id"):
                label = str(al.get('name') or al.get('id'))
                while label in al_options:
                    label += " "
                al_options[label] = str(al.get("id"))
                
        cars_options = {}
        for c in call_analysis_sets:
            if isinstance(c, dict) and c.get("id"):
                label = str(c.get('name') or c.get('id'))
                while label in cars_options:
                    label += " "
                cars_options[label] = str(c.get("id"))

        strat_tabs = st.tabs(["Arama Limitleri (Attempt Limits)", "Çağrı Analiz Testleri (CARS)"])

        with strat_tabs[0]:
            st.caption("Bir müşterinin gün içinde/toplamda en fazla kaç kez aranacağını ve meşgul/cevapsız durumlarında ne kadar bekleneceğini belirler.")
            al_rows = []
            for al in attempt_limits:
                if not isinstance(al, dict):
                    continue
                recall = al.get("recallEntries") or {}
                busy_min = recall.get("busy", {}).get("minutesBetweenAttempts", "-")
                no_ans_min = recall.get("noAnswer", {}).get("minutesBetweenAttempts", "-")
                ans_mach_min = recall.get("answeringMachine", {}).get("minutesBetweenAttempts", "-")
                al_rows.append({
                    "Strateji Adı": al.get("name"),
                    "Maks. Arama": al.get("maxAttemptsPerContact"),
                    "Meşgul Bekleme (dk)": busy_min,
                    "Cevapsız Bekleme (dk)": no_ans_min,
                    "Sekreter Bekleme (dk)": ans_mach_min,
                    "ID": al.get("id"),
                })
            if al_rows:
                st.dataframe(pd.DataFrame(al_rows), width='stretch', hide_index=True)
            else:
                st.info("Arama limiti şablonu bulunmamaktadır.")

            with st.expander("🛠️ Yeni Arama Limiti (Attempt Limit) Oluştur veya Güncelle", expanded=False):
                action_al = st.radio("İşlem", ["Yeni Şablon", "Mevcut Şablonu Güncelle"], horizontal=True, key="al_action")
                sel_al_id = None
                default_name = f"Strateji {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                default_max = 5
                default_busy = 15
                default_noans = 60
                default_sec = 120

                if action_al == "Mevcut Şablonu Güncelle":
                    sel_al_label = st.selectbox("Şablon Seç", [""] + list(al_options.keys()), key="al_select")
                    if sel_al_label:
                        sel_al_id = al_options[sel_al_label]
                        al_detail = next((al for al in attempt_limits if al.get("id") == sel_al_id), {})
                        default_name = str(al_detail.get("name") or "")
                        default_max = int(al_detail.get("maxAttemptsPerContact") or 5)
                        rec = al_detail.get("recallEntries") or {}
                        default_busy = int(rec.get("busy", {}).get("minutesBetweenAttempts") or 15)
                        default_noans = int(rec.get("noAnswer", {}).get("minutesBetweenAttempts") or 60)
                        default_sec = int(rec.get("answeringMachine", {}).get("minutesBetweenAttempts") or 120)

                al_name = st.text_input("Şablon Adı", value=default_name, key="al_name_input")
                al_max = st.number_input("Maksimum Arama Sayısı (Kişi Başına)", min_value=1, max_value=50, value=default_max, key="al_max_input")

                st.markdown("**Duruma Göre Yeniden Arama Süreleri (Dakika)**")
                cl_busy, cl_noans, cl_sec = st.columns(3)
                al_busy = cl_busy.number_input("Meşgul (Busy) Bekleme", min_value=1, max_value=1440, value=default_busy, key="al_busy_input")
                al_noans = cl_noans.number_input("Cevapsız (No Answer) Bekleme", min_value=1, max_value=1440, value=default_noans, key="al_noans_input")
                al_sec = cl_sec.number_input("Sekreter (Answering Machine) Bekleme", min_value=1, max_value=1440, value=default_sec, key="al_sec_input")

                if st.button("Kaydet", key="al_save_btn", type="primary", width='stretch'):
                    if not al_name.strip():
                        st.error("Şablon adı giriniz.")
                    else:
                        payload = {
                            "name": al_name.strip(),
                            "maxAttemptsPerContact": al_max,
                            "maxAttemptsPerNumber": al_max,
                            "timeZoneId": "Europe/Istanbul",
                            "resetPeriod": "NEVER",
                            "recallEntries": {
                                "busy": {"minutesBetweenAttempts": al_busy},
                                "noAnswer": {"minutesBetweenAttempts": al_noans},
                                "answeringMachine": {"minutesBetweenAttempts": al_sec}
                            }
                        }
                        try:
                            if action_al == "Mevcut Şablonu Güncelle" and sel_al_id:
                                # fetch explicit version before updating
                                detail = api.get_outbound_attempt_limit(sel_al_id)
                                payload["id"] = sel_al_id
                                payload["version"] = detail.get("version") or 1
                                api.update_outbound_attempt_limit(sel_al_id, payload)
                                st.success("Arama Limiti güncellendi!")
                            else:
                                api.create_outbound_attempt_limit(payload)
                                st.success("Yeni Arama Limiti oluşturuldu!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Hata: {_error_detail(e)}")

        with strat_tabs[1]:
            st.caption("Karşı tarafı kimin açtığını analiz eder. Örn: Telesekreter (Answering Machine) açarsa aramayı kapat vs.")
            cars_rows = []
            for c in call_analysis_sets:
                if not isinstance(c, dict):
                    continue
                resps = c.get("responses") or {}
                am_action = resps.get("callableAnsweringMachine", {}).get("reactionType", "-")
                cars_rows.append({
                    "Analiz Seti Adı": c.get("name"),
                    "Telesekreter Aksiyonu": am_action,
                    "ID": c.get("id"),
                })
            if cars_rows:
                st.dataframe(pd.DataFrame(cars_rows), width='stretch', hide_index=True)
            else:
                st.info("CARS şablonu bulunmamaktadır.")

            st.info("CARS oluşturma ve detaylı düzenlemeler (Transfer to ACD, Drop vs.) için şu an Genesys Cloud arayüzünü kullanmanız önerilir. Görüntüleme bu panelden sağlanmaktadır.")

    with tabs[2]:
        st.caption("Liste yapısını düzenleyin, telefon kolonlarını netleştirin ve Excel yüklemelerini daha kontrollü yönetin.")

        contact_lists = []
        contact_forbidden = False
        try:
            contact_lists = api.get_outbound_contact_lists(page_size=100, max_pages=20)
        except Exception as exc:
            contact_forbidden = _is_forbidden_error(exc)
            st.error(f"Contact list verisi alınamadı: {exc}")
            if contact_forbidden:
                _render_forbidden_hint("Outbound Contact List")

        attempt_limit_names = {}
        try:
            for al in (api.get_outbound_attempt_limits_list(page_size=100, max_pages=10) or []):
                if not isinstance(al, dict):
                    continue
                al_id = str(al.get("id") or "").strip()
                al_name = str(al.get("name") or "").strip()
                if al_id and al_name:
                    attempt_limit_names[al_id] = al_name
        except Exception:
            attempt_limit_names = {}

        cl_rows = [_contact_list_row(c, attempt_limit_names=attempt_limit_names) for c in contact_lists if isinstance(c, dict)]
        total_contact_lists = len(cl_rows)
        phone_ready_lists = 0
        total_columns = 0
        for item in contact_lists:
            if not isinstance(item, dict):
                continue
            total_columns += len(list(item.get("columnNames") or []))
            if _extract_phone_columns(item):
                phone_ready_lists += 1

        cl_metric_cols = st.columns(3)
        cl_metric_cols[0].metric("Toplam Liste", total_contact_lists)
        cl_metric_cols[1].metric("Telefon Kolonu Hazır", phone_ready_lists)
        cl_metric_cols[2].metric("Ortalama Kolon", round(total_columns / total_contact_lists, 1) if total_contact_lists else 0)

        if cl_rows:
            with st.expander("📂 Contact List Envanteri", expanded=True):
                cl_df = pd.DataFrame(cl_rows)
                if "id" in cl_df.columns:
                    cl_df = cl_df.drop(columns=["id"])
                if "ID" in cl_df.columns:
                    cl_df = cl_df.drop(columns=["ID"])
                st.dataframe(cl_df, width='stretch', hide_index=True)
        else:
            st.info("Contact list bulunamadı.")

        cl_by_id = {
            str(item.get("id")): item
            for item in contact_lists
            if isinstance(item, dict) and item.get("id")
        }
        cl_options = {}
        for item in contact_lists:
            if isinstance(item, dict) and item.get("id"):
                label = str(item.get('name') or item.get('id'))
                while label in cl_options:
                    label += " "
                cl_options[label] = str(item.get("id"))

        with st.expander("🗂️ Contact List Tasarımı", expanded=False):
            st.caption("JSON kullanmadan yeni liste oluşturun veya mevcut listede telefon kolonunu güncelleyin.")

            create_cl_col1, create_cl_col2, create_cl_col3 = st.columns([2, 3, 2])
            create_cl_name = create_cl_col1.text_input(
                "Yeni Contact List Adı",
                value=f"Outbound Contacts {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                key="dialer_cl_form_name",
            )
            create_cl_columns_text = create_cl_col2.text_input(
                "Kolonlar (virgülle)",
                value="phone,firstName,lastName",
                key="dialer_cl_form_columns",
            )
            create_cl_columns = _parse_column_input(create_cl_columns_text)
            if create_cl_columns:
                phone_default_idx = 0
                for i, col in enumerate(create_cl_columns):
                    if "phone" in col.lower() or "tel" in col.lower() or "gsm" in col.lower():
                        phone_default_idx = i
                        break
                create_phone_col = create_cl_col3.selectbox(
                    "Telefon Kolonu",
                    create_cl_columns,
                    index=phone_default_idx,
                    key="dialer_cl_form_phone_col",
                )
            else:
                create_phone_col = ""

            if st.button("Yeni Contact List Oluştur", key="dialer_cl_form_create_btn", width='stretch'):
                if not create_cl_name.strip():
                    st.error("Contact list adı zorunludur.")
                elif not create_cl_columns:
                    st.error("En az bir kolon tanımlayın.")
                elif not create_phone_col:
                    st.error("Telefon kolonu seçimi zorunludur.")
                else:
                    try:
                        payload = {
                            "name": create_cl_name.strip(),
                            "columnNames": create_cl_columns,
                            "phoneColumns": [{"columnName": create_phone_col, "type": "cell"}],
                        }
                        created = api.create_outbound_contact_list(payload)
                        created_id = str((created or {}).get("id") or "")
                        _audit_user_action("dialer_contactlist_create", f"Contact list created: {created_id}", "success")
                        st.success(f"Contact list oluşturuldu. ID: {created_id or '-'}")
                    except Exception as exc:
                        st.error(f"Contact list oluşturma hatası: {_error_detail(exc)}")

            st.divider()
            upd_label = st.selectbox(
                "Güncellenecek Contact List",
                [""] + list(cl_options.keys()),
                key="dialer_cl_form_update_select",
            )

            if upd_label:
                upd_id = cl_options.get(upd_label)
                detail = cl_by_id.get(upd_id) or {}
                try:
                    detail = api.get_outbound_contact_list(upd_id)
                except Exception:
                    pass

                upd_name = st.text_input(
                    "Contact List Adı",
                    value=str((detail or {}).get("name") or ""),
                    key="dialer_cl_form_update_name",
                )

                upd_columns = list((detail or {}).get("columnNames") or [])
                if not upd_columns:
                    upd_columns = ["phone", "firstName", "lastName"]
                upd_phone_columns = _extract_phone_columns(detail if isinstance(detail, dict) else {})
                upd_phone_default = 0
                if upd_phone_columns:
                    for i, col in enumerate(upd_columns):
                        if col == upd_phone_columns[0]:
                            upd_phone_default = i
                            break

                upd_phone_col = st.selectbox(
                    "Telefon Kolonu",
                    upd_columns,
                    index=upd_phone_default,
                    key="dialer_cl_form_update_phone_col",
                )

                if upd_phone_columns:
                    st.caption(f"Mevcut phoneColumns: {', '.join(upd_phone_columns)}")
                else:
                    st.warning("Bu contact list'te phoneColumns tanımlı değil. Kampanya oluşturmak için tanımlamanız gerekir.")

                if st.button("Contact List'i Güncelle", key="dialer_cl_form_update_btn", width='stretch'):
                    try:
                        detail_version = (detail or {}).get("version") if isinstance(detail, dict) else None
                        if detail_version is None:
                            st.error("Contact list sürüm bilgisi alınamadı (version). Lütfen listeyi yenileyip tekrar deneyin.")
                        else:
                            payload = {
                                "id": upd_id,
                                "name": upd_name.strip() or str((detail or {}).get("name") or ""),
                                "columnNames": upd_columns,
                                "phoneColumns": [{"columnName": upd_phone_col, "type": "cell"}],
                                "version": detail_version,
                            }
                            api.update_outbound_contact_list(upd_id, payload)
                            _audit_user_action("dialer_contactlist_update", f"Contact list updated: {upd_id}", "success")
                            st.success("Contact list güncellendi. Telefon kolonu tanımlandı.")
                    except Exception as exc:
                        st.error(f"Contact list güncelleme hatası: {_error_detail(exc)}")

        with st.expander("📤 Excel / CSV Yükleme", expanded=False):
            st.caption("Kolon eşleme sihirbazıyla dosyayı mevcut bir listeye ekleyin veya yükleme sırasında yeni liste oluşturun.")
            upload = st.file_uploader("Dosya yükle (.xlsx, .xls, .csv)", type=["xlsx", "xls", "csv"], key="dialer_excel_upload")
        if upload is not None:
            try:
                if str(upload.name).lower().endswith(".csv"):
                    df = pd.read_csv(upload, dtype=str)
                else:
                    excel_data = pd.ExcelFile(upload)
                    sheet = st.selectbox("Sheet seç", excel_data.sheet_names, key="dialer_excel_sheet")
                    df = pd.read_excel(excel_data, sheet_name=sheet, dtype=str)
                df.columns = [str(c).strip() for c in df.columns]
            except Exception as exc:
                st.error(f"Dosya okunamadı: {exc}")
                df = pd.DataFrame()

            if not df.empty:
                st.caption(f"Satır: {len(df)} | Sütun: {len(df.columns)}")
                st.dataframe(df.head(100), width='stretch', hide_index=True)

                target_mode = st.radio("Hedef", ["Mevcut Contact List", "Yeni Contact List"], horizontal=True)
                target_contact_list_id = None
                target_contact_columns = []
                target_phone_columns = []
                if target_mode == "Mevcut Contact List":
                    selected_cl = st.selectbox("Contact List", [""] + list(cl_options.keys()), key="dialer_excel_target_cl")
                    if selected_cl:
                        target_contact_list_id = cl_options[selected_cl]
                        source_item = cl_by_id.get(target_contact_list_id) or {}
                        target_contact_columns = list(source_item.get("columnNames") or [])
                        if not target_contact_columns:
                            try:
                                detail = api.get_outbound_contact_list(target_contact_list_id)
                                target_contact_columns = list((detail or {}).get("columnNames") or [])
                                target_phone_columns = _extract_phone_columns(detail if isinstance(detail, dict) else {})
                            except Exception:
                                target_contact_columns = []
                                target_phone_columns = []
                        else:
                            try:
                                detail = api.get_outbound_contact_list(target_contact_list_id)
                                target_phone_columns = _extract_phone_columns(detail if isinstance(detail, dict) else {})
                            except Exception:
                                target_phone_columns = []
                else:
                    new_contact_name = st.text_input(
                        "Yeni Contact List Adı",
                        value=f"Excel Import {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                        key="dialer_excel_new_contact_name",
                    )

                st.markdown("#### Kolon Eşleme Sihirbazı")
                source_columns = [str(c) for c in df.columns if str(c).strip()]
                mapping_def = [
                    ("phone", "Telefon", ["phone", "telefon", "gsm", "mobile", "phone_number", "tel"]),
                    ("firstName", "Ad", ["firstname", "first_name", "ad", "isim", "name"]),
                    ("lastName", "Soyad", ["lastname", "last_name", "soyad", "surname"]),
                    ("email", "E-posta", ["email", "mail", "e-posta"]),
                    ("externalId", "External ID", ["externalid", "external_id", "id", "customerid", "musterino"]),
                ]

                none_label = "-- Kullanma --"
                field_map = {}
                map_cols_left, map_cols_right = st.columns(2)
                for idx, (target_field, label, aliases) in enumerate(mapping_def):
                    widget_col = map_cols_left if idx % 2 == 0 else map_cols_right
                    auto_source = _auto_map_source(source_columns, aliases)
                    options = [none_label] + source_columns
                    default_index = options.index(auto_source) if auto_source in options else 0
                    selected = widget_col.selectbox(
                        f"{label} alanı",
                        options,
                        index=default_index,
                        key=f"dialer_map_{target_field}",
                    )
                    field_map[target_field] = "" if selected == none_label else selected

                used_sources = {src for src in field_map.values() if src}
                custom_candidates = [col for col in source_columns if col not in used_sources]
                custom_columns = st.multiselect(
                    "Custom alanlar (eklenecek)",
                    custom_candidates,
                    default=[c for c in custom_candidates if c.lower().startswith("custom")][:5],
                    key="dialer_map_custom_columns",
                )

                if not field_map.get("phone"):
                    st.warning("Telefon alanı eşlenmeden upload başlatılamaz.")

                max_rows = st.number_input("Yüklenecek maksimum satır", min_value=1, max_value=200000, value=50000, step=1000)
                chunk_size = st.number_input("Batch boyutu", min_value=10, max_value=5000, value=500, step=10)

                mapped_records_preview, skipped_preview = _build_mapped_records(
                    df,
                    field_map=field_map,
                    custom_columns=custom_columns,
                    max_rows=min(int(max_rows), 500),
                )

                if target_contact_columns:
                    adapted_preview = []
                    for item in mapped_records_preview:
                        adapted = _adapt_record_to_target_columns(item, target_contact_columns, target_phone_columns)
                        if _has_any_phone_value(adapted, target_phone_columns):
                            adapted_preview.append(adapted)
                    mapped_records_preview = adapted_preview

                st.caption(
                    f"Önizleme: {len(mapped_records_preview)} kayıt hazır, telefon eksikliği nedeniyle atlanan: {skipped_preview}"
                )
                if mapped_records_preview:
                    st.dataframe(pd.DataFrame(mapped_records_preview).head(50), width='stretch', hide_index=True)

                mapped_column_names = [k for k, v in field_map.items() if v]
                for col in custom_columns:
                    if col not in mapped_column_names:
                        mapped_column_names.append(col)

                if target_mode == "Yeni Contact List":
                    st.caption("Yeni contact list, eşlenen kolon adlarıyla oluşturulacak.")
                    if st.button("Eşlemeyle Yeni Contact List Oluştur", key="dialer_create_target_cl_btn"):
                        if not mapped_column_names or "phone" not in mapped_column_names:
                            st.error("Yeni contact list için en az telefon alanı eşlenmelidir.")
                        else:
                            try:
                                create_payload = {
                                    "name": new_contact_name,
                                    "columnNames": mapped_column_names,
                                }
                                created = api.create_outbound_contact_list(create_payload)
                                target_contact_list_id = str((created or {}).get("id") or "").strip()
                                if target_contact_list_id:
                                    st.session_state["dialer_created_target_contact_list_id"] = target_contact_list_id
                                    st.success(f"Contact list oluşturuldu: {target_contact_list_id}")
                            except Exception as exc:
                                st.error(f"Contact list oluşturulamadı: {_error_detail(exc)}")
                    target_contact_list_id = target_contact_list_id or st.session_state.get("dialer_created_target_contact_list_id")

                if target_contact_columns:
                    missing_cols = [c for c in mapped_column_names if c not in target_contact_columns]
                    if missing_cols:
                        st.warning(
                            "Seçilen contact list içinde bulunmayan eşlenmiş kolonlar var: "
                            + ", ".join(missing_cols)
                        )

                if st.button("Eşlenmiş Veriyi Contact List'e Yükle", key="dialer_upload_excel_btn", width='stretch'):
                    if not target_contact_list_id:
                        st.error("Önce hedef contact list seçin/oluşturun.")
                    elif not field_map.get("phone"):
                        st.error("Telefon alanı eşlenmeden upload yapılamaz.")
                    else:
                        records, skipped_missing_phone = _build_mapped_records(
                            df,
                            field_map=field_map,
                            custom_columns=custom_columns,
                            max_rows=int(max_rows),
                        )

                        if target_contact_columns:
                            adapted_records = []
                            skipped_after_adapt = 0
                            for item in records:
                                adapted = _adapt_record_to_target_columns(item, target_contact_columns, target_phone_columns)
                                if not adapted or not _has_any_phone_value(adapted, target_phone_columns):
                                    skipped_after_adapt += 1
                                    continue
                                adapted_records.append(adapted)
                            records = adapted_records
                            skipped_missing_phone += skipped_after_adapt

                        if not records:
                            st.error("Yüklenecek geçerli kayıt bulunamadı.")
                        else:
                            ok_count = 0
                            fail_count = 0
                            progress = st.progress(0.0)
                            total_chunks = max(1, (len(records) + int(chunk_size) - 1) // int(chunk_size))
                            last_error = None
                            for idx, chunk in enumerate(_chunk_list(records, int(chunk_size)), start=1):
                                try:
                                    api.add_contacts_to_outbound_contact_list(target_contact_list_id, chunk)
                                    ok_count += len(chunk)
                                except Exception as exc:
                                    last_error = str(exc)
                                    fail_count += len(chunk)
                                progress.progress(min(1.0, idx / total_chunks))

                            _audit_user_action(
                                "dialer_excel_upload",
                                (
                                    f"Excel upload completed contactList={target_contact_list_id} "
                                    f"ok={ok_count} fail={fail_count} skipped_no_phone={skipped_missing_phone}"
                                ),
                                "success" if fail_count == 0 else "warning",
                                metadata={
                                    "contact_list_id": target_contact_list_id,
                                    "ok_count": ok_count,
                                    "fail_count": fail_count,
                                    "skipped_missing_phone": skipped_missing_phone,
                                },
                            )
                            st.success(
                                (
                                    f"Yükleme tamamlandı. Başarılı: {ok_count}, Hatalı: {fail_count}, "
                                    f"Telefonu boş olduğu için atlanan: {skipped_missing_phone}"
                                )
                            )
                            if last_error and fail_count > 0:
                                st.caption(f"Son hata özeti: {last_error}")

        if contact_forbidden:
            st.stop()

    with tabs[3]:
        st.caption("Seçili kampanya wrap-up/sonuç analizi, kampanya bazlı kod yönetimi ve manuel senkron araçları.")
        action_c1, action_c2, action_c3 = st.columns(3)
        refresh_tracking = action_c1.button("Takip Verisini Yenile", key="dialer_tracking_force_refresh_btn", width="stretch")
        refresh_wrapup_catalog = action_c2.button("Wrap-up Kodlarını Yenile", key="dialer_wrapup_catalog_refresh_btn", width="stretch")
        clear_conv_cache = action_c3.button("Konuşma Cache Temizle", key="dialer_conv_cache_clear_btn", width="stretch")
        if clear_conv_cache:
            st.session_state.pop("dialer_conv_call_cache", None)
            st.success("Konuşma cache temizlendi.")

        try:
            utc_offset_local = float(globals().get("utc_offset_hours", 3.0) or 3.0)
        except Exception:
            utc_offset_local = 3.0

        users_map_rev = _build_users_reverse_map()
        wrapup_map_key = "dialer_wrapup_id_to_name_cache"
        wrapup_list_key = "dialer_wrapup_codes_listing_cache"
        wrapup_resolve_cache_key = "dialer_wrapup_id_resolution_cache"

        if refresh_wrapup_catalog or wrapup_map_key not in st.session_state:
            try:
                st.session_state[wrapup_map_key] = api.get_wrapup_codes() or {}
            except Exception:
                st.session_state[wrapup_map_key] = {}
        if refresh_wrapup_catalog or wrapup_list_key not in st.session_state:
            try:
                st.session_state[wrapup_list_key] = api.get_wrapup_codes_listing(page_size=100, max_pages=50) or []
            except Exception:
                st.session_state[wrapup_list_key] = []
        wrapup_id_to_name = st.session_state.get(wrapup_map_key) or {}
        wrapup_codes_listing = st.session_state.get(wrapup_list_key) or []
        wrapup_id_resolution_cache = st.session_state.get(wrapup_resolve_cache_key)
        if not isinstance(wrapup_id_resolution_cache, dict):
            wrapup_id_resolution_cache = {}
            st.session_state[wrapup_resolve_cache_key] = wrapup_id_resolution_cache
        conv_call_cache = st.session_state.get("dialer_conv_call_cache")
        if not isinstance(conv_call_cache, dict):
            conv_call_cache = {}
            st.session_state["dialer_conv_call_cache"] = conv_call_cache

        campaign_options: Dict[str, Dict[str, Any]] = {}
        for campaign in campaigns:
            if not isinstance(campaign, dict) or not campaign.get("id"):
                continue
            label = f"{str(campaign.get('name') or campaign.get('id'))} [{str(campaign.get('campaignStatus') or campaign.get('status') or '-')}]"
            while label in campaign_options:
                label += " "
            campaign_options[label] = campaign

        if not campaign_options:
            st.info("Takip ekranı için kampanya bulunamadı.")
        else:
            with st.expander("🔍 Seçili Kampanya Sonuç Analizi", expanded=True):
                selected_tracking_label = st.selectbox(
                    "Sonuçlarını görmek istediğiniz kampanya",
                    list(campaign_options.keys()),
                    key="dialer_tracking_campaign_select",
                )
                tracking_campaign = campaign_options.get(selected_tracking_label) or {}
                tracking_campaign_id = str(tracking_campaign.get("id") or "").strip()

                tracking_detail = {}
                try:
                    tracking_detail = api.get_outbound_campaign(tracking_campaign_id) or {}
                except Exception:
                    tracking_detail = tracking_campaign if isinstance(tracking_campaign, dict) else {}

                tracking_queue_id = str(
                    (tracking_detail.get("queue") or {}).get("id")
                    or tracking_detail.get("queueId")
                    or (tracking_campaign.get("queue") or {}).get("id")
                    or tracking_campaign.get("queueId")
                    or ""
                ).strip()
                tracking_contact_list_id = str(
                    (tracking_detail.get("contactList") or {}).get("id")
                    or tracking_detail.get("contactListId")
                    or (tracking_campaign.get("contactList") or {}).get("id")
                    or tracking_campaign.get("contactListId")
                    or ""
                ).strip()
                tracking_caller_address = _normalize_phone_value(
                    tracking_detail.get("callerAddress") or tracking_campaign.get("callerAddress") or ""
                )

                t1, t2 = st.columns([1, 1])
                lookback_hours = int(
                    t1.number_input(
                        "Periyot (saat)",
                        min_value=1,
                        max_value=168,
                        value=24,
                        step=1,
                        key=f"dialer_tracking_lookback_h_{tracking_campaign_id}",
                    )
                )
                fetch_dashboard = t2.button(
                    "Dashboard Yenile",
                    key=f"dialer_tracking_refresh_btn_{tracking_campaign_id}",
                    width="stretch",
                )

                if not tracking_queue_id:
                    st.warning("Seçilen kampanyada queue bilgisi yok. Analiz için queue tanımlı olmalı.")
                else:
                    cache_key = f"dialer_campaign_dashboard_{tracking_campaign_id}_{lookback_hours}"
                    if refresh_tracking or fetch_dashboard or cache_key not in st.session_state:
                        end_utc = datetime.now(timezone.utc)
                        start_utc = end_utc - timedelta(hours=lookback_hours)
                        try:
                            queue_conversations = api.get_outbound_conversations_by_queue(
                                tracking_queue_id,
                                start_utc,
                                end_utc,
                                page_size=100,
                                max_pages=20,
                            )
                        except Exception as exc:
                            st.error(f"Konuşma verisi alınamadı: {_error_detail(exc)}")
                            queue_conversations = []

                        interactions = _normalize_interaction_rows(
                            api=api,
                            conversations=queue_conversations,
                            campaign_id=tracking_campaign_id,
                            contact_list_id=tracking_contact_list_id,
                            utc_offset_hours=utc_offset_local,
                            users_map_rev=users_map_rev,
                            wrapup_id_to_name=wrapup_id_to_name,
                            wrapup_id_resolution_cache=wrapup_id_resolution_cache,
                            caller_address=tracking_caller_address,
                            conv_call_cache=conv_call_cache,
                        )
                        st.session_state[cache_key] = {
                            "interactions": interactions,
                            "start_utc": start_utc,
                            "end_utc": end_utc,
                            "queue_id": tracking_queue_id,
                            "campaign_id": tracking_campaign_id,
                            "contact_list_id": tracking_contact_list_id,
                        }

                    payload = st.session_state.get(cache_key) or {}
                    st.session_state["dialer_last_tracking_payload"] = payload
                    df_int = pd.DataFrame(payload.get("interactions") or [])
                    start_utc = payload.get("start_utc")
                    end_utc = payload.get("end_utc")

                    total_calls = len(df_int.index)
                    busy_calls = int((df_int["Sonuç Sınıfı"] == "Meşgul").sum()) if not df_int.empty else 0
                    unreachable_calls = int((df_int["Sonuç Sınıfı"] == "Ulaşılamadı").sum()) if not df_int.empty else 0
                    completed_calls = int((df_int["Sonuç Sınıfı"] == "Tamamlanan").sum()) if not df_int.empty else 0
                    retry_calls = int((df_int["Sonuç Sınıfı"] == "Tekrar Aranacak").sum()) if not df_int.empty else 0

                    left, right = st.columns([3, 2])
                    with left:
                        _section_header("Campaign Overview")
                        o1, o2, o3, o4, o5 = st.columns(5)
                        o1.metric("Toplam", total_calls)
                        o2.metric("Meşgul", busy_calls)
                        o3.metric("Ulaşılamadı", unreachable_calls)
                        o4.metric("Tamamlanan", completed_calls)
                        o5.metric("Tekrar Aranacak", retry_calls)
                        if start_utc and end_utc:
                            st.caption(
                                f"Dönem: {_format_local_timestamp(start_utc.isoformat(), utc_offset_local)} - "
                                f"{_format_local_timestamp(end_utc.isoformat(), utc_offset_local)} | "
                                f"Queue: {tracking_queue_id} | ContactList: {tracking_contact_list_id or '-'}"
                            )

                    with right:
                        _section_header("Wrap-up Details")
                        if df_int.empty:
                            st.info("Wrap-up dağılımı için veri bulunamadı.")
                        else:
                            wrapup_dist = (
                                df_int["Wrap-up Kodu"]
                                .astype(str)
                                .str.strip()
                                .replace("", "(Boş)")
                                .value_counts()
                                .reset_index()
                            )
                            wrapup_dist.columns = ["Wrap-up", "Adet"]
                            st.bar_chart(wrapup_dist.set_index("Wrap-up")["Adet"], width="stretch")

                    st.divider()
                    _section_header("Interactions")
                    if df_int.empty:
                        st.info("Seçilen kampanya için etkileşim bulunamadı.")
                    else:
                        order_cols = [
                            "Agent",
                            "Telefon",
                            "Çağrı Tipi",
                            "Başlangıç",
                            "Bitiş",
                            "Durasyon (sn)",
                            "Sonuç Sınıfı",
                            "Wrap-up Kodu",
                            "Konuşma ID",
                        ]
                        display_df = df_int.reindex(columns=order_cols)
                        st.dataframe(display_df, width="stretch", hide_index=True)
                        st.download_button(
                            label="⬇️ Interaction Verisini İndir (CSV)",
                            data=display_df.to_csv(index=False).encode("utf-8-sig"),
                            file_name=f"campaign_{tracking_campaign_id}_interactions.csv",
                            mime="text/csv",
                            key=f"dialer_tracking_download_btn_{tracking_campaign_id}",
                        )

            with st.expander("🧩 Kampanya Sonuç Kodu Yönetimi", expanded=False):
                mgmt_label = st.selectbox(
                    "Kampanya",
                    list(campaign_options.keys()),
                    key="dialer_wrapup_mgmt_campaign_select",
                )
                mgmt_campaign = campaign_options.get(mgmt_label) or {}
                mgmt_campaign_id = str(mgmt_campaign.get("id") or "").strip()
                mgmt_detail = {}
                try:
                    mgmt_detail = api.get_outbound_campaign(mgmt_campaign_id) or {}
                except Exception:
                    mgmt_detail = mgmt_campaign if isinstance(mgmt_campaign, dict) else {}

                mgmt_queue_id = str(
                    (mgmt_detail.get("queue") or {}).get("id")
                    or mgmt_detail.get("queueId")
                    or (mgmt_campaign.get("queue") or {}).get("id")
                    or mgmt_campaign.get("queueId")
                    or ""
                ).strip()
                if not mgmt_queue_id:
                    st.warning("Kampanya için queue bilgisi bulunamadı. Sonuç kodu yönetimi queue seviyesinde çalışır.")
                else:
                    same_queue_campaigns = []
                    for item in campaigns:
                        if not isinstance(item, dict):
                            continue
                        item_queue_id = str((item.get("queue") or {}).get("id") or item.get("queueId") or "").strip()
                        if item_queue_id == mgmt_queue_id:
                            same_queue_campaigns.append(str(item.get("name") or item.get("id") or ""))
                    if len(same_queue_campaigns) > 1:
                        st.warning(
                            "Bu queue birden fazla kampanya tarafından kullanılıyor. "
                            "Yapacağınız wrap-up kod değişikliği aynı queue'yu kullanan kampanyaları da etkiler."
                        )
                        st.caption("Etkilenen kampanyalar: " + ", ".join(same_queue_campaigns))

                    queue_codes_cache_key = f"dialer_queue_wrapup_codes_{mgmt_queue_id}"
                    queue_refresh_clicked = st.button(
                        "Queue Sonuç Kodlarını Getir",
                        key=f"dialer_wrapup_queue_refresh_{mgmt_queue_id}",
                        width="stretch",
                    )
                    if refresh_tracking or refresh_wrapup_catalog or queue_refresh_clicked or queue_codes_cache_key not in st.session_state:
                        try:
                            st.session_state[queue_codes_cache_key] = api.get_queue_wrapup_codes(
                                mgmt_queue_id,
                                page_size=100,
                                max_pages=20,
                            ) or []
                        except Exception as exc:
                            st.error(f"Queue wrap-up kodları alınamadı: {_error_detail(exc)}")
                            st.session_state[queue_codes_cache_key] = []
                    queue_codes = st.session_state.get(queue_codes_cache_key) or []

                    st.metric("Queue'ya Atalı Sonuç Kodu", len(queue_codes))
                    if queue_codes:
                        queue_df = pd.DataFrame(
                            [
                                {
                                    "Kod": str(item.get("name") or item.get("id") or "").strip(),
                                    "Açıklama": str(item.get("description") or "").strip(),
                                }
                                for item in queue_codes
                                if isinstance(item, dict)
                            ]
                        )
                        st.dataframe(queue_df, width="stretch", hide_index=True)
                    else:
                        st.info("Queue için wrap-up kodu bulunamadı.")

                    assigned_ids = {
                        str(item.get("id") or "").strip()
                        for item in queue_codes
                        if isinstance(item, dict) and str(item.get("id") or "").strip()
                    }

                    add_options: Dict[str, str] = {}
                    for item in wrapup_codes_listing:
                        if not isinstance(item, dict):
                            continue
                        code_id = str(item.get("id") or "").strip()
                        code_name = str(item.get("name") or code_id).strip()
                        if not code_id or code_id in assigned_ids:
                            continue
                        label = code_name
                        if item.get("description"):
                            label = f"{label} - {str(item.get('description')).strip()}"
                        label = f"{label} [{code_id}]"
                        while label in add_options:
                            label += " "
                        add_options[label] = code_id

                    mgmt_add_col1, mgmt_add_col2 = st.columns([4, 1])
                    selected_to_add = mgmt_add_col1.multiselect(
                        "Queue'ya Eklenecek Hazır Sonuç Kodları",
                        list(add_options.keys()),
                        key=f"dialer_wrapup_queue_add_select_{mgmt_queue_id}",
                    )
                    if mgmt_add_col2.button(
                        "Ekle",
                        key=f"dialer_wrapup_queue_add_btn_{mgmt_queue_id}",
                        width="stretch",
                        disabled=not selected_to_add,
                    ):
                        selected_ids = [add_options[label] for label in selected_to_add if label in add_options]
                        try:
                            api.add_queue_wrapup_codes(mgmt_queue_id, selected_ids)
                            st.success(f"{len(selected_ids)} sonuç kodu queue'ya eklendi.")
                            st.session_state.pop(queue_codes_cache_key, None)
                            st.session_state.pop(wrapup_map_key, None)
                            st.session_state.pop(wrapup_list_key, None)
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Queue sonuç kodu ekleme hatası: {_error_detail(exc)}")

                    remove_options: Dict[str, str] = {}
                    for item in queue_codes:
                        if not isinstance(item, dict):
                            continue
                        code_id = str(item.get("id") or "").strip()
                        if not code_id:
                            continue
                        label = f"{str(item.get('name') or code_id).strip()} [{code_id}]"
                        while label in remove_options:
                            label += " "
                        remove_options[label] = code_id

                    mgmt_rm_col1, mgmt_rm_col2 = st.columns([4, 1])
                    remove_label = mgmt_rm_col1.selectbox(
                        "Queue'dan Çıkarılacak Sonuç Kodu",
                        ["(Seçim Yok)"] + list(remove_options.keys()),
                        key=f"dialer_wrapup_queue_remove_select_{mgmt_queue_id}",
                    )
                    remove_id = remove_options.get(remove_label)
                    if mgmt_rm_col2.button(
                        "Çıkar",
                        key=f"dialer_wrapup_queue_remove_btn_{mgmt_queue_id}",
                        width="stretch",
                        disabled=not remove_id,
                    ):
                        try:
                            api.remove_queue_wrapup_code(mgmt_queue_id, remove_id)
                            st.success("Sonuç kodu queue'dan çıkarıldı.")
                            st.session_state.pop(queue_codes_cache_key, None)
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Queue sonuç kodu çıkarma hatası: {_error_detail(exc)}")

                    st.divider()
                    st.caption("Yeni sonuç kodu tenant seviyesinde oluşturulur. İsterseniz aynı anda seçili queue'ya da atanır.")
                    new_c1, new_c2 = st.columns([2, 1])
                    new_name = new_c1.text_input(
                        "Yeni Sonuç Kodu Adı",
                        key=f"dialer_wrapup_create_name_{mgmt_queue_id}",
                    ).strip()
                    new_desc = new_c1.text_input(
                        "Açıklama",
                        key=f"dialer_wrapup_create_desc_{mgmt_queue_id}",
                    ).strip()
                    assign_after_create = new_c2.checkbox(
                        "Queue'ya da ata",
                        value=True,
                        key=f"dialer_wrapup_create_assign_{mgmt_queue_id}",
                    )
                    if new_c2.button(
                        "Yeni Kod Oluştur",
                        key=f"dialer_wrapup_create_btn_{mgmt_queue_id}",
                        width="stretch",
                        disabled=not new_name,
                    ):
                        try:
                            created = api.create_wrapup_code(new_name, description=new_desc)
                            created_id = str((created or {}).get("id") or "").strip()
                            if assign_after_create and created_id:
                                api.add_queue_wrapup_codes(mgmt_queue_id, [created_id])
                            st.success("Yeni sonuç kodu oluşturuldu.")
                            st.session_state.pop(queue_codes_cache_key, None)
                            st.session_state.pop(wrapup_map_key, None)
                            st.session_state.pop(wrapup_list_key, None)
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Sonuç kodu oluşturma hatası: {_error_detail(exc)}")

            with st.expander("🎯 Agent Wrap-up Kodları", expanded=False):
                wup_sel_label = st.selectbox(
                    "Kampanya",
                    list(campaign_options.keys()),
                    key="dialer_wupq_campaign_select",
                )
                wup_campaign = campaign_options.get(wup_sel_label) or {}
                wup_campaign_id = str(wup_campaign.get("id") or "").strip()
                wup_lookback_h = int(
                    st.number_input(
                        "Geriye dön (saat)",
                        min_value=1,
                        max_value=168,
                        value=24,
                        step=1,
                        key="dialer_wupq_lookback_h",
                    )
                )
                wup_fetch = st.button("Getir", key="dialer_wupq_fetch_btn", width="stretch")

                wup_detail = {}
                try:
                    wup_detail = api.get_outbound_campaign(wup_campaign_id) or {}
                except Exception:
                    wup_detail = wup_campaign if isinstance(wup_campaign, dict) else {}
                wup_queue_id = str(
                    (wup_detail.get("queue") or {}).get("id")
                    or wup_detail.get("queueId")
                    or (wup_campaign.get("queue") or {}).get("id")
                    or wup_campaign.get("queueId")
                    or ""
                ).strip()
                wup_contact_list_id = str(
                    (wup_detail.get("contactList") or {}).get("id")
                    or wup_detail.get("contactListId")
                    or (wup_campaign.get("contactList") or {}).get("id")
                    or wup_campaign.get("contactListId")
                    or ""
                ).strip()
                wup_caller_address = _normalize_phone_value(
                    wup_detail.get("callerAddress") or wup_campaign.get("callerAddress") or ""
                )
                if not wup_queue_id:
                    st.warning("Seçilen kampanya için queue bulunamadı.")
                else:
                    wup_cache_key = f"dialer_wupq_cache_{wup_campaign_id}_{wup_lookback_h}"
                    if refresh_tracking or wup_fetch or wup_cache_key not in st.session_state:
                        with st.spinner("Konuşmalar getiriliyor..."):
                            try:
                                end_utc = datetime.now(timezone.utc)
                                start_utc = end_utc - timedelta(hours=wup_lookback_h)
                                wup_conversations = api.get_outbound_conversations_by_queue(
                                    wup_queue_id,
                                    start_utc,
                                    end_utc,
                                    page_size=100,
                                    max_pages=20,
                                )
                                wup_rows = _normalize_interaction_rows(
                                    api=api,
                                    conversations=wup_conversations,
                                    campaign_id=wup_campaign_id,
                                    contact_list_id=wup_contact_list_id,
                                    utc_offset_hours=utc_offset_local,
                                    users_map_rev=users_map_rev,
                                    wrapup_id_to_name=wrapup_id_to_name,
                                    wrapup_id_resolution_cache=wrapup_id_resolution_cache,
                                    caller_address=wup_caller_address,
                                    conv_call_cache=conv_call_cache,
                                )
                                st.session_state[wup_cache_key] = wup_rows
                            except Exception as exc:
                                st.error(f"Konuşmalar alınamadı: {_error_detail(exc)}")
                                st.session_state[wup_cache_key] = []

                    wup_rows = st.session_state.get(wup_cache_key) or []
                    wup_df = pd.DataFrame(wup_rows)
                    w1, w2, w3 = st.columns(3)
                    w1.metric("Toplam Konuşma", len(wup_df))
                    has_wup = int(wup_df["Wrap-up Kodu"].astype(str).str.strip().str.len().gt(0).sum()) if not wup_df.empty else 0
                    w2.metric("Wrap-up Girilmiş", has_wup)
                    w3.metric("Wrap-up Girilmemiş", len(wup_df) - has_wup)
                    if wup_df.empty:
                        st.info("Seçilen dönemde bu kampanya için wrap-up verisi bulunamadı.")
                    else:
                        wup_order_cols = [
                            "Başlangıç",
                            "Bitiş",
                            "Telefon",
                            "Çağrı Tipi",
                            "Agent",
                            "Wrap-up Kodu",
                            "Konuşma ID",
                        ]
                        wup_display_df = wup_df.reindex(columns=wup_order_cols)
                        st.dataframe(wup_display_df, width="stretch", hide_index=True)
                        st.download_button(
                            label="⬇️ Wrap-up Listesini İndir (CSV)",
                            data=wup_display_df.to_csv(index=False).encode("utf-8-sig"),
                            file_name=f"wrapup_{wup_campaign_id}_{wup_lookback_h}h.csv",
                            mime="text/csv",
                            key="dialer_wupq_download_btn",
                        )

            with st.expander("🔄 Sonuç Kodu Otomatik Senkron", expanded=False):
                st.caption("Bu bölüm manuel çalışır. Otomatik sayfa yenileme kapalıdır.")
                auto_col1, auto_col2, auto_col3 = st.columns([1, 2, 1])
                auto_lookback_min = int(
                    auto_col1.number_input(
                        "Geriye dönük pencere (dk)",
                        min_value=5,
                        max_value=180,
                        value=int(st.session_state.get("dialer_auto_result_lookback", 30)),
                        step=5,
                        key="dialer_auto_result_lookback",
                    )
                )
                auto_result_column = auto_col2.text_input(
                    "Sonuç kodu data kolonu",
                    value=str(st.session_state.get("dialer_auto_result_column", "resultCode")),
                    key="dialer_auto_result_column",
                ).strip() or "resultCode"
                run_sync = auto_col3.button("Senkronu Çalıştır", key="dialer_auto_result_manual_run_btn", width="stretch")

                if run_sync:
                    with st.spinner("Sonuç kodları manuel senkronize ediliyor..."):
                        stats = _auto_sync_result_codes(
                            api,
                            campaigns=campaigns,
                            result_column=auto_result_column,
                            lookback_minutes=auto_lookback_min,
                        )
                    st.session_state["dialer_auto_result_last_stats"] = stats
                    st.session_state["dialer_auto_result_last_run"] = datetime.now(timezone.utc).isoformat()

                auto_stats = st.session_state.get("dialer_auto_result_last_stats") or {}
                if auto_stats:
                    stat1, stat2, stat3, stat4 = st.columns(4)
                    stat1.metric("Taranan", int(auto_stats.get("scanned", 0)))
                    stat2.metric("İşlenen", int(auto_stats.get("updated", 0)))
                    stat3.metric("Eşleşmeyen", int(auto_stats.get("not_found", 0)))
                    stat4.metric("Hata", int(auto_stats.get("errors", 0)))
                else:
                    st.info("Henüz manuel senkron çalıştırılmadı.")

                last_run_iso = str(st.session_state.get("dialer_auto_result_last_run") or "").strip()
                if last_run_iso:
                    st.caption(f"Son manuel senkron: {_format_local_timestamp(last_run_iso, utc_offset_local)}")

            with st.expander("Ham Sonuç JSON", expanded=False):
                st.code(_safe_json_dumps(st.session_state.get("dialer_last_tracking_payload") or {}), language="json")
