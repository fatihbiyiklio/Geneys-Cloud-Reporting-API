import json
from datetime import datetime
from typing import Any, Callable, Dict, List

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


def _contact_list_row(item):
    column_names = item.get("columnNames")
    if isinstance(column_names, list):
        columns = len(column_names)
    else:
        columns = 0
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "columns": columns,
        "attemptLimit": item.get("attemptLimit"),
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


def render_dialer_service(context: Dict[str, Any]) -> None:
    """Render Dialer page for outbound campaign management and tracking."""
    bind_context(globals(), context)
    st.title(f"📞 {get_text(lang, 'menu_dialer')}")
    st.caption("Genesys Cloud Outbound kampanya, contact list ve kampanya takip ekranı")

    api_client = st.session_state.get("api_client")
    if not api_client:
        st.warning(get_text(lang, "genesys_not_connected"))
        return

    api = GenesysAPI(api_client)

    tabs = st.tabs([
        "Kampanya Yönetimi",
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
        if campaign_rows:
            st.dataframe(pd.DataFrame(campaign_rows), use_container_width=True, hide_index=True)
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
            campaign_options[f"{name} [{status}] ({cid})"] = cid

        if campaign_options:
            selected_label = st.selectbox("Kampanya Seç", list(campaign_options.keys()), key="dialer_campaign_select")
            selected_campaign_id = campaign_options[selected_label]

            c1, c2, c3 = st.columns([1, 1, 2])
            if c1.button("Kampanyayı Başlat", use_container_width=True):
                try:
                    api.start_outbound_campaign(selected_campaign_id)
                    _audit_user_action("dialer_campaign_start", f"Campaign started: {selected_campaign_id}", "success")
                    st.success("Kampanya başlatıldı.")
                except Exception as exc:
                    st.error(f"Başlatma hatası: {exc}")
            if c2.button("Kampanyayı Durdur", use_container_width=True):
                try:
                    api.stop_outbound_campaign(selected_campaign_id)
                    _audit_user_action("dialer_campaign_stop", f"Campaign stopped: {selected_campaign_id}", "success")
                    st.success("Kampanya durduruldu.")
                except Exception as exc:
                    st.error(f"Durdurma hatası: {exc}")
            if c3.button("Kampanya Detayını Yenile", use_container_width=True):
                st.session_state.pop(f"dialer_campaign_json_{selected_campaign_id}", None)

            try:
                campaign_detail = api.get_outbound_campaign(selected_campaign_id)
            except Exception:
                campaign_detail = next((c for c in campaigns if c.get("id") == selected_campaign_id), {})

            json_key = f"dialer_campaign_json_{selected_campaign_id}"
            if json_key not in st.session_state:
                st.session_state[json_key] = _safe_json_dumps(campaign_detail)
            edited_json = st.text_area(
                "Kampanya JSON (tüm ayarlar)",
                key=json_key,
                height=340,
                help="Genesys outbound campaign payload alanlarını buradan düzenleyebilirsiniz.",
            )
            col_u1, col_u2 = st.columns([1, 1])
            if col_u1.button("Kampanya Ayarlarını Kaydet", use_container_width=True):
                payload, err = _safe_json_loads(edited_json)
                if err:
                    st.error(f"JSON parse hatası: {err}")
                elif not isinstance(payload, dict):
                    st.error("Payload bir JSON object olmalıdır.")
                else:
                    payload["id"] = selected_campaign_id
                    try:
                        api.update_outbound_campaign(selected_campaign_id, payload)
                        _audit_user_action("dialer_campaign_update", f"Campaign updated: {selected_campaign_id}", "success")
                        st.success("Kampanya ayarları güncellendi.")
                    except Exception as exc:
                        st.error(f"Güncelleme hatası: {exc}")

            if col_u2.button("Bu JSON ile Yeni Kampanya Oluştur", use_container_width=True):
                payload, err = _safe_json_loads(edited_json)
                if err:
                    st.error(f"JSON parse hatası: {err}")
                elif not isinstance(payload, dict):
                    st.error("Payload bir JSON object olmalıdır.")
                else:
                    payload.pop("id", None)
                    try:
                        created = api.create_outbound_campaign(payload)
                        created_id = str((created or {}).get("id") or "")
                        _audit_user_action("dialer_campaign_create", f"Campaign created: {created_id}", "success")
                        st.success(f"Yeni kampanya oluşturuldu. ID: {created_id or '-'}")
                    except Exception as exc:
                        st.error(f"Kampanya oluşturma hatası: {exc}")

        with st.expander("Yeni kampanya oluştur (JSON template)", expanded=False):
            default_template = {
                "name": f"Yeni Kampanya {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                "dialingMode": "preview",
            }
            create_key = "dialer_campaign_create_json"
            if create_key not in st.session_state:
                st.session_state[create_key] = _safe_json_dumps(default_template)
            create_json = st.text_area("Yeni Kampanya JSON", key=create_key, height=220)
            if st.button("Template ile Kampanya Oluştur", key="dialer_create_campaign_template_btn"):
                payload, err = _safe_json_loads(create_json)
                if err:
                    st.error(f"JSON parse hatası: {err}")
                elif not isinstance(payload, dict):
                    st.error("Payload bir JSON object olmalıdır.")
                else:
                    try:
                        created = api.create_outbound_campaign(payload)
                        created_id = str((created or {}).get("id") or "")
                        _audit_user_action("dialer_campaign_create_template", f"Campaign created via template: {created_id}", "success")
                        st.success(f"Kampanya oluşturuldu. ID: {created_id or '-'}")
                    except Exception as exc:
                        st.error(f"Kampanya oluşturma hatası: {exc}")

        if campaigns_forbidden:
            st.stop()

    with tabs[1]:
        contact_lists = []
        contact_forbidden = False
        try:
            contact_lists = api.get_outbound_contact_lists(page_size=100, max_pages=20)
        except Exception as exc:
            contact_forbidden = _is_forbidden_error(exc)
            st.error(f"Contact list verisi alınamadı: {exc}")
            if contact_forbidden:
                _render_forbidden_hint("Outbound Contact List")

        cl_rows = [_contact_list_row(c) for c in contact_lists if isinstance(c, dict)]
        if cl_rows:
            st.dataframe(pd.DataFrame(cl_rows), use_container_width=True, hide_index=True)
        else:
            st.info("Contact list bulunamadı.")

        cl_by_id = {
            str(item.get("id")): item
            for item in contact_lists
            if isinstance(item, dict) and item.get("id")
        }
        cl_options = {
            f"{item.get('name') or item.get('id')} ({item.get('id')})": str(item.get("id"))
            for item in contact_lists
            if isinstance(item, dict) and item.get("id")
        }

        with st.expander("Contact List Oluştur / Güncelle (JSON)", expanded=False):
            default_cl_template = {
                "name": f"Outbound Contacts {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                "columnNames": ["phone", "firstName", "lastName"],
            }
            cl_json_key = "dialer_contactlist_json"
            if cl_json_key not in st.session_state:
                st.session_state[cl_json_key] = _safe_json_dumps(default_cl_template)
            cl_json = st.text_area("Contact List JSON", key=cl_json_key, height=220)
            ccl1, ccl2 = st.columns([1, 1])
            if ccl1.button("Yeni Contact List Oluştur", use_container_width=True):
                payload, err = _safe_json_loads(cl_json)
                if err:
                    st.error(f"JSON parse hatası: {err}")
                elif not isinstance(payload, dict):
                    st.error("Payload bir JSON object olmalıdır.")
                else:
                    try:
                        created = api.create_outbound_contact_list(payload)
                        created_id = str((created or {}).get("id") or "")
                        _audit_user_action("dialer_contactlist_create", f"Contact list created: {created_id}", "success")
                        st.success(f"Contact list oluşturuldu. ID: {created_id or '-'}")
                    except Exception as exc:
                        st.error(f"Contact list oluşturma hatası: {exc}")

            selected_cl_update = ccl2.selectbox(
                "Güncellenecek Contact List",
                [""] + list(cl_options.keys()),
                key="dialer_cl_update_select",
            )
            if ccl2.button("Seçili Contact List'i Güncelle", use_container_width=True):
                if not selected_cl_update:
                    st.error("Güncellenecek contact list seçin.")
                else:
                    payload, err = _safe_json_loads(cl_json)
                    if err:
                        st.error(f"JSON parse hatası: {err}")
                    elif not isinstance(payload, dict):
                        st.error("Payload bir JSON object olmalıdır.")
                    else:
                        target_id = cl_options[selected_cl_update]
                        payload["id"] = target_id
                        try:
                            api.update_outbound_contact_list(target_id, payload)
                            _audit_user_action("dialer_contactlist_update", f"Contact list updated: {target_id}", "success")
                            st.success("Contact list güncellendi.")
                        except Exception as exc:
                            st.error(f"Contact list güncelleme hatası: {exc}")

        st.markdown("### Excel/CSV ile Contact Yükleme")
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
                st.dataframe(df.head(100), use_container_width=True, hide_index=True)

                target_mode = st.radio("Hedef", ["Mevcut Contact List", "Yeni Contact List"], horizontal=True)
                target_contact_list_id = None
                target_contact_columns = []
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
                            except Exception:
                                target_contact_columns = []
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
                st.caption(
                    f"Önizleme: {len(mapped_records_preview)} kayıt hazır, telefon eksikliği nedeniyle atlanan: {skipped_preview}"
                )
                if mapped_records_preview:
                    st.dataframe(pd.DataFrame(mapped_records_preview).head(50), use_container_width=True, hide_index=True)

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
                                st.error(f"Contact list oluşturulamadı: {exc}")
                    target_contact_list_id = target_contact_list_id or st.session_state.get("dialer_created_target_contact_list_id")

                if target_contact_columns:
                    missing_cols = [c for c in mapped_column_names if c not in target_contact_columns]
                    if missing_cols:
                        st.warning(
                            "Seçilen contact list içinde bulunmayan eşlenmiş kolonlar var: "
                            + ", ".join(missing_cols)
                        )

                if st.button("Eşlenmiş Veriyi Contact List'e Yükle", key="dialer_upload_excel_btn", use_container_width=True):
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

    with tabs[2]:
        st.markdown("### Outbound Kampanya Takip")
        refresh = st.button("Takip Verisini Yenile", key="dialer_progress_refresh_btn")
        progress_cache_key = "dialer_progress_cache"

        if refresh or progress_cache_key not in st.session_state:
            try:
                st.session_state[progress_cache_key] = api.get_outbound_campaign_progress()
            except Exception as exc:
                st.error(f"Kampanya progress verisi alınamadı: {exc}")
                if _is_forbidden_error(exc):
                    _render_forbidden_hint("Campaign Progress")
                st.session_state[progress_cache_key] = {}

        progress_payload = st.session_state.get(progress_cache_key) or {}
        progress_entities = _as_list_entities(progress_payload)
        if not progress_entities and isinstance(progress_payload, dict) and progress_payload:
            progress_entities = [progress_payload]

        if not progress_entities:
            st.info("Kampanya takip verisi bulunamadı. Campaign Progress endpoint erişimini kontrol edin.")
        else:
            rows: List[Dict[str, Any]] = []
            for item in progress_entities:
                if not isinstance(item, dict):
                    continue
                camp = item.get("campaign") if isinstance(item.get("campaign"), dict) else {}
                row = {
                    "campaignId": item.get("campaignId") or camp.get("id") or "",
                    "campaignName": item.get("campaignName") or camp.get("name") or "",
                    "status": item.get("campaignStatus") or item.get("status") or camp.get("campaignStatus") or "",
                    "attempted": _summary_value(item, ["attempted", "attempts", "attemptedCount", "contactAttempts"], 0),
                    "connected": _summary_value(item, ["connected", "connectedCount", "liveAnswers"], 0),
                    "abandoned": _summary_value(item, ["abandoned", "abandonedCount"], 0),
                    "completed": _summary_value(item, ["completed", "completedCount", "done"], 0),
                    "contactable": _summary_value(item, ["contactable", "contactableCount"], 0),
                    "total": _summary_value(item, ["total", "totalCount", "totalRecords"], 0),
                }
                rows.append(row)

            df_progress = pd.DataFrame(rows)
            st.dataframe(df_progress, use_container_width=True, hide_index=True)

            c1, c2, c3 = st.columns(3)
            c1.metric("Toplam Kampanya", int(df_progress["campaignId"].astype(str).str.len().gt(0).sum()))
            c2.metric("Toplam Attempt", int(pd.to_numeric(df_progress["attempted"], errors="coerce").fillna(0).sum()))
            c3.metric("Toplam Connected", int(pd.to_numeric(df_progress["connected"], errors="coerce").fillna(0).sum()))

            with st.expander("Ham Progress JSON", expanded=False):
                st.code(_safe_json_dumps(progress_payload), language="json")
