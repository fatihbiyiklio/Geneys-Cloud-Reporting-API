from typing import Any, Dict

from src.app.context import bind_context


def render_reports_service(context: Dict[str, Any]) -> None:
    """Render reports page using injected app context."""
    bind_context(globals(), context)
    st.title(get_text(lang, "menu_reports"))
    # --- SAVED VIEWS (Compact) ---
    with st.expander(f"📂 {get_text(lang, 'saved_views')}", expanded=False):
        presets = load_presets(org)
        # Single row layout for better alignment
        # Using vertical_alignment="bottom" (Streamlit 1.35+) to align button with inputs
        c_p1, c_p2, c_p3, c_p4 = st.columns([3, 2, 1, 1], gap="small", vertical_alignment="bottom")
        
        with c_p1:
            sel_p = st.selectbox(get_text(lang, "select_view"), [get_text(lang, "no_view_selected")] + [p['name'] for p in presets], key="preset_selector")
        
        def_p = {}
        if sel_p != get_text(lang, "no_view_selected"):
            p = next((p for p in presets if p['name'] == sel_p), None)
            if p:
                def_p = p
                for k in ["type", "names", "metrics", "granularity_label", "fill_gaps"]:
                    if k in p: st.session_state[f"rep_{k[:3]}"] = p[k]
                # Restore saved table layout (column order/visibility/sort) for this preset.
                try:
                    preset_type = str(p.get("type") or st.session_state.get("rep_typ") or "report_agent")
                    # New format: keep per-report table states bundle.
                    table_states = p.get("table_view_states")
                    if isinstance(table_states, dict):
                        for state_key, state_val in table_states.items():
                            if not (isinstance(state_key, str) and state_key.startswith("_report_table_view_")):
                                continue
                            if isinstance(state_val, dict):
                                st.session_state[state_key] = json.loads(
                                    json.dumps(state_val, ensure_ascii=False)
                                )
                    # Backward compatibility: single state payload.
                    table_state = p.get("table_view_state")
                    if isinstance(table_state, dict):
                        st.session_state[f"_report_table_view_{_safe_state_token(preset_type)}"] = json.loads(
                            json.dumps(table_state, ensure_ascii=False)
                        )
                except Exception:
                    pass
        
        with c_p2:
            p_name_save = st.text_input(get_text(lang, "preset_name"), placeholder=get_text(lang, "preset_name_placeholder"))
            
        with c_p3:
            if st.button(f"💾 {get_text(lang, 'save')}", key="btn_save_view", width='stretch') and p_name_save:
                preset_type = st.session_state.get("rep_typ", "report_agent")
                table_state_key = f"_report_table_view_{_safe_state_token(preset_type)}"
                raw_table_state = st.session_state.get(table_state_key)
                safe_table_state = None
                if isinstance(raw_table_state, dict):
                    try:
                        safe_table_state = json.loads(json.dumps(raw_table_state, ensure_ascii=False))
                    except Exception:
                        safe_table_state = None
                # Persist all known report table states so every saved view keeps layout/sort choices.
                safe_table_states = {}
                for state_key, state_val in st.session_state.items():
                    if not (isinstance(state_key, str) and state_key.startswith("_report_table_view_")):
                        continue
                    if not isinstance(state_val, dict):
                        continue
                    try:
                        safe_table_states[state_key] = json.loads(json.dumps(state_val, ensure_ascii=False))
                    except Exception:
                        continue
                new_p = {
                    "name": p_name_save,
                    "type": preset_type,
                    "names": st.session_state.get("rep_nam", []),
                    "metrics": st.session_state.get("rep_met", DEFAULT_METRICS),
                    "granularity_label": st.session_state.get("rep_gra", "Toplam"),
                    "fill_gaps": st.session_state.get("rep_fil", False),
                    "table_view_state": safe_table_state,
                    "table_view_states": safe_table_states,
                }
                presets = [p for p in presets if p['name'] != p_name_save] + [new_p]
                save_presets(org, presets); st.success(get_text(lang, "view_saved")); st.rerun()

        with c_p4:
            can_delete = sel_p != get_text(lang, "no_view_selected")
            if st.button(f"🗑️ {get_text(lang, 'delete_view')}", key="btn_delete_view", width='stretch', disabled=not can_delete) and can_delete:
                presets = [p for p in presets if p['name'] != sel_p]
                save_presets(org, presets); st.success(get_text(lang, "view_deleted")); st.rerun()

    st.divider()

    # --- PRIMARY FILTERS (Always Visible) ---
    c1, c2 = st.columns(2)
    with c1:
        role = st.session_state.app_user['role']
        if role == "Reports User":
            rep_types = ["report_agent", "report_queue", "report_detailed", "report_agent_skill_detail", "report_agent_dnis_skill_detail", "interaction_search", "missed_interactions"]
        else: # Admin, Manager
            rep_types = ["report_agent", "report_queue", "report_detailed", "report_agent_skill_detail", "report_agent_dnis_skill_detail", "interaction_search", "chat_detail", "missed_interactions"]
        
        r_type = st.selectbox(
            get_text(lang, "report_type"), 
            rep_types, 
            format_func=lambda x: get_text(lang, x),
            key="rep_typ", index=rep_types.index(def_p.get("type", "report_agent")) if def_p.get("type", "report_agent") in rep_types else 0)
    with c2:
        is_agent = r_type == "report_agent"
        if is_agent and (not st.session_state.get("users_map")):
            recover_org_maps_if_needed(org, force=True)
            if (not st.session_state.get("users_map")) and st.session_state.get("users_info"):
                fallback_users_map = {}
                for uid, uinfo in (st.session_state.get("users_info") or {}).items():
                    base_name = str((uinfo or {}).get("name") or (uinfo or {}).get("username") or uid or "").strip() or str(uid)
                    candidate = base_name
                    suffix = 2
                    while candidate in fallback_users_map and fallback_users_map.get(candidate) != uid:
                        candidate = f"{base_name} ({suffix})"
                        suffix += 1
                    fallback_users_map[candidate] = uid
                if fallback_users_map:
                    st.session_state.users_map = fallback_users_map
            if (not st.session_state.get("users_map")) and st.session_state.get("api_client"):
                try:
                    api = GenesysAPI(st.session_state.api_client)
                    users = api.get_users()
                    if isinstance(users, list) and users:
                        st.session_state.users_map = {
                            str(u.get("name") or u.get("username") or u.get("id")): u.get("id")
                            for u in users if isinstance(u, dict) and u.get("id")
                        }
                except Exception:
                    pass
        if (not is_agent) and (not st.session_state.get("queues_map")):
            recover_org_maps_if_needed(org, force=True)
            if (not st.session_state.get("queues_map")) and st.session_state.get("api_client"):
                try:
                    api = GenesysAPI(st.session_state.api_client)
                    queues = api.get_queues()
                    if isinstance(queues, list) and queues:
                        st.session_state.queues_map = {
                            str(q.get("name") or q.get("id")): q.get("id")
                            for q in queues if isinstance(q, dict) and q.get("id")
                        }
                except Exception:
                    pass
        opts = list(st.session_state.users_map.keys()) if is_agent else list(st.session_state.queues_map.keys())
        if "rep_nam" in st.session_state:
            st.session_state.rep_nam = [n for n in st.session_state.rep_nam if n in opts]
        sel_names = st.multiselect(get_text(lang, "select_agents" if is_agent else "select_workgroups"), opts, key="rep_nam")
        sel_ids = [(st.session_state.users_map if is_agent else st.session_state.queues_map)[n] for n in sel_names]

    # Date & Time Selection (One Row)
    c_d1, c_d2, c_d3, c_d4 = st.columns(4)
    sd = c_d1.date_input("Start Date", datetime.today())
    st_ = c_d2.time_input(get_text(lang, "start_time"), time(0, 0))
    ed = c_d3.date_input("End Date", datetime.today())
    et = c_d4.time_input(get_text(lang, "end_time"), time(23, 59))
    
    # --- ADVANCED FILTERS (Collapsible) ---
    with st.expander(f"⚙️ {get_text(lang, 'advanced_filters')}", expanded=False):
        g1, g2 = st.columns(2)
        gran_opt = {get_text(lang, "total"): "P1D", get_text(lang, "30min"): "PT30M", get_text(lang, "1hour"): "PT1H"}
        sel_gran = g1.selectbox(get_text(lang, "granularity"), list(gran_opt.keys()), key="rep_gra")
        do_fill = g2.checkbox(get_text(lang, "fill_gaps"), key="rep_fil")
        
        # Media Type Filter
        MEDIA_TYPE_OPTIONS = ["voice", "chat", "email", "callback", "message"]
        sel_media_types = st.multiselect(
            get_text(lang, "media_type"),
            MEDIA_TYPE_OPTIONS,
            default=[],
            format_func=lambda x: x.capitalize(),
            key="rep_media",
            help=get_text(lang, "media_type_help")
        )

        if r_type in ["interaction_search", "chat_detail", "missed_interactions"]:
            st.session_state.rep_max_records = st.number_input(
                "Maksimum kayıt (performans için)",
                min_value=100,
                max_value=20000,
                value=int(st.session_state.get("rep_max_records", 5000)),
                step=100,
                help="Yüksek aralıklar bellek kullanımını artırır. Varsayılan 5000 kayıt ile sınırlandırılır."
            )
        if r_type == "chat_detail":
            st.session_state.rep_enrich_limit = st.number_input(
                "Zenginleştirilecek chat sayısı (attributes)",
                min_value=50,
                max_value=5000,
                value=int(st.session_state.get("rep_enrich_limit", 500)),
                step=50,
                help="Her chat için ek API çağrısı yapılır. Limit yükseldikçe bellek ve süre artar."
            )
        st.session_state.rep_auto_row_limit = st.number_input(
            "Maksimum satır (gösterim/indirme, 0=limitsiz)",
            min_value=0,
            max_value=500000,
            value=int(st.session_state.get("rep_auto_row_limit", 50000)),
            step=5000,
            help="Büyük raporlarda bellek/disk baskısını azaltmak için çıktı satırını sınırlar."
        )

        # Metrics Selection
        user_metrics = st.session_state.app_user.get('metrics', [])
        selection_options = user_metrics if user_metrics and role != "Admin" else ALL_METRICS
        
        if r_type not in ["interaction_search", "chat_detail", "missed_interactions"]:
            if "last_metrics" in st.session_state and st.session_state.last_metrics:
                auto_def_metrics = [m for m in st.session_state.last_metrics if m in selection_options]
            else:
                auto_def_metrics = [m for m in ["nOffered", "nAnswered", "tAnswered", "tTalk", "tHandle"] if m in selection_options]
            sel_mets = st.multiselect(get_text(lang, "metrics"), selection_options, default=auto_def_metrics, format_func=lambda x: get_text(lang, x), key="rep_met")
        else:
            sel_mets = []

        st.radio(
            "Sure Gosterimi",
            ["HH:MM:SS", "Saniye"],
            horizontal=True,
            key="rep_duration_mode",
            help="Raporlardaki sure kolonlarini saat:dakika:saniye veya toplam saniye olarak gosterir.",
        )

    def _apply_selected_duration_view(df_in):
        df_out = df_in.copy()
        if st.session_state.get("rep_duration_mode", "HH:MM:SS") == "HH:MM:SS":
            return apply_duration_formatting(df_out)
        # Seconds mode: keep duration columns numeric and clean integer-like display
        target_cols = [
            c for c in df_out.columns
            if (c.startswith('t') or c.startswith('Avg') or c in ['col_staffed_time', 'Duration', 'col_duration'])
            and pd.api.types.is_numeric_dtype(df_out[c])
        ]
        for col in target_cols:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce").fillna(0).round(0).astype("int64")
        return df_out

    report_rendered_this_run = False

    if r_type == "report_agent_skill_detail":
        st.info(get_text(lang, "skill_report_info"))
    if r_type == "report_agent_dnis_skill_detail":
        st.info(get_text(lang, "dnis_skill_report_info"))

    if r_type == "chat_detail":
            st.info(get_text(lang, "chat_detail_info"))
            if st.button(get_text(lang, "fetch_chat_data"), type="primary", width='stretch'):
             with st.spinner(get_text(lang, "fetching_data")):
                 start_date = datetime.combine(sd, st_) - timedelta(hours=utc_offset_hours)
                 end_date = datetime.combine(ed, et) - timedelta(hours=utc_offset_hours)
                 
                 api = GenesysAPI(st.session_state.api_client)
                 max_records = int(st.session_state.get("rep_max_records", 5000))
                 dfs = []
                 total_rows = 0
                 u_offset = utc_offset_hours
                 skill_lookup = st.session_state.get("skills_map", {}) or api.get_routing_skills()
                 st.session_state.skills_map = skill_lookup
                 language_lookup = api.get_languages()
                 if language_lookup:
                     st.session_state.languages_map = language_lookup
                 else:
                     language_lookup = st.session_state.get("languages_map", {})

                 for page in _iter_conversation_pages(api, start_date, end_date, max_records=max_records, chunk_days=3):
                     df_chunk = process_conversation_details(
                         {"conversations": page},
                         st.session_state.users_info,
                         st.session_state.queues_map,
                         st.session_state.wrapup_map,
                         include_attributes=True,
                         utc_offset=u_offset,
                         skill_map=skill_lookup,
                         language_map=language_lookup
                     )
                     if not df_chunk.empty:
                         dfs.append(df_chunk)
                         total_rows += len(df_chunk)
                 df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                 if max_records and total_rows >= max_records:
                     st.warning(f"Maksimum kayıt limiti ({max_records}) uygulandı. Daha geniş aralıklar için limiti artırabilirsiniz.")
                 
                 if not df.empty:
                     # Filter for Chat/Message types FIRST to reduce API calls
                     chat_types = ['chat', 'message', 'webchat', 'whatsapp', 'facebook', 'twitter', 'line', 'telegram']
                     df_chat = df[df['MediaType'].isin(chat_types)].copy()

                     if not df_chat.empty:
                         st.info(get_text(lang, "fetching_details_info").format(len(df_chat)))
                         
                         # Create a progress bar
                         progress_bar = st.progress(0)
                         enrich_limit = int(st.session_state.get("rep_enrich_limit", 500))
                         if enrich_limit and len(df_chat) > enrich_limit:
                             st.warning(f"Zenginleştirme limiti uygulandı: ilk {enrich_limit} kayıt.")
                             df_chat = df_chat.head(enrich_limit).copy()
                         total_chats = len(df_chat)
                         
                         # Prepare a list to collect updated attributes
                         enrichment_data = []

                         # Use the helper to fetch
                         api_instance = GenesysAPI(st.session_state.api_client)

                         for index, (idx, row) in enumerate(df_chat.iterrows()):
                             conv_id = row['Id']
                             # Update progress
                             progress_bar.progress((index + 1) / total_chats)
                             
                             try:
                                 # ENRICHMENT: Call Standard Conversation API
                                 # This is necessary because Analytics API often omits 'attributes' (Participant Data)
                                 full_conv = api_instance._get(f"/api/v2/conversations/{conv_id}")
                                 
                                 attrs = {}
                                 if full_conv and 'participants' in full_conv:
                                     # Look for customer participant who usually holds the attributes
                                     # Prioritize customer, then check others
                                     found_attrs = False
                                     for p in full_conv['participants']:
                                         if p.get('purpose') == 'customer' and 'attributes' in p and p['attributes']:
                                             attrs = p['attributes']
                                             found_attrs = True
                                             break
                                     
                                     if not found_attrs:
                                          for p in full_conv['participants']:
                                              if 'attributes' in p and p['attributes']:
                                                  attrs = p['attributes']
                                                  break
                                 
                                 # Append dict to list
                                 enrichment_data.append(attrs)
                                 
                             except Exception as e:
                                 # If individual fetch fails
                                 enrichment_data.append({})
                         
                         progress_bar.empty()
                         
                         # Merge attributes into the DataFrame
                         # usage of 'at' for direct assignment
                         for i, attrs in enumerate(enrichment_data):
                             original_index = df_chat.index[i]
                             for k, v in attrs.items():
                                 # Determine content to set. If it's a date or number, pandas might complain if column is object type,
                                 # but usually it handles it. 
                                 # We cast to string if needed to be safe? No, let pandas handle types.
                                 df_chat.at[original_index, k] = v
                     
                     if df_chat.empty and not df.empty:
                         _clear_report_result("chat_detail")
                         st.warning("Seçilen tarih aralığında hiç 'Chat/Mesaj' kaydı bulunamadı. (Sesli çağrılar hariç tutuldu)")
                     elif not df_chat.empty:
                         df_chat = _apply_report_row_limit(df_chat, label="Chat detay raporu")
                         df_chat_view = render_table_with_export_view(df_chat, "chat_detail")
                         _store_report_result("chat_detail", df_chat, "chat_detail")
                         render_downloads(df_chat_view, "chat_detail", key_base="chat_detail")
                         report_rendered_this_run = True
                     else:
                         _clear_report_result("chat_detail")
                         st.warning(get_text(lang, "no_data"))
                 else:
                     _clear_report_result("chat_detail")
                     st.warning(get_text(lang, "no_data"))
                 try:
                     import gc as _gc
                     _gc.collect()
                 except Exception:
                     pass

    # --- MISSED INTERACTIONS REPORT ---
    if r_type == "missed_interactions":
        st.info(get_text(lang, "missed_interactions_info"))
        
        # Dynamic Column Selection (Reuse interaction cols)
        from src.lang import INTERACTION_COLUMNS
        default_cols = [c for c in INTERACTION_COLUMNS if c not in ["col_attributes", "col_media"]]
         # Let user select
        selected_cols_keys = st.multiselect(get_text(lang, "select_columns"), INTERACTION_COLUMNS, default=INTERACTION_COLUMNS, format_func=lambda x: get_text(lang, x))

        if st.button(get_text(lang, "fetch_missed_report"), type="primary", width='stretch'):
             with st.spinner(get_text(lang, "fetching_data")):
                 # Fetch data
                 # We need to fetch conversation details
                 # api = GenesysAPI(st.session_state.api_client) # already initialized above if needed, but let's re-init
                 api = GenesysAPI(st.session_state.api_client)
                 
                 s_dt = datetime.combine(sd, st_) - timedelta(hours=utc_offset_hours)
                 e_dt = datetime.combine(ed, et) - timedelta(hours=utc_offset_hours)
                 
                 # Get details
                 max_records = int(st.session_state.get("rep_max_records", 5000))
                 dfs = []
                 total_rows = 0
                 details_conversation_filters = [
                     {
                         "type": "and",
                         "predicates": [
                             {
                                 "type": "dimension",
                                 "dimension": "originatingDirection",
                                 "operator": "matches",
                                 "value": "inbound",
                             },
                             {
                                 "type": "metric",
                                 "metric": "tAbandon",
                                 "range": {"gt": 0},
                             },
                         ],
                     }
                 ]
                 details_segment_clauses = [
                     {
                         "type": "or",
                         "predicates": [
                             {
                                 "type": "dimension",
                                 "dimension": "direction",
                                 "operator": "matches",
                                 "value": "inbound",
                             }
                         ],
                     }
                 ]
                 selected_queue_ids = [qid for qid in (sel_ids or []) if qid]
                 if selected_queue_ids:
                     queue_preds = [
                         {
                             "type": "dimension",
                             "dimension": "queueId",
                             "operator": "matches",
                             "value": qid,
                         }
                         for qid in selected_queue_ids
                     ]
                     details_segment_clauses.append({"type": "or", "predicates": queue_preds})

                 selected_media_types = [
                     str(mt).strip().lower()
                     for mt in (sel_media_types or [])
                     if str(mt).strip()
                 ]
                 if selected_media_types:
                     media_preds = [
                         {
                             "type": "dimension",
                             "dimension": "mediaType",
                             "operator": "matches",
                             "value": mt,
                         }
                         for mt in selected_media_types
                     ]
                     details_segment_clauses.append({"type": "or", "predicates": media_preds})

                 details_segment_filters = None
                 if len(details_segment_clauses) == 1:
                     details_segment_filters = [details_segment_clauses[0]]
                 elif len(details_segment_clauses) > 1:
                     details_segment_filters = [{"type": "and", "clauses": details_segment_clauses}]

                 skill_lookup = st.session_state.get("skills_map", {}) or api.get_routing_skills()
                 st.session_state.skills_map = skill_lookup
                 language_lookup = api.get_languages()
                 if language_lookup:
                     st.session_state.languages_map = language_lookup
                 else:
                     language_lookup = st.session_state.get("languages_map", {})

                 for page in _iter_conversation_pages(
                     api,
                     s_dt,
                     e_dt,
                     max_records=max_records,
                     chunk_days=3,
                     conversation_filters=details_conversation_filters,
                     segment_filters=details_segment_filters,
                 ):
                     df_chunk = process_conversation_details(
                         {"conversations": page},
                         user_map=st.session_state.users_info,
                         queue_map=st.session_state.queues_map,
                         wrapup_map=st.session_state.wrapup_map,
                         include_attributes=True,
                         utc_offset=utc_offset_hours,
                         skill_map=skill_lookup,
                         language_map=language_lookup
                     )
                     if not df_chunk.empty:
                         dfs.append(df_chunk)
                         total_rows += len(df_chunk)
                 df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                 if max_records and total_rows >= max_records:
                     st.warning(f"Maksimum kayıt limiti ({max_records}) uygulandı. Daha geniş aralıklar için limiti artırabilirsiniz.")
                 
                 if not df.empty:
                     # Filter for unanswered inbound interactions only.
                     
                     target_status = "Kaçan/Cevapsız"
                     # Filter logic
                     if all(c in df.columns for c in ["ConnectionStatus", "Direction", "Queue"]):
                         # Only unanswered inbound interactions with a non-empty workgroup name.
                         queue_name = df["Queue"].astype(str).str.strip()
                         invalid_queue_values = {"", "-", "nan", "none", "null", "n/a"}
                         has_end = True
                         if "End" in df.columns:
                             end_value = df["End"].astype(str).str.strip().str.lower()
                             has_end = ~end_value.isin(invalid_queue_values)
                         df_missed = df[
                             (df["ConnectionStatus"].astype(str).str.strip() == target_status) &
                             (df["Direction"].astype(str).str.lower() == "inbound") &
                             (~queue_name.str.lower().isin(invalid_queue_values)) &
                             has_end
                         ]
                     else:
                         df_missed = pd.DataFrame() # Should not happen

                     if not df_missed.empty:
                         # Rename columns
                         col_map_internal = {
                             "Direction": "col_direction",
                             "Ani": "col_ani",
                             "Dnis": "col_dnis",
                             "Wrapup": "col_wrapup",
                             "MediaType": "col_media",
                             "Duration": "col_duration",
                             "DisconnectType": "col_disconnect",
                             "Alert": "col_alert",
                             "HoldCount": "col_hold_count",
                             "ConnectionStatus": "col_connection",
                             "Start": "start_time",
                             "End": "end_time",
                             "Agent": "col_agent",
                             "Username": "col_username",
                             "Queue": "col_workgroup",
                             "Skill": "col_skill",
                             "Language": "col_language"
                         }
                         
                         final_cols = [k for k, v in col_map_internal.items() if v in selected_cols_keys]
                         df_filtered = df_missed[final_cols]
                         rename_final = {k: get_text(lang, col_map_internal[k]) for k in final_cols}
                         
                         final_df = df_filtered.rename(columns=rename_final)
                         final_df = _apply_report_row_limit(final_df, label="Kaçan etkileşim raporu")
                         
                         st.success(f"{len(final_df)} adet kaçan etkileşim bulundu.")
                         final_df_view = render_table_with_export_view(final_df, "missed_interactions")
                         _store_report_result("missed_interactions", final_df, "missed_interactions")
                         render_downloads(final_df_view, "missed_interactions", key_base="missed_interactions")
                         report_rendered_this_run = True
                     else:
                         _clear_report_result("missed_interactions")
                         st.warning("Seçilen kriterlere uygun kaçan çağrı/etkileşim bulunamadı.")
                 else:
                     _clear_report_result("missed_interactions")
                     st.warning(get_text(lang, "no_data"))
                 try:
                     import gc as _gc
                     _gc.collect()
                 except Exception:
                     pass

    # --- INTERACTION SEARCH ---
    if r_type == "interaction_search":
        st.info(get_text(lang, "interaction_search_info"))
        
        # Dynamic Column Selection
        from src.lang import INTERACTION_COLUMNS
        default_cols = [c for c in INTERACTION_COLUMNS if c not in ["col_media", "col_wrapup"]] # Default subset
        
        # Allow user to customize columns if needed
        selected_cols_keys = st.multiselect(get_text(lang, "select_columns"), INTERACTION_COLUMNS, default=INTERACTION_COLUMNS, format_func=lambda x: get_text(lang, x))
        
        if st.button(get_text(lang, "fetch_interactions"), type="primary", width='stretch'):
             with st.spinner(get_text(lang, "fetching_data")):
                 start_date = datetime.combine(sd, st_) - timedelta(hours=utc_offset_hours)
                 end_date = datetime.combine(ed, et) - timedelta(hours=utc_offset_hours)
                 # Fetch data
                 api = GenesysAPI(st.session_state.api_client)
                 max_records = int(st.session_state.get("rep_max_records", 5000))
                 dfs = []
                 total_rows = 0
                 skill_lookup = st.session_state.get("skills_map", {}) or api.get_routing_skills()
                 st.session_state.skills_map = skill_lookup
                 language_lookup = api.get_languages()
                 if language_lookup:
                     st.session_state.languages_map = language_lookup
                 else:
                     language_lookup = st.session_state.get("languages_map", {})

                 for page in _iter_conversation_pages(api, start_date, end_date, max_records=max_records, chunk_days=3):
                     df_chunk = process_conversation_details(
                         {"conversations": page},
                         st.session_state.users_info,
                         st.session_state.queues_map,
                         st.session_state.wrapup_map,
                         utc_offset=utc_offset_hours,
                         skill_map=skill_lookup,
                         language_map=language_lookup
                     )
                     if not df_chunk.empty:
                         dfs.append(df_chunk)
                         total_rows += len(df_chunk)
                 df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                 if max_records and total_rows >= max_records:
                     st.warning(f"Maksimum kayıt limiti ({max_records}) uygulandı. Daha geniş aralıklar için limiti artırabilirsiniz.")
                 
                 if not df.empty:
                     # Rename columns first to internal keys then to display names
                     col_map_internal = {
                         "Direction": "col_direction",
                         "Ani": "col_ani",
                         "Dnis": "col_dnis",
                         "Wrapup": "col_wrapup",
                         "MediaType": "col_media",
                         "Duration": "col_duration",
                         "DisconnectType": "col_disconnect",
                         "Alert": "col_alert",
                         "HoldCount": "col_hold_count",
                         "ConnectionStatus": "col_connection",
                         "Start": "start_time",
                         "End": "end_time",
                         "Agent": "col_agent",
                         "Username": "col_username",
                         "Queue": "col_workgroup",
                         "Skill": "col_skill",
                         "Language": "col_language"
                     }
                     
                     # Filter columns based on selection
                     final_cols = [k for k, v in col_map_internal.items() if v in selected_cols_keys]
                     df_filtered = df[final_cols]
                     
                     # Rename to Display Names
                     rename_final = {k: get_text(lang, col_map_internal[k]) for k in final_cols}
                     
                     df_filtered = _apply_selected_duration_view(df_filtered)
                     final_df = df_filtered.rename(columns=rename_final)
                     final_df = _apply_report_row_limit(final_df, label="Etkileşim arama raporu")
                     final_df_view = render_table_with_export_view(final_df, "interaction_search")
                     _store_report_result("interaction_search", final_df, "interactions")
                     render_downloads(final_df_view, "interactions", key_base="interaction_search")
                     report_rendered_this_run = True
                 else:
                     _clear_report_result("interaction_search")
                     st.warning(get_text(lang, "no_data"))
                 try:
                     import gc as _gc
                     _gc.collect()
                 except Exception:
                     pass

    # --- STANDARD REPORTS ---
    elif r_type not in ["chat_detail", "missed_interactions"] and st.button(get_text(lang, "fetch_report"), type="primary", width='stretch'):
        if not sel_mets: st.warning("Lütfen metrik seçiniz.")
        else:
            unsupported_aggregate_metrics = {
                "tOrganizationResponse", "tAcdWait", "nConsultConnected", "nConsultAnswered",
                # Runtime-unsupported by conversations/aggregates (confirmed by API 400 responses).
                "nConversations", "tAgentVideoConnected", "tScreenMonitoring", "tSnippetRecord",
            }
            dropped_metrics = [m for m in sel_mets if m in unsupported_aggregate_metrics]
            sel_mets_effective = [m for m in sel_mets if m not in unsupported_aggregate_metrics]
            bad_metric_cache_key = f"_agg_bad_metrics_{r_type}"
            cached_bad_metrics = set(st.session_state.get(bad_metric_cache_key, []) or [])
            queue_all_mode = False
            if r_type == "report_queue":
                # Queue aggregate endpoint does not return agent presence/login-style metrics.
                queue_incompatible_metrics = {
                    "tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue",
                    "col_staffed_time", "col_login", "col_logout",
                }
                dropped_queue_metrics = [m for m in sel_mets_effective if m in queue_incompatible_metrics]
                if dropped_queue_metrics:
                    st.warning(
                        "Kuyruk raporunda desteklenmeyen metrikler çıkarıldı: "
                        + ", ".join(dropped_queue_metrics)
                    )
                sel_mets_effective = [m for m in sel_mets_effective if m not in queue_incompatible_metrics]
            if dropped_metrics:
                st.warning(f"Bu metrikler aggregate endpoint tarafından desteklenmiyor ve çıkarıldı: {', '.join(dropped_metrics)}")
            if cached_bad_metrics:
                cached_removed = [m for m in sel_mets_effective if m in cached_bad_metrics]
                if cached_removed:
                    st.info(
                        "Önceden API 400 veren metrikler otomatik çıkarıldı: "
                        + ", ".join(cached_removed[:20])
                    )
                sel_mets_effective = [m for m in sel_mets_effective if m not in cached_bad_metrics]
            if r_type == "report_queue" and not sel_ids:
                # No filter => all queues. This avoids large predicate payload errors.
                if not st.session_state.get("queues_map"):
                    recover_org_maps_if_needed(org, force=True)
                sel_ids = None
                queue_all_mode = True
                st.info("Kuyruk seçilmediği için tüm kuyruklar baz alındı.")
            if r_type == "report_agent" and not sel_ids:
                sel_ids = None
                st.info("Agent seçilmediği için tüm agentlar baz alındı.")
            if not sel_mets_effective:
                st.warning("Desteklenen bir metrik seçiniz.")
                st.stop()

            # Auto-save last used metrics
            st.session_state.last_metrics = sel_mets
            with st.spinner(get_text(lang, "fetching_data")):
                api = GenesysAPI(st.session_state.api_client)
                s_dt, e_dt = (
                    datetime.combine(sd, st_) - timedelta(hours=utc_offset_hours),
                    datetime.combine(ed, et) - timedelta(hours=utc_offset_hours),
                )
                # Use timezone-aware UTC datetimes to avoid deprecated utcnow().
                s_dt = s_dt.replace(tzinfo=timezone.utc)
                e_dt = e_dt.replace(tzinfo=timezone.utc)
                now_utc = datetime.now(timezone.utc)
                if e_dt > now_utc:
                    e_dt = now_utc
                    st.info("Bitiş zamanı gelecekte olduğu için mevcut zamana çekildi.")
                if s_dt >= e_dt:
                    st.warning("Başlangıç zamanı bitiş zamanından küçük olmalıdır.")
                    st.stop()
                is_skill_detailed = r_type == "report_agent_skill_detail"
                is_dnis_skill_detailed = r_type == "report_agent_dnis_skill_detail"
                is_queue_skill = (r_type == "report_queue") and (not queue_all_mode)
                r_kind = "Agent" if r_type == "report_agent" else ("Workgroup" if r_type == "report_queue" else "Detailed")
                g_by = ['userId'] if r_kind == "Agent" else ((['queueId', 'requestedRoutingSkillId', 'requestedLanguageId'] if is_queue_skill else ['queueId']) if r_kind == "Workgroup" else (['userId', 'dnis', 'requestedRoutingSkillId', 'requestedLanguageId', 'queueId'] if is_dnis_skill_detailed else (['userId', 'requestedRoutingSkillId', 'requestedLanguageId', 'queueId'] if is_skill_detailed else ['userId', 'queueId'])))
                f_type = 'user' if r_kind == "Agent" else 'queue'
                
                resp = api.get_analytics_conversations_aggregate(s_dt, e_dt, granularity=gran_opt[sel_gran], group_by=g_by, filter_type=f_type, filter_ids=sel_ids or None, metrics=sel_mets_effective, media_types=sel_media_types or None)
                agg_errors = resp.get("_errors") if isinstance(resp, dict) else None
                dropped_bad_request_metrics = resp.get("_dropped_metrics") if isinstance(resp, dict) else None
                if dropped_bad_request_metrics:
                    merged_bad = sorted(set(cached_bad_metrics).union(set(dropped_bad_request_metrics)))
                    st.session_state[bad_metric_cache_key] = merged_bad
                    try:
                        current_selected = st.session_state.get("rep_met")
                        if isinstance(current_selected, list):
                            st.session_state.rep_met = [m for m in current_selected if m not in set(dropped_bad_request_metrics)]
                    except Exception:
                        pass
                    st.warning(
                        "API 400 nedeniyle bazı metrikler çıkarıldı: "
                        + ", ".join(str(m) for m in dropped_bad_request_metrics[:20])
                    )
                if agg_errors:
                    filtered_runtime_errors = []
                    for err in agg_errors:
                        err_l = str(err).lower()
                        is_metric_400 = ("metric=" in err_l) and ("400 client error" in err_l or " bad request" in err_l)
                        if not is_metric_400:
                            filtered_runtime_errors.append(err)
                    agg_errors = filtered_runtime_errors
                if agg_errors:
                    err_blob = " | ".join(str(e) for e in agg_errors[:5]).lower()
                    if "429" in err_blob:
                        reason_hint = "Rate limit (429) nedeniyle bazı parçalar alınamadı."
                    elif "401" in err_blob or "403" in err_blob:
                        reason_hint = "Yetki/Oturum hatası nedeniyle bazı parçalar alınamadı."
                    elif "timeout" in err_blob or "timed out" in err_blob:
                        reason_hint = "Zaman aşımı nedeniyle bazı parçalar alınamadı."
                    else:
                        reason_hint = "API bazı parçalarda hata döndü."
                    st.warning(
                        f"Aggregate sorgusunda {len(agg_errors)} parça hatası oluştu. "
                        f"{reason_hint} Kısmi veri gösteriliyor olabilir."
                    )
                    with st.expander("Aggregate hata detayı", expanded=False):
                        for err_line in agg_errors[:8]:
                            st.caption(str(err_line))
                q_lookup = {v: k for k, v in st.session_state.queues_map.items()}
                skill_lookup = {}
                language_lookup = {}
                if is_skill_detailed or is_dnis_skill_detailed or is_queue_skill:
                    skill_lookup = st.session_state.get("skills_map", {}) or api.get_routing_skills()
                    st.session_state.skills_map = skill_lookup
                    language_lookup = api.get_languages()
                    if language_lookup:
                        st.session_state.languages_map = language_lookup
                    else:
                        language_lookup = st.session_state.get("languages_map", {})
                
                # For detailed report, we still need users_info for userId lookup, even though filter is queue
                lookup_map = st.session_state.users_info if r_kind in ["Agent", "Detailed"] else q_lookup
                report_type_key = "detailed_dnis_skill" if is_dnis_skill_detailed and r_kind == "Detailed" else ("detailed_skill" if is_skill_detailed and r_kind == "Detailed" else ("workgroup_skill" if is_queue_skill and r_kind == "Workgroup" else r_kind.lower()))
                df = process_analytics_response(
                    resp,
                    lookup_map,
                    report_type_key,
                    queue_map=q_lookup,
                    utc_offset=utc_offset_hours,
                    skill_map=skill_lookup,
                    language_map=language_lookup
                )
                
                if df.empty and is_agent:
                    agent_data = []
                    for uid in (sel_ids or st.session_state.users_info.keys()):
                        u = st.session_state.users_info.get(uid, {})
                        row = {"Name": u.get('name', uid), "Username": u.get('username', "").split('@')[0], "Id": uid}
                        if r_kind == "Detailed": row.update({"WorkgroupName": "-", "AgentName": row["Name"], "Id": f"{uid}|-"})
                        agent_data.append(row)
                    df = pd.DataFrame(agent_data)
                
                if not df.empty:
                    p_keys = ["tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue", "col_staffed_time", "nNotResponding"]
                    if any(m in sel_mets_effective for m in p_keys) and is_agent:
                        p_map = process_user_aggregates(api.get_user_aggregates(s_dt, e_dt, sel_ids or list(st.session_state.users_info.keys())), st.session_state.get('presence_map'))
                        for pk in ["tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue", "StaffedTime", "nNotResponding"]:
                            target_col = pk if pk != "StaffedTime" and pk != "nNotResponding" else ("col_staffed_time" if pk == "StaffedTime" else "nNotResponding")
                            fallback_series = df["Id"].apply(
                                lambda x: p_map.get(x.split('|')[0] if '|' in x else x, {}).get(pk, 0)
                            )
                            if target_col not in df.columns:
                                df[target_col] = fallback_series
                            else:
                                existing_numeric = pd.to_numeric(df[target_col], errors="coerce")
                                keep_existing = existing_numeric.notna() & (existing_numeric != 0)
                                df[target_col] = existing_numeric.where(keep_existing, fallback_series)
                    
                    if any(m in sel_mets_effective for m in ["col_login", "col_logout"]) and is_agent:
                        u_offset = utc_offset_hours
                        d_map = process_user_details(api.get_user_status_details(s_dt, e_dt, sel_ids or list(st.session_state.users_info.keys())), utc_offset=u_offset)
                        if "col_login" in sel_mets: df["col_login"] = df["Id"].apply(lambda x: d_map.get(x.split('|')[0] if '|' in x else x, {}).get("Login", "N/A"))
                        if "col_logout" in sel_mets: df["col_logout"] = df["Id"].apply(lambda x: d_map.get(x.split('|')[0] if '|' in x else x, {}).get("Logout", "N/A"))

                    if do_fill and gran_opt[sel_gran] != "P1D": df = fill_interval_gaps(df, datetime.combine(sd, st_), datetime.combine(ed, et), gran_opt[sel_gran])

                    if is_dnis_skill_detailed and r_kind == "Detailed":
                        base = ["AgentName", "Username", "Dnis", "SkillName", "LanguageName", "WorkgroupName"]
                    elif is_skill_detailed and r_kind == "Detailed":
                        base = ["AgentName", "Username", "SkillName", "LanguageName", "WorkgroupName"]
                    elif is_queue_skill and r_kind == "Workgroup":
                        base = ["Name", "SkillName", "LanguageName"]
                    else:
                        base = (["AgentName", "Username", "WorkgroupName"] if r_kind == "Detailed" else (["Name", "Username"] if is_agent else ["Name"]))
                    if "Interval" in df.columns: base = ["Interval"] + base
                    for sm in sel_mets_effective:
                        if sm not in df.columns: df[sm] = 0
                    # Show only explicitly selected metrics.
                    # AvgHandle is computed as a helper in processor; it should not be auto-added.
                    mets_to_show = [m for m in sel_mets if m in df.columns]
                    final_df = df[[c for c in base if c in df.columns] + mets_to_show]
                    
                    # Apply duration formatting
                    final_df = _apply_selected_duration_view(final_df)

                    rename = {"Interval": get_text(lang, "col_interval"), "AgentName": get_text(lang, "col_agent"), "Username": get_text(lang, "col_username"), "WorkgroupName": get_text(lang, "col_workgroup"), "Name": get_text(lang, "col_agent" if is_agent else "col_workgroup"), "AvgHandle": get_text(lang, "col_avg_handle"), "col_staffed_time": get_text(lang, "col_staffed_time"), "col_login": get_text(lang, "col_login"), "col_logout": get_text(lang, "col_logout"), "SkillName": get_text(lang, "col_skill"), "SkillId": get_text(lang, "col_skill_id"), "LanguageName": get_text(lang, "col_language"), "LanguageId": get_text(lang, "col_language_id"), "Dnis": get_text(lang, "col_dnis")}
                    rename.update({m: get_text(lang, m) for m in sel_mets_effective if m not in rename})
                    df_out = final_df.rename(columns=rename)
                    df_out = _apply_report_row_limit(df_out, label="Standart rapor")
                    df_out_view = render_table_with_export_view(df_out, r_type)
                    _store_report_result(r_type, df_out, f"report_{r_type}")
                    report_rendered_this_run = True

                    # Queue report chart based on selected interval
                    if r_kind == "Workgroup":
                        try:
                            if "Interval" in df.columns:
                                metric_for_chart = mets_to_show[0] if mets_to_show else None
                                if metric_for_chart and metric_for_chart in df.columns:
                                    chart_df = df[["Interval", metric_for_chart]].copy()
                                    chart_df["Interval"] = pd.to_datetime(chart_df["Interval"], errors="coerce")
                                    chart_df = chart_df.dropna(subset=["Interval"])
                                    if not chart_df.empty:
                                        # Daily statistics: aggregate per day across selected filters.
                                        chart_df["Interval"] = chart_df["Interval"].dt.normalize()
                                        chart_df = chart_df.groupby("Interval", as_index=False)[metric_for_chart].sum()
                                        metric_label = get_text(lang, metric_for_chart)
                                        chart_df = chart_df.rename(columns={metric_for_chart: metric_label})
                                        st.subheader(get_text(lang, "daily_stat"))
                                        render_24h_time_line_chart(
                                            chart_df,
                                            "Interval",
                                            [metric_label],
                                            aggregate_by_label="sum",
                                            label_mode="date",
                                            x_index_name="Tarih",
                                        )
                        except Exception as e:
                            monitor.log_error("REPORT_RENDER", "Queue report chart render failed", str(e))
                    render_downloads(df_out_view, f"report_{r_type}", key_base=r_type)
                else:
                    _clear_report_result(r_type)
                    st.warning(get_text(lang, "no_data"))

    if not report_rendered_this_run:
        cached_report = _get_report_result(r_type)
        if cached_report:
            cached_df = cached_report.get("df")
            if isinstance(cached_df, pd.DataFrame) and not cached_df.empty:
                cached_base_name = str(cached_report.get("base_name") or f"report_{r_type}")
                cached_view_df = render_table_with_export_view(cached_df, r_type)
                render_downloads(cached_view_df, cached_base_name, key_base=r_type)
