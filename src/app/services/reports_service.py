from typing import Any, Dict

from src.app.context import bind_context


def render_reports_service(context: Dict[str, Any]) -> None:
    """Render reports page using injected app context."""
    bind_context(globals(), context)
    st.title(get_text(lang, "menu_reports"))

    def _is_table_view_state_payload_key(state_key: Any) -> bool:
        if not (isinstance(state_key, str) and state_key.startswith("_report_table_view_")):
            return False
        widget_suffixes = (
            "_move_col",
            "_move_up",
            "_move_down",
            "_move_reset",
            "_cfg",
            "_sort_col",
            "_sort_dir",
        )
        return not any(state_key.endswith(sfx) for sfx in widget_suffixes)
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
                            if not _is_table_view_state_payload_key(state_key):
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
                    if not _is_table_view_state_payload_key(state_key):
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
    interaction_agent_names = []
    interaction_agent_ids = []
    exclude_without_workgroup = bool(st.session_state.get("rep_exclude_no_workgroup", False))
    
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
            exclude_without_workgroup = st.checkbox(
                get_text(lang, "exclude_no_workgroup"),
                value=exclude_without_workgroup,
                key="rep_exclude_no_workgroup",
                help=get_text(lang, "exclude_no_workgroup_help"),
            )
        else:
            exclude_without_workgroup = False

        if r_type in ["interaction_search", "chat_detail", "missed_interactions"]:
            if not st.session_state.get("users_map"):
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
            interaction_agent_opts = list((st.session_state.get("users_map") or {}).keys())
            if "rep_int_agent_names" in st.session_state:
                st.session_state.rep_int_agent_names = [
                    n for n in st.session_state.rep_int_agent_names if n in interaction_agent_opts
                ]
            interaction_agent_names = st.multiselect(
                get_text(lang, "select_agents"),
                interaction_agent_opts,
                key="rep_int_agent_names",
            )
            users_map = st.session_state.get("users_map", {})
            interaction_agent_ids = [
                users_map[n] for n in interaction_agent_names if n in users_map
            ]
            st.session_state.rep_max_records = st.number_input(
                "Maksimum kayıt (performans için)",
                min_value=0,
                max_value=500000,
                value=int(st.session_state.get("rep_max_records", 5000)),
                step=500,
                help="0 = limitsiz. Yüksek aralıklar bellek kullanımını artırır."
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
        selection_options = list(user_metrics) if user_metrics and role != "Admin" else list(ALL_METRICS)
        # Ensure newly added derived metrics are visible even for users with legacy metric permission sets.
        always_visible_metrics = [
            "tLongestTalk", "tAverageTalk", "tChatTalk", "tBreak", "oEfficiency",
            "nChatAnswered", "nChatOffered",
            "oAnswerRate", "nInbound", "nTotalInboundOutbound", "tPhoneTalk",
            "tASA", "tACHT",
        ]
        for m in always_visible_metrics:
            if m not in selection_options and m in ALL_METRICS:
                selection_options.append(m)
        detailed_only_metrics = {"nChatAnswered", "nChatOffered"}
        if r_type != "report_detailed":
            selection_options = [m for m in selection_options if m not in detailed_only_metrics]
        if r_type == "report_queue":
            # Queue aggregate endpoint does not provide agent presence/login derived metrics.
            queue_incompatible_metrics = {
                "tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue",
                "tBreak", "oEfficiency", "col_staffed_time", "col_login", "col_logout",
            }
            selection_options = [m for m in selection_options if m not in queue_incompatible_metrics]
        if "rep_met" in st.session_state:
            st.session_state.rep_met = [m for m in st.session_state.rep_met if m in selection_options]
        
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
            numeric = pd.to_numeric(df_out[col], errors="coerce").replace([float("inf"), float("-inf")], 0)
            df_out[col] = numeric.fillna(0).round(0).astype("int64")
        return df_out

    def _sanitize_numeric_series(series_in, index=None):
        if isinstance(series_in, pd.Series):
            series = pd.to_numeric(series_in, errors="coerce")
            if index is not None and not series.index.equals(index):
                series = series.reindex(index)
        else:
            series = pd.Series(series_in, index=index, dtype="float64")
            series = pd.to_numeric(series, errors="coerce")
        return series.replace([float("inf"), float("-inf")], 0).fillna(0)

    def _normalize_detail_media_token(raw_media):
        token = str(raw_media or "").strip().lower()
        if not token:
            return ""
        if "callback" in token:
            return "callback"
        if token in {"voice", "call", "phone", "telephony"} or "voice" in token:
            return "voice"
        if token in {"chat", "webchat"} or "chat" in token:
            return "chat"
        if token == "email" or "email" in token:
            return "email"
        message_aliases = {
            "message", "messages", "sms", "whatsapp", "facebook", "twitter", "line", "telegram",
            "webmessaging", "openmessaging",
        }
        if token in message_aliases or any(alias in token for alias in ["whatsapp", "facebook", "twitter", "line", "telegram", "messag"]):
            return "message"
        return token

    def _build_detail_query_filters(
        selected_queue_ids=None,
        selected_media_types=None,
        selected_agent_ids=None,
        require_agent_user_exists=False,
        exclude_without_workgroup=False,
        base_conversation_filters=None,
        base_segment_clauses=None,
    ):
        details_conversation_filters = list(base_conversation_filters or [])
        details_segment_clauses = list(base_segment_clauses or [])

        queue_ids = [qid for qid in (selected_queue_ids or []) if qid]
        if queue_ids:
            queue_preds = [
                {
                    "type": "dimension",
                    "dimension": "queueId",
                    "operator": "matches",
                    "value": qid,
                }
                for qid in queue_ids
            ]
            details_segment_clauses.append({"type": "or", "predicates": queue_preds})

        if exclude_without_workgroup:
            details_segment_clauses.append(
                {
                    "type": "or",
                    "predicates": [
                        {
                            "type": "dimension",
                            "dimension": "queueId",
                            "operator": "exists",
                        }
                    ],
                }
            )

        media_types = [
            str(mt).strip().lower()
            for mt in (selected_media_types or [])
            if str(mt).strip()
        ]
        if media_types:
            media_preds = [
                {
                    "type": "dimension",
                    "dimension": "mediaType",
                    "operator": "matches",
                    "value": mt,
                }
                for mt in media_types
            ]
            details_segment_clauses.append({"type": "or", "predicates": media_preds})

        if require_agent_user_exists:
            details_segment_clauses.append(
                {
                    "type": "or",
                    "predicates": [
                        {
                            "type": "dimension",
                            "dimension": "userId",
                            "operator": "exists",
                        }
                    ],
                }
            )

        agent_ids = [aid for aid in (selected_agent_ids or []) if aid]
        if agent_ids:
            agent_preds = [
                {
                    "type": "dimension",
                    "dimension": "userId",
                    "operator": "matches",
                    "value": aid,
                }
                for aid in agent_ids
            ]
            details_segment_clauses.append({"type": "or", "predicates": agent_preds})

        details_segment_filters = None
        if len(details_segment_clauses) == 1:
            details_segment_filters = [details_segment_clauses[0]]
        elif len(details_segment_clauses) > 1:
            details_segment_filters = [{"type": "and", "clauses": details_segment_clauses}]

        if not details_conversation_filters:
            details_conversation_filters = None

        return details_conversation_filters, details_segment_filters

    def _build_agent_filter_tokens(selected_agent_names=None, selected_agent_ids=None):
        tokens = {
            str(name).strip().lower()
            for name in (selected_agent_names or [])
            if str(name).strip()
        }
        users_info = st.session_state.get("users_info") or {}
        for aid in (selected_agent_ids or []):
            if not aid:
                continue
            tokens.add(str(aid).strip().lower())
            u_obj = users_info.get(aid) or {}
            if isinstance(u_obj, dict):
                for candidate in [
                    u_obj.get("name"),
                    u_obj.get("username"),
                    u_obj.get("email"),
                ]:
                    if not candidate:
                        continue
                    clean_candidate = str(candidate).strip()
                    if not clean_candidate:
                        continue
                    tokens.add(clean_candidate.lower())
                    if "@" in clean_candidate:
                        tokens.add(clean_candidate.split("@", 1)[0].lower())
        return tokens

    def _apply_interaction_dataframe_filters(
        df_in,
        selected_queue_names=None,
        selected_agent_names=None,
        selected_agent_ids=None,
        selected_media_types=None,
    ):
        if not isinstance(df_in, pd.DataFrame) or df_in.empty:
            return df_in
        df_out = df_in.copy()

        queue_tokens = {
            str(q).strip().lower()
            for q in (selected_queue_names or [])
            if str(q).strip()
        }
        if queue_tokens and "Queue" in df_out.columns:
            queue_series = df_out["Queue"].astype(str).str.strip().str.lower()
            df_out = df_out[queue_series.isin(queue_tokens)]

        agent_tokens = _build_agent_filter_tokens(selected_agent_names, selected_agent_ids)
        if agent_tokens:
            mask = pd.Series(False, index=df_out.index)
            if "Agent" in df_out.columns:
                mask = mask | df_out["Agent"].astype(str).str.strip().str.lower().isin(agent_tokens)
            if "Username" in df_out.columns:
                username_series = df_out["Username"].astype(str).str.strip().str.lower()
                mask = mask | username_series.isin(agent_tokens)
                mask = mask | username_series.str.split("@").str[0].isin(agent_tokens)
            df_out = df_out[mask]

        media_tokens = {
            _normalize_detail_media_token(mt)
            for mt in (selected_media_types or [])
            if str(mt).strip()
        }
        media_tokens = {mt for mt in media_tokens if mt}
        if media_tokens and "MediaType" in df_out.columns:
            media_series = df_out["MediaType"].apply(_normalize_detail_media_token)
            df_out = df_out[media_series.isin(media_tokens)]

        return df_out

    def _extract_aggregate_issues(resp):
        dropped = resp.get("_dropped_metrics") if isinstance(resp, dict) else None
        agg_errors = resp.get("_errors") if isinstance(resp, dict) else None
        cleaned_errors = []
        for err in agg_errors or []:
            err_l = str(err).lower()
            is_metric_400 = ("metric=" in err_l) and ("400 client error" in err_l or " bad request" in err_l)
            if not is_metric_400:
                cleaned_errors.append(err)
        return cleaned_errors, list(dropped or [])

    def _show_aggregate_errors(agg_errors, label="Aggregate"):
        if not agg_errors:
            return
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
            f"{label} sorgusunda {len(agg_errors)} parça hatası oluştu. "
            f"{reason_hint} Kısmi veri gösteriliyor olabilir."
        )
        with st.expander(f"{label} hata detayı", expanded=False):
            for err_line in agg_errors[:8]:
                st.caption(str(err_line))

    def _cache_dropped_metrics(cache_key, cached_bad_metrics, dropped_metrics, sync_selection=False, label="API 400"):
        if not dropped_metrics:
            return cached_bad_metrics
        merged_bad = sorted(set(cached_bad_metrics).union(set(dropped_metrics)))
        st.session_state[cache_key] = merged_bad
        if sync_selection:
            try:
                current_selected = st.session_state.get("rep_met")
                if isinstance(current_selected, list):
                    st.session_state.rep_met = [m for m in current_selected if m not in set(dropped_metrics)]
            except Exception:
                pass
        st.warning(
            f"{label} nedeniyle bazı metrikler çıkarıldı: "
            + ", ".join(str(m) for m in dropped_metrics[:20])
        )
        return set(merged_bad)

    def _build_detailed_queue_totals(df_queue):
        if not isinstance(df_queue, pd.DataFrame) or df_queue.empty:
            return pd.DataFrame()
        q_df = df_queue.copy()
        q_df["AgentName"] = "Kuyruk Toplamı"
        q_df["Username"] = "-"
        q_df["WorkgroupName"] = q_df.get("Name", "-")
        queue_ids = q_df["Id"] if "Id" in q_df.columns else pd.Series("-", index=q_df.index)
        q_df["Id"] = queue_ids.apply(
            lambda qid: f"queue_total|{qid}" if pd.notna(qid) and str(qid).strip() else "queue_total|-"
        )
        if "Name" in q_df.columns:
            q_df = q_df.drop(columns=["Name"])
        return q_df

    def _append_chat_metric_columns(df_in):
        if not isinstance(df_in, pd.DataFrame) or df_in.empty:
            return pd.DataFrame()
        df_chat = df_in.copy()
        connected = _sanitize_numeric_series(df_chat["nConnected"] if "nConnected" in df_chat.columns else 0, df_chat.index)
        handled = _sanitize_numeric_series(df_chat["nHandled"] if "nHandled" in df_chat.columns else 0, df_chat.index)
        talk_count = _sanitize_numeric_series(df_chat["nTalk"] if "nTalk" in df_chat.columns else 0, df_chat.index)
        answered = connected.where(connected > 0, handled.where(handled > 0, talk_count))
        offered_api = _sanitize_numeric_series(df_chat["nOffered"] if "nOffered" in df_chat.columns else 0, df_chat.index)
        offered_alert = _sanitize_numeric_series(df_chat["nAlert"] if "nAlert" in df_chat.columns else 0, df_chat.index)
        offered_raw = pd.concat([offered_api, offered_alert], axis=1).max(axis=1)
        offered = pd.concat([offered_raw, answered], axis=1).max(axis=1)
        df_chat["nChatAnswered"] = answered.clip(lower=0).round(0).astype("int64")
        df_chat["nChatOffered"] = offered.clip(lower=0).round(0).astype("int64")
        return df_chat

    def _merge_detailed_chat_metrics(base_df, chat_df):
        if not isinstance(base_df, pd.DataFrame):
            base_df = pd.DataFrame()
        if not isinstance(chat_df, pd.DataFrame) or chat_df.empty:
            out = base_df.copy()
            for c in ["nChatAnswered", "nChatOffered"]:
                if c not in out.columns:
                    out[c] = 0
            return out

        key_cols = ["AgentName", "Username", "WorkgroupName", "Id"]
        if "Interval" in base_df.columns or "Interval" in chat_df.columns:
            key_cols = ["Interval"] + key_cols

        base_work = base_df.copy()
        chat_work = chat_df.copy()
        for key_col in key_cols:
            if key_col not in base_work.columns:
                base_work[key_col] = "-"
            if key_col not in chat_work.columns:
                chat_work[key_col] = "-"

        chat_metric_cols = ["nChatAnswered", "nChatOffered"]
        for c in chat_metric_cols:
            if c not in chat_work.columns:
                chat_work[c] = 0

        chat_merge = chat_work[key_cols + chat_metric_cols].copy()
        chat_merge = chat_merge.groupby(key_cols, as_index=False)[chat_metric_cols].sum()

        if base_work.empty:
            merged = chat_merge
        else:
            merged = base_work.merge(chat_merge, on=key_cols, how="outer")

        for c in chat_metric_cols:
            merged[c] = _sanitize_numeric_series(merged.get(c, 0), merged.index).clip(lower=0).round(0).astype("int64")
        return merged

    def _backfill_queue_totals_from_agent_rows(df_in, selected_metrics):
        if not isinstance(df_in, pd.DataFrame) or df_in.empty:
            return df_in
        required_cols = {"AgentName", "Id"}
        if not required_cols.issubset(set(df_in.columns)):
            return df_in

        df_out = df_in.copy()
        work = df_out.copy()

        def _extract_queue_key(raw_id):
            s = str(raw_id or "")
            if s.startswith("queue_total|"):
                return s.split("|", 1)[1]
            if "|" in s:
                return s.split("|", 1)[1]
            return ""

        work["_queue_key"] = work["Id"].apply(_extract_queue_key)
        is_total = work["AgentName"].astype(str) == "Kuyruk Toplamı"
        totals = work[is_total].copy()
        agents = work[(~is_total) & (work["_queue_key"].astype(str).str.len() > 0)].copy()
        if totals.empty or agents.empty:
            return df_out

        join_keys = ["_queue_key"]
        if "Interval" in work.columns:
            join_keys = ["Interval"] + join_keys

        non_additive = {"oServiceLevel", "oServiceTarget", "oEfficiency", "AvgHandle"}
        candidate_metrics = []
        for m in selected_metrics or []:
            if m in non_additive:
                continue
            if m in work.columns and (m.startswith("n") or m.startswith("t") or m == "col_staffed_time"):
                candidate_metrics.append(m)
        if not candidate_metrics:
            return df_out

        for metric in candidate_metrics:
            agent_sum = (
                agents[join_keys + [metric]]
                .copy()
                .assign(**{metric: lambda x: _sanitize_numeric_series(x[metric], x.index)})
                .groupby(join_keys, as_index=False)[metric]
                .sum()
                .rename(columns={metric: f"{metric}__agent_sum"})
            )
            totals = totals.merge(agent_sum, on=join_keys, how="left")
            current = _sanitize_numeric_series(totals.get(metric, 0), totals.index)
            summed = _sanitize_numeric_series(totals.get(f"{metric}__agent_sum", 0), totals.index)
            totals[metric] = pd.concat([current, summed], axis=1).max(axis=1)
            if f"{metric}__agent_sum" in totals.columns:
                totals = totals.drop(columns=[f"{metric}__agent_sum"])

        passthrough_cols = [c for c in work.columns if c != "_queue_key"]
        rebuilt = pd.concat([work[~is_total][passthrough_cols], totals[passthrough_cols]], ignore_index=True, sort=False)
        return rebuilt

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
                 selected_queue_ids = [qid for qid in (sel_ids or []) if qid]
                 chat_media_query_types = [
                     _normalize_detail_media_token(mt)
                     for mt in (sel_media_types or [])
                     if _normalize_detail_media_token(mt) in {"chat", "message"}
                 ]
                 if not chat_media_query_types:
                     chat_media_query_types = ["chat", "message"]
                 details_conversation_filters, details_segment_filters = _build_detail_query_filters(
                     selected_queue_ids=selected_queue_ids,
                     selected_media_types=chat_media_query_types,
                     selected_agent_ids=interaction_agent_ids,
                     require_agent_user_exists=True,
                     exclude_without_workgroup=exclude_without_workgroup,
                 )
                 skill_lookup = st.session_state.get("skills_map", {}) or api.get_routing_skills()
                 st.session_state.skills_map = skill_lookup
                 language_lookup = api.get_languages()
                 if language_lookup:
                     st.session_state.languages_map = language_lookup
                 else:
                     language_lookup = st.session_state.get("languages_map", {})

                 for page in _iter_conversation_pages(
                     api,
                     start_date,
                     end_date,
                     max_records=max_records,
                     chunk_days=3,
                     conversation_filters=details_conversation_filters,
                     segment_filters=details_segment_filters,
                     allow_unfiltered_fallback=False,
                 ):
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
                     df = _apply_interaction_dataframe_filters(
                         df,
                         selected_queue_names=sel_names,
                         selected_agent_names=interaction_agent_names,
                         selected_agent_ids=interaction_agent_ids,
                         selected_media_types=chat_media_query_types,
                     )
                     media_tokens = (
                         df["MediaType"].apply(_normalize_detail_media_token)
                         if "MediaType" in df.columns
                         else pd.Series(dtype="object")
                     )
                     df_chat = df[media_tokens.isin({"chat", "message"})].copy() if not df.empty else pd.DataFrame()

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
                 base_segment_clauses = [
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
                 details_conversation_filters, details_segment_filters = _build_detail_query_filters(
                     selected_queue_ids=selected_queue_ids,
                     selected_media_types=sel_media_types,
                     selected_agent_ids=interaction_agent_ids,
                     exclude_without_workgroup=exclude_without_workgroup,
                     base_conversation_filters=details_conversation_filters,
                     base_segment_clauses=base_segment_clauses,
                 )

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
                     allow_unfiltered_fallback=(not exclude_without_workgroup),
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
                     df_missed = _apply_interaction_dataframe_filters(
                         df_missed,
                         selected_queue_names=sel_names,
                         selected_agent_names=interaction_agent_names,
                         selected_agent_ids=interaction_agent_ids,
                         selected_media_types=sel_media_types,
                     )

                     if not df_missed.empty:
                         # Rename columns
                         col_map_internal = {
                             "Id": "col_interaction_id",
                             "Direction": "col_direction",
                             "Ani": "col_ani",
                             "Dnis": "col_dnis",
                             "Wrapup": "col_wrapup",
                             "MediaType": "col_media",
                             "Duration": "col_duration",
                             "DisconnectType": "col_disconnect",
                             "InternalParticipants": "col_internal_participants",
                             "InternalDisconnectReason": "col_internal_disconnect",
                             "ExternalParticipants": "col_external_participants",
                             "ExternalDisconnectReason": "col_external_disconnect",
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
                 selected_queue_ids = [qid for qid in (sel_ids or []) if qid]
                 details_conversation_filters, details_segment_filters = _build_detail_query_filters(
                     selected_queue_ids=selected_queue_ids,
                     selected_media_types=sel_media_types,
                     selected_agent_ids=interaction_agent_ids,
                     exclude_without_workgroup=exclude_without_workgroup,
                 )
                 skill_lookup = st.session_state.get("skills_map", {}) or api.get_routing_skills()
                 st.session_state.skills_map = skill_lookup
                 language_lookup = api.get_languages()
                 if language_lookup:
                     st.session_state.languages_map = language_lookup
                 else:
                     language_lookup = st.session_state.get("languages_map", {})

                 for page in _iter_conversation_pages(
                     api,
                     start_date,
                     end_date,
                     max_records=max_records,
                     chunk_days=3,
                     conversation_filters=details_conversation_filters,
                     segment_filters=details_segment_filters,
                     allow_unfiltered_fallback=(not exclude_without_workgroup),
                 ):
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
                     df = _apply_interaction_dataframe_filters(
                         df,
                         selected_queue_names=sel_names,
                         selected_agent_names=interaction_agent_names,
                         selected_agent_ids=interaction_agent_ids,
                         selected_media_types=sel_media_types,
                     )
                     # Rename columns first to internal keys then to display names
                     col_map_internal = {
                         "Id": "col_interaction_id",
                         "Direction": "col_direction",
                         "Ani": "col_ani",
                         "Dnis": "col_dnis",
                         "Wrapup": "col_wrapup",
                         "MediaType": "col_media",
                         "Duration": "col_duration",
                         "DisconnectType": "col_disconnect",
                         "InternalParticipants": "col_internal_participants",
                         "InternalDisconnectReason": "col_internal_disconnect",
                         "ExternalParticipants": "col_external_participants",
                         "ExternalDisconnectReason": "col_external_disconnect",
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
                    "tBreak", "oEfficiency",
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
            sel_mets_effective_base = list(sel_mets_effective)
            chat_metric_keys = {"nChatAnswered", "nChatOffered"}
            chat_metrics_requested = (r_type == "report_detailed") and any(
                m in chat_metric_keys for m in (sel_mets or [])
            )
            if not sel_mets_effective and not chat_metrics_requested:
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
                agg_errors, dropped_bad_request_metrics = _extract_aggregate_issues(resp)
                cached_bad_metrics = _cache_dropped_metrics(
                    bad_metric_cache_key,
                    cached_bad_metrics,
                    dropped_bad_request_metrics,
                    sync_selection=True,
                    label="API 400",
                )
                if dropped_bad_request_metrics:
                    dropped_set = set(dropped_bad_request_metrics)
                    sel_mets_effective = [m for m in sel_mets_effective if m not in dropped_set]
                _show_aggregate_errors(agg_errors, label="Aggregate")
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

                if r_type == "report_detailed":
                    queue_total_resp = api.get_analytics_conversations_aggregate(
                        s_dt,
                        e_dt,
                        granularity=gran_opt[sel_gran],
                        group_by=["queueId"],
                        filter_type="queue",
                        filter_ids=sel_ids or None,
                        metrics=sel_mets_effective_base,
                        media_types=sel_media_types or None,
                    )
                    queue_total_errors, queue_total_dropped = _extract_aggregate_issues(queue_total_resp)
                    _cache_dropped_metrics(
                        f"{bad_metric_cache_key}_queue_total",
                        set(st.session_state.get(f"{bad_metric_cache_key}_queue_total", []) or []),
                        queue_total_dropped,
                        sync_selection=False,
                        label="Kuyruk toplamı API 400",
                    )
                    _show_aggregate_errors(queue_total_errors, label="Kuyruk toplamı aggregate")
                    df_queue_totals = process_analytics_response(
                        queue_total_resp,
                        q_lookup,
                        "queue",
                        queue_map=q_lookup,
                        utc_offset=utc_offset_hours,
                        skill_map=skill_lookup,
                        language_map=language_lookup
                    )
                    df_queue_totals = _build_detailed_queue_totals(df_queue_totals)
                    if not df_queue_totals.empty:
                        df = pd.concat([df, df_queue_totals], ignore_index=True, sort=False)

                if chat_metrics_requested:
                    selected_media_lower = [
                        str(mt).strip().lower()
                        for mt in (sel_media_types or [])
                        if str(mt).strip()
                    ]
                    chat_media_types = ["chat", "message"]
                    if selected_media_lower:
                        chat_media_types = [mt for mt in chat_media_types if mt in selected_media_lower]

                    chat_frames = []
                    if chat_media_types:
                        chat_agent_metrics = ["nConnected", "nHandled", "tTalk", "nAlert"]
                        chat_queue_metrics = ["nConnected", "nHandled", "tTalk", "nOffered", "nAlert"]
                        chat_agent_resp = api.get_analytics_conversations_aggregate(
                            s_dt,
                            e_dt,
                            granularity=gran_opt[sel_gran],
                            group_by=["userId", "queueId"],
                            filter_type="queue",
                            filter_ids=sel_ids or None,
                            metrics=chat_agent_metrics,
                            media_types=chat_media_types,
                        )
                        chat_agent_errors, chat_agent_dropped = _extract_aggregate_issues(chat_agent_resp)
                        _cache_dropped_metrics(
                            f"{bad_metric_cache_key}_chat_agent",
                            set(st.session_state.get(f"{bad_metric_cache_key}_chat_agent", []) or []),
                            chat_agent_dropped,
                            sync_selection=False,
                            label="Chat agent API 400",
                        )
                        _show_aggregate_errors(chat_agent_errors, label="Chat agent aggregate")
                        df_chat_agent = process_analytics_response(
                            chat_agent_resp,
                            st.session_state.users_info,
                            "detailed",
                            queue_map=q_lookup,
                            utc_offset=utc_offset_hours,
                            skill_map=skill_lookup,
                            language_map=language_lookup
                        )
                        if not df_chat_agent.empty:
                            chat_frames.append(df_chat_agent)

                        chat_queue_resp = api.get_analytics_conversations_aggregate(
                            s_dt,
                            e_dt,
                            granularity=gran_opt[sel_gran],
                            group_by=["queueId"],
                            filter_type="queue",
                            filter_ids=sel_ids or None,
                            metrics=chat_queue_metrics,
                            media_types=chat_media_types,
                        )
                        chat_queue_errors, chat_queue_dropped = _extract_aggregate_issues(chat_queue_resp)
                        _cache_dropped_metrics(
                            f"{bad_metric_cache_key}_chat_queue",
                            set(st.session_state.get(f"{bad_metric_cache_key}_chat_queue", []) or []),
                            chat_queue_dropped,
                            sync_selection=False,
                            label="Chat kuyruk API 400",
                        )
                        _show_aggregate_errors(chat_queue_errors, label="Chat kuyruk aggregate")
                        df_chat_queue = process_analytics_response(
                            chat_queue_resp,
                            q_lookup,
                            "queue",
                            queue_map=q_lookup,
                            utc_offset=utc_offset_hours,
                            skill_map=skill_lookup,
                            language_map=language_lookup
                        )
                        df_chat_queue = _build_detailed_queue_totals(df_chat_queue)
                        if not df_chat_queue.empty:
                            chat_frames.append(df_chat_queue)
                    elif sel_media_types:
                        st.info("Seçili medya tipi chat/message içermediği için chat sayaçları 0 gösterildi.")

                    if chat_frames:
                        chat_df = pd.concat(chat_frames, ignore_index=True, sort=False)
                        chat_df = _append_chat_metric_columns(chat_df)
                        df = _merge_detailed_chat_metrics(df, chat_df)
                    else:
                        for c in ["nChatAnswered", "nChatOffered"]:
                            if c not in df.columns:
                                df[c] = 0
                if r_type == "report_detailed":
                    df = _backfill_queue_totals_from_agent_rows(df, sel_mets)
                
                if df.empty and is_agent:
                    agent_data = []
                    for uid in (sel_ids or st.session_state.users_info.keys()):
                        u = st.session_state.users_info.get(uid, {})
                        row = {"Name": u.get('name', uid), "Username": u.get('username', "").split('@')[0], "Id": uid}
                        if r_kind == "Detailed": row.update({"WorkgroupName": "-", "AgentName": row["Name"], "Id": f"{uid}|-"})
                        agent_data.append(row)
                    df = pd.DataFrame(agent_data)
                
                if not df.empty:
                    p_keys = [
                        "tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining",
                        "tOnQueue", "tBreak", "oEfficiency", "col_staffed_time", "nNotResponding",
                    ]
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

                    # Derived/custom metrics requested from report UI.
                    selected_metric_set = set(sel_mets or [])
                    def _metric_series_or_zero(col_name):
                        if col_name in df.columns:
                            return _sanitize_numeric_series(df[col_name], df.index)
                        return pd.Series(0, index=df.index, dtype="float64")

                    def _inbound_series():
                        inbound_direct = _metric_series_or_zero("nInbound")
                        offered = _metric_series_or_zero("nOffered")
                        alert = _metric_series_or_zero("nAlert")
                        inbound = inbound_direct.where(inbound_direct > 0, offered.where(offered > 0, alert))
                        return inbound.clip(lower=0)

                    if "tLongestTalk" in selected_metric_set:
                        if "tLongestTalk" in df.columns:
                            df["tLongestTalk"] = pd.to_numeric(df["tLongestTalk"], errors="coerce").fillna(0).round(2)
                        elif "_tTalkMax" in df.columns:
                            df["tLongestTalk"] = pd.to_numeric(df["_tTalkMax"], errors="coerce").fillna(0).round(2)
                        elif "tTalk" in df.columns:
                            df["tLongestTalk"] = pd.to_numeric(df["tTalk"], errors="coerce").fillna(0).round(2)
                    if "tAverageTalk" in selected_metric_set:
                        if "tAverageTalk" in df.columns:
                            df["tAverageTalk"] = pd.to_numeric(df["tAverageTalk"], errors="coerce").fillna(0).round(2)
                        else:
                            talk_sum = _metric_series_or_zero("tTalk")
                            talk_count = _metric_series_or_zero("nTalk")
                            talk_count = talk_count.where(talk_count > 0, _metric_series_or_zero("nAnswered"))
                            talk_count = talk_count.where(talk_count > 0, _metric_series_or_zero("nHandled"))
                            safe_count = talk_count.where(talk_count > 0, talk_sum.gt(0).astype("float64"))
                            df["tAverageTalk"] = talk_sum.divide(safe_count.where(safe_count > 0), fill_value=0).fillna(0).round(2)
                    if "tBreak" in selected_metric_set:
                        break_parts = [
                            _metric_series_or_zero("tAway"),
                            _metric_series_or_zero("tMeal"),
                            _metric_series_or_zero("tMeeting"),
                            _metric_series_or_zero("tTraining"),
                        ]
                        df["tBreak"] = sum(break_parts).round(2)
                    if "oEfficiency" in selected_metric_set:
                        handle_sum = _metric_series_or_zero("tHandle")
                        staffed_sum = (
                            _metric_series_or_zero("col_staffed_time")
                            if "col_staffed_time" in df.columns
                            else _metric_series_or_zero("tOnQueue")
                        )
                        df["oEfficiency"] = (
                            handle_sum.divide(staffed_sum.where(staffed_sum > 0), fill_value=0).fillna(0) * 100
                        ).round(2)
                    if "tChatTalk" in selected_metric_set:
                        df["tChatTalk"] = _metric_series_or_zero("tTalk").round(2)
                    if "tPhoneTalk" in selected_metric_set:
                        df["tPhoneTalk"] = _metric_series_or_zero("tTalk").round(2)
                    if "tASA" in selected_metric_set:
                        answer_sum = _metric_series_or_zero("tAnswered")
                        answer_count = _metric_series_or_zero("nAnswered")
                        answer_count = answer_count.where(answer_count > 0, _metric_series_or_zero("nTalk"))
                        safe_answer_count = answer_count.where(answer_count > 0, answer_sum.gt(0).astype("float64"))
                        df["tASA"] = answer_sum.divide(safe_answer_count.where(safe_answer_count > 0), fill_value=0).fillna(0).round(2)
                    if "tACHT" in selected_metric_set:
                        existing_aht = _metric_series_or_zero("AvgHandle")
                        if existing_aht.gt(0).any():
                            df["tACHT"] = existing_aht.round(2)
                        else:
                            handle_sum = _metric_series_or_zero("tHandle")
                            handle_count = _metric_series_or_zero("nHandled")
                            handle_count = handle_count.where(handle_count > 0, _metric_series_or_zero("nHandle"))
                            handle_count = handle_count.where(handle_count > 0, _metric_series_or_zero("nAnswered"))
                            safe_handle_count = handle_count.where(handle_count > 0, handle_sum.gt(0).astype("float64"))
                            df["tACHT"] = handle_sum.divide(safe_handle_count.where(safe_handle_count > 0), fill_value=0).fillna(0).round(2)
                    if "nInbound" in selected_metric_set:
                        df["nInbound"] = _inbound_series().round(0)
                    if "nTotalInboundOutbound" in selected_metric_set:
                        inbound_total = _inbound_series()
                        outbound_total = _metric_series_or_zero("nOutbound").clip(lower=0)
                        df["nTotalInboundOutbound"] = (inbound_total + outbound_total).round(0)
                    if "oAnswerRate" in selected_metric_set:
                        inbound_total = _inbound_series()
                        answered_total = _metric_series_or_zero("nAnswered").clip(lower=0)
                        df["oAnswerRate"] = (
                            answered_total.divide(inbound_total.where(inbound_total > 0), fill_value=0).fillna(0) * 100
                        ).round(2)

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
                    for sm in sel_mets:
                        if sm not in df.columns:
                            df[sm] = 0
                        elif sm.startswith(("n", "t", "o", "Avg")) or sm in ["col_staffed_time"]:
                            df[sm] = _sanitize_numeric_series(df[sm], df.index)
                    # Show only explicitly selected metrics.
                    # AvgHandle is computed as a helper in processor; it should not be auto-added.
                    mets_to_show = [m for m in sel_mets if m in df.columns]
                    final_df = df[[c for c in base if c in df.columns] + mets_to_show]
                    
                    # Apply duration formatting
                    final_df = _apply_selected_duration_view(final_df)

                    rename = {"Interval": get_text(lang, "col_interval"), "AgentName": get_text(lang, "col_agent"), "Username": get_text(lang, "col_username"), "WorkgroupName": get_text(lang, "col_workgroup"), "Name": get_text(lang, "col_agent" if is_agent else "col_workgroup"), "AvgHandle": get_text(lang, "col_avg_handle"), "col_staffed_time": get_text(lang, "col_staffed_time"), "col_login": get_text(lang, "col_login"), "col_logout": get_text(lang, "col_logout"), "SkillName": get_text(lang, "col_skill"), "SkillId": get_text(lang, "col_skill_id"), "LanguageName": get_text(lang, "col_language"), "LanguageId": get_text(lang, "col_language_id"), "Dnis": get_text(lang, "col_dnis")}
                    rename.update({m: get_text(lang, m) for m in sel_mets if m not in rename})
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
