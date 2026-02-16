from typing import Any, Dict

from src.app.context import bind_context


def render_dashboard_service(context: Dict[str, Any]) -> None:
    """Render live dashboard page using injected app context."""
    bind_context(globals(), context)
    dashboard_profile_total_t0 = pytime.perf_counter()
    _dashboard_profile_tick()
    # (Config already loaded at top level)
    maps_recover_t0 = pytime.perf_counter()
    maps_recovered = recover_org_maps_if_needed(org, force=False)
    _dashboard_profile_record("dashboard.maps_recover", pytime.perf_counter() - maps_recover_t0)
    if maps_recovered:
        dm_sig = _dashboard_dm_signature(org)
        if st.session_state.get("_dashboard_dm_sig") != dm_sig:
            dm_refresh_t0 = pytime.perf_counter()
            refresh_data_manager_queues()
            _dashboard_profile_record("dashboard.dm_refresh", pytime.perf_counter() - dm_refresh_t0)
            st.session_state._dashboard_dm_sig = dm_sig
    controls_t0 = pytime.perf_counter()
    st.title(get_text(lang, "menu_dashboard"))
    if ENABLE_DASHBOARD_PROFILING:
        profile_state = _dashboard_profile_state()
        profile_rows = _dashboard_profile_rows(limit=12)
        with st.expander("⏱️ Dashboard Profiling", expanded=bool(profile_state.get("enabled"))):
            profile_duration = st.number_input(
                "Profil süresi (sn)",
                min_value=60,
                max_value=900,
                step=30,
                value=int(profile_state.get("duration_s", 180) or 180),
                key="dashboard_profile_duration_s",
            )
            p_c1, p_c2, p_c3 = st.columns(3)
            if p_c1.button("Profili Başlat", key="dashboard_profile_start_btn", width='stretch'):
                _dashboard_profile_start(duration_s=profile_duration)
                st.rerun()
            if p_c2.button("Profili Durdur", key="dashboard_profile_stop_btn", width='stretch'):
                _dashboard_profile_stop()
                st.rerun()
            if p_c3.button("Profili Temizle", key="dashboard_profile_clear_btn", width='stretch'):
                _dashboard_profile_clear(duration_s=profile_duration)
                st.rerun()
            if profile_state.get("enabled"):
                elapsed_s = max(0, int(pytime.time() - float(profile_state.get("started_ts", 0) or 0)))
                remaining_s = max(0, int(profile_state.get("duration_s", 180)) - elapsed_s)
                st.caption(f"Profil aktif. Geçen: {elapsed_s}s | Kalan: {remaining_s}s | Rerun: {profile_state.get('runs', 0)}")
            else:
                st.caption(f"Profil pasif. Son kayıt rerun sayısı: {profile_state.get('runs', 0)}")
            if profile_rows:
                st.dataframe(pd.DataFrame(profile_rows), width='stretch', hide_index=True)
            else:
                st.caption("Henüz profil verisi yok.")
    c_c1, c_c2, c_c3 = st.columns([1, 2, 1])
    if c_c1.button(get_text(lang, "add_group"), width='stretch'):
        st.session_state.dashboard_cards.append({"id": max([c['id'] for c in st.session_state.dashboard_cards], default=-1)+1, "title": "", "queues": [], "size": "medium", "live_metrics": ["Waiting", "Interacting", "On Queue"], "daily_metrics": ["Offered", "Answered", "Abandoned", "Answer Rate"]})
        save_dashboard_config(org, st.session_state.dashboard_layout, st.session_state.dashboard_cards)
        refresh_data_manager_queues()
        st.session_state._dashboard_dm_sig = _dashboard_dm_signature(org)
        st.rerun()
    
    with c_c2:
        sc1, sc2 = st.columns([2, 3])
        lo = sc1.radio(get_text(lang, "layout"), [1, 2, 3, 4], format_func=lambda x: f"Grid: {x}", index=min(st.session_state.dashboard_layout-1, 3), horizontal=True, label_visibility="collapsed")
        if lo != st.session_state.dashboard_layout:
            st.session_state.dashboard_layout = lo; save_dashboard_config(org, lo, st.session_state.dashboard_cards); st.rerun()
        m_opts = ["Live", "Yesterday", "Date"]
        if 'dashboard_mode' not in st.session_state: st.session_state.dashboard_mode = "Live"
        st.session_state.dashboard_mode = sc2.radio(get_text(lang, "mode"), m_opts, format_func=lambda x: get_text(lang, f"mode_{x.lower()}"), index=m_opts.index(st.session_state.dashboard_mode), horizontal=True, label_visibility="collapsed")

    if c_c3:
        if st.session_state.dashboard_mode == "Date": st.session_state.dashboard_date = st.date_input(get_text(lang, "mode_date"), datetime.today(), label_visibility="collapsed")
        elif st.session_state.dashboard_mode == "Live": 
            c_auto, c_time, c_spacer, c_agent, c_call = st.columns([1, 1, 1, 1, 1])
            auto_ref = c_auto.toggle(
                get_text(lang, "auto_refresh"),
                value=st.session_state.get("dashboard_auto_refresh", True),
                key="dashboard_auto_refresh",
            )
            # Toggle moved to far right
            show_agent_panel = c_agent.toggle(f"👤 {get_text(lang, 'agent_panel')}", value=st.session_state.get('show_agent_panel', False), key='toggle_agent_panel')
            show_call_panel = c_call.toggle(f"📞 {get_text(lang, 'call_panel')}", value=st.session_state.get('show_call_panel', False), key='toggle_call_panel')
            st.session_state.show_agent_panel = show_agent_panel
            st.session_state.show_call_panel = show_call_panel
            if not show_call_panel and st.session_state.get('notifications_manager'):
                st.session_state.notifications_manager.stop()
            if not show_agent_panel:
                try:
                    store = _shared_notif_store()
                    with store["lock"]:
                        nm = store["agent"].get(org)
                    if nm:
                        nm.stop()
                except Exception:
                    pass
            
            # Show Last Update Time
            if data_manager.last_update_time > 0:
                last_upd = datetime.fromtimestamp(data_manager.last_update_time).strftime('%H:%M:%S')
                c_time.markdown(
                    f'<div class="last-update">Last Update: <span>{_escape_html(last_upd)}</span></div>',
                    unsafe_allow_html=True,
                )
            
        if st.session_state.dashboard_mode == "Live":
            # DataManager is managed centrally by refresh_data_manager_queues()
            # which is called on login, hot-reload, and config changes.
            ref_int = _resolve_refresh_interval_seconds(org, minimum=10, default=10)
            if st.session_state.get("dashboard_auto_refresh", True):
                _safe_autorefresh(interval=ref_int * 1000, key="data_refresh")
        if not st.session_state.get("queues_map"):
            st.caption("Queue listesi yuklenemedi.")
            if st.button("Queue Listesini Yenile", key="reload_dashboard_queue_map"):
                recover_org_maps_if_needed(org, force=True)
                refresh_data_manager_queues()
                st.session_state._dashboard_dm_sig = _dashboard_dm_signature(org)
                st.rerun()
    _dashboard_profile_record("dashboard.controls", pytime.perf_counter() - controls_t0)

    # Available metric options
    # Available metric options
    LIVE_METRIC_OPTIONS = ["Waiting", "Interacting", "Idle Agent", "On Queue", "Available", "Busy", "Away", "Break", "Meal", "Meeting", "Training"]
    DAILY_METRIC_OPTIONS = ["Offered", "Answered", "Abandoned", "Answer Rate", "Service Level", "Avg Handle Time", "Avg Wait Time"]

    # Define labels for consistent usage in Settings and Display
    live_labels = {
        "Waiting": get_text(lang, "waiting"), 
        "Interacting": get_text(lang, "interacting"), 
        "Idle Agent": "Boşta (Idle)",
        "On Queue": get_text(lang, "on_queue_agents"), 
        "Available": get_text(lang, "available_agents"), 
        "Busy": "Meşgul", "Away": "Uzakta", "Break": "Mola", 
        "Meal": "Yemek", "Meeting": "Toplantı", "Training": "Eğitim"
    }
    
    daily_labels = {
        "Offered": get_text(lang, "offered"), 
        "Answered": get_text(lang, "answered"), 
        "Abandoned": get_text(lang, "abandoned"), 
        "Answer Rate": get_text(lang, "answer_rate"), 
        "Service Level": get_text(lang, "avg_service_level"), 
        "Avg Handle Time": "Ort. İşlem", 
        "Avg Wait Time": "Ort. Bekleme"
    }

    show_agent = st.session_state.get('show_agent_panel', False)
    show_call = st.session_state.get('show_call_panel', False)
    if show_agent and show_call:
        # Agent panel +10%, call panel +20% relative to current widths.
        main_c, agent_c, call_c = st.columns([6, 1.1, 1.2])
    elif show_agent or show_call:
        side_weight = 1.1 if show_agent else 1.2
        main_c, side_c = st.columns([7, side_weight])
        agent_c = side_c if show_agent else None
        call_c = side_c if show_call else None
    else:
        main_c = st.container()
        agent_c = None
        call_c = None

    grid = main_c.columns(st.session_state.dashboard_layout)
    cards_total_t0 = pytime.perf_counter()
    to_del = []
    for idx, card in enumerate(st.session_state.dashboard_cards):
        card_total_t0 = pytime.perf_counter()
        try:
            with grid[idx % st.session_state.dashboard_layout]:
                # Determine Container Height based on size
                c_size = card.get('size', 'medium')
                visuals_cfg = card.get('visual_metrics', ["Service Level"])
                visual_count = max(1, len(visuals_cfg))
                visuals_per_row = 1 if c_size == 'xsmall' else (2 if c_size in ['small', 'medium'] else 3)
                visual_rows = max(1, (visual_count + visuals_per_row - 1) // visuals_per_row)
                gauge_base_h = 110 if c_size == 'xsmall' else (125 if c_size == 'small' else (140 if c_size == 'medium' else 160))
                gauge_row_h = gauge_base_h + 20
    
                live_sel = card.get('live_metrics', ["Waiting", "Interacting", "On Queue"])
                daily_sel = card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"])
                live_rows = ((len(live_sel) - 1) // 5 + 1) if (st.session_state.dashboard_mode == "Live" and live_sel) else 0
                daily_rows = ((len(daily_sel) - 1) // 5 + 1) if daily_sel else 0
                metric_row_h = 74
    
                header_block_h = 96
                caption_block_h = 28 if daily_rows else 0
                metrics_block_h = (live_rows + daily_rows) * metric_row_h
                visuals_block_h = visual_rows * gauge_row_h
                padding_h = 28
                c_height = header_block_h + caption_block_h + metrics_block_h + visuals_block_h + padding_h
    
                min_height_map = {'xsmall': 380, 'small': 470, 'medium': 560, 'large': 650}
                c_height = max(c_height, min_height_map.get(c_size, 560))
    
                with st.container(height=c_height, border=True):
                    card_title = card['title'] if card['title'] else f"Grup #{card['id']+1}"
                    st.markdown(f"### {card_title}")
                    with st.expander(f"⚙️ Settings", expanded=False):
                        prev_card = dict(card)
                        card['title'] = st.text_input("Title", value=card['title'], key=f"t_{card['id']}")
                        size_opts = ["xsmall", "small", "medium", "large"]
                        card['size'] = st.selectbox("Size", size_opts, index=size_opts.index(card.get('size', 'medium')), key=f"sz_{card['id']}")
                        card['visual_metrics'] = st.multiselect("Visuals", ["Service Level", "Answer Rate", "Abandon Rate"], default=card.get('visual_metrics', ["Service Level"]), key=f"vm_{card['id']}")
                        queue_options = list(st.session_state.queues_map.keys())
                        option_by_lower = {q.strip().lower(): q for q in queue_options}
                        queue_defaults = []
                        for q in card.get('queues', []):
                            if not isinstance(q, str):
                                continue
                            q_clean = q.replace("(not loaded)", "").strip()
                            if q_clean in queue_options:
                                queue_defaults.append(q_clean)
                                continue
                            canonical = option_by_lower.get(q_clean.lower())
                            if canonical:
                                queue_defaults.append(canonical)
                        if queue_options:
                            selected_queues = st.multiselect("Queues", queue_options, default=queue_defaults, key=f"q_{card['id']}")
                            card['queues'] = selected_queues
                        else:
                            fallback_default = ", ".join([q for q in card.get('queues', []) if isinstance(q, str)])
                            manual_queues = st.text_input(
                                "Queues (manual)",
                                value=fallback_default,
                                key=f"q_manual_{card['id']}",
                                placeholder="Queue1, Queue2",
                            )
                            card['queues'] = [q.strip() for q in manual_queues.split(",") if q.strip()]
                        card['media_types'] = st.multiselect("Media Types", ["voice", "chat", "email", "callback", "message"], default=card.get('media_types', []), key=f"mt_{card['id']}")
    
                        st.write("---")
                        st.caption("📡 Canlı Metrikler")
                        card['live_metrics'] = st.multiselect("Live Metrics", LIVE_METRIC_OPTIONS, default=card.get('live_metrics', ["Waiting", "Interacting", "On Queue"]), format_func=lambda x: live_labels.get(x, x), key=f"lm_{card['id']}")
    
                        st.caption("📊 Günlük Metrikler")
                        card['daily_metrics'] = st.multiselect("Daily Metrics", DAILY_METRIC_OPTIONS, default=card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"]), format_func=lambda x: daily_labels.get(x, x), key=f"dm_{card['id']}")
    
                        if st.button("Delete", key=f"d_{card['id']}"):
                            to_del.append(idx)
                        if card != prev_card:
                            save_dashboard_config(org, st.session_state.dashboard_layout, st.session_state.dashboard_cards)
                            if prev_card.get("queues") != card.get("queues"):
                                refresh_data_manager_queues()
                                st.session_state._dashboard_dm_sig = _dashboard_dm_signature(org)
    
                    if not card.get('queues'):
                        st.info("Select queues")
                        continue
                    resolved_card_queues, unresolved_card_queues = _resolve_card_queue_names(
                        card.get('queues', []),
                        st.session_state.get("queues_map", {}),
                    )
                    if unresolved_card_queues:
                        st.caption("⚠️ Eşleşmeyen kuyruk: " + ", ".join(unresolved_card_queues[:3]))
                    if not resolved_card_queues:
                        st.warning("Seçili kuyruklar sistemde bulunamadı. Queue listesini yenileyip tekrar seçin.")
                        continue
                    
                    # Determine date range based on mode
                    data_fetch_t0 = pytime.perf_counter()
                    if st.session_state.dashboard_mode == "Live":
                        # Use cached live data
                        obs_map, daily_map, _ = st.session_state.data_manager.get_data(resolved_card_queues)
                        items_live = [obs_map.get(q) for q in resolved_card_queues if obs_map.get(q)]
                        items_daily = [daily_map.get(q) for q in resolved_card_queues if daily_map.get(q)]
                        # No per-card direct API fallback in live mode to protect API budget.
                    else:
                        # Fetch historical data via API
                        items_live = []  # No live data for historical
                        
                        if st.session_state.dashboard_mode == "Yesterday":
                            start_dt, end_dt = _dashboard_interval_utc("Yesterday", saved_creds)
                        else:  # Date mode
                            start_dt, end_dt = _dashboard_interval_utc(
                                "Date",
                                saved_creds,
                                selected_date=st.session_state.get("dashboard_date", datetime.today()),
                            )
                        
                        # Fetch aggregate data for selected queues
                        queue_ids = [
                            st.session_state.queues_map.get(q)
                            for q in resolved_card_queues
                            if st.session_state.queues_map.get(q)
                        ]
                        
                        items_daily = []
                        if queue_ids:
                            try:
                                interval = f"{start_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
                                cache_key = f"{interval}|{','.join(sorted(queue_ids))}"
                                daily_data = _get_shared_daily_stats(org, cache_key, max_age_seconds=300)
                                if daily_data is None:
                                    api = GenesysAPI(st.session_state.api_client)
                                    resp = api.get_queue_daily_stats(queue_ids, interval=interval)
                                    daily_data = {}
                                    if resp and resp.get('results'):
                                        id_map = {v: k for k, v in st.session_state.queues_map.items()}
                                        from src.processor import process_daily_stats
                                        daily_data = process_daily_stats(resp, id_map) or {}
                                    _set_shared_daily_stats(org, cache_key, daily_data, pytime.time(), max_items=60)
                                items_daily = [daily_data.get(q) for q in resolved_card_queues if daily_data.get(q)]
                            except Exception as e:
                                st.warning(f"Veri çekilemedi: {e}")
                    _dashboard_profile_record("cards.data_fetch", pytime.perf_counter() - data_fetch_t0)
                    
                    # Calculate aggregates
                    calc_t0 = pytime.perf_counter()
                    n_q = len(items_live) or 1
                    n_s = len(resolved_card_queues) or 1
                    
                    # Live Metric Helper: Sum based on selected media types
                    selected_media = card.get('media_types', [])
                    
                    def get_media_sum(item, metric_key):
                        # If metric is NOT dict (old data or non-media metric), return it directly
                        val = item.get(metric_key, 0)
                        if not isinstance(val, dict): return val
                        
                        # If dict, filter by selected media types
                        if not selected_media: return val.get('Total', 0)
                        
                        return sum(val.get(m, 0) for m in selected_media)
    
                    off = sum(get_media_sum(d, 'Offered') for d in items_daily)
                    ans = sum(get_media_sum(d, 'Answered') for d in items_daily)
                    abn = sum(get_media_sum(d, 'Abandoned') for d in items_daily)
                    s_n = sum(d.get('SL_Numerator', 0) for d in items_daily)
                    s_d = sum(d.get('SL_Denominator', 0) for d in items_daily)
                    sl = (s_n / s_d * 100) if s_d > 0 else 0
                    handle_sum = sum(get_media_sum(d, 'Handle_Sum') for d in items_daily)
                    handle_count = sum(get_media_sum(d, 'Handle_Count') for d in items_daily)
                    wait_sum = sum(get_media_sum(d, 'Wait_Sum') for d in items_daily)
                    wait_count = sum(get_media_sum(d, 'Wait_Count') for d in items_daily)
                    avg_handle = (handle_sum / handle_count) if handle_count > 0 else 0
                    avg_wait = (wait_sum / wait_count) if wait_count > 0 else 0
                    
                    # Live metrics mapping (Genesys API aligned, low-API model):
                    # 1) Queue observations => waiting/interacting queue counts
                    # 2) Routing activity entities => deduped agent states per userId
                    def _safe_int(v):
                        try:
                            return int(v or 0)
                        except Exception:
                            return 0

                    def _obs_onqueue_total(item):
                        base = _safe_int(item.get("OnQueue", 0))
                        idle_v = _safe_int(item.get("OnQueueIdle", 0))
                        int_v = _safe_int(item.get("OnQueueInteracting", 0))
                        return base if base > 0 else (idle_v + int_v)

                    obs_waiting = sum(get_media_sum(d, 'Waiting') for d in items_live) if items_live else 0
                    obs_interacting = sum(get_media_sum(d, 'Interacting') for d in items_live) if items_live else 0
                    obs_onqueue_max = max((_obs_onqueue_total(d) for d in items_live), default=0) if items_live else 0
                    obs_idle_max = max((_safe_int(d.get("OnQueueIdle", 0)) for d in items_live), default=0) if items_live else 0

                    presence_defs = st.session_state.get("presence_map") or {}

                    def _entity_ts(entity):
                        try:
                            raw = entity.get("activity_date") or entity.get("activityDate")
                            if not raw:
                                return 0.0
                            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp()
                        except Exception:
                            return 0.0

                    def _routing_status_token(value):
                        return str(value or "").strip().upper().replace(" ", "_")

                    def _routing_is_onqueue(value):
                        token = _routing_status_token(value)
                        if not token or token in {"OFF_QUEUE", "OFFLINE"}:
                            return False
                        return True

                    def _routing_status_rank(value):
                        token = _routing_status_token(value)
                        if token in {"INTERACTING", "COMMUNICATING"}:
                            return 4
                        if token == "IDLE":
                            return 3
                        if token in {"NOT_RESPONDING", "ON_QUEUE"}:
                            return 2
                        if _routing_is_onqueue(token):
                            return 1
                        return 0

                    def _presence_bucket(entity):
                        org_presence_id = str(
                            entity.get("organization_presence_id")
                            or entity.get("organizationPresenceId")
                            or ""
                        ).strip()
                        system_presence = str(
                            entity.get("system_presence")
                            or entity.get("systemPresence")
                            or ""
                        ).strip()

                        mapped_label = ""
                        mapped_system = ""
                        if org_presence_id and org_presence_id in presence_defs:
                            p_info = presence_defs.get(org_presence_id)
                            if isinstance(p_info, dict):
                                mapped_label = str(p_info.get("label") or "").strip()
                                mapped_system = str(p_info.get("systemPresence") or "").strip()
                            else:
                                mapped_label = str(p_info or "").strip()

                        raw = " ".join([
                            mapped_label.lower(),
                            mapped_system.lower(),
                            system_presence.lower(),
                        ]).strip()

                        if "break" in raw:
                            return "Break"
                        if "meal" in raw:
                            return "Meal"
                        if "meeting" in raw:
                            return "Meeting"
                        if "training" in raw:
                            return "Training"
                        if "away" in raw:
                            return "Away"
                        if "busy" in raw or "do not disturb" in raw or "dnd" in raw:
                            return "Busy"
                        if "available" in raw:
                            return "Available"
                        return None

                    routing_snapshot_by_queue = {}
                    try:
                        routing_snapshot_by_queue = st.session_state.data_manager.get_routing_activity(resolved_card_queues) or {}
                    except Exception:
                        routing_snapshot_by_queue = {}

                    routing_users_dedup = {}
                    for q_name in resolved_card_queues:
                        q_entities = routing_snapshot_by_queue.get(q_name) or {}
                        if not isinstance(q_entities, dict) or not q_entities:
                            continue
                        for uid, entity in q_entities.items():
                            uid_s = str(uid or "").strip()
                            if not uid_s:
                                continue
                            curr = dict(entity or {})
                            curr.setdefault("user_id", uid_s)
                            prev = routing_users_dedup.get(uid_s)
                            if not prev:
                                routing_users_dedup[uid_s] = curr
                                continue
                            prev_rank = _routing_status_rank(prev.get("routing_status"))
                            curr_rank = _routing_status_rank(curr.get("routing_status"))
                            if curr_rank > prev_rank:
                                routing_users_dedup[uid_s] = curr
                                continue
                            if curr_rank == prev_rank and _entity_ts(curr) >= _entity_ts(prev):
                                routing_users_dedup[uid_s] = curr

                    routing_has_payload = bool(routing_users_dedup)
                    obs_pres_max = {
                        "Available": 0,
                        "Busy": 0,
                        "Away": 0,
                        "Break": 0,
                        "Meal": 0,
                        "Meeting": 0,
                        "Training": 0,
                    }
                    for d in items_live:
                        pres = d.get("Presences") or {}
                        for k in obs_pres_max.keys():
                            obs_pres_max[k] = max(obs_pres_max[k], _safe_int(pres.get(k, 0)))

                    cnt_interacting = 0
                    cnt_idle = 0
                    cnt_on_queue = 0
                    cnt_available = 0
                    cnt_busy = 0
                    cnt_away = 0
                    cnt_break = 0
                    cnt_meal = 0
                    cnt_meeting = 0
                    cnt_training = 0

                    if routing_has_payload:
                        has_routing_status = False
                        has_presence_details = False
                        for entity in routing_users_dedup.values():
                            routing_status = _routing_status_token(entity.get("routing_status"))
                            if routing_status:
                                has_routing_status = True
                            if routing_status in {"INTERACTING", "COMMUNICATING"}:
                                cnt_interacting += 1
                            if routing_status == "IDLE":
                                cnt_idle += 1
                            if _routing_is_onqueue(routing_status):
                                cnt_on_queue += 1
                            bucket = _presence_bucket(entity)
                            if bucket:
                                has_presence_details = True
                            if bucket == "Available":
                                cnt_available += 1
                            elif bucket == "Busy":
                                cnt_busy += 1
                            elif bucket == "Away":
                                cnt_away += 1
                            elif bucket == "Break":
                                cnt_break += 1
                            elif bucket == "Meal":
                                cnt_meal += 1
                            elif bucket == "Meeting":
                                cnt_meeting += 1
                            elif bucket == "Training":
                                cnt_training += 1

                        # Queue metriclerinde interacting her zaman on-queue kapsaminda olmalidir.
                        cnt_on_queue = max(cnt_on_queue, cnt_interacting)

                        # Routing detaylari eksik gelirse observation degerlerine geri don.
                        if not has_routing_status:
                            cnt_interacting = obs_interacting
                            cnt_on_queue = max(cnt_interacting, obs_onqueue_max)
                            cnt_idle = obs_idle_max
                        else:
                            if cnt_on_queue == 0 and obs_onqueue_max > 0:
                                cnt_on_queue = max(cnt_interacting, obs_onqueue_max)
                            if cnt_idle == 0 and obs_idle_max > 0:
                                cnt_idle = obs_idle_max

                        if not has_presence_details:
                            cnt_available = obs_pres_max["Available"]
                            cnt_busy = obs_pres_max["Busy"]
                            cnt_away = obs_pres_max["Away"]
                            cnt_break = obs_pres_max["Break"]
                            cnt_meal = obs_pres_max["Meal"]
                            cnt_meeting = obs_pres_max["Meeting"]
                            cnt_training = obs_pres_max["Training"]
                    else:
                        # Fallback to queue observations when routing detail payload is unavailable.
                        cnt_interacting = obs_interacting
                        cnt_on_queue = max(cnt_interacting, obs_onqueue_max)
                        cnt_idle = obs_idle_max
                        cnt_available = obs_pres_max["Available"]
                        cnt_busy = obs_pres_max["Busy"]
                        cnt_away = obs_pres_max["Away"]
                        cnt_break = obs_pres_max["Break"]
                        cnt_meal = obs_pres_max["Meal"]
                        cnt_meeting = obs_pres_max["Meeting"]
                        cnt_training = obs_pres_max["Training"]

                    live_values = {
                        "Waiting": obs_waiting,
                        "Interacting": cnt_interacting,
                        "Idle Agent": cnt_idle,
                        "On Queue": cnt_on_queue,
                        "Available": cnt_available,
                        "Busy": cnt_busy,
                        "Away": cnt_away,
                        "Break": cnt_break,
                        "Meal": cnt_meal,
                        "Meeting": cnt_meeting,
                        "Training": cnt_training,
                    }
                    
                    # Daily metrics mapping
                    daily_values = {
                        "Offered": off,
                        "Answered": ans,
                        "Abandoned": abn,
                        "Answer Rate": f"%{(ans/off*100) if off>0 else 0:.1f}",
                        "Service Level": f"%{sl:.1f}",
                        "Avg Handle Time": f"{avg_handle/60:.1f}m" if avg_handle else "0",
                        "Avg Wait Time": f"{avg_wait:.0f}s" if avg_wait else "0",
                    }
                    # Short-lived fallback cache to avoid empty flashes between refresh cycles.
                    now_ts = pytime.time()
                    card_cache = st.session_state.get("_dashboard_card_last_by_target", {})
                    if not isinstance(card_cache, dict):
                        card_cache = {}
                    refresh_s_local = _resolve_refresh_interval_seconds(org, minimum=10, default=10)
                    fallback_ttl = 20
                    mode = st.session_state.get("dashboard_mode", "Live")
                    if mode == "Date":
                        mode_sig = f"Date:{st.session_state.get('dashboard_date', datetime.today()).isoformat()}"
                    else:
                        mode_sig = str(mode)
                    media_sig = ",".join(sorted(str(m).strip().lower() for m in (selected_media or []) if m))
                    queue_sig = ",".join(sorted(str(q).strip().lower() for q in resolved_card_queues))
                    card_cache_key = f"v2|mode:{mode_sig}|card:{card.get('id')}|q:{queue_sig}|media:{media_sig}"
                    has_source_data = bool(items_daily) if mode != "Live" else bool(items_live or items_daily or routing_has_payload)
                    if has_source_data:
                        card_cache[card_cache_key] = {
                            "ts": now_ts,
                            "live_values": dict(live_values),
                            "daily_values": dict(daily_values),
                            "off": off,
                            "ans": ans,
                            "abn": abn,
                            "sl": sl,
                        }
                        if len(card_cache) > 200:
                            oldest = sorted(card_cache.items(), key=lambda kv: kv[1].get("ts", 0))[:len(card_cache) - 200]
                            for k, _ in oldest:
                                card_cache.pop(k, None)
                        st.session_state["_dashboard_card_last_by_target"] = card_cache
                    else:
                        cached = card_cache.get(card_cache_key) or {}
                        if cached and (now_ts - float(cached.get("ts", 0) or 0)) <= fallback_ttl:
                            live_values = dict(cached.get("live_values") or live_values)
                            daily_values = dict(cached.get("daily_values") or daily_values)
                            off = float(cached.get("off", off) or 0)
                            ans = float(cached.get("ans", ans) or 0)
                            abn = float(cached.get("abn", abn) or 0)
                            sl = float(cached.get("sl", sl) or 0)
                    _dashboard_profile_record("cards.compute", pytime.perf_counter() - calc_t0)
                    
                    render_t0 = pytime.perf_counter()
                    if st.session_state.dashboard_mode == "Live":
                        # Show selected live metrics (Responsive Grid)
                        sel_live = card.get('live_metrics', ["Waiting", "Interacting", "On Queue"])
                        if sel_live:
                            # Use 5 columns per row
                            cols_per_row = 5
                            for i in range(0, len(sel_live), cols_per_row):
                                batch = sel_live[i:i+cols_per_row]
                                cols = st.columns(cols_per_row)
                                for j, metric in enumerate(batch):
                                    cols[j].metric(live_labels.get(metric, metric), live_values.get(metric, 0))
                        
                        # Show daily summary below live (Today's stats)
                        # Show daily summary below live (Today's stats)
                        st.caption(f"📅 Bugünün Özeti")
                        sel_daily = card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"])
                        if sel_daily:
                            cols_per_row = 5
                            for i in range(0, len(sel_daily), cols_per_row):
                                batch = sel_daily[i:i+cols_per_row]
                                cols = st.columns(cols_per_row)
                                for j, metric in enumerate(batch):
                                    cols[j].metric(daily_labels.get(metric, metric), daily_values.get(metric, 0))
                        
                        # Render selected visuals (wrapped rows to prevent overflow)
                        visuals = card.get('visual_metrics', ["Service Level"])
                        size_now = card.get('size')
                        base_h = 110 if size_now == 'xsmall' else (125 if size_now == 'small' else (140 if size_now == 'medium' else 160))
                        panel_key_suffix = "open" if (st.session_state.get('show_agent_panel', False) or st.session_state.get('show_call_panel', False)) else "closed"
    
                        if visuals:
                            per_row = 1 if size_now == 'xsmall' else (2 if size_now in ['small', 'medium'] else 3)
                            for start in range(0, len(visuals), per_row):
                                row = visuals[start:start + per_row]
                                cols = st.columns(per_row)
                                for idx, vis in enumerate(row):
                                    with cols[idx]:
                                        if vis == "Service Level":
                                            st.plotly_chart(create_gauge_chart(sl, get_text(lang, "avg_service_level"), base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_sl_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                                        elif vis == "Answer Rate":
                                            ar_val = (ans / off * 100) if off > 0 else 0
                                            st.plotly_chart(create_gauge_chart(ar_val, "Answer Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ar_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                                        elif vis == "Abandon Rate":
                                            ab_val = (abn / off * 100) if off > 0 else 0
                                            st.plotly_chart(create_gauge_chart(ab_val, "Abandon Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ab_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                    
                    else:
                        # Historical mode (Yesterday/Date) - show daily stats with gauge
                        sel_daily = card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"])
                        
                        # Show daily metrics first for ALL sizes
                        st.caption(f"📅 {get_text(lang, 'daily_stat')}")
                        if sel_daily:
                            cols_per_row = 5
                            for i in range(0, len(sel_daily), cols_per_row):
                                batch = sel_daily[i:i+cols_per_row]
                                cols = st.columns(cols_per_row)
                                for j, metric in enumerate(batch):
                                    cols[j].metric(daily_labels.get(metric, metric), daily_values.get(metric, 0))
                        
                        # Render selected visuals (wrapped rows to prevent overflow)
                        visuals = card.get('visual_metrics', ["Service Level"])
                        size_now = card.get('size')
                        base_h = 110 if size_now == 'xsmall' else (125 if size_now == 'small' else (140 if size_now == 'medium' else 160))
                        panel_key_suffix = "open" if (st.session_state.get('show_agent_panel', False) or st.session_state.get('show_call_panel', False)) else "closed"
    
                        if visuals:
                            per_row = 1 if size_now == 'xsmall' else (2 if size_now in ['small', 'medium'] else 3)
                            for start in range(0, len(visuals), per_row):
                                row = visuals[start:start + per_row]
                                cols = st.columns(per_row)
                                for idx, vis in enumerate(row):
                                    with cols[idx]:
                                        if vis == "Service Level":
                                            st.plotly_chart(create_gauge_chart(sl, get_text(lang, "avg_service_level"), base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_sl_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                                        elif vis == "Answer Rate":
                                            ar_val = (ans / off * 100) if off > 0 else 0
                                            st.plotly_chart(create_gauge_chart(ar_val, "Answer Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ar_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                                        elif vis == "Abandon Rate":
                                            ab_val = (abn / off * 100) if off > 0 else 0
                                            st.plotly_chart(create_gauge_chart(ab_val, "Abandon Rate", base_h), width='stretch', config={'displayModeBar': False, 'responsive': True}, key=f"g_ab_{card['id']}_{panel_key_suffix}_{start}_{idx}")
                    _dashboard_profile_record("cards.render", pytime.perf_counter() - render_t0)
        finally:
            _dashboard_profile_record("cards.card_total", pytime.perf_counter() - card_total_t0)

    _dashboard_profile_record("cards.total", pytime.perf_counter() - cards_total_t0)

    if to_del:
        for i in sorted(to_del, reverse=True): del st.session_state.dashboard_cards[i]
        save_dashboard_config(org, st.session_state.dashboard_layout, st.session_state.dashboard_cards)
        refresh_data_manager_queues()
        st.session_state._dashboard_dm_sig = _dashboard_dm_signature(org)
        st.rerun()

    # --- SIDE PANEL LOGIC ---
    if st.session_state.get('show_agent_panel', False) and agent_c:
        agent_panel_t0 = pytime.perf_counter()
        with agent_c:
            # --- Compact CSS for Agent List and Filters ---
            st.markdown("""
                <style>
                    /* Target the sidebar vertical blocks to reduce gap */
                    [data-testid="stSidebarUserContent"] .stVerticalBlock {
                        gap: 0.5rem !important;
                    }
                    /* Prevent blur/flicker on agent panel */
                    [data-testid="column"]:has(.agent-card) {
                        backface-visibility: hidden !important;
                        -webkit-backface-visibility: hidden !important;
                        transform: translateZ(0);
                        filter: none !important;
                    }
                    .agent-card {
                        padding: 6px 10px !important;
                        margin-bottom: 6px !important;
                        border-radius: 8px !important;
                        border: 1px solid #f1f5f9 !important;
                        background: #ffffff;
                        display: flex;
                        align-items: center;
                        gap: 10px;
                        animation: none !important;
                        transition: none !important;
                        filter: none !important;
                    }
                    .status-dot {
                        width: 12px;
                        height: 12px;
                        border-radius: 50%;
                        display: inline-block;
                        flex-shrink: 0;
                    }
                    .agent-name {
                        font-size: 1.1rem !important;
                        font-weight: 600 !important;
                        color: #1e293b;
                        margin: 0 !important;
                        line-height: 1.2;
                    }
                    .agent-status {
                        font-size: 1.0rem !important;
                        color: #64748b;
                        margin: 0 !important;
                        line-height: 1.2;
                    }
                    .aktif-sayisi {
                        font-size: 0.85rem;
                        color: #64748b;
                        margin-top: -5px !important;
                        margin-bottom: 5px !important;
                    }
                </style>
            """, unsafe_allow_html=True)

            # Group Filter (Genesys Groups) + submit-based filters (no rerun on each keypress)
            now_ts = pytime.time()
            groups_api_t0 = pytime.perf_counter()
            groups_cache = st.session_state.get("dashboard_groups_cache", [])
            groups_ts = st.session_state.get("dashboard_groups_ts", 0)
            if (not groups_cache) or ((now_ts - groups_ts) > 600):
                try:
                    api = GenesysAPI(st.session_state.api_client)
                    groups_cache = api.get_groups()
                    st.session_state.dashboard_groups_cache = groups_cache
                    st.session_state.dashboard_groups_ts = now_ts
                except Exception:
                    pass
            _dashboard_profile_record("agent_panel.groups_api", pytime.perf_counter() - groups_api_t0)
            group_options = ["Hepsi (All)"] + [g.get('name', '') for g in groups_cache if g.get('name')]
            if "agent_panel_search" not in st.session_state:
                st.session_state.agent_panel_search = ""
            if st.session_state.get("agent_panel_group") not in group_options:
                st.session_state.agent_panel_group = "Hepsi (All)"
            with st.form("agent_panel_filters_form", clear_on_submit=False, border=False):
                search_term = st.text_input(
                    "🔍 Agent Ara",
                    label_visibility="collapsed",
                    placeholder="Agent Ara...",
                    key="agent_panel_search",
                )
                selected_group = st.selectbox("📌 Grup Filtresi", group_options, key="agent_panel_group")
                st.form_submit_button("Filtreyi Uygula", use_container_width=True)
            search_term = str(search_term or "").strip().lower()
            
            if st.session_state.dashboard_mode != "Live":
                st.warning("Agent detayları sadece CANLI modda görünür.")
            elif not st.session_state.get('api_client'):
                st.warning(get_text(lang, "genesys_not_connected"))
            else:
                users_info_map = st.session_state.get('users_info') or {}
                if users_info_map:
                    st.session_state._users_info_last = dict(users_info_map)
                    users_info_refresh_ts = float(st.session_state.get("_users_info_full_refresh_ts", 0) or 0)
                    if (now_ts - users_info_refresh_ts) > 600:
                        users_info_api_t0 = pytime.perf_counter()
                        try:
                            api = GenesysAPI(st.session_state.api_client)
                            refreshed = get_shared_org_maps(org, api, ttl_seconds=300, force_refresh=True)
                            refreshed_users_info = refreshed.get("users_info", {}) or {}
                            if refreshed_users_info:
                                users_info_map = refreshed_users_info
                                st.session_state.users_info = users_info_map
                                st.session_state._users_info_last = dict(users_info_map)
                            st.session_state._users_info_full_refresh_ts = now_ts
                        except Exception:
                            pass
                        _dashboard_profile_record("agent_panel.users_info_api", pytime.perf_counter() - users_info_api_t0)
                else:
                    # Recover from last-known users, lazy org refresh, then users_map inversion.
                    cached_users_info = st.session_state.get('_users_info_last') or {}
                    if cached_users_info:
                        users_info_map = dict(cached_users_info)
                    else:
                        last_try = float(st.session_state.get("_users_info_recover_ts", 0) or 0)
                        if (now_ts - last_try) > 20:
                            st.session_state._users_info_recover_ts = now_ts
                            try:
                                api = GenesysAPI(st.session_state.api_client)
                                refreshed = get_shared_org_maps(org, api, ttl_seconds=300, force_refresh=True)
                                users_info_map = refreshed.get("users_info", {}) or {}
                                st.session_state.users_info = users_info_map
                                if users_info_map:
                                    st.session_state._users_info_last = dict(users_info_map)
                            except Exception:
                                pass
                    if not users_info_map:
                        users_map = st.session_state.get("users_map") or {}
                        if users_map:
                            users_info_map = {uid: {"name": name, "username": "", "email": ""} for name, uid in users_map.items()}
                            st.session_state.users_info = users_info_map
                            st.session_state._users_info_last = dict(users_info_map)
                if not users_info_map:
                    st.info("Kullanıcı bilgileri yükleniyor...")

                agent_notif = ensure_agent_notifications_manager()
                agent_notif.update_client(
                    st.session_state.api_client,
                    st.session_state.queues_map,
                    users_info_map,
                    st.session_state.get('presence_map')
                )

                # Determine target user ids (group-based)
                if selected_group != "Hepsi (All)":
                    selected_group_obj = next((g for g in groups_cache if g.get('name') == selected_group), None)
                    group_member_ids = set()
                    if selected_group_obj and selected_group_obj.get("id"):
                        group_id = selected_group_obj.get("id")
                        members_cache = st.session_state.get("dashboard_group_members_cache", {})
                        if members_cache:
                            stale_ids = [gid for gid, ent in members_cache.items() if (now_ts - ent.get("ts", 0)) > 1800]
                            for gid in stale_ids:
                                members_cache.pop(gid, None)
                            if len(members_cache) > 40:
                                oldest = sorted(members_cache.items(), key=lambda kv: kv[1].get("ts", 0))[:len(members_cache) - 40]
                                for gid, _ in oldest:
                                    members_cache.pop(gid, None)
                            st.session_state.dashboard_group_members_cache = members_cache
                        entry = members_cache.get(group_id, {})
                        if (not entry) or ((now_ts - entry.get("ts", 0)) > 600):
                            group_members_api_t0 = pytime.perf_counter()
                            try:
                                api = GenesysAPI(st.session_state.api_client)
                                members = api.get_group_members(group_id)
                                members_cache[group_id] = {"ts": now_ts, "members": members}
                                st.session_state.dashboard_group_members_cache = members_cache
                                entry = members_cache.get(group_id, {})
                            except Exception:
                                pass
                            _dashboard_profile_record("agent_panel.group_members_api", pytime.perf_counter() - group_members_api_t0)
                        for m in (entry.get("members") or []):
                            if m.get("id"):
                                group_member_ids.add(m.get("id"))
                    all_user_ids = sorted(group_member_ids)
                else:
                    all_user_ids = sorted(users_info_map.keys())

                # Seed from API if notifications cache is empty/stale
                refresh_s = _resolve_refresh_interval_seconds(org, minimum=10, default=10)
                shared_ts, shared_presence, shared_routing = _get_shared_agent_seed(org)
                last_msg = getattr(agent_notif, "last_message_ts", 0)
                last_evt = getattr(agent_notif, "last_event_ts", 0)
                seed_interval_s = max(180, int(refresh_s) * 12)
                stale_after = seed_interval_s
                notif_stale = (not agent_notif.connected) or (last_msg == 0) or ((now_ts - last_evt) > stale_after)
                periodic_seed_due = (now_ts - float(shared_ts or 0)) >= seed_interval_s
                if all_user_ids:
                    seeded_from_api = False
                    reserved_seed = False
                    needs_seed = (
                        (not getattr(agent_notif, "user_presence", {}) and not getattr(agent_notif, "user_routing", {}))
                        or notif_stale
                        or periodic_seed_due
                    )
                    if needs_seed and _reserve_agent_seed(org, now_ts, min_interval=seed_interval_s):
                        reserved_seed = True
                        seed_api_t0 = pytime.perf_counter()
                        try:
                            api = GenesysAPI(st.session_state.api_client)
                            snap = api.get_users_status_scan(target_user_ids=all_user_ids)
                            pres = snap.get("presence") or {}
                            rout = snap.get("routing") or {}
                            if pres or rout:
                                agent_notif.seed_users(pres, rout)
                                _merge_agent_seed(org, pres, rout, now_ts)
                                seeded_from_api = True
                        except Exception:
                            if reserved_seed:
                                _rollback_agent_seed(org, now_ts, fallback_ts=shared_ts)
                        _dashboard_profile_record("agent_panel.seed_api", pytime.perf_counter() - seed_api_t0)
                    if reserved_seed and not seeded_from_api:
                        _rollback_agent_seed(org, now_ts, fallback_ts=shared_ts)
                    if (not seeded_from_api) and (shared_presence or shared_routing):
                        pres = {uid: shared_presence.get(uid) for uid in all_user_ids if uid in shared_presence}
                        rout = {uid: shared_routing.get(uid) for uid in all_user_ids if uid in shared_routing}
                        if pres or rout:
                            agent_notif.seed_users_missing(pres, rout)

                # Keep WS subscriptions on the full target set so offline->active transitions are captured.
                max_users = (agent_notif.MAX_TOPICS_PER_CHANNEL * agent_notif.MAX_CHANNELS) // 3
                ws_user_ids = all_user_ids[:max_users]
                if len(all_user_ids) > max_users:
                    st.caption(f"⚠️ WebSocket limiti: {max_users}/{len(all_user_ids)} kullanıcı anlık takipte")
                if ws_user_ids:
                    agent_notif.start(ws_user_ids)

                # Only keep non-OFFLINE users for display.
                active_user_ids = []
                for uid in all_user_ids:
                    presence = agent_notif.get_user_presence(uid) if agent_notif else {}
                    sys_presence = (presence.get('presenceDefinition', {}).get('systemPresence', '')).upper()
                    if sys_presence and sys_presence != "OFFLINE":
                        active_user_ids.append(uid)

                # Build agent_data from active users
                agent_data = {"_all": []}
                for uid in active_user_ids:
                    user_info = users_info_map.get(uid, {})
                    name = user_info.get("name", "Unknown")
                    presence = agent_notif.get_user_presence(uid) if agent_notif else {}
                    routing = agent_notif.get_user_routing(uid) if agent_notif else {}
                    agent_data["_all"].append({
                        "id": uid,
                        "user": {"id": uid, "name": name, "presence": presence},
                        "routingStatus": routing,
                    })

                # Flatten, deduplicate and filter.
                filter_sort_t0 = pytime.perf_counter()
                all_members = []
                if agent_data.get("_all"):
                    unique_members = {}
                    for q_name, members in agent_data.items():
                        for m in members:
                            mid = m['id']
                            if mid not in unique_members:
                                unique_members[mid] = m
                    for m in unique_members.values():
                        name = m.get('user', {}).get('name', 'Unknown')
                        if search_term in name.lower():
                            all_members.append(m)

                # Custom order: Break, Meal, On Queue, Available
                def get_sort_score(m):
                    user_obj = m.get('user', {})
                    presence_obj = user_obj.get('presence', {})
                    p = presence_obj.get('presenceDefinition', {}).get('systemPresence', 'OFFLINE').upper()
                    routing_obj = m.get('routingStatus', {})
                    rs = routing_obj.get('status', 'OFF_QUEUE').upper()
                    
                    # Secondary sort: longest duration first.
                    start_str = routing_obj.get('startTime')
                    if not start_str or rs == 'OFF_QUEUE':
                        start_str = presence_obj.get('modifiedDate')
                    try:
                        if start_str:
                            start_str = start_str.replace("Z", "+00:00")
                            start_dt = datetime.fromisoformat(start_str)
                            duration_sec = (datetime.now(timezone.utc) - start_dt).total_seconds()
                            neg_duration = -duration_sec
                        else:
                            neg_duration = 0
                    except Exception:
                        neg_duration = 0

                    score = 10
                    if p == 'OFFLINE':
                        score = 99
                    elif p == 'BREAK':
                        score = 1
                    elif p == 'MEAL':
                        score = 2
                    elif p in ['ON_QUEUE', 'ON QUEUE'] or rs in ['INTERACTING', 'COMMUNICATING', 'IDLE', 'NOT_RESPONDING']:
                        score = 3
                    elif p == 'AVAILABLE':
                        score = 4
                    elif p == 'BUSY':
                        score = 5
                    elif p == 'MEETING':
                        score = 6
                    elif p == 'TRAINING':
                        score = 7
                    return (score, neg_duration)

                all_members.sort(key=get_sort_score)

                # Keep previous agent list visible while waiting for fresh data.
                agent_cache_key = f"{selected_group}|{search_term}"
                agent_cache = st.session_state.get("_agent_panel_last_by_filter", {})
                if not isinstance(agent_cache, dict):
                    agent_cache = {}
                fallback_ttl = 20
                if all_members:
                    agent_cache[agent_cache_key] = {"ts": now_ts, "data": list(all_members)}
                    if len(agent_cache) > 20:
                        oldest = sorted(agent_cache.items(), key=lambda kv: kv[1].get("ts", 0))[:len(agent_cache) - 20]
                        for k, _ in oldest:
                            agent_cache.pop(k, None)
                    st.session_state["_agent_panel_last_by_filter"] = agent_cache
                else:
                    cached = agent_cache.get(agent_cache_key) or {}
                    if cached and (now_ts - cached.get("ts", 0)) <= fallback_ttl:
                        all_members = list(cached.get("data") or [])
                _dashboard_profile_record("agent_panel.filter_sort", pytime.perf_counter() - filter_sort_t0)

                render_t0 = pytime.perf_counter()
                if not all_members:
                    st.info("Aktif agent bulunamadı.")
                else:
                    st.markdown(f'<p class="aktif-sayisi">Aktif: {len(all_members)}</p>', unsafe_allow_html=True)
                    max_display = 500
                    render_members = all_members[:max_display]
                    extra_count = max(0, len(all_members) - max_display)
                    agent_cards_html = []
                    for m in render_members:
                        user_obj = m.get('user', {})
                        name = user_obj.get('name', 'Unknown')
                        presence_obj = user_obj.get('presence', {})
                        presence = presence_obj.get('presenceDefinition', {}).get('systemPresence', 'OFFLINE').upper()
                        routing_obj = m.get('routingStatus', {})
                        routing = routing_obj.get('status', 'OFF_QUEUE').upper()
                        
                        duration_str = format_status_time(presence_obj.get('modifiedDate'), routing_obj.get('startTime'))
                        dot_color = "#94a3b8"
                        label = presence_obj.get('presenceDefinition', {}).get('label')
                        status_text = label if label else presence.replace("_", " ").capitalize()

                        if routing in ["INTERACTING", "COMMUNICATING"]:
                            dot_color = "#3b82f6"
                            status_text = "Görüşmede"
                        elif routing == "IDLE":
                            dot_color = "#22c55e"
                            status_text = "On Queue"
                        elif routing == "NOT_RESPONDING":
                            dot_color = "#ef4444"
                            status_text = "Cevapsız"
                        elif presence == "AVAILABLE":
                            dot_color = "#22c55e"
                            status_text = "Müsait"
                        elif presence in ["ON_QUEUE", "ON QUEUE"]:
                            dot_color = "#22c55e"
                            status_text = "On Queue"
                        elif presence == "BUSY":
                            dot_color = "#ef4444"
                            if not label:
                                status_text = "Meşgul"
                        elif presence in ["AWAY", "BREAK", "MEAL"]:
                            dot_color = "#f59e0b"
                        elif presence == "MEETING":
                            dot_color = "#ef4444"
                            if not label:
                                status_text = "Toplantı"

                        display_status = f"{status_text} - {duration_str}" if duration_str else status_text
                        safe_name = _escape_html(name)
                        safe_status = _escape_html(display_status)
                        agent_cards_html.append(f"""
                            <div class="agent-card">
                                <span class="status-dot" style="background-color: {dot_color};"></span>
                                <div>
                                        <p class="agent-name">{safe_name}</p>
                                        <p class="agent-status">{safe_status}</p>
                                    </div>
                                </div>
                            """)
                    if agent_cards_html:
                        st.markdown("".join(agent_cards_html), unsafe_allow_html=True)
                    if extra_count:
                        st.caption(f"+{extra_count} daha fazla kayıt")
                _dashboard_profile_record("agent_panel.render", pytime.perf_counter() - render_t0)
        _dashboard_profile_record("agent_panel.total", pytime.perf_counter() - agent_panel_t0)

    # --- CALL PANEL LOGIC ---
    if st.session_state.get('show_call_panel', False) and call_c:
        call_panel_t0 = pytime.perf_counter()
        with call_c:

            st.markdown("""
                <style>
                    .call-card {
                        padding: 6px 10px !important;
                        margin-bottom: 6px !important;
                        border-radius: 8px !important;
                        border: 1px solid #f1f5f9 !important;
                        background: #ffffff;
                        display: flex;
                        align-items: center;
                        justify-content: space-between;
                        gap: 10px;
                        opacity: 1;
                        animation: none !important;
                        transition: none !important;
                    }
                    .call-queue {
                        font-size: 1.0rem !important;
                        font-weight: 600 !important;
                        color: #1e293b;
                        line-height: 1.2;
                    }
                    .call-wait {
                        font-size: 0.95rem !important;
                        color: #334155;
                        font-weight: 600;
                        white-space: nowrap;
                    }
                    .call-meta {
                        font-size: 0.9rem;
                        color: #94a3b8;
                        margin-top: 3px;
                    }
                    .call-queue { line-height: 1.15; }
                    .call-info { min-width: 0; }
                    .panel-count {
                        font-size: 0.85rem;
                        color: #64748b;
                        margin-top: -4px !important;
                        margin-bottom: 5px !important;
                    }
                    .st-key-call_panel_queue_search,
                    .st-key-call_panel_group,
                    .st-key-call_panel_hide_mevcut,
                    .st-key-call_panel_type_filters {
                        margin-bottom: 0.2rem !important;
                    }
                    .st-key-call_panel_type_filters {
                        margin-bottom: 0.05rem !important;
                    }
                </style>
            """, unsafe_allow_html=True)

            # Filter options (group filter is optional - data is queue-independent)
            waiting_calls = []
            group_options = ["Hepsi (All)"] + [card['title'] or f"Grup #{idx+1}" for idx, card in enumerate(st.session_state.dashboard_cards)]
            call_filter_options = ["inbound", "outbound", "waiting", "connected", "callback", "message", "voice"]
            direction_filter_options = ["inbound", "outbound"]
            state_filter_options = ["waiting", "connected"]
            media_filter_options = ["callback", "message", "voice"]
            full_direction_filter_set = set(direction_filter_options)
            full_state_filter_set = set(state_filter_options)
            full_media_filter_set = set(media_filter_options)
            call_filter_labels = {
                "inbound": "Inbound",
                "outbound": "Outbound",
                "waiting": "Bekleyen",
                "connected": "Bağlandı",
                "callback": "Callback",
                "message": "Message",
                "voice": "Voice",
            }

            if "call_panel_queue_search" not in st.session_state:
                st.session_state.call_panel_queue_search = ""
            if st.session_state.get("call_panel_group") not in group_options:
                st.session_state.call_panel_group = "Hepsi (All)"
            if "call_panel_hide_mevcut" not in st.session_state:
                st.session_state.call_panel_hide_mevcut = False
            raw_type_filters = st.session_state.get("call_panel_type_filters")
            if not isinstance(raw_type_filters, list):
                st.session_state.call_panel_type_filters = list(call_filter_options)
            else:
                st.session_state.call_panel_type_filters = [f for f in raw_type_filters if f in call_filter_options]

            with st.form("call_panel_filters_form", clear_on_submit=False, border=False):
                queue_search_term = st.text_input(
                    "🔍 Kuyruk Ara",
                    label_visibility="collapsed",
                    placeholder="Kuyruk Ara...",
                    key="call_panel_queue_search",
                )
                selected_group = st.selectbox("📌 Grup Filtresi", group_options, key="call_panel_group")
                hide_mevcut = st.checkbox("Mevcut içeren kuyrukları gizle", key="call_panel_hide_mevcut")
                selected_call_filters = st.multiselect(
                    "🎛️ Yön / Kanal / Durum Filtresi",
                    options=call_filter_options,
                    key="call_panel_type_filters",
                    format_func=lambda x: call_filter_labels.get(x, str(x).title()),
                )
                st.form_submit_button("Filtreyi Uygula", use_container_width=True)

            queue_search_term = str(queue_search_term or "").strip().lower()
            selected_filter_set = {str(x).lower() for x in (selected_call_filters or []) if x}
            selected_direction_filters = selected_filter_set.intersection(full_direction_filter_set)
            selected_state_filters = selected_filter_set.intersection(full_state_filter_set)
            selected_media_filters = selected_filter_set.intersection(full_media_filter_set)
            is_filter_none = len(selected_filter_set) == 0
            is_filter_all = (
                is_filter_none
                or (
                    selected_direction_filters == full_direction_filter_set
                    and selected_state_filters == full_state_filter_set
                    and selected_media_filters == full_media_filter_set
                )
            )
            group_queues_lower = set()
            if selected_group != "Hepsi (All)":
                for idx, card in enumerate(st.session_state.dashboard_cards):
                    label = card['title'] or f"Grup #{idx+1}"
                    if label == selected_group and card.get('queues'):
                        resolved_group_queues, _ = _resolve_card_queue_names(
                            card.get("queues", []),
                            st.session_state.get("queues_map", {}),
                        )
                        group_queues_lower = {str(q).strip().lower() for q in resolved_group_queues}
                        break

            if st.session_state.dashboard_mode != "Live":
                st.warning(get_text(lang, "call_panel_live_only"))
            elif not st.session_state.get('api_client'):
                st.warning(get_text(lang, "genesys_not_connected"))
            else:
                refresh_s = _resolve_refresh_interval_seconds(org, minimum=10, default=10)
                now_ts = pytime.time()
                queue_id_to_name = {v: k for k, v in st.session_state.queues_map.items()}

                # ========================================
                # QUEUE-INDEPENDENT CALL PANEL (org-wide)
                # Hybrid: API seed + WebSocket updates
                # Seed/refresh logic aligned with agent panel
                # ========================================
                global_notif = ensure_global_conversation_manager()
                global_notif.update_client(st.session_state.api_client, st.session_state.queues_map)

                all_queue_ids = list(st.session_state.queues_map.values()) if st.session_state.get("queues_map") else []
                global_topics = [f"v2.routing.queues.{qid}.conversations" for qid in all_queue_ids if qid]
                global_notif.start(global_topics)

                last_msg = getattr(global_notif, "last_message_ts", 0)
                last_evt = getattr(global_notif, "last_event_ts", 0)
                snapshot_interval_s = max(90, int(refresh_s) * 8)
                notif_stale = (not global_notif.connected) or (last_msg == 0) or ((now_ts - last_evt) > snapshot_interval_s)

                # Basic model: WS is primary, periodic API snapshot fully replaces cache.
                # Snapshot throttling is org-wide (shared), not per session.
                shared_snapshot_ts, shared_snapshot_calls = _get_shared_call_seed(org)
                if shared_snapshot_calls:
                    with global_notif._lock:
                        if not global_notif.active_conversations:
                            seed_map = {}
                            for c in shared_snapshot_calls:
                                cid = c.get("conversation_id")
                                if not cid:
                                    continue
                                item = dict(c)
                                item.setdefault("last_update", shared_snapshot_ts or now_ts)
                                seed_map[cid] = item
                            if seed_map:
                                global_notif.active_conversations = seed_map

                should_snapshot = notif_stale or (now_ts - shared_snapshot_ts >= snapshot_interval_s)
                if should_snapshot and _reserve_call_seed(org, now_ts, min_interval=snapshot_interval_s):
                    snapshot_api_t0 = pytime.perf_counter()
                    try:
                        snapshot_started_ts = pytime.time()
                        api = GenesysAPI(st.session_state.api_client)
                        end_dt = datetime.now(timezone.utc)
                        start_dt = end_dt - timedelta(minutes=15)
                        convs = api.get_conversation_details_recent(start_dt, end_dt, page_size=50, max_pages=1, order="desc")
                        snapshot_calls = _build_active_calls(
                            [c for c in (convs or []) if not c.get("conversationEnd")],
                            lang,
                            queue_id_to_name=queue_id_to_name,
                            users_info=st.session_state.get("users_info"),
                        )
                        now_update = pytime.time()
                        for c in snapshot_calls:
                            c.setdefault("state", "waiting")
                            c.setdefault("wg", c.get("queue_name"))
                            c["last_update"] = now_update
                            if not c.get("media_type"):
                                c["media_type"] = _extract_media_type(c)
                        _update_call_seed(org, snapshot_calls, now_update, max_items=300)
                        _update_call_meta(org, snapshot_calls, now_update, max_items=300)
                        new_map = {c.get("conversation_id"): c for c in snapshot_calls if c.get("conversation_id")}
                        with global_notif._lock:
                            existing_map = dict(global_notif.active_conversations or {})
                            merged_map = {}

                            # 1) Start from snapshot, but never overwrite fresher WS data.
                            for cid, snap_item in new_map.items():
                                ex = existing_map.get(cid) or {}
                                ex_ts = ex.get("last_update", 0) or 0
                                if ex and ex_ts > snapshot_started_ts:
                                    merged_map[cid] = ex
                                else:
                                    merged_map[cid] = _merge_call(ex, snap_item) if ex else snap_item

                            # 2) Keep very fresh WS-only items that may lag in analytics snapshot.
                            for cid, ex in existing_map.items():
                                if cid in merged_map:
                                    continue
                                ex_ts = ex.get("last_update", 0) or 0
                                if ex_ts and (snapshot_started_ts - ex_ts) <= 30:
                                    merged_map[cid] = ex

                            global_notif.active_conversations = merged_map
                    except Exception:
                        pass
                    _dashboard_profile_record("call_panel.snapshot_api", pytime.perf_counter() - snapshot_api_t0)

                active_conversations = global_notif.get_active_conversations(max_age_seconds=300)
                for c in active_conversations:
                    if "state" not in c:
                        c["state"] = "waiting"
                    if "wg" not in c or not c.get("wg"):
                        c["wg"] = c.get("queue_name")

                combined = {}
                for c in active_conversations:
                    cid = c.get("conversation_id")
                    if cid:
                        combined[cid] = dict(c)

                # Enrich from shared meta cache first (org-wide).
                shared_meta = _get_shared_call_meta(org, max_age_seconds=600)
                if shared_meta:
                    for cid, item in list(combined.items()):
                        meta = shared_meta.get(cid)
                        if meta:
                            combined[cid] = _merge_call(item, meta)

                # Small enrichment pass for missing queue/phone/direction fields.
                missing_ids = []
                for cid, item in combined.items():
                    has_queue = bool(item.get("queue_name")) and not _is_generic_queue_name(item.get("queue_name"))
                    has_wg = bool(item.get("wg"))
                    has_phone = bool(item.get("phone"))
                    has_direction = bool(item.get("direction_label")) or bool(item.get("direction"))
                    if not (has_queue and has_wg and has_phone and has_direction):
                        missing_ids.append(cid)

                meta_poll_interval_s = max(60, int(refresh_s) * 6)
                meta_target_ids = _reserve_call_meta_targets(
                    org,
                    missing_ids,
                    now_ts,
                    min_interval=meta_poll_interval_s,
                    cooldown_seconds=max(120, int(refresh_s) * 12),
                    max_items=1,
                )
                if meta_target_ids:
                    meta_api_t0 = pytime.perf_counter()
                    try:
                        api = GenesysAPI(st.session_state.api_client)
                        users_info = st.session_state.get("users_info") or {}
                        for cid in meta_target_ids:
                            meta = _fetch_conversation_meta(api, cid, queue_id_to_name, users_info=users_info)
                            if meta:
                                if meta.get("ended"):
                                    combined.pop(cid, None)
                                    try:
                                        with global_notif._lock:
                                            global_notif.active_conversations.pop(cid, None)
                                    except Exception:
                                        pass
                                    continue
                                combined[cid] = _merge_call(combined.get(cid), meta)
                    except Exception:
                        pass
                    _dashboard_profile_record("call_panel.meta_api", pytime.perf_counter() - meta_api_t0)

                # Final cleanup.
                for cid in list(combined.keys()):
                    item = combined.get(cid) or {}
                    if item.get("ended"):
                        combined.pop(cid, None)
                        continue
                    lu = item.get("last_update", 0) or 0
                    if lu and (now_ts - lu) > 300:
                        combined.pop(cid, None)
                        continue

                waiting_calls = list(combined.values())
                try:
                    _update_call_meta(org, waiting_calls, now_ts, max_items=300)
                except Exception:
                    pass

                filter_sort_t0 = pytime.perf_counter()
                if group_queues_lower:
                    waiting_calls = [
                        c for c in waiting_calls
                        if str(c.get("queue_name") or "").strip().lower() in group_queues_lower
                    ]
                if hide_mevcut:
                    waiting_calls = [c for c in waiting_calls if "mevcut" not in (c.get("queue_name") or "").lower()]
                if not is_filter_all:
                    waiting_calls = [
                        c for c in waiting_calls
                        if _call_matches_filters(
                            c,
                            direction_filters=selected_direction_filters,
                            media_filters=selected_media_filters,
                            state_filters=selected_state_filters,
                        )
                    ]
                if queue_search_term:
                    waiting_calls = [
                        c for c in waiting_calls
                        if (
                            queue_search_term in str(c.get("queue_name") or "").lower()
                            or queue_search_term in str(c.get("wg") or "").lower()
                        )
                    ]

                waiting_calls.sort(key=lambda x: x.get("wait_seconds") if x.get("wait_seconds") is not None else -1, reverse=True)

                # Short-lived fallback cache to avoid empty flashes between refresh cycles.
                if is_filter_all:
                    filter_sig = "all"
                elif selected_filter_set:
                    filter_sig = ",".join(sorted(selected_filter_set))
                else:
                    filter_sig = "none"
                search_sig = queue_search_term.replace("|", " ").strip() if queue_search_term else ""
                call_cache_key = f"v3|{selected_group}|{int(bool(hide_mevcut))}|{filter_sig}|q:{search_sig}"
                call_cache = st.session_state.get("_call_panel_last_by_filter", {})
                if not isinstance(call_cache, dict):
                    call_cache = {}
                fallback_ttl = 20
                if waiting_calls:
                    call_cache[call_cache_key] = {"ts": now_ts, "data": list(waiting_calls)}
                    if len(call_cache) > 20:
                        oldest = sorted(call_cache.items(), key=lambda kv: kv[1].get("ts", 0))[:len(call_cache) - 20]
                        for k, _ in oldest:
                            call_cache.pop(k, None)
                    st.session_state["_call_panel_last_by_filter"] = call_cache
                else:
                    if is_filter_all or selected_filter_set:
                        cached = call_cache.get(call_cache_key) or {}
                        if cached and (now_ts - cached.get("ts", 0)) <= fallback_ttl:
                            waiting_calls = list(cached.get("data") or [])
                _dashboard_profile_record("call_panel.filter_sort", pytime.perf_counter() - filter_sort_t0)

                count_label = "Aktif"
                st.markdown(f'<p class="panel-count">{count_label}: {len(waiting_calls)}</p>', unsafe_allow_html=True)

                render_t0 = pytime.perf_counter()
                if not waiting_calls:
                    st.info(get_text(lang, "no_waiting_calls"))
                else:
                    max_display = 200
                    render_calls = waiting_calls[:max_display]
                    extra_count = max(0, len(waiting_calls) - max_display)
                    call_cards_html = []
                    for item in render_calls:
                        wait_str = format_duration_seconds(item.get("wait_seconds"))
                        q = item.get("queue_name", "")
                        wg = item.get("wg")
                        queue_display = q
                        if _is_generic_queue_name(queue_display):
                            if wg and not _is_generic_queue_name(wg):
                                queue_display = wg
                            else:
                                queue_display = "-"
                        queue_text = f"{queue_display}"
                        conv_id = item.get("conversation_id")
                        conv_short = conv_id[-6:] if conv_id and len(conv_id) > 6 else conv_id
                        phone = item.get("phone")
                        direction_label = item.get("direction_label")
                        state_label = item.get("state_label")
                        media_type = item.get("media_type")
                        agent_name = item.get("agent_name")
                        agent_id = item.get("agent_id")
                        ivr_selection = item.get("ivr_selection")
                        state_value = (item.get("state") or "").lower()
                        if not direction_label:
                            d = (item.get("direction") or "").lower()
                            if "inbound" in d:
                                direction_label = "Inbound"
                            elif "outbound" in d:
                                direction_label = "Outbound"
                        if media_type and media_type.lower() == "callback":
                            media_type = "Callback"
                            if not direction_label:
                                direction_label = "Outbound"
                        elif _normalize_call_media_token(media_type) == "voice":
                            media_type = "Voice"
                        elif _normalize_call_media_token(media_type) == "message":
                            media_type = "Message"
                        meta_parts = []
                        is_interacting = bool(agent_name) or bool(agent_id) or state_value == "interacting" or (state_label == get_text(lang, "interacting"))
                        state_label = "Bağlandı" if is_interacting else "Bekleyen"
                        if agent_name and is_interacting:
                            meta_parts.append(f"Agent: {agent_name}")
                        if wg and str(wg).strip() and str(wg).strip().lower() != str(queue_display).strip().lower():
                            meta_parts.append(f"WG: {wg}")
                        if ivr_selection:
                            meta_parts.append(f"🔢 {ivr_selection}")
                        if direction_label:
                            meta_parts.append(str(direction_label))
                        if state_label:
                            meta_parts.append(str(state_label))
                        if media_type:
                            meta_parts.append(str(media_type))
                        if phone:
                            meta_parts.append(str(phone))
                        if conv_short:
                            meta_parts.append(f"#{conv_short}")
                        meta_text = " • ".join(meta_parts)
                        safe_queue_text = _escape_html(queue_text)
                        safe_meta_text = _escape_html(meta_text)
                        safe_wait_str = _escape_html(wait_str)
                        meta_html = f'<div class="call-meta">{safe_meta_text}</div>' if safe_meta_text else ""

                        call_cards_html.append(f"""
                            <div class="call-card">
                                <div class="call-info">
                                    <div class="call-queue">{safe_queue_text}</div>
                                    {meta_html}
                                </div>
                                <div class="call-wait">{safe_wait_str}</div>
                            </div>
                        """)
                    if call_cards_html:
                        st.markdown("".join(call_cards_html), unsafe_allow_html=True)
                    if extra_count:
                        st.caption(f"+{extra_count} daha fazla kayıt")
                _dashboard_profile_record("call_panel.render", pytime.perf_counter() - render_t0)
            _dashboard_profile_record("call_panel.total", pytime.perf_counter() - call_panel_t0)

    _dashboard_profile_record("dashboard.total", pytime.perf_counter() - dashboard_profile_total_t0)
    _dashboard_profile_commit_run()
