from typing import Any, Dict

from src.app.context import bind_context


def render_org_settings_service(context: Dict[str, Any]) -> None:
    """Render organization settings page using injected app context."""
    bind_context(globals(), context)
    st.title(f"🏢 {get_text(lang, 'menu_org_settings')}")
    org = st.session_state.app_user.get('org_code', 'default')
    conf = _resolve_org_config(org_code=org, force_reload=True)

    # --- ORGANIZATION MANAGEMENT ---
    org_list = auth_manager.get_organizations()
    if org == "default":
        st.subheader(get_text(lang, "org_management"))
        if org_list:
            st.dataframe(pd.DataFrame({get_text(lang, "organization"): sorted(org_list)}), width='stretch')
        else:
            st.info(get_text(lang, "no_orgs_found"))

        c_org1, c_org2 = st.columns(2)
        with c_org1:
            with st.form("add_org_form"):
                st.markdown(f"**{get_text(lang, 'org_add')}**")
                new_org = st.text_input(get_text(lang, "org_code_label"), value="")
                new_admin = st.text_input(get_text(lang, "admin_username"), value="admin")
                new_admin_pw = st.text_input(get_text(lang, "admin_password"), type="password")
                if st.form_submit_button(get_text(lang, "create_org"), width='stretch'):
                    if not new_org or not new_admin or not new_admin_pw:
                        st.warning("Organization code, admin username and password are required.")
                    else:
                        try:
                            new_org_safe = _safe_org_code(new_org, allow_default=False)
                        except ValueError as exc:
                            st.error(str(exc))
                            new_org_safe = None
                        if not new_org_safe:
                            ok, msg = False, "Invalid organization code"
                        else:
                            ok, msg = auth_manager.add_organization(new_org_safe, new_admin, new_admin_pw)
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)

        with c_org2:
            with st.form("delete_org_form"):
                st.markdown(f"**{get_text(lang, 'org_delete')}**")
                deletable_orgs = [o for o in org_list if o not in ["default", org]]
                if deletable_orgs:
                    del_org = st.selectbox(get_text(lang, "organization"), options=deletable_orgs)
                else:
                    del_org = ""
                    st.info(get_text(lang, "no_deletable_orgs"))
                confirm = st.checkbox(get_text(lang, "confirm_delete_org"))
                if st.form_submit_button(get_text(lang, "delete_org"), width='stretch'):
                    if not del_org:
                        st.warning("No organization selected.")
                    elif not confirm:
                        st.warning("Please confirm deletion.")
                    else:
                        ok, msg = auth_manager.delete_organization(del_org)
                        if ok:
                            delete_org_files(del_org)
                            remove_shared_data_manager(del_org)
                            try:
                                store = _shared_org_session_store()
                                with store["lock"]:
                                    store["orgs"].pop(del_org, None)
                            except Exception:
                                pass
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
    else:
        st.subheader(get_text(lang, "organization"))
        st.info(get_text(lang, "org_restricted"))

    # --- ORG DATA MANAGER (CURRENT ORG) ---
    st.subheader(get_text(lang, "org_dm_title"))
    current_org = st.session_state.app_user.get('org_code', 'default')
    if current_org == "default":
        org_sel = st.selectbox(get_text(lang, "org_dm_select"), options=sorted(org_list), index=sorted(org_list).index(current_org) if current_org in org_list else 0)
    else:
        org_sel = current_org
        st.write(f"{get_text(lang, 'organization')}: {org_sel}")
    if org_sel == current_org and st.session_state.get('data_manager'):
        dm = st.session_state.data_manager
    else:
        dm = get_existing_data_manager(org_sel)
    running = dm.is_running() if dm else False
    metric_q = len(dm.queues_map) if dm else 0
    agent_q = len(dm.agent_queues_map) if dm else 0
    last_upd = datetime.fromtimestamp(dm.last_update_time).strftime('%H:%M:%S') if dm and dm.last_update_time else "Never"
    sess_count, union_metric_q, union_agent_q = _get_org_session_stats(org_sel)
    c_dm1, c_dm2, c_dm3, c_dm4 = st.columns(4)
    c_dm1.metric(get_text(lang, "organization"), org_sel)
    c_dm2.metric(get_text(lang, "org_dm_running"), "Yes" if running else "No")
    c_dm3.metric(get_text(lang, "org_dm_metric_queues"), metric_q)
    c_dm4.metric(get_text(lang, "org_dm_last_update"), last_upd)
    st.caption(get_text(lang, "org_dm_agent_queues") + f": {agent_q}")
    st.caption(f"Aktif Oturum: {sess_count} | Birlesik MetricQ: {union_metric_q} | Birlesik AgentQ: {union_agent_q}")
    if org_sel != current_org and not running:
        st.info(get_text(lang, "org_dm_requires_session"))
    if org_sel == current_org and not st.session_state.get('api_client'):
        st.info(get_text(lang, "genesys_not_connected"))
    c_dm5, c_dm6 = st.columns(2)
    with c_dm5:
        if st.button(get_text(lang, "org_dm_start")):
            if org_sel == current_org:
                set_dm_enabled(current_org, True)
                ensure_data_manager()
                if st.session_state.get('data_manager'):
                    st.session_state.data_manager.resume()
                if st.session_state.get('api_client') and st.session_state.get('queues_map'):
                    refresh_data_manager_queues()
            else:
                set_dm_enabled(org_sel, True)
                dm = get_shared_data_manager(org_sel)
                if dm and dm.is_running() is False:
                    dm.resume()
                    # Start with empty queues; will populate on next active session for that org
                    dm.start({}, {})
            st.rerun()
    with c_dm6:
        if st.button(get_text(lang, "org_dm_stop")):
            dm = get_existing_data_manager(org_sel)
            if dm and dm.is_running():
                dm.force_stop()
            set_dm_enabled(org_sel, False)
            st.rerun()

    with st.form("org_settings_form"):
        st.subheader(get_text(lang, "general_settings"))
        # UTC Offset
        u_off = st.number_input(get_text(lang, "utc_offset"), value=int(conf.get("utc_offset", 3)), step=1)
        # Refresh Interval
        ref_i = st.number_input(
            get_text(lang, "refresh_interval"),
            value=10,
            min_value=10,
            max_value=10,
            step=1,
            help="Canli metrik yenileme suresi 10 saniye olarak sabitlenmistir.",
        )
        
        if st.form_submit_button(get_text(lang, "save"), width='stretch'):
            prev_u_off = _resolve_utc_offset_hours(conf.get("utc_offset", 3), default=3.0)
            new_u_off = _resolve_utc_offset_hours(u_off, default=3.0)
            # Update credentials file (preserving sensitive data)
            save_credentials(org, conf.get("client_id"), conf.get("client_secret"), conf.get("region"), utc_offset=u_off, refresh_interval=ref_i)
            # Update session state for immediate effect
            st.session_state.org_config = load_credentials(org)
            st.session_state["_org_config_owner"] = org
            st.session_state.data_manager.update_settings(new_u_off, ref_i)
            if abs(prev_u_off - new_u_off) > 1e-9:
                try:
                    dm = st.session_state.get("data_manager")
                    if dm:
                        with dm._lock:
                            dm.daily_data_cache = {}
                            dm.last_daily_refresh = 0
                    _clear_live_panel_caches(org, clear_shared=True)
                except Exception:
                    pass
            st.success(get_text(lang, "view_saved"))
            st.rerun()

    # --- GENESYS API SETTINGS (ORG-SCOPED) ---
    st.subheader(get_text(lang, "genesys_api_creds"))
    c_id = st.text_input("Client ID", value=conf.get("client_id", ""), type="password")
    c_sec = st.text_input("Client Secret", value=conf.get("client_secret", ""), type="password")
    regions = ["mypurecloud.ie", "mypurecloud.com", "mypurecloud.de"]
    region = st.selectbox("Region", regions, index=regions.index(conf.get("region", "mypurecloud.ie")) if conf.get("region") in regions else 0)
    st.caption("API bilgileri organizasyon için şifreli saklanır ve aynı organizasyondaki tüm kullanıcılar için kullanılır.")
    
    if st.button(get_text(lang, "login_genesys")):
        if c_id and c_sec:
            with st.spinner("Authenticating..."):
                client, err = authenticate(c_id, c_sec, region, org_code=org)
                if client:
                    st.session_state.api_client = client
                    st.session_state.genesys_logged_out = False
                    # Use existing offsets/intervals if available
                    cur_off = conf.get("utc_offset", 3)
                    cur_ref = conf.get("refresh_interval", 10)
                    save_credentials(org, c_id, c_sec, region, utc_offset=cur_off, refresh_interval=cur_ref)
                    
                    api = GenesysAPI(client)
                    maps = get_shared_org_maps(org, api, ttl_seconds=300, force_refresh=True)
                    st.session_state.users_map = maps.get("users_map", {})
                    st.session_state.users_info = maps.get("users_info", {})
                    if st.session_state.users_info:
                        st.session_state._users_info_last = dict(st.session_state.users_info)
                    st.session_state.queues_map = maps.get("queues_map", {})
                    st.session_state.wrapup_map = maps.get("wrapup", {})
                    st.session_state.presence_map = maps.get("presence", {})
                    st.session_state.org_config = load_credentials(org)
                    st.session_state["_org_config_owner"] = org
                    
                    st.session_state.data_manager.update_api_client(client, st.session_state.presence_map)
                    st.session_state.data_manager.update_settings(_resolve_utc_offset_hours(cur_off, default=3.0), cur_ref)
                    # Start with empty queues to prevent full fetch
                    st.session_state.data_manager.start({}, {}) 
                    st.rerun()
                else: st.error(f"Error: {err}")
    
    if st.session_state.api_client:
        st.divider()
        if st.button(get_text(lang, "logout_genesys"), type="primary"):
            # Stop background fetch for this org and clear API client
            dm = st.session_state.get('data_manager')
            if dm:
                dm.force_stop()
                dm.queues_map = {}
                dm.agent_queues_map = {}
                dm.obs_data_cache = {}
                dm.daily_data_cache = {}
                dm.agent_details_cache = {}
                dm.queue_members_cache = {}
            st.session_state.api_client = None
            st.session_state.queues_map = {}
            st.session_state.users_map = {}
            st.session_state.users_info = {}
            st.session_state.presence_map = {}
            with _org_maps_lock:
                _org_maps_cache.pop(org, None)
            try:
                token_cache = os.path.join(_org_dir(org), ".token_cache.json")
                if os.path.exists(token_cache):
                    os.remove(token_cache)
            except Exception:
                pass
            st.session_state.genesys_logged_out = True
            set_dm_enabled(org, False)
            # Keep app profile session intact; this only logs out Genesys API for current browser session.
            st.rerun()
