import time as pytime
from typing import Any, Dict

from src.app.context import bind_context


ADMIN_STATUS_AUDIT_CACHE_TTL_SECONDS = 1800
ADMIN_STATUS_AUDIT_CACHE_MAX_ENTRIES = 20
ADMIN_QUEUE_AUDIT_CACHE_TTL_SECONDS = 1800
ADMIN_QUEUE_AUDIT_MAX_ATTEMPTS = 40
ADMIN_QUEUE_AUDIT_MAX_PREVIEW_ROWS = 80
ADMIN_QUEUE_AUDIT_MAX_ROWS = 5000


def _trim_attempts_for_storage(attempts, max_items=ADMIN_QUEUE_AUDIT_MAX_ATTEMPTS, max_filters=8, max_error_len=280):
    trimmed = []
    if not isinstance(attempts, list):
        return trimmed
    for item in attempts[: max(1, int(max_items or 1))]:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        filters_val = row.get("filters")
        if isinstance(filters_val, list):
            compact_filters = []
            for f in filters_val[: max(1, int(max_filters or 1))]:
                if not isinstance(f, dict):
                    continue
                prop = str(f.get("property") or "").strip()
                value = str(f.get("value") or "").strip()
                if not prop or not value:
                    continue
                compact_filters.append({"property": prop, "value": value})
            row["filters"] = compact_filters
        err = str(row.get("error") or "").strip()
        if len(err) > max_error_len:
            row["error"] = err[: max_error_len - 3] + "..."
        trimmed.append(row)
    return trimmed


def _prune_admin_runtime_caches(session_state, current_org):
    """Trim admin panel session caches to avoid long-running session bloat."""
    now_ts = pytime.time()

    # 1) Status audit cache: TTL + max entry bound.
    status_cache = session_state.get("admin_status_audit_cache")
    if isinstance(status_cache, dict):
        cleaned = {}
        indexed = []
        for key, payload in status_cache.items():
            if not isinstance(payload, dict):
                continue
            cached_at = float(payload.get("_cached_at_ts", 0) or 0)
            if cached_at <= 0:
                cached_at = now_ts
                payload = dict(payload)
                payload["_cached_at_ts"] = cached_at
            if (now_ts - cached_at) > ADMIN_STATUS_AUDIT_CACHE_TTL_SECONDS:
                continue
            payload = dict(payload)
            payload["_attempts"] = _trim_attempts_for_storage(payload.get("_attempts"))
            cleaned[key] = payload
            indexed.append((key, cached_at))

        if len(cleaned) > ADMIN_STATUS_AUDIT_CACHE_MAX_ENTRIES:
            keep_keys = {
                key
                for key, _ in sorted(indexed, key=lambda kv: kv[1], reverse=True)[:ADMIN_STATUS_AUDIT_CACHE_MAX_ENTRIES]
            }
            cleaned = {k: v for k, v in cleaned.items() if k in keep_keys}
        session_state["admin_status_audit_cache"] = cleaned

    # 2) Queue audit tab cache (per org): bound row/attempt/preview sizes + TTL for stale org keys.
    queue_meta_keys = [
        key
        for key in list(session_state.keys())
        if key.startswith("admin_queue_audit_tab7_") and key.endswith("_meta")
    ]
    for meta_key in queue_meta_keys:
        meta = session_state.get(meta_key) or {}
        if not isinstance(meta, dict):
            continue

        cached_at = float(meta.get("_cached_at_ts", 0) or 0)
        if cached_at <= 0:
            cached_at = now_ts
            meta = dict(meta)
            meta["_cached_at_ts"] = cached_at

        is_current_org_key = meta_key.startswith(f"admin_queue_audit_tab7_{str(current_org or '').strip()}_")
        if (not is_current_org_key) and ((now_ts - cached_at) > ADMIN_QUEUE_AUDIT_CACHE_TTL_SECONDS):
            session_state.pop(meta_key, None)
            rows_key = meta_key[:-5] + "_rows"
            session_state.pop(rows_key, None)
            continue

        attempts = _trim_attempts_for_storage(meta.get("attempts"))
        raw_preview_rows = meta.get("raw_preview_rows")
        if isinstance(raw_preview_rows, list):
            raw_preview_rows = raw_preview_rows[:ADMIN_QUEUE_AUDIT_MAX_PREVIEW_ROWS]
        else:
            raw_preview_rows = []
        meta["attempts"] = attempts
        meta["raw_preview_rows"] = raw_preview_rows
        session_state[meta_key] = meta

        rows_key = meta_key[:-5] + "_rows"
        rows = session_state.get(rows_key)
        if isinstance(rows, list) and len(rows) > ADMIN_QUEUE_AUDIT_MAX_ROWS:
            session_state[rows_key] = rows[:ADMIN_QUEUE_AUDIT_MAX_ROWS]
            meta["warning"] = str(meta.get("warning") or "").strip() or (
                f"Sonuçlar bellek koruması için ilk {ADMIN_QUEUE_AUDIT_MAX_ROWS} satır ile sınırlandı."
            )
            meta["_rows_truncated"] = True
            meta["_rows_original_count"] = len(rows)
            session_state[meta_key] = meta


def render_admin_panel_service(context: Dict[str, Any]) -> None:
    """Render admin panel page using injected app context."""
    bind_context(globals(), context)
    current_org = str((st.session_state.get("app_user") or {}).get("org_code") or globals().get("org") or "default").strip() or "default"
    _prune_admin_runtime_caches(st.session_state, current_org)
    st.title(f"🛡️ {get_text(lang, 'admin_panel')}")
    
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([f"📊 {get_text(lang, 'api_usage')}", f"📋 {get_text(lang, 'error_logs')}", "🧪 Diagnostics", f"🔌 {get_text(lang, 'manual_disconnect')}", f"👥 {get_text(lang, 'group_management')}", "🔍 Kullanıcı Arama", "🧾 Kuyruk Üyelik Değişimleri"])
    
    with tab1:
        stats = monitor.get_stats()
        st.subheader(get_text(lang, "general_status"))
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(get_text(lang, "total_api_calls"), stats["total_calls"])
        c2.metric(get_text(lang, "total_errors"), stats["error_count"])
        uptime_hours = stats["uptime_seconds"] / 3600
        c3.metric(get_text(lang, "uptime"), f"{uptime_hours:.1f} sa")
        avg_rate = monitor.get_avg_rate_per_minute()
        recent_rate = monitor.get_rate_per_minute(minutes=1)
        c4.metric("API/Dk", f"{recent_rate:.1f}", help=f"Son 1 dk: {recent_rate:.1f} | Ortalama: {avg_rate:.1f}")
        
        st.divider()
        st.subheader(get_text(lang, "endpoint_usage"))
        if stats["endpoint_stats"]:
            df_endpoints = pd.DataFrame([
                {"Endpoint": k, "Adet": v} for k, v in stats["endpoint_stats"].items()
            ]).sort_values("Adet", ascending=False)
            # Ensure x and y are passed correctly to bar_chart
            df_endpoints = sanitize_numeric_df(df_endpoints)
            st.bar_chart(df_endpoints.set_index("Endpoint"))
        else:
            st.info("Henüz API çağrısı kaydedilmedi.")
        
        st.divider()
        st.subheader(get_text(lang, "minutely_traffic"))
        minutely_window = 60
        minutely = monitor.get_minutely_stats(minutes=minutely_window)
        if minutely:
            now_dt = datetime.now().replace(second=0, microsecond=0)
            start_dt = now_dt - timedelta(minutes=minutely_window - 1)
            timeline = pd.date_range(start=start_dt, end=now_dt, freq="min")
            counts = {}
            for k, v in minutely.items():
                ts = pd.to_datetime(k, errors="coerce")
                if pd.notna(ts):
                    counts[ts.floor("min")] = int(v or 0)
            df_minutely = pd.DataFrame({
                "Zaman": timeline,
                "İstek Adet": [int(counts.get(ts, 0) or 0) for ts in timeline],
            })
            df_minutely = sanitize_numeric_df(df_minutely)
            render_24h_time_line_chart(df_minutely, "Zaman", ["İstek Adet"], aggregate_by_label="sum")
        else:
            st.info("Son 60 dakikada trafik yok.")

        st.subheader(get_text(lang, "hourly_traffic_24h"))
        hourly = monitor.get_hourly_stats()
        if hourly:
            now_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
            start_hour = now_hour - timedelta(hours=23)
            timeline = pd.date_range(start=start_hour, end=now_hour, freq="h")
            counts = {}
            for k, v in hourly.items():
                ts = pd.to_datetime(k, errors="coerce")
                if pd.notna(ts):
                    counts[ts.replace(minute=0, second=0, microsecond=0)] = int(v or 0)
            df_hourly = pd.DataFrame({
                "Zaman": timeline,
                "İstek Adet": [int(counts.get(ts, 0) or 0) for ts in timeline],
            })
            df_hourly = sanitize_numeric_df(df_hourly)
            render_24h_time_line_chart(df_hourly, "Zaman", ["İstek Adet"], aggregate_by_label="sum")
        else:
            st.info("Son 24 saatte trafik yok.")

        st.divider()
    with tab2:
        st.subheader(get_text(lang, "system_errors"))
        errors = monitor.get_errors()
        if errors:
            for idx, err in enumerate(errors):
                with st.expander(f"❌ {err['timestamp'].strftime('%H:%M:%S')} - {err['module']}: {err['message']}", expanded=idx==0):
                    st.code(err['details'], language="json")
        else:
            st.success("Sistemde kayıtlı hata bulunmuyor.")

    with tab3:
        _start_memory_monitor(sample_interval=10, max_samples=720)
        st.subheader("Uygulama Kontrol")
        st.warning("Bu işlem uygulamayı yeniden başlatır. Aktif kullanıcı oturumları kısa süreli kesilir.")
        st.caption("URL tetikleme: `/rebootme` veya `/?rebootme=1` (aktif oturum gerektirmez).")
        st.caption("Not: URL tetiklemesi doğrudan reboot başlatır.")
        reboot_confirm = st.checkbox("Uygulamayı yeniden başlatmayı onaylıyorum", key="admin_reboot_confirm")
        if st.button("🔄 Uygulamayı Reboot Et", type="primary", key="admin_reboot_btn", disabled=not reboot_confirm):
            app_user = st.session_state.get('app_user', {}) or {}
            admin_user = app_user.get('username', 'unknown')
            admin_org = app_user.get('org_code', 'default')
            logger.warning(f"[ADMIN REBOOT] Restart requested by {admin_user}")
            reboot_event_writer = globals().get("_append_reboot_event")
            if callable(reboot_event_writer):
                try:
                    reboot_event_writer(
                        source="admin-panel",
                        username=admin_user,
                        org_code=admin_org,
                        note="Admin Panel reboot button.",
                    )
                except Exception:
                    pass
            st.success("Reboot isteği alındı. Uygulama yeniden başlatılıyor...")
            _soft_memory_cleanup()
            _silent_restart(reason="admin-panel-button")

        reboot_event_reader = globals().get("_get_reboot_events")
        reboot_events = []
        if callable(reboot_event_reader):
            try:
                reboot_events = reboot_event_reader(limit=50) or []
            except Exception:
                reboot_events = []
        st.markdown("### 🗒️ Reboot Notları")
        if reboot_events:
            rows = []
            for item in reboot_events:
                if not isinstance(item, dict):
                    continue
                rows.append({
                    "Tarih/Saat": item.get("timestamp") or "-",
                    "Kaynak": item.get("source") or "-",
                    "Kullanıcı": item.get("username") or "-",
                    "Org": item.get("org_code") or "-",
                    "Not": item.get("note") or "-",
                })
            if rows:
                st.dataframe(pd.DataFrame(rows), width='stretch')
            else:
                st.info("Henüz reboot kaydı yok.")
        else:
            st.info("Henüz reboot kaydı yok.")

        st.divider()
        st.subheader("Sistem Durumu")
        proc = psutil.Process(os.getpid())
        try:
            rss_mb = proc.memory_info().rss / (1024 * 1024)
        except Exception:
            rss_mb = 0
        try:
            cpu_pct = proc.cpu_percent(interval=0.1)
        except Exception:
            cpu_pct = 0
        thread_count = len(threading.enumerate())
        c1, c2, c3 = st.columns(3)
        c1.metric("RSS Bellek (MB)", f"{rss_mb:.1f}")
        c2.metric("CPU %", f"{cpu_pct:.1f}")
        c3.metric("Thread", thread_count)

        st.divider()
        st.subheader("Disk Bakımı")
        if st.button("🧹 Geçici Dosyaları Temizle", key="admin_temp_cleanup_btn", width='stretch'):
            with st.spinner("Geçici dosyalar temizleniyor..."):
                cleanup_result = _maybe_periodic_temp_cleanup(
                    force=True,
                    max_age_hours=TEMP_FILE_MANUAL_MAX_AGE_HOURS,
                ) or {}
            st.session_state["admin_temp_cleanup_result"] = cleanup_result
            st.session_state["admin_temp_cleanup_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        last_temp_cleanup = st.session_state.get("admin_temp_cleanup_result")
        if last_temp_cleanup:
            run_ts = st.session_state.get("admin_temp_cleanup_ts", "-")
            removed_files = int(last_temp_cleanup.get("removed_files", 0) or 0)
            truncated_files = int(last_temp_cleanup.get("truncated_files", 0) or 0)
            removed_dirs = int(last_temp_cleanup.get("removed_dirs", 0) or 0)
            freed_text = _format_bytes(last_temp_cleanup.get("freed_bytes", 0))
            error_count = len(last_temp_cleanup.get("errors", []) or [])
            st.caption(
                f"Son temizlik: {run_ts} | Silinen dosya: {removed_files} | "
                f"Sıfırlanan dosya: {truncated_files} | Silinen klasör: {removed_dirs} | Açılan alan: {freed_text}"
            )
            if error_count > 0:
                st.warning(f"Temizlik sırasında {error_count} dosya/klasör silinemedi.")

        st.divider()
        st.subheader("DataManager Cache Durumu")
        dm = st.session_state.get("data_manager")
        if dm:
            c1, c2, c3 = st.columns(3)
            c1.metric("Obs Cache", len(getattr(dm, "obs_data_cache", {}) or {}))
            c2.metric("Daily Cache", len(getattr(dm, "daily_data_cache", {}) or {}))
            c3.metric("Agent Detail Cache", len(getattr(dm, "agent_details_cache", {}) or {}))
            st.write(f"Queue Members Cache: {len(getattr(dm, 'queue_members_cache', {}) or {})}")
            st.write(f"Last Update: {datetime.fromtimestamp(getattr(dm, 'last_update_time', 0)).strftime('%H:%M:%S') if getattr(dm, 'last_update_time', 0) else 'N/A'}")
        else:
            st.info("DataManager bulunamadi.")

        st.divider()
        st.subheader("API Trafik")
        avg_rate = monitor.get_avg_rate_per_minute()
        recent_rate = monitor.get_rate_per_minute(minutes=1)
        st.write(f"Son 1 dk: {recent_rate:.1f} istek/dk")
        st.write(f"Ortalama: {avg_rate:.1f} istek/dk")

        st.divider()
        st.subheader("Bellek Trendi")
        store = _shared_memory_store()
        with store["lock"]:
            samples = list(store.get("samples") or [])
        if samples:
            df_mem = pd.DataFrame(samples)
            df_mem["timestamp"] = pd.to_datetime(df_mem["timestamp"], errors="coerce")
            df_mem = df_mem.dropna(subset=["timestamp"])
            if not df_mem.empty:
                render_24h_time_line_chart(df_mem, "timestamp", ["rss_mb"], include_seconds=True, aggregate_by_label="last")
        else:
            st.info("Bellek örneği henüz yok.")

        st.divider()
        st.subheader("🔍 Bellek Kaynak Analizi (RSS Şişme Tespiti)")
        
        # Calculate memory usage of each component
        memory_breakdown = []
        
        # 1. Session State
        try:
            session_keys = list(st.session_state.keys())
            session_size_estimate = 0
            session_details = []
            for key in session_keys:
                try:
                    val = st.session_state[key]
                    size_kb = sys.getsizeof(val) / 1024
                    if hasattr(val, '__dict__'):
                        size_kb += sys.getsizeof(val.__dict__) / 1024
                    if isinstance(val, (dict, list)):
                        # Deep size estimate for collections
                        size_kb = len(json.dumps(val, default=str)) / 1024 if size_kb < 1 else size_kb
                    session_size_estimate += size_kb
                    if size_kb > 10:  # Only show items > 10KB
                        session_details.append({"key": key, "size_kb": size_kb, "type": type(val).__name__})
                except:
                    pass
            memory_breakdown.append({"Kaynak": "Session State", "Boyut (KB)": session_size_estimate, "Öğe Sayısı": len(session_keys)})
        except:
            pass
        
        # 2. Notification Managers
        try:
            org_code = st.session_state.app_user.get('org_code', 'default') if st.session_state.app_user else 'default'
            store = _shared_notif_store()
            with store["lock"]:
                call_nm = store["call"].get(org_code)
                agent_nm = store["agent"].get(org_code)
                global_nm = store["global"].get(org_code)
            
            # Waiting calls cache
            if call_nm:
                wc = getattr(call_nm, 'waiting_calls', {}) or {}
                wc_size = len(json.dumps(list(wc.values()), default=str)) / 1024 if wc else 0
                memory_breakdown.append({"Kaynak": "Waiting Calls Cache", "Boyut (KB)": wc_size, "Öğe Sayısı": len(wc)})
            
            # User presence/routing cache
            if agent_nm:
                up = getattr(agent_nm, 'user_presence', {}) or {}
                ur = getattr(agent_nm, 'user_routing', {}) or {}
                ac = getattr(agent_nm, 'active_calls', {}) or {}
                qm = getattr(agent_nm, 'queue_members_cache', {}) or {}
                
                up_size = len(json.dumps(up, default=str)) / 1024 if up else 0
                ur_size = len(json.dumps(ur, default=str)) / 1024 if ur else 0
                ac_size = len(json.dumps(list(ac.values()), default=str)) / 1024 if ac else 0
                qm_size = len(json.dumps(qm, default=str)) / 1024 if qm else 0
                
                memory_breakdown.append({"Kaynak": "User Presence Cache", "Boyut (KB)": up_size, "Öğe Sayısı": len(up)})
                memory_breakdown.append({"Kaynak": "User Routing Cache", "Boyut (KB)": ur_size, "Öğe Sayısı": len(ur)})
                memory_breakdown.append({"Kaynak": "Active Calls Cache", "Boyut (KB)": ac_size, "Öğe Sayısı": len(ac)})
                memory_breakdown.append({"Kaynak": "Queue Members Cache", "Boyut (KB)": qm_size, "Öğe Sayısı": len(qm)})
            
            # Global conversations
            if global_nm:
                gc = getattr(global_nm, 'active_conversations', {}) or {}
                gc_size = len(json.dumps(list(gc.values()), default=str)) / 1024 if gc else 0
                memory_breakdown.append({"Kaynak": "Global Conversations Cache", "Boyut (KB)": gc_size, "Öğe Sayısı": len(gc)})
        except:
            pass
        
        # 3. DataManager Caches
        try:
            dm = st.session_state.get("data_manager")
            if dm:
                obs = getattr(dm, 'obs_data_cache', {}) or {}
                daily = getattr(dm, 'daily_data_cache', {}) or {}
                agent = getattr(dm, 'agent_details_cache', {}) or {}
                qmem = getattr(dm, 'queue_members_cache', {}) or {}
                
                obs_size = len(json.dumps(obs, default=str)) / 1024 if obs else 0
                daily_size = len(json.dumps(daily, default=str)) / 1024 if daily else 0
                agent_size = len(json.dumps(agent, default=str)) / 1024 if agent else 0
                qmem_size = len(json.dumps(qmem, default=str)) / 1024 if qmem else 0
                
                memory_breakdown.append({"Kaynak": "DM Obs Cache", "Boyut (KB)": obs_size, "Öğe Sayısı": len(obs)})
                memory_breakdown.append({"Kaynak": "DM Daily Cache", "Boyut (KB)": daily_size, "Öğe Sayısı": len(daily)})
                memory_breakdown.append({"Kaynak": "DM Agent Cache", "Boyut (KB)": agent_size, "Öğe Sayısı": len(agent)})
                memory_breakdown.append({"Kaynak": "DM Queue Members", "Boyut (KB)": qmem_size, "Öğe Sayısı": len(qmem)})
        except:
            pass
        
        # 4. Shared Seed Store
        try:
            seed_store = _shared_seed_store()
            with seed_store["lock"]:
                orgs = seed_store.get("orgs", {})
                for org_key, org_data in orgs.items():
                    call_seed = org_data.get("call_seed_data", [])
                    ivr_calls = org_data.get("ivr_calls_data", [])
                    call_meta = org_data.get("call_meta", {})
                    agent_pres = org_data.get("agent_presence", {})
                    agent_rout = org_data.get("agent_routing", {})
                    
                    cs_size = len(json.dumps(call_seed, default=str)) / 1024 if call_seed else 0
                    ivr_size = len(json.dumps(ivr_calls, default=str)) / 1024 if ivr_calls else 0
                    cm_size = len(json.dumps(call_meta, default=str)) / 1024 if call_meta else 0
                    ap_size = len(json.dumps(agent_pres, default=str)) / 1024 if agent_pres else 0
                    ar_size = len(json.dumps(agent_rout, default=str)) / 1024 if agent_rout else 0
                    
                    memory_breakdown.append({"Kaynak": f"Seed: Call Data ({org_key})", "Boyut (KB)": cs_size, "Öğe Sayısı": len(call_seed)})
                    memory_breakdown.append({"Kaynak": f"Seed: IVR Calls ({org_key})", "Boyut (KB)": ivr_size, "Öğe Sayısı": len(ivr_calls)})
                    memory_breakdown.append({"Kaynak": f"Seed: Call Meta ({org_key})", "Boyut (KB)": cm_size, "Öğe Sayısı": len(call_meta)})
                    memory_breakdown.append({"Kaynak": f"Seed: Agent Presence ({org_key})", "Boyut (KB)": ap_size, "Öğe Sayısı": len(agent_pres)})
                    memory_breakdown.append({"Kaynak": f"Seed: Agent Routing ({org_key})", "Boyut (KB)": ar_size, "Öğe Sayısı": len(agent_rout)})
        except:
            pass
        
        # 5. Monitor Logs
        try:
            api_log = getattr(monitor, 'api_calls_log', []) or []
            error_log = getattr(monitor, 'error_logs', []) or []
            api_size = len(json.dumps(api_log, default=str)) / 1024 if api_log else 0
            err_size = len(json.dumps(error_log, default=str)) / 1024 if error_log else 0
            memory_breakdown.append({"Kaynak": "API Calls Log (Memory)", "Boyut (KB)": api_size, "Öğe Sayısı": len(api_log)})
            memory_breakdown.append({"Kaynak": "Error Log (Memory)", "Boyut (KB)": err_size, "Öğe Sayısı": len(error_log)})
        except:
            pass
        
        # Display breakdown
        if memory_breakdown:
            df_breakdown = pd.DataFrame(memory_breakdown)
            df_breakdown = df_breakdown.sort_values("Boyut (KB)", ascending=False)
            df_breakdown["Boyut (KB)"] = df_breakdown["Boyut (KB)"].round(1)
            
            # Show top memory consumers
            total_kb = df_breakdown["Boyut (KB)"].sum()
            st.metric("Toplam Cache Boyutu", f"{total_kb:.1f} KB ({total_kb/1024:.2f} MB)")
            
            # Find the biggest consumer
            if not df_breakdown.empty:
                top = df_breakdown.iloc[0]
                pct = (top["Boyut (KB)"] / total_kb * 100) if total_kb > 0 else 0
                if pct > 30:
                    st.warning(f"⚠️ En büyük kaynak: **{top['Kaynak']}** - {top['Boyut (KB)']:.1f} KB ({pct:.1f}%)")
                else:
                    st.success(f"✅ En büyük kaynak: **{top['Kaynak']}** - {top['Boyut (KB)']:.1f} KB ({pct:.1f}%)")
            
            st.dataframe(df_breakdown, width='stretch', hide_index=True)
            
            # Show session state details if significant
            if session_details:
                with st.expander("📦 Session State Detayları (>10 KB)"):
                    df_session = pd.DataFrame(session_details).sort_values("size_kb", ascending=False)
                    df_session["size_kb"] = df_session["size_kb"].round(1)
                    st.dataframe(df_session, width='stretch', hide_index=True)
        else:
            st.info("Bellek analizi yapılamadı.")

    with tab4:
        st.subheader(f"🔌 {get_text(lang, 'manual_disconnect')}")
        st.warning(get_text(lang, "disconnect_warning"))
        
        if not st.session_state.get('api_client'):
            st.error(get_text(lang, "disconnect_genesys_required"))
        else:
            import re as _re_disconnect
            _uuid_pattern = _re_disconnect.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
            
            st.info(
                "⚠️ Bu uygulama **Client Credentials** OAuth kullanmaktadır. "
                "Etkileşim sonlandırma işlemi için **kullanıcı bağlamı olan bir token** gereklidir.\n\n"
                "**Token nasıl alınır:**\n"
                "1. [Genesys Cloud Developer Center](https://developer.mypurecloud.ie) → API Explorer'a gidin\n"
                "2. Sağ üstten Genesys Cloud hesabınızla giriş yapın\n"
                "3. Herhangi bir API çağrısında Authorization başlığındaki `Bearer` sonrası token'ı kopyalayın\n"
                "4. Aşağıdaki **User Token** alanına yapıştırın"
            )
            
            user_token = st.text_input(
                "🔑 User Token (Bearer)",
                type="password",
                placeholder="Genesys Cloud user token yapıştırın",
                help="Genesys Cloud Developer Center'dan aldığınız kullanıcı OAuth token'ı",
                key="admin_disconnect_user_token"
            )
            
            dc_col1, dc_col2 = st.columns([3, 1])
            with dc_col1:
                disconnect_id = st.text_input(
                    get_text(lang, "disconnect_interaction_id"),
                    placeholder="e.g. 3fa85f64-5717-4562-b3fc-2c963f66afa6",
                    help=get_text(lang, "disconnect_interaction_id_help"),
                    key="admin_disconnect_id"
                )
            
            with dc_col2:
                st.markdown("<br>", unsafe_allow_html=True)
                disconnect_clicked = st.button(
                    f"🔌 {get_text(lang, 'disconnect_btn')}",
                    type="primary",
                    use_container_width=True,
                    key="admin_disconnect_btn"
                )
            
            if disconnect_clicked:
                disconnect_id_clean = disconnect_id.strip() if disconnect_id else ""
                user_token_clean = user_token.strip() if user_token else ""
                
                if not disconnect_id_clean:
                    st.error(get_text(lang, "disconnect_empty_id"))
                elif not _uuid_pattern.match(disconnect_id_clean):
                    st.error(get_text(lang, "disconnect_invalid_id"))
                elif not user_token_clean:
                    st.error("Lütfen bir User Token girin. Client Credentials ile bu işlem yapılamaz.")
                else:
                    try:
                        # Use user token instead of client credentials
                        user_api_client = {
                            "access_token": user_token_clean,
                            "api_host": st.session_state.api_client.get("api_host", "https://api.mypurecloud.ie"),
                            "region": st.session_state.api_client.get("region", "mypurecloud.ie"),
                        }
                        api = GenesysAPI(user_api_client)
                        
                        with st.spinner("Etkileşim sonlandırılıyor..."):
                            result = api.disconnect_conversation(disconnect_id_clean)
                        
                        admin_user = st.session_state.get('app_user', {}).get('username', 'unknown')
                        media_type = result.get("media_type", "unknown")
                        
                        disconnected = result.get("disconnected", [])
                        skipped = result.get("skipped", [])
                        errors = result.get("errors", [])
                        
                        if disconnected:
                            st.success(f"✅ {get_text(lang, 'disconnect_success')} (ID: {disconnect_id_clean} | Tip: {media_type})")
                            for d in disconnected:
                                action = d.get('action', '')
                                if 'wrapup_submitted' in action:
                                    action_txt = " (Wrap-up kodu gönderildi)"
                                elif 'wrapup_fallback' in action:
                                    action_txt = " (Wrap-up ile kapatıldı)"
                                elif 'wrapup' in action:
                                    action_txt = " (Wrap-up atlandı)"
                                else:
                                    action_txt = ""
                                st.write(f"  ✔️ {d['purpose']}: {d['name']}{action_txt}")
                            logging.info(f"[ADMIN DISCONNECT] Interaction {disconnect_id_clean} — {len(disconnected)} participant(s) disconnected by {admin_user}")
                        
                        if skipped:
                            with st.expander(f"⏭️ Atlanan katılımcılar ({len(skipped)})", expanded=False):
                                for s in skipped:
                                    reason = s.get('reason', '')
                                    reason_txt = "Sistem katılımcısı" if reason == "system" else "Aktif oturum yok"
                                    st.write(f"  ⏭️ {s['purpose']}: {s['name']} — {reason_txt} ({s['state']})")
                        
                        if errors:
                            for er in errors:
                                st.error(f"❌ {er['purpose']}: {er['name']} — {er['error']}")
                            logging.error(f"[ADMIN DISCONNECT] Interaction {disconnect_id_clean} — {len(errors)} error(s) by {admin_user}")
                        
                        if not disconnected and not errors:
                            st.info("Tüm katılımcılar zaten sonlanmış durumda.")
                    except Exception as e:
                        error_msg = str(e)
                        st.error(f"❌ {get_text(lang, 'disconnect_error')}: {error_msg}")
                        logging.error(f"[ADMIN DISCONNECT] Failed to disconnect {disconnect_id_clean}: {error_msg}")

    with tab5:
        st.subheader(f"👥 {get_text(lang, 'group_management')}")
        
        if not st.session_state.get('api_client'):
            st.error(get_text(lang, "disconnect_genesys_required"))
        else:
            try:
                api = GenesysAPI(st.session_state.api_client)
                
                # Fetch groups
                current_org = str(org or "").strip()
                cached_groups_org = str(st.session_state.get("admin_groups_cache_org") or "").strip()
                need_groups_refresh = (
                    ('admin_groups_cache' not in st.session_state)
                    or bool(st.session_state.get('admin_groups_refresh'))
                    or (cached_groups_org != current_org)
                )
                if need_groups_refresh:
                    with st.spinner(get_text(lang, "group_loading")):
                        st.session_state.admin_groups_cache = api.get_groups()
                        st.session_state.admin_groups_cache_org = current_org
                        st.session_state.admin_groups_refresh = False
                
                groups = st.session_state.get('admin_groups_cache', [])
                
                if not groups:
                    st.info(get_text(lang, "group_no_groups"))
                else:
                    _prune_admin_group_member_cache(max_entries=40)
                    # Refresh button
                    if st.button("🔄 Grupları Yenile", key="refresh_groups_btn"):
                        st.session_state.admin_groups_refresh = True
                        st.rerun()
                    
                    group_options = {g['id']: f"{g['name']} ({g['memberCount']} üye)" for g in groups}
                    if True:

                        # --- Bulk Add Users to Multiple Groups ---
                        st.divider()
                        st.markdown("### ⚡ Çoklu Gruba Toplu Üye Ekle")

                        # Fetch all users for bulk selection
                        if 'admin_all_users_cache' not in st.session_state:
                            with st.spinner("Kullanıcılar yükleniyor..."):
                                st.session_state.admin_all_users_cache = api.get_users()
                        all_users = st.session_state.get('admin_all_users_cache', [])

                        multi_group_options = {g['id']: f"{g['name']} ({g.get('memberCount', 0)} üye)" for g in groups}
                        active_user_options = {
                            u['id']: f"{u.get('name', '')} ({u.get('email', '')})"
                            for u in all_users
                            if u.get('id') and u.get('state') == 'active'
                        }

                        multi_group_search = st.text_input(
                            "🔍 Toplu ekleme için kullanıcı ara",
                            placeholder="İsim veya e-posta ile filtrele",
                            key="admin_multi_group_user_search"
                        )
                        if multi_group_search:
                            filtered_multi_user_options = {
                                uid: label for uid, label in active_user_options.items()
                                if multi_group_search.lower() in label.lower()
                            }
                        else:
                            filtered_multi_user_options = active_user_options

                        selected_multi_user_ids = st.multiselect(
                            "Eklenecek kullanıcılar (aktif)",
                            options=list(filtered_multi_user_options.keys()),
                            format_func=lambda x: filtered_multi_user_options.get(x, active_user_options.get(x, x)),
                            key="admin_multi_group_user_ids"
                        )
                        selected_multi_group_ids = st.multiselect(
                            "Eklenecek gruplar (birden fazla seçilebilir)",
                            options=list(multi_group_options.keys()),
                            format_func=lambda x: multi_group_options.get(x, x),
                            key="admin_multi_group_group_ids"
                        )

                        preview_group_options = selected_multi_group_ids if selected_multi_group_ids else list(multi_group_options.keys())
                        preview_group_id = st.selectbox(
                            "Üyeleri listelenecek grup",
                            options=preview_group_options,
                            format_func=lambda x: multi_group_options.get(x, x),
                            key="admin_multi_group_preview_group"
                        )

                        if preview_group_id:
                            preview_cache_key = f"admin_group_members_{preview_group_id}"
                            if preview_cache_key not in st.session_state or st.session_state.get(f'refresh_{preview_cache_key}'):
                                st.session_state[preview_cache_key] = api.get_group_members(preview_group_id)
                                st.session_state[f'refresh_{preview_cache_key}'] = False
                            preview_members = st.session_state.get(preview_cache_key, [])
                            st.caption(f"Seçilen grubun üye sayısı: {len(preview_members)}")
                            if preview_members:
                                preview_df = pd.DataFrame(preview_members)
                                cols = [c for c in ["name", "email", "state"] if c in preview_df.columns]
                                if cols:
                                    preview_df = preview_df[cols].copy()
                                    preview_df.columns = ["Ad", "E-posta", "Durum"][:len(cols)]
                                st.dataframe(preview_df, width='stretch', hide_index=True)
                            else:
                                st.info("Seçilen grupta üye bulunamadı.")

                        if selected_multi_user_ids and selected_multi_group_ids and st.button(
                            "🚀 Seçili Kullanıcıları Seçili Gruplara Ekle",
                            type="primary",
                            key="admin_multi_group_add_submit_btn"
                        ):
                            with st.spinner("Toplu grup üyeliği ekleniyor..."):
                                total_added = 0
                                total_skipped_existing = 0
                                failed_groups = []

                                for gid in selected_multi_group_ids:
                                    g_name = multi_group_options.get(gid, gid)
                                    members_cache_key = f"admin_group_members_{gid}"
                                    try:
                                        group_members = api.get_group_members(gid)
                                        st.session_state[members_cache_key] = group_members
                                        existing_ids = {m.get("id") for m in group_members if m.get("id")}
                                        to_add_ids = [uid for uid in selected_multi_user_ids if uid not in existing_ids]

                                        total_skipped_existing += (len(selected_multi_user_ids) - len(to_add_ids))
                                        if to_add_ids:
                                            api.add_group_members(gid, to_add_ids)
                                            total_added += len(to_add_ids)

                                        st.session_state[f'refresh_{members_cache_key}'] = True
                                    except Exception as e:
                                        failed_groups.append((g_name, str(e)))

                                if total_added:
                                    st.success(f"✅ Toplam {total_added} yeni grup üyeliği eklendi.")
                                if total_skipped_existing:
                                    st.info(f"ℹ️ {total_skipped_existing} üyelik zaten mevcut olduğu için atlandı.")
                                if failed_groups:
                                    st.warning(f"⚠️ {len(failed_groups)} grupta hata oluştu.")
                                    for g_name, err in failed_groups:
                                        st.caption(f"❌ {g_name}: {err}")

                                st.session_state.admin_groups_refresh = True
                                if not failed_groups:
                                    st.rerun()
                        
                        # --- Assign Group to Queues ---
                        st.divider()
                        st.markdown(f"### 📋 {get_text(lang, 'group_to_queue')}")
                        selected_group_for_queue_id = st.selectbox(
                            get_text(lang, "group_select"),
                            options=list(group_options.keys()),
                            format_func=lambda x: group_options.get(x, x),
                            key="admin_group_select_for_queue"
                        )
                        selected_group_for_queue = next((g for g in groups if g['id'] == selected_group_for_queue_id), None)
                        if selected_group_for_queue:
                            st.info(f"**{selected_group_for_queue['name']}** grubu, seçtiğiniz kuyruklara üye grup olarak eklenecek veya çıkarılacaktır.")
                        
                        # Fetch queues
                        if 'admin_queues_cache' not in st.session_state:
                            with st.spinner("Kuyruklar yükleniyor..."):
                                st.session_state.admin_queues_cache = api.get_queues()
                        
                        all_queues = st.session_state.get('admin_queues_cache', [])
                        
                        if all_queues:
                            queue_options = {q['id']: q['name'] for q in all_queues}
                            
                            # Search filter for queues
                            queue_search = st.text_input("🔍 Kuyruk Ara", placeholder="Kuyruk adı ile filtrele", key=f"queue_search_{selected_group_for_queue_id}")
                            
                            if queue_search:
                                filtered_queues = {qid: qname for qid, qname in queue_options.items() if queue_search.lower() in qname.lower()}
                            else:
                                filtered_queues = queue_options
                            
                            selected_queue_ids = st.multiselect(
                                get_text(lang, "group_queue_select"),
                                options=list(filtered_queues.keys()),
                                format_func=lambda x: filtered_queues.get(x, queue_options.get(x, x)),
                                key=f"queue_assign_{selected_group_for_queue_id}"
                            )
                            
                            col_add_q, col_remove_q = st.columns(2)
                            
                            with col_add_q:
                                if selected_queue_ids and st.button(f"➕ {len(selected_queue_ids)} Kuyruğa Ekle", type="primary", key=f"add_to_queue_btn_{selected_group_for_queue_id}"):
                                    with st.spinner("Grup kuyruklara ekleniyor..."):
                                        results = api.add_group_to_queues(selected_group_for_queue_id, selected_queue_ids)
                                        success_count = sum(1 for r in results.values() if r['success'])
                                        fail_count = sum(1 for r in results.values() if not r['success'])
                                        
                                        if fail_count == 0:
                                            st.success(f"✅ {get_text(lang, 'group_queue_success')} ({success_count} kuyruk)")
                                        elif success_count == 0:
                                            st.error(f"❌ {get_text(lang, 'group_queue_error')}")
                                        else:
                                            st.warning(f"⚠️ {get_text(lang, 'group_queue_partial')} ✅ {success_count} / ❌ {fail_count}")
                                        
                                        for qid, result in results.items():
                                            qname = queue_options.get(qid, qid)
                                            if result.get('success') and result.get('already'):
                                                st.caption(f"ℹ️ {qname}: Grup zaten bu kuyruğun üyesi")
                                            elif result.get('success'):
                                                st.caption(f"✅ {qname}")
                                            else:
                                                st.caption(f"❌ {qname}: {result.get('error', '')}")
                                        
                                        logging.info(f"[ADMIN GROUP] Added group '{selected_group_for_queue.get('name', selected_group_for_queue_id)}' to {success_count}/{len(selected_queue_ids)} queues")
                            
                            with col_remove_q:
                                if selected_queue_ids and st.button(f"🗑️ {len(selected_queue_ids)} Kuyruktan Çıkar", type="secondary", key=f"remove_from_queue_btn_{selected_group_for_queue_id}"):
                                    with st.spinner("Grup kuyruklardan çıkarılıyor..."):
                                        results = api.remove_group_from_queues(selected_group_for_queue_id, selected_queue_ids)
                                        success_count = sum(1 for r in results.values() if r['success'])
                                        fail_count = sum(1 for r in results.values() if not r['success'])
                                        
                                        if fail_count == 0:
                                            st.success(f"✅ {get_text(lang, 'group_queue_remove_success')} ({success_count} kuyruk)")
                                        elif success_count == 0:
                                            st.error(f"❌ {get_text(lang, 'group_queue_remove_error')}")
                                        else:
                                            st.warning(f"⚠️ {get_text(lang, 'group_queue_partial')} ✅ {success_count} / ❌ {fail_count}")
                                        
                                        for qid, result in results.items():
                                            qname = queue_options.get(qid, qid)
                                            if result.get('success') and result.get('not_found'):
                                                st.caption(f"ℹ️ {qname}: Grup bu kuyruğun üyesi değildi")
                                            elif result.get('success'):
                                                st.caption(f"✅ {qname}")
                                            else:
                                                st.caption(f"❌ {qname}: {result.get('error', '')}")
                                        
                                        logging.info(f"[ADMIN GROUP] Removed group '{selected_group_for_queue.get('name', selected_group_for_queue_id)}' from {success_count}/{len(selected_queue_ids)} queues")

                            # --- Queue Wrap-up Timeout Management ---
                            st.divider()
                            st.markdown("### ⏱️ Kuyruk Bazlı Wrap-up Süresi")
                            st.caption("Kuyrukların wrap-up süresini (saniye) API'den çekip çoklu seçimle toplu güncelleyebilirsiniz.")

                            wrapup_queue_select_key = "admin_wrapup_queue_ids"
                            existing_wrapup_selection = st.session_state.get(wrapup_queue_select_key, [])
                            if not isinstance(existing_wrapup_selection, list):
                                st.session_state[wrapup_queue_select_key] = []

                            selected_wrapup_queue_ids = st.multiselect(
                                "Wrap-up süresi yönetilecek kuyruklar",
                                options=list(queue_options.keys()),
                                format_func=lambda x: queue_options.get(x, x),
                                key=wrapup_queue_select_key,
                            )

                            wrapup_snapshot_key = f"admin_wrapup_snapshot_{current_org}"
                            wrapup_update_result_key = f"admin_wrapup_update_results_{current_org}"
                            if st.button(
                                "📥 Seçili Kuyrukların Süresini Getir",
                                key="admin_wrapup_fetch_btn",
                                disabled=not selected_wrapup_queue_ids,
                                use_container_width=True,
                            ):
                                with st.spinner("Wrap-up süreleri getiriliyor..."):
                                    st.session_state[wrapup_snapshot_key] = api.get_queues_wrapup_timeouts(selected_wrapup_queue_ids)

                            wrapup_snapshot = st.session_state.get(wrapup_snapshot_key, {}) or {}
                            wrapup_rows = []
                            wrapup_errors = []
                            for qid in selected_wrapup_queue_ids:
                                row = wrapup_snapshot.get(qid)
                                if not isinstance(row, dict):
                                    continue
                                if row.get("success"):
                                    queue_name = str(row.get("queue_name") or queue_options.get(qid, qid))
                                    timeout_seconds = row.get("timeout_seconds")
                                    wrapup_rows.append({
                                        "Kuyruk": queue_name,
                                        "Kuyruk ID": qid,
                                        "Wrap-up Süresi (sn)": timeout_seconds if timeout_seconds is not None else "-",
                                        "Kaynak Alan": row.get("field_path") or "-",
                                    })
                                else:
                                    wrapup_errors.append((qid, row.get("error", "Bilinmeyen hata")))

                            if wrapup_rows:
                                st.dataframe(pd.DataFrame(wrapup_rows), width='stretch', hide_index=True)
                            if wrapup_errors:
                                st.warning(f"{len(wrapup_errors)} kuyruk için wrap-up süresi okunamadı.")
                                for qid, err in wrapup_errors[:10]:
                                    st.caption(f"❌ {queue_options.get(qid, qid)}: {err}")

                            target_wrapup_seconds = st.number_input(
                                "Yeni wrap-up süresi (saniye)",
                                min_value=0,
                                max_value=7200,
                                value=int(st.session_state.get("admin_wrapup_timeout_seconds", 30)),
                                step=5,
                                key="admin_wrapup_timeout_seconds",
                            )
                            apply_only_fetched = st.checkbox(
                                "Sadece süresi başarıyla okunan kuyruklara uygula",
                                value=True,
                                key="admin_wrapup_apply_only_fetched",
                            )

                            update_target_queue_ids = list(selected_wrapup_queue_ids)
                            if apply_only_fetched:
                                update_target_queue_ids = [
                                    qid for qid in update_target_queue_ids
                                    if isinstance(wrapup_snapshot.get(qid), dict) and wrapup_snapshot[qid].get("success")
                                ]

                            if st.button(
                                "💾 Seçili Kuyruklara Süreyi Uygula",
                                type="primary",
                                key="admin_wrapup_apply_btn",
                                disabled=not update_target_queue_ids,
                                use_container_width=True,
                            ):
                                with st.spinner("Wrap-up süreleri güncelleniyor..."):
                                    update_results = api.set_queues_wrapup_timeout(
                                        update_target_queue_ids,
                                        int(target_wrapup_seconds),
                                    )
                                st.session_state[wrapup_update_result_key] = update_results
                                try:
                                    refreshed = api.get_queues_wrapup_timeouts(update_target_queue_ids)
                                    merged_snapshot = dict(wrapup_snapshot)
                                    merged_snapshot.update(refreshed)
                                    st.session_state[wrapup_snapshot_key] = merged_snapshot
                                except Exception:
                                    pass

                            wrapup_update_results = st.session_state.get(wrapup_update_result_key, {}) or {}
                            if wrapup_update_results:
                                success_count = sum(
                                    1 for item in wrapup_update_results.values()
                                    if isinstance(item, dict) and item.get("success")
                                )
                                fail_count = max(0, len(wrapup_update_results) - success_count)
                                if success_count:
                                    st.success(f"{success_count} kuyruk için wrap-up süresi güncellendi.")
                                if fail_count:
                                    st.warning(f"{fail_count} kuyruk için wrap-up güncellemesi başarısız.")
                                for qid, item in list(wrapup_update_results.items())[:20]:
                                    if not isinstance(item, dict):
                                        continue
                                    queue_name = str(item.get("queue_name") or queue_options.get(qid, qid))
                                    if item.get("success"):
                                        prev_text = item.get("previous_seconds")
                                        prev_label = f"{prev_text} sn" if prev_text is not None else "-"
                                        st.caption(
                                            f"✅ {queue_name}: {prev_label} → {item.get('updated_seconds', '-') } sn "
                                            f"({item.get('field_path', '-')})"
                                        )
                                    else:
                                        st.caption(f"❌ {queue_name}: {item.get('error', 'Bilinmeyen hata')}")
                        else:
                            st.warning("Kuyruk bulunamadı.")

                        # --- User & Workgroup Inventory + Excel Export ---
                        st.divider()
                        st.markdown("### 📥 Kullanıcı & Workgroup Envanteri")
                        st.caption("Genesys Cloud kullanıcılarını ve bağlı oldukları workgroup (grup) üyeliklerini listeleyip Excel olarak indirebilirsiniz.")

                        export_group_options = {g['id']: g['name'] for g in groups}
                        selected_export_group_ids = st.multiselect(
                            "Envantere dahil edilecek workgroup'lar",
                            options=list(export_group_options.keys()),
                            format_func=lambda x: export_group_options.get(x, x),
                            key="admin_inventory_group_filter"
                        )
                        include_users_without_group = st.checkbox(
                            "Workgroup'u olmayan kullanıcıları da ekle",
                            value=True,
                            key="admin_inventory_include_unassigned"
                        )

                        if st.button("🔄 Envanteri Hazırla", key="admin_inventory_build_btn"):
                            with st.spinner("Kullanıcı/workgroup envanteri hazırlanıyor..."):
                                if 'admin_all_users_cache' not in st.session_state:
                                    st.session_state.admin_all_users_cache = api.get_users()

                                inventory_users = st.session_state.get('admin_all_users_cache', [])
                                users_by_id = {u.get("id"): u for u in inventory_users if u.get("id")}
                                target_group_ids = selected_export_group_ids or list(export_group_options.keys())
                                inventory_rows = []
                                grouped_user_ids = set()

                                for gid in target_group_ids:
                                    group_name = export_group_options.get(gid, gid)
                                    members_cache_key = f"admin_group_members_{gid}"
                                    if members_cache_key not in st.session_state:
                                        st.session_state[members_cache_key] = api.get_group_members(gid)
                                    group_members = st.session_state.get(members_cache_key, [])

                                    for member in group_members:
                                        uid = member.get("id")
                                        if uid:
                                            grouped_user_ids.add(uid)
                                        user_obj = users_by_id.get(uid, {})
                                        inventory_rows.append({
                                            "_user_id": uid or "",
                                            "Username": user_obj.get("name") or member.get("name", ""),
                                            "Email": user_obj.get("email") or member.get("email", ""),
                                            "State": user_obj.get("state") or member.get("state", ""),
                                            "Workgroup Name": group_name
                                        })

                                if include_users_without_group:
                                    for user_obj in inventory_users:
                                        uid = user_obj.get("id")
                                        if uid and uid not in grouped_user_ids:
                                            inventory_rows.append({
                                                "_user_id": uid,
                                                "Username": user_obj.get("name", ""),
                                                "Email": user_obj.get("email", ""),
                                                "State": user_obj.get("state", ""),
                                                "Workgroup Name": ""
                                            })

                                if 'admin_queues_cache' not in st.session_state:
                                    st.session_state.admin_queues_cache = api.get_queues()
                                all_queues_for_inventory = st.session_state.get('admin_queues_cache', [])
                                inventory_user_ids = {
                                    row.get("_user_id")
                                    for row in inventory_rows
                                    if row.get("_user_id")
                                }
                                user_queue_map = api.get_user_queue_map(
                                    user_ids=list(inventory_user_ids),
                                    queues=all_queues_for_inventory
                                )
                                for row in inventory_rows:
                                    uid = row.get("_user_id")
                                    queues = user_queue_map.get(uid, []) if uid else []
                                    row["Queue List"] = ", ".join(queues)

                                inventory_df = pd.DataFrame(inventory_rows)
                                if not inventory_df.empty:
                                    if "_user_id" in inventory_df.columns:
                                        inventory_df = inventory_df.drop(columns=["_user_id"])
                                    sort_cols = [c for c in ["Workgroup Name", "Username"] if c in inventory_df.columns]
                                    if sort_cols:
                                        inventory_df = inventory_df.sort_values(sort_cols).reset_index(drop=True)
                                st.session_state.admin_user_workgroup_inventory_df = inventory_df

                        inventory_df = st.session_state.get("admin_user_workgroup_inventory_df")
                        if isinstance(inventory_df, pd.DataFrame) and not inventory_df.empty:
                            st.dataframe(inventory_df, width='stretch', hide_index=True)
                            st.download_button(
                                "📥 Kullanıcı-Workgroup Excel İndir",
                                data=to_excel(inventory_df),
                                file_name=f"user_workgroup_inventory_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="admin_inventory_download_btn"
                            )
                        elif isinstance(inventory_df, pd.DataFrame):
                            st.info("Seçilen filtrelerle envanter sonucu bulunamadı.")

                        # --- Bulk Assign Users to Queues ---
                        st.divider()
                        st.markdown("### ⚡ Toplu Kuyruk Atama")
                        st.caption("Seçtiğiniz kullanıcıları, seçtiğiniz birden fazla kuyruğa tek seferde atar.")

                        if 'admin_all_users_cache' not in st.session_state:
                            st.session_state.admin_all_users_cache = api.get_users()
                        bulk_all_users = st.session_state.get('admin_all_users_cache', [])
                        bulk_active_users = [u for u in bulk_all_users if u.get("id") and u.get("state") == "active"]

                        bulk_user_options = {u['id']: f"{u.get('name', '')} ({u.get('email', '')})" for u in bulk_active_users}
                        if 'admin_queues_cache' not in st.session_state:
                            st.session_state.admin_queues_cache = api.get_queues()
                        bulk_all_queues = st.session_state.get('admin_queues_cache', [])
                        bulk_queue_options = {q['id']: q.get('name', q['id']) for q in bulk_all_queues}

                        bulk_search = st.text_input(
                            "🔍 Toplu atama için kullanıcı ara",
                            placeholder="İsim veya e-posta ile filtrele",
                            key="admin_bulk_assign_user_search"
                        )
                        if bulk_search:
                            filtered_bulk_user_options = {
                                uid: label for uid, label in bulk_user_options.items()
                                if bulk_search.lower() in label.lower()
                            }
                        else:
                            filtered_bulk_user_options = bulk_user_options

                        selected_bulk_user_ids = st.multiselect(
                            "Atanacak kullanıcılar (aktif)",
                            options=list(filtered_bulk_user_options.keys()),
                            format_func=lambda x: filtered_bulk_user_options.get(x, bulk_user_options.get(x, x)),
                            key="admin_bulk_assign_users"
                        )
                        bulk_group_options = {g['id']: g.get('name', g['id']) for g in groups}
                        selected_source_group_ids = st.multiselect(
                            "Agent çekilecek gruplar",
                            options=list(bulk_group_options.keys()),
                            format_func=lambda x: bulk_group_options.get(x, x),
                            key="admin_bulk_assign_source_groups",
                            help="Seçtiğiniz grupların üyeleri otomatik olarak kullanıcı listesine eklenir."
                        )

                        pulled_user_ids = set()
                        pulled_inactive_count = 0
                        if selected_source_group_ids:
                            active_user_id_set = set(bulk_user_options.keys())
                            for gid in selected_source_group_ids:
                                members_cache_key = f"admin_group_members_{gid}"
                                if members_cache_key not in st.session_state or st.session_state.get(f'refresh_{members_cache_key}'):
                                    st.session_state[members_cache_key] = api.get_group_members(gid)
                                    st.session_state[f'refresh_{members_cache_key}'] = False
                                for member in st.session_state.get(members_cache_key, []) or []:
                                    uid = member.get("id")
                                    if not uid:
                                        continue
                                    if uid in active_user_id_set:
                                        pulled_user_ids.add(uid)
                                    else:
                                        pulled_inactive_count += 1

                        effective_bulk_user_ids = sorted(set(selected_bulk_user_ids) | pulled_user_ids)
                        st.caption(
                            f"Toplam hedef kullanıcı: {len(effective_bulk_user_ids)} "
                            f"(manuel: {len(selected_bulk_user_ids)}, gruptan: {len(pulled_user_ids)})"
                            + (f" | pasif atlanan: {pulled_inactive_count}" if pulled_inactive_count else "")
                        )
                        selected_bulk_queue_ids = st.multiselect(
                            "Atanacak kuyruklar",
                            options=list(bulk_queue_options.keys()),
                            format_func=lambda x: bulk_queue_options.get(x, x),
                            key="admin_bulk_assign_queues"
                        )

                        col_bulk_add, col_bulk_remove = st.columns(2)

                        with col_bulk_add:
                            if effective_bulk_user_ids and selected_bulk_queue_ids and st.button(
                                "🚀 Toplu Kuyruk Ataması Yap",
                                type="primary",
                                key="admin_bulk_assign_submit_btn"
                            ):
                                with st.spinner("Toplu atama yapılıyor..."):
                                    results = api.add_users_to_queues(
                                        user_ids=effective_bulk_user_ids,
                                        queue_ids=selected_bulk_queue_ids
                                    )
                                    success_count = sum(1 for r in results.values() if r.get("success"))
                                    fail_count = sum(1 for r in results.values() if not r.get("success"))
                                    total_added = sum(int(r.get("added", 0)) for r in results.values())
                                    total_skipped = sum(int(r.get("skipped_existing", 0)) for r in results.values())

                                    if fail_count == 0:
                                        st.success(f"✅ Toplu kuyruk ataması tamamlandı. Kuyruk: {success_count}, yeni üyelik: {total_added}")
                                    elif success_count == 0:
                                        st.error("❌ Toplu kuyruk ataması başarısız oldu.")
                                    else:
                                        st.warning(f"⚠️ Kısmi başarı: ✅ {success_count} / ❌ {fail_count} kuyruk")

                                    if total_skipped:
                                        st.info(f"ℹ️ {total_skipped} üyelik zaten mevcut olduğu için atlandı.")

                                    for qid, result in results.items():
                                        qname = bulk_queue_options.get(qid, qid)
                                        if result.get("success"):
                                            st.caption(
                                                f"✅ {qname}: +{result.get('added', 0)} eklendi"
                                                + (f", {result.get('skipped_existing', 0)} zaten üyeydi" if result.get('skipped_existing', 0) else "")
                                            )
                                        else:
                                            st.caption(f"❌ {qname}: {result.get('error', '')}")

                        with col_bulk_remove:
                            if effective_bulk_user_ids and selected_bulk_queue_ids and st.button(
                                "🗑️ Toplu Kuyruk Çıkarma Yap",
                                type="secondary",
                                key="admin_bulk_remove_submit_btn"
                            ):
                                with st.spinner("Toplu kuyruk çıkarma yapılıyor..."):
                                    results = api.remove_users_from_queues(
                                        user_ids=effective_bulk_user_ids,
                                        queue_ids=selected_bulk_queue_ids
                                    )
                                    success_count = sum(1 for r in results.values() if r.get("success"))
                                    fail_count = sum(1 for r in results.values() if not r.get("success"))
                                    total_removed = sum(int(r.get("removed", 0)) for r in results.values())
                                    total_skipped = sum(int(r.get("skipped_missing", 0)) for r in results.values())

                                    if fail_count == 0:
                                        st.success(f"✅ Toplu kuyruk çıkarma tamamlandı. Kuyruk: {success_count}, silinen üyelik: {total_removed}")
                                    elif success_count == 0:
                                        st.error("❌ Toplu kuyruk çıkarma başarısız oldu.")
                                    else:
                                        st.warning(f"⚠️ Kısmi başarı: ✅ {success_count} / ❌ {fail_count} kuyruk")

                                    if total_skipped:
                                        st.info(f"ℹ️ {total_skipped} üyelik kuyruklarda bulunamadığı için atlandı.")

                                    for qid, result in results.items():
                                        qname = bulk_queue_options.get(qid, qid)
                                        if result.get("success"):
                                            st.caption(
                                                f"✅ {qname}: -{result.get('removed', 0)} çıkarıldı"
                                                + (f", {result.get('skipped_missing', 0)} kullanıcı zaten yoktu" if result.get('skipped_missing', 0) else "")
                                            )
                                        else:
                                            st.caption(f"❌ {qname}: {result.get('error', '')}")
            except Exception as e:
                st.error(f"❌ {get_text(lang, 'group_fetch_error')}: {e}")

    with tab6:
        st.subheader("🔍 Kullanıcı Arama (User ID)")
        st.info("Genesys Cloud kullanıcı kimliği (UUID) ile kullanıcı bilgilerini sorgulayabilirsiniz.")
        st.caption("Kuyruk aktif/pasif yönetimi, kullanıcıyı arattıktan sonra açılan detay ekranında `🎛️ Kuyruk Üyelikleri (Aktif/Pasif)` başlığı altındadır.")
        
        if not st.session_state.get('api_client'):
            st.error("Bu özellik için Genesys Cloud bağlantısı gereklidir.")
        else:
            import re as _re_user_search
            _uuid_pattern_user = _re_user_search.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

            available_users_map = st.session_state.get("users_map", {}) or {}
            if not available_users_map:
                recover_org_maps_if_needed(org, force=True)
                available_users_map = st.session_state.get("users_map", {}) or {}
                if (not available_users_map) and st.session_state.get("api_client"):
                    try:
                        _tmp_api = GenesysAPI(st.session_state.api_client)
                        _users = _tmp_api.get_users()
                        if isinstance(_users, list) and _users:
                            available_users_map = {
                                str(u.get("name") or u.get("username") or u.get("id")): u.get("id")
                                for u in _users if isinstance(u, dict) and u.get("id")
                            }
                            st.session_state.users_map = available_users_map
                    except Exception:
                        available_users_map = {}
            if available_users_map:
                agent_options = ["Manuel ID Gireceğim"] + sorted(list(available_users_map.keys()))
                selected_agent_label = st.selectbox(
                    "Ajan Seç (Opsiyonel)",
                    options=agent_options,
                    key="admin_user_picker",
                    help="İsterseniz listeden agent seçebilirsiniz; seçim yaparsanız ID alanı otomatik doldurulur."
                )
                if selected_agent_label != "Manuel ID Gireceğim":
                    selected_agent_id = available_users_map.get(selected_agent_label)
                    if selected_agent_id and st.session_state.get("admin_user_search_id") != selected_agent_id:
                        st.session_state.admin_user_search_id = selected_agent_id
            
            search_col1, search_col2 = st.columns([3, 1])
            with search_col1:
                user_id_input = st.text_input(
                    "Kullanıcı ID (UUID)",
                    placeholder="e.g. 24331d74-80bf-4069-a67c-51bc851fdc3e",
                    help="Genesys Cloud kullanıcı kimliğini (UUID formatında) girin",
                    key="admin_user_search_id"
                )
            
            with search_col2:
                st.markdown("<br>", unsafe_allow_html=True)
                search_clicked = st.button("🔍 Ara", type="primary", use_container_width=True, key="admin_user_search_btn")
            if search_clicked:
                st.session_state.admin_user_search_triggered = True
                st.session_state.admin_user_search_last_id = (user_id_input or "").strip()

            hist_cfg_col1, hist_cfg_col2 = st.columns(2)
            with hist_cfg_col1:
                audit_history_days = st.number_input(
                    "Statü geçmişi (gün)",
                    min_value=1,
                    max_value=14,
                    value=int(st.session_state.get("admin_status_history_days", 3)),
                    step=1,
                    key="admin_status_history_days",
                    help="Realtime audit endpoint en fazla 14 gün verisi döner."
                )
            with hist_cfg_col2:
                audit_history_pages = st.number_input(
                    "Maksimum sayfa",
                    min_value=1,
                    max_value=50,
                    value=int(st.session_state.get("admin_status_history_pages", 8)),
                    step=1,
                    key="admin_status_history_pages",
                    help="Her sayfa en fazla 100 kayıt döner."
                )
            
            active_user_search = bool(st.session_state.get("admin_user_search_triggered"))
            if active_user_search:
                user_id_clean = (user_id_input or "").strip()
                if not user_id_clean:
                    user_id_clean = str(st.session_state.get("admin_user_search_last_id") or "").strip()
                
                if not user_id_clean:
                    st.session_state.admin_user_search_triggered = False
                    st.error("Lütfen bir kullanıcı ID girin.")
                elif not _uuid_pattern_user.match(user_id_clean):
                    st.session_state.admin_user_search_triggered = False
                    st.error("Geçersiz UUID formatı. Örnek: 24331d74-80bf-4069-a67c-51bc851fdc3e")
                else:
                    try:
                        st.session_state.admin_user_search_last_id = user_id_clean
                        api = GenesysAPI(st.session_state.api_client)
                        with st.spinner("Kullanıcı bilgileri getiriliyor..."):
                            user_data = api.get_user_by_id(
                                user_id_clean, 
                                expand=['presence', 'routingStatus', 'groups', 'skills', 'languages']
                            )
                        
                        if user_data:
                            st.success(f"✅ Kullanıcı bulundu: **{user_data.get('name', 'N/A')}**")
                            
                            # Basic Info
                            st.markdown("### 👤 Temel Bilgiler")
                            info_col1, info_col2 = st.columns(2)
                            with info_col1:
                                st.markdown(f"**Ad:** {user_data.get('name', 'N/A')}")
                                st.markdown(f"**E-posta:** {user_data.get('email', 'N/A')}")
                                st.markdown(f"**Kullanıcı Adı:** {user_data.get('username', 'N/A')}")
                                st.markdown(f"**Durum:** {user_data.get('state', 'N/A')}")
                            with info_col2:
                                st.markdown(f"**Departman:** {user_data.get('department', 'N/A')}")
                                st.markdown(f"**Ünvan:** {user_data.get('title', 'N/A')}")
                                st.markdown(f"**Yönetici:** {user_data.get('manager', 'N/A')}")
                                st.markdown(f"**Division:** {user_data.get('divisionName', 'N/A')}")
                            
                            # Presence & Routing Status
                            presence = user_data.get('presence', {})
                            routing = user_data.get('routingStatus', {})
                            if presence or routing:
                                st.divider()
                                st.markdown("### 📍 Anlık Durum")
                                status_col1, status_col2 = st.columns(2)
                                with status_col1:
                                    if presence:
                                        pres_def = presence.get('presenceDefinition', {})
                                        st.markdown(f"**Presence:** {pres_def.get('systemPresence', 'N/A')}")
                                        st.markdown(f"**Presence ID:** `{pres_def.get('id', 'N/A')}`")
                                        if presence.get('modifiedDate'):
                                            st.markdown(f"**Son Değişiklik:** {presence.get('modifiedDate', 'N/A')}")
                                with status_col2:
                                    if routing:
                                        st.markdown(f"**Routing Status:** {routing.get('status', 'N/A')}")
                                        if routing.get('startTime'):
                                            st.markdown(f"**Başlangıç:** {routing.get('startTime', 'N/A')}")

                            # Status Change History (Audit)
                            st.divider()
                            st.markdown("### 🧭 Statü Değişim Geçmişi (Audit)")
                            st.caption("Bu bölümde seçilen agent için status/presence/routing değişiklikleri ve değişikliği yapan kullanıcı listelenir.")
                            audit_enabled = st.toggle(
                                "Audit sorgusunu yükle",
                                value=False,
                                key=f"admin_status_audit_enabled_{user_id_clean}",
                                help="Açık olduğunda Genesys Audit API çağrılır. Kapalı olduğunda çağrı atılmaz.",
                            )
                            audit_refresh_clicked = st.button(
                                "🔄 Audit Geçmişini Yenile",
                                key=f"admin_status_audit_refresh_{user_id_clean}",
                                disabled=not audit_enabled,
                                use_container_width=True,
                            )
                            try:
                                offset_hours = _resolve_org_utc_offset_hours(org_code=org, default=3.0, force_reload=False)
                            except Exception:
                                offset_hours = 3.0

                            if not audit_enabled:
                                st.info("Audit sorgusu kapalı. İhtiyaç olduğunda yukarıdan açıp manuel yenileyin.")
                            else:
                                audit_cache_store = st.session_state.get("admin_status_audit_cache")
                                if not isinstance(audit_cache_store, dict):
                                    audit_cache_store = {}
                                st.session_state.admin_status_audit_cache = audit_cache_store

                                audit_cache_key = (
                                    f"{org}|{user_id_clean}|mode:status|days:{int(audit_history_days)}|"
                                    f"pages:{int(audit_history_pages)}"
                                )
                                if audit_refresh_clicked and audit_cache_key in audit_cache_store:
                                    audit_cache_store.pop(audit_cache_key, None)
                                    st.session_state.admin_status_audit_cache = audit_cache_store

                                if audit_cache_key not in audit_cache_store:
                                    history_end_utc = datetime.now(timezone.utc)
                                    history_start_utc = history_end_utc - timedelta(days=int(audit_history_days))
                                    with st.spinner("Statü değişim geçmişi getiriliyor..."):
                                        audit_payload = api.get_user_status_audit_logs(
                                            user_id=user_id_clean,
                                            start_date=history_start_utc,
                                            end_date=history_end_utc,
                                            page_size=100,
                                            max_pages=int(audit_history_pages),
                                            service_name="Presence",
                                        )
                                        primary_entities = (audit_payload or {}).get("entities") or []
                                        if not primary_entities:
                                            fallback_payload = api.get_user_status_audit_logs(
                                                user_id=user_id_clean,
                                                start_date=history_start_utc,
                                                end_date=history_end_utc,
                                                page_size=100,
                                                max_pages=int(audit_history_pages),
                                                service_name=None,
                                            )
                                            fallback_entities = (fallback_payload or {}).get("entities") or []
                                            if fallback_entities:
                                                fb_warning = str((fallback_payload or {}).get("_warning") or "").strip()
                                                extra_warning = "Presence servisinde kayıt bulunamadığı için tüm servislerde tarama yapıldı."
                                                if fb_warning:
                                                    fallback_payload["_warning"] = f"{extra_warning} {fb_warning}".strip()
                                                else:
                                                    fallback_payload["_warning"] = extra_warning
                                                audit_payload = fallback_payload
                                            elif (fallback_payload or {}).get("_error") and not (audit_payload or {}).get("_error"):
                                                audit_payload = fallback_payload
                                    cache_payload = dict(audit_payload or {})
                                    cache_payload["_cached_at_ts"] = pytime.time()
                                    cache_payload["_attempts"] = _trim_attempts_for_storage(cache_payload.get("_attempts"))
                                    audit_cache_store[audit_cache_key] = cache_payload
                                    st.session_state.admin_status_audit_cache = audit_cache_store

                                audit_payload = audit_cache_store.get(audit_cache_key) or {}
                                audit_error = (audit_payload or {}).get("_error")
                                audit_entities = (audit_payload or {}).get("entities") or []
                                audit_source = str((audit_payload or {}).get("source") or "-")
                                audit_filter_variant = (audit_payload or {}).get("filter_variant") or []
                                audit_warning = str((audit_payload or {}).get("_warning") or "").strip()
                                if audit_error:
                                    audit_err_text = str(audit_error)
                                    audit_err_lower = audit_err_text.lower()
                                    if ("audits:audit:view" in audit_err_lower) or ("missing the following permission" in audit_err_lower):
                                        st.warning(
                                            "Audit geçmişi alınamadı. Yetki gerekli: `audits:audit:view`.\n\n"
                                            f"Hata detayı: {audit_error}"
                                        )
                                    else:
                                        st.warning(f"Audit geçmişi sorgusunda hata oluştu.\n\nHata detayı: {audit_error}")
                                else:
                                    status_rows = _build_status_audit_rows(
                                        audit_entities=audit_entities,
                                        target_user_id=user_id_clean,
                                        users_info=st.session_state.get("users_info", {}),
                                        presence_map=st.session_state.get("presence_map", {}),
                                        utc_offset_hours=offset_hours,
                                    )
                                    if status_rows:
                                        df_status_history = pd.DataFrame(status_rows)
                                        st.success(
                                            f"{len(df_status_history)} adet statü değişim kaydı bulundu "
                                            f"(tarama aralığı: son {int(audit_history_days)} gün)."
                                        )
                                        st.caption(f"Audit kaynağı: `{audit_source}`")
                                        if audit_filter_variant:
                                            st.caption(f"Audit filtreleri: `{audit_filter_variant}`")
                                        if audit_warning:
                                            st.info(audit_warning)
                                        st.dataframe(df_status_history, width='stretch')
                                        st.download_button(
                                            "📥 Statü Geçmişini CSV İndir",
                                            data=df_status_history.to_csv(index=False).encode("utf-8"),
                                            file_name=f"status_history_{user_id_clean}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                            mime="text/csv",
                                            key=f"admin_status_history_download_{user_id_clean}",
                                        )
                                    else:
                                        if audit_entities:
                                            st.warning(
                                                f"Audit tarafında {len(audit_entities)} kayıt bulundu ancak status filtresine uymadı. "
                                                "Aşağıda ham audit önizlemesi gösteriliyor."
                                            )
                                            st.caption(f"Audit kaynağı: `{audit_source}`")
                                            if audit_filter_variant:
                                                st.caption(f"Audit filtreleri: `{audit_filter_variant}`")
                                            if audit_warning:
                                                st.info(audit_warning)
                                            raw_preview_rows = []
                                            for item in audit_entities[:300]:
                                                if not isinstance(item, dict):
                                                    continue
                                                actor_obj = item.get("user") or {}
                                                actor_id = str(actor_obj.get("id") or "").strip()
                                                actor_name = _resolve_user_label(
                                                    user_id=actor_id,
                                                    users_info=st.session_state.get("users_info", {}),
                                                    fallback_name=actor_obj.get("name") or actor_obj.get("displayName"),
                                                )
                                                entity_obj = item.get("entity") or {}
                                                message_obj = item.get("message") or {}
                                                raw_preview_rows.append({
                                                    "Zaman": _format_iso_with_utc_offset(item.get("eventDate"), utc_offset_hours=offset_hours),
                                                    "Servis": item.get("serviceName") or "-",
                                                    "Aksiyon": item.get("action") or "-",
                                                    "EntityType": item.get("entityType") or "-",
                                                    "EntityId": entity_obj.get("id") if isinstance(entity_obj, dict) else "-",
                                                    "Değiştiren": actor_name,
                                                    "Değiştiren ID": actor_id or "-",
                                                    "Mesaj": (
                                                        message_obj.get("message")
                                                        or message_obj.get("messageWithParams")
                                                        or "-"
                                                    ) if isinstance(message_obj, dict) else "-",
                                                    "Audit ID": item.get("id") or "-",
                                                })
                                            if raw_preview_rows:
                                                st.dataframe(pd.DataFrame(raw_preview_rows), width='stretch')
                                        else:
                                            st.info(
                                                "Seçilen aralıkta statü değişim kaydı bulunamadı. "
                                                "Gün aralığını artırıp tekrar deneyin."
                                            )
                                            if audit_filter_variant:
                                                st.caption(f"Audit filtreleri: `{audit_filter_variant}`")
                                            if audit_warning:
                                                st.info(audit_warning)

                            # Groups
                            current_org = str(org or "").strip()
                            cached_groups_org = str(st.session_state.get("admin_groups_cache_org") or "").strip()
                            all_groups = st.session_state.get("admin_groups_cache") or []
                            if (cached_groups_org != current_org) or (not all_groups):
                                try:
                                    all_groups = api.get_groups()
                                    st.session_state.admin_groups_cache = all_groups
                                    st.session_state.admin_groups_cache_org = current_org
                                except Exception as ge:
                                    all_groups = []
                                    st.warning(f"Grup listesi alınamadı: {ge}")
                            all_group_options = {
                                str(g.get("id")): str(g.get("name") or g.get("id"))
                                for g in all_groups
                                if isinstance(g, dict) and g.get("id")
                            }
                            group_name_cache = st.session_state.get("admin_group_name_cache") or {}
                            if not isinstance(group_name_cache, dict):
                                group_name_cache = {}
                            st.session_state.admin_group_name_cache = group_name_cache

                            user_groups = user_data.get('groups', [])
                            if not isinstance(user_groups, list):
                                user_groups = []
                            user_group_rows = []
                            user_group_name_map = {}
                            for g in user_groups:
                                if not isinstance(g, dict):
                                    continue
                                gid = str(g.get("id") or "").strip()
                                gname_raw = str(g.get("name") or "").strip()
                                if (not gname_raw) or (gname_raw == gid):
                                    gname_raw = str(all_group_options.get(gid) or gname_raw or "").strip()
                                if gid and ((not gname_raw) or (gname_raw == gid)):
                                    cached_name = str(group_name_cache.get(gid) or "").strip()
                                    if cached_name:
                                        gname_raw = cached_name
                                if gid and ((not gname_raw) or (gname_raw == gid)):
                                    try:
                                        g_detail = api.get_group_by_id(gid)
                                    except Exception:
                                        g_detail = None
                                    if isinstance(g_detail, dict):
                                        resolved_name = str(g_detail.get("name") or "").strip()
                                        if resolved_name:
                                            gname_raw = resolved_name
                                            group_name_cache[gid] = resolved_name
                                            all_group_options[gid] = resolved_name
                                gname = str(gname_raw or gid or "-").strip()
                                gtype = str(g.get("type") or "GROUP").strip()
                                if gid:
                                    user_group_name_map[gid] = gname
                                user_group_rows.append({
                                    "Grup Adı": gname,
                                    "Grup ID": gid or "-",
                                    "Tür": gtype or "-",
                                })
                            st.divider()
                            st.markdown(f"### 👥 Grup Üyelikleri ({len(user_group_rows)})")
                            if user_group_rows:
                                st.dataframe(pd.DataFrame(user_group_rows), width='stretch', hide_index=True)
                            else:
                                st.info("Kullanıcı henüz herhangi bir gruba dahil değil.")

                            if all_group_options:
                                current_group_ids = [row["Grup ID"] for row in user_group_rows if row.get("Grup ID") and row.get("Grup ID") != "-"]
                                current_group_id_set = set(current_group_ids)
                                add_candidates = [gid for gid in all_group_options.keys() if gid not in current_group_id_set]
                                remove_candidates = list(current_group_ids)

                                grp_col_add, grp_col_remove = st.columns(2)
                                with grp_col_add:
                                    add_selection = st.multiselect(
                                        "Kullanıcıyı Ekle (Gruplar)",
                                        options=add_candidates,
                                        format_func=lambda x: all_group_options.get(x, x),
                                        key=f"admin_user_group_add_sel_{user_id_clean}",
                                        help="Seçilen kullanıcı bu gruplara üye olarak eklenecek."
                                    )
                                    if st.button(
                                        "➕ Seçili Gruplara Ekle",
                                        key=f"admin_user_group_add_btn_{user_id_clean}",
                                        disabled=not add_selection,
                                        use_container_width=True,
                                    ):
                                        added = 0
                                        failed = []
                                        with st.spinner("Grup üyeliği ekleniyor..."):
                                            for gid in add_selection:
                                                try:
                                                    api.add_group_members(gid, [user_id_clean])
                                                    added += 1
                                                except Exception as e:
                                                    failed.append((all_group_options.get(gid, gid), str(e)))
                                        if added:
                                            st.success(f"{added} grup için kullanıcı üyeliği eklendi.")
                                            st.session_state.admin_groups_refresh = True
                                        if failed:
                                            st.warning(f"{len(failed)} grup için ekleme başarısız.")
                                            for gname, err in failed:
                                                st.caption(f"❌ {gname}: {err}")
                                        if added:
                                            st.rerun()

                                with grp_col_remove:
                                    remove_selection = st.multiselect(
                                        "Kullanıcıyı Çıkar (Gruplar)",
                                        options=remove_candidates,
                                        format_func=lambda x: user_group_name_map.get(x, all_group_options.get(x, x)),
                                        key=f"admin_user_group_remove_sel_{user_id_clean}",
                                        help="Seçilen kullanıcı bu gruplardan çıkarılacak."
                                    )
                                    if st.button(
                                        "➖ Seçili Gruplardan Çıkar",
                                        key=f"admin_user_group_remove_btn_{user_id_clean}",
                                        disabled=not remove_selection,
                                        use_container_width=True,
                                    ):
                                        removed = 0
                                        failed = []
                                        with st.spinner("Grup üyeliği çıkarılıyor..."):
                                            for gid in remove_selection:
                                                try:
                                                    api.remove_group_members(gid, [user_id_clean])
                                                    removed += 1
                                                except Exception as e:
                                                    failed.append((user_group_name_map.get(gid, gid), str(e)))
                                        if removed:
                                            st.success(f"{removed} grup için kullanıcı üyeliği çıkarıldı.")
                                            st.session_state.admin_groups_refresh = True
                                        if failed:
                                            st.warning(f"{len(failed)} grup için çıkarma başarısız.")
                                            for gname, err in failed:
                                                st.caption(f"❌ {gname}: {err}")
                                        if removed:
                                            st.rerun()
                            
                            # Queue Memberships (Active/Passive without removing membership)
                            st.divider()
                            st.markdown("### 🎛️ Kuyruk Üyelikleri (Aktif/Pasif)")
                            st.caption("Bu bölümde kullanıcının üye olduğu kuyruklar listelenir. Üyelik silinmez, sadece aktif/pasif (joined) durumu değiştirilir.")
                            st.caption("Not: Genesys bu güncellemeyi asenkron işler; değişikliklerin görünmesi birkaç saniye sürebilir.")

                            queue_memberships_cache_key = f"admin_user_queue_memberships_{org}_{user_id_clean}"
                            queue_memberships_refresh_key = f"{queue_memberships_cache_key}_refresh"
                            if st.button(
                                "🔄 Kuyruk Üyeliklerini Yenile",
                                key=f"admin_user_queue_refresh_btn_{user_id_clean}",
                                use_container_width=True,
                            ):
                                st.session_state[queue_memberships_refresh_key] = True

                            should_refresh_queue_memberships = bool(st.session_state.get(queue_memberships_refresh_key))
                            if queue_memberships_cache_key not in st.session_state:
                                should_refresh_queue_memberships = True

                            if should_refresh_queue_memberships:
                                with st.spinner("Kullanıcının kuyruk üyelikleri getiriliyor..."):
                                    try:
                                        st.session_state[queue_memberships_cache_key] = api.get_user_queues(
                                            user_id_clean,
                                            joined=None,
                                        )
                                        st.session_state[queue_memberships_refresh_key] = False
                                    except Exception as qe:
                                        st.warning(f"Kuyruk üyelikleri alınamadı: {qe}")
                                        st.session_state[queue_memberships_refresh_key] = True
                                        if queue_memberships_cache_key not in st.session_state:
                                            st.session_state[queue_memberships_cache_key] = []

                            user_queue_memberships = st.session_state.get(queue_memberships_cache_key, []) or []
                            if user_queue_memberships:
                                active_queue_count = sum(
                                    1 for q in user_queue_memberships
                                    if isinstance(q, dict) and bool(q.get("joined"))
                                )
                                passive_queue_count = max(0, len(user_queue_memberships) - active_queue_count)
                                st.caption(
                                    f"Toplam kuyruk: {len(user_queue_memberships)} | "
                                    f"Aktif: {active_queue_count} | Pasif: {passive_queue_count}"
                                )

                                queue_search = st.text_input(
                                    "Kuyruk Ara (Ad veya ID)",
                                    key=f"admin_user_queue_search_{user_id_clean}",
                                    placeholder="Kuyruk adı veya ID ile filtrele",
                                )
                                queue_search_lower = str(queue_search or "").strip().lower()
                                filtered_queue_memberships = []
                                for q in user_queue_memberships:
                                    if not isinstance(q, dict):
                                        continue
                                    qid = str(q.get("id") or "").strip()
                                    qname = str(q.get("name") or qid).strip()
                                    if queue_search_lower and (queue_search_lower not in qname.lower()) and (queue_search_lower not in qid.lower()):
                                        continue
                                    filtered_queue_memberships.append({
                                        "Kuyruk Adı": qname,
                                        "Kuyruk ID": qid,
                                        "Durum": "Aktif" if bool(q.get("joined")) else "Pasif",
                                    })

                                if filtered_queue_memberships:
                                    st.dataframe(
                                        pd.DataFrame(filtered_queue_memberships),
                                        width='stretch',
                                        hide_index=True,
                                    )
                                else:
                                    st.info("Filtreye uygun kuyruk bulunamadı.")

                                selectable_queue_options = {}
                                for q in user_queue_memberships:
                                    if not isinstance(q, dict):
                                        continue
                                    qid = str(q.get("id") or "").strip()
                                    if not qid:
                                        continue
                                    qname = str(q.get("name") or qid).strip()
                                    status_text = "Aktif" if bool(q.get("joined")) else "Pasif"
                                    selectable_queue_options[qid] = f"{qname} ({status_text})"

                                selectable_queue_ids = list(selectable_queue_options.keys())
                                queue_toggle_multiselect_key = f"admin_user_queue_toggle_sel_{user_id_clean}"
                                queue_sel_col1, queue_sel_col2 = st.columns(2)
                                with queue_sel_col1:
                                    if st.button(
                                        "☑️ Tümünü Seç",
                                        key=f"admin_user_queue_select_all_btn_{user_id_clean}",
                                        use_container_width=True,
                                    ):
                                        st.session_state[queue_toggle_multiselect_key] = list(selectable_queue_ids)
                                with queue_sel_col2:
                                    if st.button(
                                        "🧹 Seçimi Temizle",
                                        key=f"admin_user_queue_clear_sel_btn_{user_id_clean}",
                                        use_container_width=True,
                                    ):
                                        st.session_state[queue_toggle_multiselect_key] = []

                                selected_queue_ids_for_toggle = st.multiselect(
                                    "Durumu değiştirilecek kuyruklar",
                                    options=selectable_queue_ids,
                                    format_func=lambda x: selectable_queue_options.get(x, x),
                                    key=queue_toggle_multiselect_key,
                                )

                                queue_col_activate, queue_col_deactivate = st.columns(2)
                                with queue_col_activate:
                                    if st.button(
                                        "✅ Seçili Kuyrukları Aktif Yap",
                                        key=f"admin_user_queue_activate_btn_{user_id_clean}",
                                        disabled=not selected_queue_ids_for_toggle,
                                        use_container_width=True,
                                    ):
                                        with st.spinner("Seçili kuyruklar aktif yapılıyor..."):
                                            results = api.set_user_queues_joined(
                                                user_id_clean,
                                                selected_queue_ids_for_toggle,
                                                joined=True,
                                            )
                                        success_ids = [qid for qid, r in results.items() if isinstance(r, dict) and r.get("success")]
                                        failed_rows = [
                                            (qid, (r or {}).get("error", "Bilinmeyen hata"))
                                            for qid, r in results.items()
                                            if not (isinstance(r, dict) and r.get("success"))
                                        ]
                                        if success_ids:
                                            st.success(f"{len(success_ids)} kuyruk aktif yapıldı.")
                                            st.session_state[queue_memberships_refresh_key] = True
                                        if failed_rows:
                                            st.warning(f"{len(failed_rows)} kuyruk için aktif etme başarısız oldu.")
                                            for qid, err in failed_rows[:10]:
                                                st.caption(f"❌ {selectable_queue_options.get(qid, qid)}: {err}")
                                        if success_ids:
                                            st.rerun()

                                with queue_col_deactivate:
                                    if st.button(
                                        "⏸️ Seçili Kuyrukları Pasif Yap",
                                        key=f"admin_user_queue_deactivate_btn_{user_id_clean}",
                                        disabled=not selected_queue_ids_for_toggle,
                                        use_container_width=True,
                                    ):
                                        with st.spinner("Seçili kuyruklar pasif yapılıyor..."):
                                            results = api.set_user_queues_joined(
                                                user_id_clean,
                                                selected_queue_ids_for_toggle,
                                                joined=False,
                                            )
                                        success_ids = [qid for qid, r in results.items() if isinstance(r, dict) and r.get("success")]
                                        failed_rows = [
                                            (qid, (r or {}).get("error", "Bilinmeyen hata"))
                                            for qid, r in results.items()
                                            if not (isinstance(r, dict) and r.get("success"))
                                        ]
                                        if success_ids:
                                            st.success(f"{len(success_ids)} kuyruk pasif yapıldı.")
                                            st.session_state[queue_memberships_refresh_key] = True
                                        if failed_rows:
                                            st.warning(f"{len(failed_rows)} kuyruk için pasif etme başarısız oldu.")
                                            for qid, err in failed_rows[:10]:
                                                st.caption(f"❌ {selectable_queue_options.get(qid, qid)}: {err}")
                                        if success_ids:
                                            st.rerun()
                            else:
                                st.info("Kullanıcının kuyruk üyeliği bulunamadı.")

                            # Skills
                            skills = user_data.get('skills', [])
                            if skills:
                                st.divider()
                                st.markdown(f"### 🎯 Yetenekler ({len(skills)})")
                                skill_info = [f"{s.get('name', 'N/A')} (Seviye: {s.get('proficiency', 'N/A')})" for s in skills]
                                for s in skill_info[:10]:  # Show max 10
                                    st.caption(s)
                                if len(skills) > 10:
                                    st.caption(f"... ve {len(skills) - 10} daha")
                            
                            # Languages
                            languages = user_data.get('languages', [])
                            if languages:
                                st.divider()
                                st.markdown(f"### 🌐 Diller ({len(languages)})")
                                lang_info = [f"{l.get('name', 'N/A')} (Seviye: {l.get('proficiency', 'N/A')})" for l in languages]
                                st.write(", ".join(lang_info) if lang_info else "Dil yok")
                            
                        else:
                            st.warning(f"⚠️ Kullanıcı bulunamadı: `{user_id_clean}`")
                    except Exception as e:
                        st.error(f"❌ Hata: {e}")

    with tab7:
        st.subheader("🧾 Kuyruk Üyelik Değişimleri (Audit)")
        st.caption(
            "Bu sayfa kuyruk üyelik değişimlerini (ekleme, çıkarma, aktif/pasif) listeler. "
            "Filtreler: değişen kullanıcı (etkilenen) ve değiştiren kullanıcı (actor)."
        )
        st.caption("Realtime audit endpoint 14 günle sınırlıdır; bu sayfa gerektiğinde async audit fallback kullanır.")

        if not st.session_state.get("api_client"):
            st.error("Bu özellik için Genesys Cloud bağlantısı gereklidir.")
        else:
            try:
                offset_hours = _resolve_org_utc_offset_hours(org_code=org, default=3.0, force_reload=False)
            except Exception:
                offset_hours = 3.0
            tz_local = timezone(timedelta(hours=float(offset_hours or 0)))
            today_local = datetime.now(tz_local).date()
            default_start_date = today_local - timedelta(days=3)

            state_prefix = f"admin_queue_audit_tab7_{org}"
            rows_key = f"{state_prefix}_rows"
            meta_key = f"{state_prefix}_meta"

            if not st.session_state.get("users_info") or not st.session_state.get("queues_map"):
                try:
                    recover_org_maps_if_needed(org, force=False)
                except Exception:
                    pass

            cached_rows = st.session_state.get(rows_key) or []
            user_options = [""]
            user_labels = {"": "(Tümü)"}
            users_info = st.session_state.get("users_info", {}) or {}
            if isinstance(users_info, dict):
                for uid, info in users_info.items():
                    user_id_opt = str(uid or "").strip()
                    if not user_id_opt:
                        continue
                    if user_id_opt not in user_options:
                        user_options.append(user_id_opt)
                    info_obj = info if isinstance(info, dict) else {}
                    user_name_opt = str(info_obj.get("name") or info_obj.get("username") or user_id_opt).strip()
                    user_labels[user_id_opt] = f"{user_name_opt} ({user_id_opt})"

            if isinstance(cached_rows, list):
                for row in cached_rows:
                    if not isinstance(row, dict):
                        continue
                    for id_key, name_key in (("Değiştiren ID", "Değiştiren"), ("Etkilenen ID", "Etkilenen")):
                        uid_val = str(row.get(id_key) or "").strip()
                        uname_val = str(row.get(name_key) or "").strip()
                        if not uid_val or uid_val == "-":
                            continue
                        if uid_val not in user_options:
                            user_options.append(uid_val)
                        if uid_val not in user_labels:
                            user_labels[uid_val] = f"{(uname_val or uid_val)} ({uid_val})"
            user_options = [""] + sorted(
                [opt for opt in user_options if opt],
                key=lambda x: user_labels.get(x, x).lower(),
            )

            cfg_col1, cfg_col2, cfg_col3 = st.columns(3)
            with cfg_col1:
                queue_start_date = st.date_input(
                    "Başlangıç Tarihi",
                    value=st.session_state.get(f"{state_prefix}_start_date", default_start_date),
                    key=f"{state_prefix}_start_date",
                )
            with cfg_col2:
                queue_end_date = st.date_input(
                    "Bitiş Tarihi",
                    value=st.session_state.get(f"{state_prefix}_end_date", today_local),
                    key=f"{state_prefix}_end_date",
                )
            with cfg_col3:
                queue_max_pages = int(
                    st.number_input(
                        "Maksimum Sayfa",
                        min_value=1,
                        max_value=50,
                        value=int(st.session_state.get(f"{state_prefix}_max_pages", 8)),
                        step=1,
                        key=f"{state_prefix}_max_pages",
                        help="Her sayfada en fazla 100 audit kaydı çekilir.",
                    )
                )

            filter_col1, filter_col2 = st.columns(2)
            with filter_col1:
                selected_target_user = st.selectbox(
                    "Değişen Kullanıcı (Etkilenen) Filtresi",
                    options=user_options,
                    format_func=lambda x: user_labels.get(x, x),
                    key=f"{state_prefix}_target_filter",
                )
            with filter_col2:
                selected_actor_user = st.selectbox(
                    "Değiştiren Kullanıcı (Actor) Filtresi",
                    options=user_options,
                    format_func=lambda x: user_labels.get(x, x),
                    key=f"{state_prefix}_actor_filter",
                )

            default_operations = [
                "Eklendi",
                "Çıkarıldı",
                "Aktif Yapıldı",
                "Pasif Yapıldı",
                "Durum Güncellendi",
                "Güncellendi",
            ]
            operation_options = list(default_operations)
            if isinstance(cached_rows, list):
                dynamic_ops = sorted(
                    {
                        str(row.get("İşlem") or "").strip()
                        for row in cached_rows
                        if isinstance(row, dict) and str(row.get("İşlem") or "").strip()
                    }
                )
                for op_name in dynamic_ops:
                    if op_name not in operation_options:
                        operation_options.append(op_name)
            selected_operations = st.multiselect(
                "İşlem Filtresi",
                options=operation_options,
                default=operation_options,
                key=f"{state_prefix}_operation_filter",
            )

            queue_search_input = str(st.text_input(
                "Kuyruk Ara (Ad veya ID)",
                placeholder="Örn: Yeni_KibrisOtel",
                key=f"{state_prefix}_queue_search",
            ) or "").strip()
            queue_search_token = queue_search_input.lower()

            action_col1, action_col2 = st.columns(2)
            with action_col1:
                run_query_clicked = st.button(
                    "🔎 Audit Sorgusunu Çalıştır",
                    type="primary",
                    use_container_width=True,
                    key=f"{state_prefix}_run_btn",
                )
            with action_col2:
                clear_results_clicked = st.button(
                    "🧹 Sonucu Temizle",
                    use_container_width=True,
                    key=f"{state_prefix}_clear_btn",
                )

            if clear_results_clicked:
                st.session_state.pop(rows_key, None)
                st.session_state.pop(meta_key, None)
                st.rerun()

            if run_query_clicked:
                queue_date_invalid = False
                if queue_end_date < queue_start_date:
                    st.error("Bitiş tarihi başlangıç tarihinden önce olamaz.")
                    queue_date_invalid = True

                if queue_date_invalid:
                    st.session_state[rows_key] = []
                    st.session_state[meta_key] = {
                        "executed": False,
                        "range_text": f"{queue_start_date.isoformat()} - {queue_end_date.isoformat()}",
                        "_cached_at_ts": pytime.time(),
                    }
                else:
                    try:
                        if ((queue_end_date - queue_start_date).days + 1) > 14:
                            st.info("14 günden uzun aralık seçildi. Realtime + async fallback ile sorgu çalıştırılıyor, işlem daha uzun sürebilir.")
                        start_local_dt = datetime(
                            queue_start_date.year,
                            queue_start_date.month,
                            queue_start_date.day,
                            0,
                            0,
                            0,
                            tzinfo=tz_local,
                        )
                        end_local_dt = datetime(
                            queue_end_date.year,
                            queue_end_date.month,
                            queue_end_date.day,
                            23,
                            59,
                            59,
                            tzinfo=tz_local,
                        )
                        queue_api = GenesysAPI(st.session_state.api_client)
                        queue_filter_ids = []
                        if queue_search_token:
                            queues_map_state = st.session_state.get("queues_map", {}) or {}
                            if isinstance(queues_map_state, dict):
                                for queue_name_raw, queue_id_raw in queues_map_state.items():
                                    queue_name_s = str(queue_name_raw or "").strip().lower()
                                    queue_id_s = str(queue_id_raw or "").strip()
                                    if not queue_id_s:
                                        continue
                                    queue_id_l = queue_id_s.lower()
                                    if (
                                        (queue_search_token in queue_name_s)
                                        or (queue_search_token == queue_id_l)
                                        or (queue_search_token in queue_id_l)
                                    ):
                                        queue_filter_ids.append(queue_id_s)
                            queue_filter_ids = sorted({str(qid).strip() for qid in queue_filter_ids if str(qid).strip()})

                        with st.spinner("Kuyruk üyelik audit kayıtları getiriliyor..."):
                            audit_payload = queue_api.get_queue_membership_audit_logs(
                                start_date=start_local_dt,
                                end_date=end_local_dt,
                                page_size=100,
                                max_pages=int(queue_max_pages),
                                service_name="Routing",
                                actor_user_id=selected_actor_user or None,
                                affected_user_id=selected_target_user or None,
                                queue_ids=queue_filter_ids or None,
                                queue_text=queue_search_input or None,
                            )
                            entities = (audit_payload or {}).get("entities") or []
                            query_error = (audit_payload or {}).get("_error")
                            query_warning = str((audit_payload or {}).get("_warning") or "").strip()
                            query_attempts = _trim_attempts_for_storage((audit_payload or {}).get("_attempts"))
                            raw_preview_rows = []
                            for item in entities[:ADMIN_QUEUE_AUDIT_MAX_PREVIEW_ROWS]:
                                if not isinstance(item, dict):
                                    continue
                                actor_obj = item.get("user") or {}
                                actor_id = str(actor_obj.get("id") or "").strip()
                                actor_name = _resolve_user_label(
                                    user_id=actor_id,
                                    users_info=st.session_state.get("users_info", {}),
                                    fallback_name=actor_obj.get("name") or actor_obj.get("displayName"),
                                )
                                entity_obj = item.get("entity") or {}
                                message_obj = item.get("message") or {}
                                raw_preview_rows.append({
                                    "Zaman": _format_iso_with_utc_offset(item.get("eventDate"), utc_offset_hours=offset_hours),
                                    "Servis": item.get("serviceName") or "-",
                                    "Aksiyon": item.get("action") or "-",
                                    "EntityType": item.get("entityType") or "-",
                                    "EntityId": entity_obj.get("id") if isinstance(entity_obj, dict) else "-",
                                    "Değiştiren": actor_name,
                                    "Değiştiren ID": actor_id or "-",
                                    "Mesaj": (
                                        message_obj.get("message")
                                        or message_obj.get("messageWithParams")
                                        or "-"
                                    ) if isinstance(message_obj, dict) else "-",
                                    "Audit ID": item.get("id") or "-",
                                })

                            queue_name_map = {}
                            queue_map_state = st.session_state.get("queues_map", {}) or {}
                            if isinstance(queue_map_state, dict):
                                for queue_name, queue_id in queue_map_state.items():
                                    queue_id_s = str(queue_id or "").strip()
                                    queue_name_s = str(queue_name or queue_id_s).strip() or queue_id_s
                                    if queue_id_s:
                                        queue_name_map[queue_id_s] = queue_name_s
                            if not queue_name_map:
                                try:
                                    queue_entities = queue_api.get_queues() or []
                                    queue_name_map = {
                                        str(item.get("id")): str(item.get("name") or item.get("id"))
                                        for item in queue_entities
                                        if isinstance(item, dict) and item.get("id")
                                    }
                                except Exception:
                                    queue_name_map = {}

                            rows = _build_status_audit_rows(
                                audit_entities=entities,
                                target_user_id=selected_target_user or None,
                                users_info=st.session_state.get("users_info", {}),
                                presence_map=st.session_state.get("presence_map", {}),
                                utc_offset_hours=offset_hours,
                                audit_mode="queue_membership",
                                queue_name_map=queue_name_map,
                            )
                            for row in rows:
                                if not isinstance(row, dict):
                                    continue
                                target_id = str(row.get("Etkilenen ID") or "").strip()
                                row["Etkilenen"] = _resolve_user_label(
                                    user_id=target_id,
                                    users_info=st.session_state.get("users_info", {}),
                                    fallback_name=row.get("Etkilenen"),
                                )
                            rows = sorted(
                                [row for row in rows if isinstance(row, dict)],
                                key=lambda row: str(row.get("Zaman") or ""),
                                reverse=True,
                            )
                            if len(rows) > ADMIN_QUEUE_AUDIT_MAX_ROWS:
                                rows = rows[:ADMIN_QUEUE_AUDIT_MAX_ROWS]
                                query_warning = (query_warning + " " if query_warning else "") + (
                                    f"Sonuçlar bellek koruması için ilk {ADMIN_QUEUE_AUDIT_MAX_ROWS} satır ile sınırlandı."
                                )

                        st.session_state[rows_key] = rows
                        st.session_state[meta_key] = {
                            "executed": True,
                            "source": str((audit_payload or {}).get("source") or "-"),
                            "service_name": str((audit_payload or {}).get("service_name") or "-"),
                            "filter_variant": (audit_payload or {}).get("filter_variant") or [],
                            "attempts": query_attempts,
                            "warning": query_warning,
                            "error": query_error,
                            "range_text": f"{queue_start_date.isoformat()} - {queue_end_date.isoformat()}",
                            "result_count": len(rows),
                            "raw_entity_count": len(entities),
                            "raw_preview_rows": raw_preview_rows,
                            "ran_at": datetime.now(tz_local).strftime("%Y-%m-%d %H:%M:%S"),
                            "_cached_at_ts": pytime.time(),
                        }
                    except Exception as qe:
                        st.session_state[rows_key] = []
                        st.session_state[meta_key] = {
                            "executed": True,
                            "error": str(qe),
                            "range_text": f"{queue_start_date.isoformat()} - {queue_end_date.isoformat()}",
                            "ran_at": datetime.now(tz_local).strftime("%Y-%m-%d %H:%M:%S"),
                            "_cached_at_ts": pytime.time(),
                        }

            rows_all = st.session_state.get(rows_key) or []
            meta = st.session_state.get(meta_key) or {}

            if meta.get("error"):
                st.warning(f"Audit sorgusunda hata oluştu.\n\nHata detayı: {meta.get('error')}")
            if meta.get("warning"):
                st.info(str(meta.get("warning")))
            if meta.get("executed") and meta.get("ran_at"):
                st.caption(f"Son sorgu zamanı: {meta.get('ran_at')}")
            if meta.get("executed"):
                st.caption(
                    f"Ham audit kaydı: {int(meta.get('raw_entity_count') or 0)} | "
                    f"Parse edilen satır: {int(meta.get('result_count') or 0)}"
                )
                attempts = meta.get("attempts") or []
                if attempts:
                    st.caption(f"Sorgu denemesi: {len(attempts)}")
                    with st.expander("Sorgu Diagnostiği", expanded=False):
                        attempt_rows = []
                        for item in attempts:
                            if not isinstance(item, dict):
                                continue
                            row = dict(item)
                            filters_val = row.get("filters")
                            if isinstance(filters_val, list):
                                filters_text = ", ".join(
                                    f"{str(f.get('property') or '').strip()}={str(f.get('value') or '').strip()}"
                                    for f in filters_val
                                    if isinstance(f, dict)
                                    and str(f.get("property") or "").strip()
                                    and str(f.get("value") or "").strip()
                                )
                                row["filters"] = filters_text or "-"
                            else:
                                row["filters"] = str(filters_val or "-")
                            attempt_rows.append(row)
                        st.dataframe(pd.DataFrame(attempt_rows), width='stretch')

            if not rows_all:
                if meta.get("executed"):
                    raw_count = int(meta.get("raw_entity_count") or 0)
                    if raw_count > 0:
                        st.warning(
                            f"Sorgu çalıştırıldı, {raw_count} ham audit kaydı bulundu ancak kuyruk üyelik satırına parse edilemedi."
                        )
                        raw_preview_rows = meta.get("raw_preview_rows") or []
                        if raw_preview_rows:
                            st.dataframe(pd.DataFrame(raw_preview_rows), width='stretch')
                    else:
                        st.info(
                            "Sorgu çalıştırıldı ancak kayıt bulunamadı. "
                            "Tarih aralığını genişletip veya filtreleri azaltıp tekrar deneyin."
                        )
                else:
                    st.info("Henüz sonuç yok. Tarih aralığını seçip sorguyu çalıştırın.")
            else:
                selected_target_user = str(selected_target_user or "").strip()
                selected_actor_user = str(selected_actor_user or "").strip()
                selected_target_user_lower = selected_target_user.lower()
                selected_actor_user_lower = selected_actor_user.lower()
                selected_operations = [
                    str(op or "").strip()
                    for op in (selected_operations or [])
                    if str(op or "").strip()
                ]

                filtered_rows = []
                for row in rows_all:
                    if not isinstance(row, dict):
                        continue
                    if selected_target_user and str(row.get("Etkilenen ID") or "").strip().lower() != selected_target_user_lower:
                        continue
                    if selected_actor_user and str(row.get("Değiştiren ID") or "").strip().lower() != selected_actor_user_lower:
                        continue
                    if selected_operations and str(row.get("İşlem") or "").strip() not in selected_operations:
                        continue
                    if queue_search_token:
                        queue_name = str(row.get("Kuyruk") or "").strip().lower()
                        queue_id = str(row.get("Kuyruk ID") or "").strip().lower()
                        queue_summary = str(row.get("Özet") or "").strip().lower()
                        queue_message = str(row.get("Mesaj") or "").strip().lower()
                        if (
                            (queue_search_token not in queue_name)
                            and (queue_search_token not in queue_id)
                            and (queue_search_token not in queue_summary)
                            and (queue_search_token not in queue_message)
                        ):
                            continue
                    filtered_rows.append(row)

                if not filtered_rows:
                    st.info("Seçilen filtrelere uygun kuyruk üyelik kaydı bulunamadı.")
                else:
                    df_queue_history = pd.DataFrame(filtered_rows)
                    preferred_cols = [
                        "Zaman",
                        "Özet",
                        "İşlem",
                        "Kuyruk",
                        "Kuyruk ID",
                        "Etkilenen",
                        "Etkilenen ID",
                        "Değiştiren",
                        "Değiştiren ID",
                        "Servis",
                        "Aksiyon",
                        "Alan",
                        "Eski Değer",
                        "Yeni Değer",
                        "Mesaj",
                        "Audit ID",
                    ]
                    visible_cols = [c for c in preferred_cols if c in df_queue_history.columns]
                    visible_cols.extend([c for c in df_queue_history.columns if c not in visible_cols])
                    df_queue_history = df_queue_history[visible_cols]

                    st.success(f"{len(df_queue_history)} adet kuyruk üyelik kaydı listeleniyor.")
                    st.caption(
                        f"Kaynak: `{meta.get('source', '-')}` | Servis: `{meta.get('service_name', '-')}` "
                        f"| Aralık: `{meta.get('range_text', '-')}`"
                    )
                    st.dataframe(df_queue_history, width='stretch')
                    st.download_button(
                        "📥 Kuyruk Üyelik Geçmişini CSV İndir",
                        data=df_queue_history.to_csv(index=False).encode("utf-8"),
                        file_name=f"queue_history_{org}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key=f"{state_prefix}_download_csv",
                    )

    # Logout moved to Organization Settings
    
    # Org DataManager controls moved to Organization Settings
