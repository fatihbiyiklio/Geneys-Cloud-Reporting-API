from typing import Any, Dict

from src.app.context import bind_context


def _audit_user_action(action, detail=None, status="info", metadata=None):
    audit_fn = globals().get("_log_user_action")
    if callable(audit_fn):
        try:
            audit_fn(
                action=action,
                detail=detail,
                status=status,
                metadata=metadata,
                source="users-service",
            )
        except Exception:
            pass


def render_users_service(context: Dict[str, Any]) -> None:
    """Render users management page using injected app context."""
    bind_context(globals(), context)
    st.title(f"👤 {get_text(lang, 'menu_users')}")
    
    with st.expander(f"➕ {get_text(lang, 'add_new_user')}", expanded=True):
        # Auto password generator
        col_gen1, col_gen2 = st.columns([3, 1])
        with col_gen2:
            if st.button(f"🔐 {get_text(lang, 'generate_password_btn')}", key="gen_pw_btn"):
                st.session_state.generated_password = generate_password(12)
        
        generated_pw = st.session_state.get("generated_password", "")
        if generated_pw:
            col_gen1.success(f"Oluşturulan Şifre: **{generated_pw}**")
        
        with st.form("add_user_form"):
            new_un = st.text_input(get_text(lang, "username"))
            new_pw = st.text_input(get_text(lang, "password"), type="password", value=generated_pw, help=get_text(lang, "password_help"))
            new_role = st.selectbox(get_text(lang, "role"), ["Admin", "Manager", "Reports User", "Dashboard User"])
            
            from src.lang import ALL_METRICS
            new_mets = st.multiselect(get_text(lang, "allowed_metrics"), ALL_METRICS, format_func=lambda x: get_text(lang, x))
            
            if st.form_submit_button(get_text(lang, "add"), width='stretch'):
                if new_un and new_pw:
                    org = st.session_state.app_user.get('org_code', 'default')
                    success, msg = auth_manager.add_user(org, new_un, new_pw, new_role, new_mets)
                    if success: 
                        _audit_user_action(
                            action="user_create",
                            detail=f"User '{new_un}' created.",
                            status="success",
                            metadata={"target_user": new_un, "role": new_role},
                        )
                        st.session_state.generated_password = ""  # Clear after use
                        st.success(msg)
                        st.rerun()
                    else:
                        _audit_user_action(
                            action="user_create",
                            detail=str(msg or f"User '{new_un}' creation failed."),
                            status="error",
                            metadata={"target_user": new_un, "role": new_role},
                        )
                        st.error(msg)
                else: st.warning("Ad ve şifre gereklidir.")
    
    st.write("---")
    st.subheader("Mevcut Kullanıcılar")
    org = st.session_state.app_user.get('org_code', 'default')
    all_users = auth_manager.get_all_users(org)
    current_username = st.session_state.app_user.get("username")
    for uname, udata in all_users.items():
        col1, col2, col3, col4 = st.columns([2, 2, 4, 1])
        col1.write(f"**{uname}**")
        col2.write(f"Rol: {udata.get('role', 'User')}")
        col3.write(f"Metrikler: {', '.join(udata.get('metrics', [])) if udata.get('metrics') else 'Hepsi'}")
        
        # Action Buttons Column
        with col4:
            if uname != current_username:
                if st.button("🗑️", key=f"del_user_{uname}", help="Kullanıcıyı Sil"):
                    ok, msg = auth_manager.delete_user(org, uname)
                    if ok:
                        _audit_user_action(
                            action="user_delete",
                            detail=f"User '{uname}' deleted.",
                            status="success",
                            metadata={"target_user": uname},
                        )
                        st.success(msg)
                        st.rerun()
                    else:
                        _audit_user_action(
                            action="user_delete",
                            detail=str(msg or f"User '{uname}' delete failed."),
                            status="error",
                            metadata={"target_user": uname},
                        )
                        st.error(msg)
        
        # Password Reset Section
        with st.expander(f"🔑 Şifre Sıfırla: {uname}"):
            with st.form(key=f"reset_pw_form_{uname}"):
                new_reset_pw = st.text_input("Yeni Şifre", type="password", key=f"new_pw_{uname}")
                if st.form_submit_button("Güncelle"):
                    if new_reset_pw:
                        success, msg = auth_manager.reset_password(org, uname, new_reset_pw)
                        if success:
                            _audit_user_action(
                                action="user_password_reset",
                                detail=f"Password reset for '{uname}'.",
                                status="success",
                                metadata={"target_user": uname},
                            )
                            st.success(msg)
                        else:
                            _audit_user_action(
                                action="user_password_reset",
                                detail=str(msg or f"Password reset failed for '{uname}'."),
                                status="error",
                                metadata={"target_user": uname},
                            )
                            st.error(msg)
                    else:
                        st.warning("Lütfen yeni şifre girin.")
        st.write("---")
