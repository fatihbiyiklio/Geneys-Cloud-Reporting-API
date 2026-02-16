from typing import Any, Dict, Mapping

from src.app.pages import (
    render_metrics_guide_page,
    render_reports_page,
    render_users_page,
    render_org_settings_page,
    render_admin_panel_page,
    render_dashboard_page,
)


def render_page(context: Mapping[str, Any]) -> None:
    """Render current page based on session selection and role."""
    page = context.get("page")
    role = context.get("role")
    get_text = context.get("get_text")
    lang = context.get("lang")

    if not callable(get_text):
        raise TypeError("context['get_text'] must be callable")

    if page == get_text(lang, "menu_metrics_guide"):
        render_metrics_guide_page(dict(context))
        return

    if page == get_text(lang, "menu_reports"):
        render_reports_page(dict(context))
        return

    if page == get_text(lang, "menu_users") and role == "Admin":
        render_users_page(dict(context))
        return

    if page == get_text(lang, "menu_org_settings") and role == "Admin":
        render_org_settings_page(dict(context))
        return

    if page == get_text(lang, "admin_panel") and role == "Admin":
        render_admin_panel_page(dict(context))
        return

    if page == get_text(lang, "menu_dashboard"):
        render_dashboard_page(dict(context))
        return
