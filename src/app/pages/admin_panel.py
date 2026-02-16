from typing import Any, Dict

from src.app.services import render_admin_panel_service


def render_admin_panel_page(context: Dict[str, Any]) -> None:
    """UI entrypoint for admin panel page."""
    render_admin_panel_service(context)
