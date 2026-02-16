from typing import Any, Dict

from src.app.services import render_org_settings_service


def render_org_settings_page(context: Dict[str, Any]) -> None:
    """UI entrypoint for org settings page."""
    render_org_settings_service(context)
