from typing import Any, Dict

from src.app.services import render_dashboard_service


def render_dashboard_page(context: Dict[str, Any]) -> None:
    """UI entrypoint for dashboard page."""
    render_dashboard_service(context)
