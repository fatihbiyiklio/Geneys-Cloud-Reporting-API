from typing import Any, Dict

from src.app.services import render_reports_service


def render_reports_page(context: Dict[str, Any]) -> None:
    """UI entrypoint for reports page."""
    render_reports_service(context)
