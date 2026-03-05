from typing import Any, Dict

from src.app.services import render_dialer_service


def render_dialer_page(context: Dict[str, Any]) -> None:
    """UI entrypoint for dialer page."""
    render_dialer_service(context)
