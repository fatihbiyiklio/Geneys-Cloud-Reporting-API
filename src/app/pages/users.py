from typing import Any, Dict

from src.app.services import render_users_service


def render_users_page(context: Dict[str, Any]) -> None:
    """UI entrypoint for users page."""
    render_users_service(context)
