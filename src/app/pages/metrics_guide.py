from typing import Any, Dict

from src.app.services import render_metrics_guide_service


def render_metrics_guide_page(context: Dict[str, Any]) -> None:
    """UI entrypoint for metrics guide page."""
    render_metrics_guide_service(context)
