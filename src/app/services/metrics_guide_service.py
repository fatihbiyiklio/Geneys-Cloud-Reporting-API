from typing import Any, Dict

from src.app.context import bind_context


def render_metrics_guide_service(context: Dict[str, Any]) -> None:
    """Render metrics guide page using injected app context."""
    bind_context(globals(), context)
    st.title(f"📘 {get_text(lang, 'menu_metrics_guide')}")
    ref_path = _resolve_resource_path("METRICS_REFERENCE.md")
    if os.path.exists(ref_path):
        try:
            with open(ref_path, "r", encoding="utf-8") as f:
                st.markdown(f.read())
        except Exception as e:
            st.error(f"Doküman okunamadı: {e}")
    else:
        st.warning(f"Referans dosyası bulunamadı: {ref_path}")

