from dataclasses import dataclass
from typing import Any, Dict, Mapping


REQUIRED_CONTEXT_KEYS = (
    "st",
    "get_text",
    "lang",
    "role",
)


@dataclass(frozen=True)
class PageContextMeta:
    """Minimal metadata used by page/router layers."""

    page: str
    role: str
    lang: str


def bind_context(module_globals: Dict[str, Any], context: Mapping[str, Any]) -> None:
    """Inject shared app context into module globals.

    `app.py` remains the composition root; page/service modules are import-safe
    and receive runtime dependencies through this context injection.
    """
    if not isinstance(context, Mapping):
        raise TypeError("context must be a mapping")

    missing = [key for key in REQUIRED_CONTEXT_KEYS if key not in context]
    if missing:
        raise KeyError(f"missing context keys: {', '.join(missing)}")

    module_globals.update(dict(context))
