"""MIT-derived field helpers from Hermes reasoning_carriers.py and chat_completion_helpers.py.

Pinned source and modifications are recorded in third_party/hermes-port-manifest.json.
"""

from typing import Any


def field(obj: Any, name: str) -> Any:
    """Read SDK extra fields without importing the upstream application facade."""
    value = getattr(obj, name, None)
    for side in ("model_extra", "provider_data"):
        if value is None and isinstance(getattr(obj, side, None), dict):
            value = getattr(obj, side).get(name)
    dump = getattr(value, "model_dump", None)
    return dump() if callable(dump) else value


def dump_if_model(value: Any) -> Any:
    if not hasattr(value, "model_dump"):
        return value
    try:
        return value.model_dump(warnings=False)
    except TypeError:
        return value.model_dump()
