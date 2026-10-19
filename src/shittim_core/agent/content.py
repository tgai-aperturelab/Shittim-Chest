# Derived from NousResearch/hermes-agent @ 9e01b44dd23978f9ff40b7837467f3a9f1582130. MIT; see third_party/hermes-port-manifest.json.
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

_NON_TEXT_PART_TYPES = {"image", "image_url", "input_image", "audio", "input_audio"}
_TEXT_KEYS = ("text", "content", "input_text", "output_text", "summary_text")


def _field(value: Any, key: str) -> Any:
    return (
        cast(Mapping[str, Any], value).get(key)
        if isinstance(value, Mapping)
        else getattr(value, key, None)
    )


def _text_from_part(part: Any) -> str:
    if part is None:
        return ""
    if isinstance(part, str):
        return part
    if str(_field(part, "type") or "").strip().lower() in _NON_TEXT_PART_TYPES:
        return ""
    for key in _TEXT_KEYS:
        text = _field(part, key)
        if isinstance(text, str):
            return text
    return ""


def flatten_message_text(content: Any, *, sep: str = "\n") -> str:
    """Return the visible text from common chat/Responses message content shapes."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return sep.join(chunk for chunk in map(_text_from_part, cast(list[Any], content)) if chunk)
    text = _text_from_part(content)
    if text:
        return text
    try:
        return str(content)
    except Exception:
        return ""
