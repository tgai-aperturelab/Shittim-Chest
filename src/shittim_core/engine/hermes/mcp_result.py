# Derived from NousResearch/hermes-agent @ 9e01b44dd23978f9ff40b7837467f3a9f1582130; MIT. See third_party/hermes-mcp-manifest.json.
"""Protocol-reserved MCP metadata filtering, without gateway/cache imports."""
from typing import Any, Optional

def _is_reserved_mcp_meta_key(key: str) -> bool:
    """True if an MCP ``_meta`` key uses a protocol-reserved prefix: a ``modelcontextprotocol``
    or ``mcp`` label followed by at least one more label. A trailing one
    (``com.example.mcp/...``) is a vendor namespace.

    Ported from MoonshotAI/kimi-code#2600.
    """
    slash = key.find("/")
    if slash <= 0:
        return False
    labels = key[:slash].split(".")
    return any(label in ("modelcontextprotocol", "mcp") and i < len(labels) - 1 for i, label in enumerate(labels))

def strip_reserved_meta_keys(meta: Any) -> Optional[dict[str, Any]]:
    """Drop protocol-reserved keys from ``_meta``; None if nothing model-facing remains or the
    input wasn't a mapping."""
    if not isinstance(meta, dict):
        return None
    out = {k: v for k, v in meta.items() if isinstance(k, str) and (not _is_reserved_mcp_meta_key(k))}
    return out or None
