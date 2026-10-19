# Derived from NousResearch/hermes-agent @ 9e01b44dd23978f9ff40b7837467f3a9f1582130; MIT. See third_party/hermes-replay-manifest.json.
"""Hermes complete call identity block; fixes read-only provider_data-backed call_id assignment."""
import hashlib
import logging
from typing import Any, Iterable
logger = logging.getLogger(__name__)

def _tc_field(tc: Any, key: str) -> Any:
    """Read ``key`` from a tool-call entry that may be a dict or an SDK object."""
    return tc.get(key) if isinstance(tc, dict) else getattr(tc, key, None)


def _tc_set(tc: Any, key: str, value: Any) -> None:
    tc.__setitem__(key, value) if isinstance(tc, dict) else setattr(tc, key, value)


# --------------------------------------------------------------------------- call_id policy — single owner
# (audit F4, incident chain I4) ---------------------------------------------------------------------------
# Three forked policy sites converged here: * agent/codex_responses_adapter.py `_deterministic_call_id` —
# hash synthesis when a provider omits call_id (fa3ab2ffd0 → e45f2b39e2). *
# run_agent.AIAgent._get_tool_call_id_static — `call_id or id` coalescing for dicts and SDK objects. *
# run_agent.AIAgent._uniquify_tool_call_ids — duplicate-id repair with deterministic `_d<n>` suffixes
# (#58327 loss class). NOT consolidated (different scheme on purpose):
# agent/transports/codex_event_projector._deterministic_call_id maps codex app-server ITEM ids
# (`codex_<type>_<item_id>`), not chat tool-call content; merging the two would change ids and invalidate
# prompt caches. HARD INVARIANT: everything here must stay deterministic (never uuid4) and byte-identical
# for existing inputs — these ids feed prompt-cache prefixes.
def deterministic_call_id(fn_name: str, arguments: str, index: int = 0) -> str:
    """Deterministic call_id fallback when the API omits one (random ids would break caching)."""
    seed = f"{fn_name}:{arguments}:{index}"
    return f"call_{hashlib.sha256(seed.encode('utf-8', errors='replace')).hexdigest()[:12]}"


def _expand_tool_id_variants(values: tuple[Any, ...]) -> frozenset[str]:
    """Every wire spelling of one tool-call identifier: Responses bridges may expose the pairing
    id and response-item id separately or as ``call_id|response_item_id``; all alias ONE call."""
    variants: set[str] = set()
    for raw in values:
        value = raw.strip() if isinstance(raw, str) else ""
        if value:
            variants.add(value)
            variants.update(p for p in (part.strip() for part in value.split("|")) if p)
    return frozenset(variants)


def tool_call_id_variants(tc: Any) -> frozenset[str]:
    """Return all pairing-id variants carried by a tool-call entry."""
    return _expand_tool_id_variants(tuple(_tc_field(tc, k) for k in ("call_id", "id", "response_item_id")))


def tool_result_id_variants(tool_call_id: Any) -> frozenset[str]:
    """Return all matching variants for a role=tool ``tool_call_id``."""
    return _expand_tool_id_variants((tool_call_id,))


def coalesce_tool_call_id(tc: Any) -> str:
    """Effective call id of a tool_call entry (dict or object); ``""`` when none. Codex Responses
    carry ``call_id`` (authoritative pairing key), Chat Completions ``id`` only, and bridge ids
    may be ``call_id|response_item_id``."""
    for raw in (_tc_field(tc, "call_id"), _tc_field(tc, "id")):
        value = raw.strip() if isinstance(raw, str) else ""
        if value:
            return value.split("|", 1)[0].strip() or value
    return ""


def uniquify_tool_call_ids(tool_calls: list, taken: Iterable[str] = ()) -> list:
    """Ensure every tool call in one assistant turn has an id no other call in the session has.

    Some providers reuse one id across a batch, and some name every call ``call_0`` turn after
    turn; the pre-API sanitizer then keeps only the first call/result pair per id, strict
    providers reject duplicates, and the desktop binds a tool card to the wrong call. ``taken``
    is the ids already in this session's history: they are never rewritten (prompt cache), so
    the incoming call is renamed instead. Collisions get a deterministic ``<id>_d<n>`` suffix
    (never uuid4 — cache-prefix stability). Mutates entries (SDK models / SimpleNamespace /
    dicts) in place. Blank ids are left for the deterministic fallback in
    ``build_assistant_message``.
    """
    seen: set = set(taken)
    for tc in tool_calls or []:
        # Same coalescing rule as coalesce_tool_call_id, tolerant of non-string ids.
        raw = _tc_field(tc, "call_id") or _tc_field(tc, "id") or ""
        raw = raw.strip() if isinstance(raw, str) else ""
        # Composite Responses ids ("call_x|fc_y") collide on the call half — the pairing key.
        cid = raw.split("|", 1)[0]
        if not cid:
            continue
        if cid not in seen:
            seen.add(cid)
            continue
        # range is bounded: at most len(seen) suffixes can already be taken.
        new_id = next(f"{cid}_d{n}" for n in range(2, len(seen) + 3) if f"{cid}_d{n}" not in seen)
        seen.add(new_id)

        try:
            # Keep a composite id's response-item half so the provider's fc_/item id survives.
            old = _tc_field(tc, "id")
            _tc_set(tc, "id", f"{new_id}|{old.split('|', 1)[1]}" if isinstance(old, str) and "|" in old else new_id)
            if _tc_field(tc, "call_id"):
                _set_provider_tool_id(tc, "call_id", new_id)
        except Exception:
            logger.warning("Could not uniquify duplicate tool call id %s", cid)
            continue
        _fn_name = _tc_field(_tc_field(tc, "function"), "name") or "?"
        logger.warning(
            "Model reused tool call id %s; renamed the duplicate to %s (tool=%s) to keep call/result "
            "pairing lossless.", cid, new_id, _fn_name,
        )
    return tool_calls


_PROVIDER_TOOL_ID_PREFIXES = ("chatcmpl-tool-",)

def normalize_provider_tool_call_ids(tool_calls: list) -> list:
    """Rewrite known provider ids when a parallel batch would be rejected on replay.

    The digest is deterministic so persisted messages and prompt-cache prefixes remain
    stable. Composite Responses ids retain their response-item half.
    """
    if len(tool_calls or []) < 2:
        return tool_calls
    # Gate on the effective id serialization and result pairing use (stripped, blank call_id
    # falls back to id), not on raw fields.
    if not all(coalesce_tool_call_id(tc).startswith(_PROVIDER_TOOL_ID_PREFIXES) for tc in tool_calls):
        return tool_calls
    logger.warning("Normalized provider-minted parallel tool-call ids for replay compatibility")
    for tc in tool_calls:
        # Rewrite each field's call half separately: ``id`` may carry the response-item
        # half while ``call_id`` is bare, and that half must survive.
        for key in ("id", "call_id"):
            value = _tc_field(tc, key)
            if not isinstance(value, str):
                continue
            primary, sep, item = value.strip().partition("|")
            primary = primary.strip()
            if not primary.startswith(_PROVIDER_TOOL_ID_PREFIXES):
                continue
            # surrogatepass: provider JSON can carry lone surrogates; strict utf-8 would raise,
            # and errors=replace would collapse distinct ids onto one digest.
            digest = hashlib.sha256(primary.encode("utf-8", "surrogatepass")).hexdigest()[:12]
            _set_provider_tool_id(tc, key, f"call_{digest}{sep}{item}")
    return tool_calls


def _set_provider_tool_id(tc: Any, key: str, value: str) -> None:
    # transports.types.ToolCall exposes call_id as a read-only view of provider_data;
    # write the backing value so id and call_id stay in agreement.
    if isinstance(getattr(type(tc), key, None), property) and isinstance(getattr(tc, "provider_data", None), dict):
        tc.provider_data[key] = value
    else:
        _tc_set(tc, key, value)


