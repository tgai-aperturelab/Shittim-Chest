"""Read received forward containers and deliver existing-message forwards via outbox.

Source-informed AstrBot adapter port. Forward payload authors remain attribution
claims; only real context inputs or acknowledged output IDs authorize a send node.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, cast
from uuid import uuid4

from shittim_core.delivery.service import CallAction, Delivery
from shittim_core.domain.types import (
    ExecutionContext,
    InputEvent,
    OutboundIntent,
    Segment,
    ToolResult,
)
from shittim_core.state import StateStore
from shittim_core.tools.executor import ToolExecutor, ToolSpec

QQ_FORWARD_TOOLS = frozenset({"read_forward", "send_forward"})


def forward_resource(context_id: str) -> str:
    return f"qq:forward:{context_id}"


def _node(value: object, index: int, text_budget: int) -> tuple[dict[str, Any], int]:
    if not isinstance(value, dict):
        return {"index": index, "status": "invalid_node"}, 0
    node = cast(dict[str, Any], value)
    if node.get("type") == "node" and isinstance(node.get("data"), dict):
        node = cast(dict[str, Any], node["data"])
    sender = node.get("sender")
    author = cast(dict[str, Any], sender) if isinstance(sender, dict) else node
    result: dict[str, Any] = {
        "index": index,
        "message_id": node.get("message_id") or node.get("id"),
        "author_id": author.get("user_id") or author.get("uin"),
        "author_name": author.get("nickname") or author.get("name"),
        "author_attribution": "forward_payload_claim",
        "time": node.get("time"),
    }
    raw = node.get("content", node.get("message", []))
    if isinstance(raw, str):
        result.update(text=raw[:text_budget], text_truncated=len(raw) > text_budget, segments=[])
        return result, min(len(raw), text_budget)
    if not isinstance(raw, list):
        result.update(status="invalid_content", segments=[])
        return result, 0
    text: list[str] = []
    segments: list[dict[str, Any]] = []
    consumed = 0
    truncated = False
    raw_items = cast(list[object], raw)
    for entry in raw_items[:200]:
        if not isinstance(entry, dict):
            continue
        item = cast(dict[str, Any], entry)
        data = item.get("data")
        if not isinstance(data, dict):
            continue
        details = cast(dict[str, Any], data)
        kind = str(item.get("type", "unknown"))
        if kind == "text":
            value_text = str(details.get("text", ""))
            remaining = max(0, text_budget - consumed)
            text.append(value_text[:remaining])
            consumed += min(len(value_text), remaining)
            truncated |= len(value_text) > remaining
        elif kind in {"forward", "node"}:
            segments.append(
                {"type": kind, "id": details.get("id") or details.get("resid"), "expanded": False}
            )
        else:
            # Keep identifiers, never eagerly resolve URLs/files or nested containers.
            segments.append(
                {
                    "type": kind,
                    **{
                        key: details[key]
                        for key in ("id", "qq", "file_id", "name", "file_name")
                        if key in details
                    },
                }
            )
    result.update(
        text="".join(text),
        text_truncated=truncated,
        segments=segments,
        segments_truncated=len(raw_items) > 200,
    )
    return result, consumed


def register_qq_forward_tools(
    tools: ToolExecutor, store: StateStore, call: CallAction, delivery: Delivery
) -> None:
    async def read(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        try:
            source = store.visible_message_sources(
                ctx.run.context_id, ctx.input_seq, (args["message_id"],)
            )[0]
        except PermissionError:
            return ToolResult("not_executed", "Source message is outside current context")
        event = source.get("event")
        if not isinstance(event, InputEvent):
            return ToolResult(
                "not_executed", "Reading a container requires a received source message"
            )
        forwards: list[tuple[int, Segment]] = [
            (index, part) for index, part in enumerate(event.segments) if part.type == "forward"
        ]
        ordinal = int(args.get("segment_index", 0))
        if ordinal >= len(forwards):
            return ToolResult("not_executed", "Source message has no such forward container")
        source_index, part = forwards[ordinal]
        container = part.data.get("id") or part.data.get("resid")
        if (
            not isinstance(container, (str, int))
            or isinstance(container, bool)
            or not str(container).strip()
            or len(str(container)) > 1024
        ):
            return ToolResult("not_executed", "Received container identifier is invalid")
        try:
            receipt = await call(
                "get_forward_msg",
                {"message_id": str(container), "self_id": event.address.bot_id},
                f"forward-read:{ctx.run.run_id}:{event.message_id}:{source_index}:{uuid4().hex}",
            )
        except Exception:
            return ToolResult("failed", "Forward contents could not be retrieved")
        if receipt.status != "acknowledged":
            return ToolResult(
                "failed", json.dumps({"status": "unavailable", "receipt_status": receipt.status})
            )
        messages = receipt.data.get("messages", receipt.data.get("message"))
        if not isinstance(messages, list):
            return ToolResult("failed", "Forward response has no message list")
        nodes = cast(list[object], messages)
        offset, limit = args.get("offset", 0), args.get("limit", 20)
        page: list[dict[str, Any]] = []
        remaining = 16000
        for index, raw in enumerate(nodes[offset : offset + limit], offset):
            parsed, consumed = _node(raw, index, remaining)
            remaining -= consumed
            page.append(parsed)
        next_offset = min(len(nodes), offset + len(page))
        payload = {
            "source_kind": "untrusted_forward",
            "source_input_id": source["input_id"],
            "source_message_id": event.message_id,
            "source_author_id": event.author_id,
            "source_address": event.address.key,
            "source_segment_index": source_index,
            "container_id": str(container),
            "offset": offset,
            "total": len(nodes),
            "next_offset": next_offset if next_offset < len(nodes) else None,
            "response_sha256": hashlib.sha256(
                json.dumps(receipt.data, sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest(),
            "messages": page,
        }
        return ToolResult(
            "succeeded",
            json.dumps(payload, ensure_ascii=False),
            evidence_refs=(source["input_id"],),
        )

    async def send(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        ids = tuple(str(value) for value in args["message_ids"])
        try:
            sources = store.visible_message_sources(ctx.run.context_id, ctx.input_seq, ids)
            intent = OutboundIntent(ctx.run.address, "", "message", forward_message_ids=ids)
            result = await delivery.submit(intent, ctx)
        except (PermissionError, ValueError):
            return ToolResult(
                "not_executed",
                "Only visible original messages in this conversation may be forwarded",
            )
        status = (
            "succeeded"
            if result.status == "acknowledged"
            else "failed"
            if result.status == "failed"
            else "not_executed"
            if result.status == "superseded"
            else "unknown"
        )
        return ToolResult(
            status,
            json.dumps(
                {
                    "delivery_id": result.delivery_id,
                    "status": result.status,
                    "message_ids": list(ids),
                    "sources": [
                        {key: value for key, value in source.items() if key != "event"}
                        for source in sources
                    ],
                },
                ensure_ascii=False,
            ),
            evidence_refs=(result.delivery_id,),
        )

    def resources(args: dict[str, Any], ctx: ExecutionContext) -> tuple[str, ...]:
        return (forward_resource(ctx.run.context_id),)

    tools.register(
        ToolSpec(
            "read_forward",
            "Read a forward container already received in this context. Nested forwards stay as identifiers. segment_index is the zero-based ordinal among forward segments.",
            {
                "type": "object",
                "properties": {
                    "message_id": {"type": "string", "minLength": 1},
                    "segment_index": {"type": "integer", "minimum": 0},
                    "offset": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                },
                "required": ["message_id"],
                "additionalProperties": False,
            },
            read,
            resources,
        )
    )
    tools.register(
        ToolSpec(
            "send_forward",
            "Forward original visible messages as one merged container to this conversation, preserving platform authors. Cannot create authors or custom content.",
            {
                "type": "object",
                "properties": {
                    "message_ids": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 100,
                        "uniqueItems": True,
                        "items": {"type": "string", "pattern": "^-?[0-9]+$"},
                    }
                },
                "required": ["message_ids"],
                "additionalProperties": False,
            },
            send,
            lambda _args, ctx: (forward_resource(ctx.run.context_id), "qq:" + ctx.run.address.key),
        )
    )
