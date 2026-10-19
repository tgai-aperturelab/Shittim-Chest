# SPDX-License-Identifier: AGPL-3.0-or-later
# Adapted from AstrBotDevs/AstrBot, commit e5e21b71aec5642f110eb115eb8e9a0d8028b7e5.
# Source ranges and modifications are recorded in third_party/astrbot-port-manifest.json.
"""AstrBot-derived OneBot codecs without its host, components or agent classes."""

from __future__ import annotations

import base64
import itertools
import math
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from typing import Any, Literal, Protocol, cast
from urllib.parse import quote

from shittim_core.domain.types import (
    ConversationAddress,
    InputEvent,
    JsonObject,
    ProtocolReceipt,
    Segment,
)


class ActionCaller(Protocol):
    async def __call__(
        self, action: str, params: Mapping[str, Any], attempt_id: str
    ) -> ProtocolReceipt: ...


def _identifier(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    result = str(value)
    return result if result.isascii() and result.removeprefix("-").isdigit() else None


def decode_event(payload: JsonObject, bot_id: str) -> InputEvent | None:
    """Decode a validated connection's message without doing network enrichment.

    Args:
        payload: Parsed OneBot object from an authenticated connection.
        bot_id: Identity bound to that connection.

    Returns:
        An application-owned message, or None for unsupported/invalid/self events.
    """
    if payload.get("post_type") != "message" or _identifier(payload.get("self_id")) != bot_id:
        return None
    author = _identifier(payload.get("user_id"))
    message_id = _identifier(payload.get("message_id"))
    kind = payload.get("message_type")
    if author is None or author == bot_id or message_id is None:
        return None
    sender = payload.get("sender")
    if isinstance(sender, dict):
        sender_id = cast(JsonObject, sender).get("user_id")
        if sender_id is not None and _identifier(sender_id) != author:
            return None
    if kind not in ("group", "private"):
        return None
    target = _identifier(payload.get("group_id")) if kind == "group" else author
    if target is None:
        return None
    raw_segments = payload.get("message")
    if not isinstance(raw_segments, list):
        return None
    parts: list[Segment] = []
    for raw_item in cast(list[object], raw_segments):
        if not isinstance(raw_item, dict):
            return None
        item = cast(JsonObject, raw_item)
        if not isinstance(item.get("type"), str) or not isinstance(item.get("data"), dict):
            return None
        parts.append(Segment(item["type"], dict(item["data"])))

    # Preserve AstrBot's adjacent-text grouping; retain all source segment IDs.
    text: list[str] = []
    for segment_type, group in itertools.groupby(parts, key=lambda part: part.type):
        if segment_type == "text":
            text.append("".join(str(part.data.get("text", "")) for part in group).strip())
        elif segment_type == "at":
            for part in group:
                qq = str(part.data.get("qq", ""))
                if qq != bot_id:
                    text.append(f" @{qq} ")
        elif segment_type == "markdown":
            text.extend(
                str(part.data.get("markdown", part.data.get("content", ""))) for part in group
            )
    reply = next((_identifier(p.data.get("id")) for p in parts if p.type == "reply"), None)
    timestamp = payload.get("time")
    return InputEvent(
        address=ConversationAddress(bot_id, kind, target),
        author_id=author,
        message_id=message_id,
        text="".join(text),
        segments=tuple(parts),
        reply_to=reply,
        mentioned_bot=any(p.type == "at" and str(p.data.get("qq")) == bot_id for p in parts),
        source_time=(
            float(timestamp)
            if isinstance(timestamp, (int, float))
            and not isinstance(timestamp, bool)
            and math.isfinite(timestamp)
            else None
        ),
        received_time=time.time(),
        raw=dict(payload),
    )


@dataclass(frozen=True)
class ReplyResolution:
    reply_id: str
    status: Literal["resolved", "unavailable", "wrong_scope"]
    author_id: str | None = None
    segments: tuple[Segment, ...] = ()
    text: str = ""


async def resolve_reply(
    event: InputEvent,
    call: ActionCaller,
    known_sent_address: ConversationAddress | None = None,
) -> ReplyResolution | None:
    """Resolve one quoted message; nested replies remain IDs rather than recursive requests.

    Args:
        event: Incoming message whose quote needs enrichment.
        call: Account-bound OneBot action function.
        known_sent_address: Trusted local mapping for a bot-authored private quote.

    Returns:
        Resolution preserving the original quote ID even when fetching fails.
    """
    if event.reply_to is None:
        return None
    reply_id = event.reply_to
    try:
        result = await call(
            "get_msg",
            {"message_id": reply_id, "self_id": event.address.bot_id},
            f"quote:{event.key}:{reply_id}",
        )
    except Exception:
        return ReplyResolution(reply_id, "unavailable")
    if result.status != "acknowledged":
        return ReplyResolution(reply_id, "unavailable")
    data = result.data
    if _identifier(data.get("message_id")) != reply_id:
        return ReplyResolution(reply_id, "wrong_scope")
    sender = data.get("sender")
    author = (
        _identifier(cast(JsonObject, sender).get("user_id")) if isinstance(sender, dict) else None
    )
    if author is None:
        return ReplyResolution(reply_id, "unavailable")
    if data.get("user_id") is not None and _identifier(data["user_id"]) != author:
        return ReplyResolution(reply_id, "wrong_scope")
    if data.get("self_id") is not None and str(data["self_id"]) != event.address.bot_id:
        return ReplyResolution(reply_id, "wrong_scope")
    if data.get("message_type") is not None and data["message_type"] != event.address.kind:
        return ReplyResolution(reply_id, "wrong_scope")
    if event.address.kind == "group":
        allowed = str(data.get("group_id")) == event.address.target_id
    else:
        allowed = not data.get("group_id") and (
            author == event.author_id
            or (author == event.address.bot_id and known_sent_address == event.address)
        )
    if not allowed:
        return ReplyResolution(reply_id, "wrong_scope")
    raw = data.get("message")
    if not isinstance(raw, list):
        return ReplyResolution(reply_id, "unavailable")
    parts: list[Segment] = []
    for raw_item in cast(list[object], raw):
        if not isinstance(raw_item, dict):
            return ReplyResolution(reply_id, "unavailable")
        item = cast(JsonObject, raw_item)
        if not isinstance(item.get("type"), str):
            return ReplyResolution(reply_id, "unavailable")
        if not isinstance(item.get("data"), dict):
            return ReplyResolution(reply_id, "unavailable")
        parts.append(Segment(item["type"], dict(item["data"])))
    return ReplyResolution(
        reply_id,
        "resolved",
        author,
        tuple(parts),
        "".join(str(p.data.get("text", "")) for p in parts if p.type == "text"),
    )


@dataclass(frozen=True)
class FileResolution:
    index: int
    file_id: str
    name: str
    status: Literal["located", "unavailable"]
    url: str | None = None
    media_type: str = "file"


async def resolve_files(event: InputEvent, call: ActionCaller) -> tuple[FileResolution, ...]:
    """Locate each attachment independently without downloading or claiming it was read.

    Args:
        event: Source event preserving ordered file segments.
        call: Account-bound action function.

    Returns:
        One result per file, including unavailable files and their original IDs.
    """
    results: list[FileResolution] = []
    for index, part in enumerate(event.segments):
        if part.type not in {"file", "image", "record", "video"}:
            continue
        file_id = str(part.data.get("file_id") or part.data.get("file") or "")
        name = str(
            part.data.get("file_name") or part.data.get("name") or part.data.get("file") or "file"
        )
        direct_url = part.data.get("url") or part.data.get("file")
        if isinstance(direct_url, str) and direct_url.startswith(("https://", "http://")):
            results.append(FileResolution(index, file_id, name, "located", direct_url, part.type))
            continue
        if part.type != "file":
            # Media lacking a remote URL is retained as unavailable, never mistaken for a
            # controller-local path. A future media resolver may use a verified OneBot API.
            results.append(
                FileResolution(index, file_id, name, "unavailable", media_type=part.type)
            )
            continue
        params: JsonObject = {"file_id": file_id, "self_id": event.address.bot_id}
        if event.address.kind == "group":
            params["group_id"] = event.address.target_id
        try:
            result = await call(
                f"get_{event.address.kind}_file_url", params, f"file:{event.key}:{index}"
            )
            url = result.data.get("url")
            if (
                result.status == "acknowledged"
                and isinstance(url, str)
                and url.startswith(("https://", "http://"))
            ):
                name = str(result.data.get("file_name") or result.data.get("name") or name)
                results.append(FileResolution(index, file_id, name, "located", url))
                continue
        except Exception:
            pass
        results.append(FileResolution(index, file_id, name, "unavailable"))
    return tuple(results)


def text_segments(text: str, reply_to: str | None = None) -> tuple[Segment, ...]:
    """Build literal text and an optional quote; this function does not parse markup."""
    result = [Segment("reply", {"id": reply_to})] if reply_to else []
    if text.strip():
        result.append(Segment("text", {"text": text}))
    return tuple(result)


def media_segment(kind: Literal["image", "record", "video"], data: bytes, path: str) -> Segment:
    """AstrBot's inline image/record conversion; video uses the shared local file."""
    if kind in {"image", "record"}:
        return Segment(kind, {"file": "base64://" + base64.b64encode(data).decode("ascii")})
    return Segment(kind, {"file": path})


def _encode(parts: Sequence[Segment]) -> list[JsonObject]:
    encoded: list[JsonObject] = []
    for part in parts:
        if part.type == "text":
            value = str(part.data.get("text", ""))
            if value.strip():
                encoded.append({"type": "text", "data": {"text": value}})
        elif part.type == "reply":
            if _identifier(part.data.get("id")) is None:
                raise ValueError("Invalid reply ID")
            encoded.append({"type": "reply", "data": {"id": str(part.data["id"])}})
        elif part.type == "at":
            qq = str(part.data.get("qq", ""))
            if qq != "all" and _identifier(qq) is None:
                raise ValueError("Invalid mention target")
            encoded.extend(
                [{"type": "at", "data": {"qq": qq}}, {"type": "text", "data": {"text": " "}}]
            )
        elif part.type in {"file", "image", "record", "video"}:
            file = str(part.data.get("file") or part.data.get("url") or "")
            if not file:
                raise ValueError("File source missing")
            if "://" not in file:
                if PureWindowsPath(file).is_absolute():
                    windows_path = PureWindowsPath(file).as_posix()
                    file = ("file:" if windows_path.startswith("//") else "file:///") + quote(
                        windows_path, safe="/:"
                    )
                elif Path(file).is_absolute():
                    file = Path(file).as_uri()
            data: JsonObject = {"file": file}
            if part.type == "file":
                data["name"] = str(part.data.get("name") or "file")
            encoded.append({"type": part.type, "data": data})
        else:
            raise ValueError(f"Unsupported output segment: {part.type}")
    return encoded


@dataclass(frozen=True)
class SendItem:
    action: str
    params: JsonObject


def plan_forward(address: ConversationAddress, message_ids: Sequence[str]) -> SendItem:
    """AstrBot group/private forward dispatch, using existing nodes without forged authors."""
    if not message_ids or len(message_ids) > 100 or len(set(message_ids)) != len(message_ids):
        raise ValueError("A merged forward needs 1..100 distinct existing messages")
    if any(
        _identifier(value) is None for value in (address.bot_id, address.target_id, *message_ids)
    ):
        raise ValueError("Forward account, destination, and message IDs must be numeric")
    return SendItem(
        f"send_{address.kind}_forward_msg",
        {
            "self_id": address.bot_id,
            "group_id" if address.kind == "group" else "user_id": int(address.target_id),
            "messages": [
                {"type": "node", "data": {"id": message_id}} for message_id in message_ids
            ],
        },
    )


def plan_send(address: ConversationAddress, parts: Sequence[Segment]) -> tuple[SendItem, ...]:
    """Preserve AstrBot's isolated file sends and group/private account routing.

    Args:
        address: Explicit destination, independent of execution session IDs.
        parts: Ordered supported segments.

    Returns:
        Ordered protocol items; delivery owns attempts and receipts, not this codec.
    """
    if _identifier(address.bot_id) is None or _identifier(address.target_id) is None:
        raise ValueError("QQ account and target IDs must be numeric")
    # Keep files isolated, but retain a quote with its adjacent text instead of
    # emitting an invalid quote-only message when a file follows it.
    batches: list[list[Segment]] = []
    current: list[Segment] = []
    for part in parts:
        if part.type == "file":
            if current:
                batches.append(current)
                current = []
            batches.append([part])
        else:
            current.append(part)
    if current:
        batches.append(current)
    result: list[SendItem] = []
    for batch in batches:
        encoded = _encode(batch)
        if encoded:
            result.append(
                SendItem(
                    f"send_{address.kind}_msg",
                    {
                        "self_id": address.bot_id,
                        "group_id" if address.kind == "group" else "user_id": int(
                            address.target_id
                        ),
                        "message": encoded,
                    },
                )
            )
    return tuple(result)
