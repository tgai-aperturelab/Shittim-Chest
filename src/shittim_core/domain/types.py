"""Application-owned identities and outcomes; protocol data is never authority."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal

type JsonObject = dict[str, Any]


def _empty_object() -> JsonObject:
    return {}


type EffectStatus = Literal["succeeded", "failed", "not_executed", "needs_input", "unknown"]
type DeliveryStatus = Literal[
    "prepared", "attempting", "acknowledged", "failed", "unknown", "superseded"
]


@dataclass(frozen=True)
class ConversationAddress:
    bot_id: str
    kind: Literal["private", "group"]
    target_id: str

    def __post_init__(self) -> None:
        if self.kind not in ("private", "group") or not self.bot_id or not self.target_id:
            raise ValueError("Invalid conversation address")

    @property
    def key(self) -> str:
        return json.dumps([self.bot_id, self.kind, self.target_id], separators=(",", ":"))


@dataclass(frozen=True)
class Segment:
    type: str
    data: JsonObject


@dataclass(frozen=True)
class InputEvent:
    address: ConversationAddress
    author_id: str
    message_id: str
    text: str
    segments: tuple[Segment, ...] = ()
    reply_to: str | None = None
    mentioned_bot: bool = False
    source_time: float | None = None
    received_time: float | None = None
    raw: JsonObject = field(default_factory=_empty_object, repr=False)

    @property
    def key(self) -> str:
        return json.dumps([self.address.key, self.message_id], separators=(",", ":"))


@dataclass(frozen=True)
class Acceptance:
    status: Literal[
        "duplicate", "stored", "associated", "needs_clarification", "ignored", "rejected"
    ]
    input_id: str | None = None
    context_id: str | None = None
    reason: str = ""


@dataclass(frozen=True)
class AccessContext:
    actor_id: str
    audience: str
    allowed_tools: frozenset[str] = frozenset()
    allowed_resources: frozenset[str] = frozenset()
    allowed_recipients: frozenset[str] = frozenset()
    policy_version: int = 1


@dataclass(frozen=True)
class RunRequest:
    run_id: str
    context_id: str
    address: ConversationAddress
    access: AccessContext
    control_epoch: int
    max_model_calls: int = 8
    max_tool_calls: int = 12
    timeout_seconds: float = 120

    def __post_init__(self) -> None:
        if self.max_model_calls < 1 or self.max_tool_calls < 1 or self.timeout_seconds <= 0:
            raise ValueError("Run budgets must be positive and finite")
        if self.timeout_seconds == float("inf") or self.timeout_seconds != self.timeout_seconds:
            raise ValueError("Run timeout must be finite")


@dataclass(frozen=True)
class ContextView:
    messages: tuple[JsonObject, ...]
    input_seq: int
    history_version: int
    policy_version: int
    control_epoch: int
    source_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExecutionContext:
    run: RunRequest
    input_seq: int
    history_version: int


@dataclass(frozen=True)
class ToolResult:
    status: EffectStatus
    content: str
    evidence_refs: tuple[str, ...] = ()
    artifact_refs: tuple[str, ...] = ()
    effect_ref: str | None = None

    def to_json(self) -> str:
        return json.dumps(
            {
                "status": self.status,
                "content": self.content,
                "evidence_refs": self.evidence_refs,
                "artifact_refs": self.artifact_refs,
                "effect_ref": self.effect_ref,
            },
            ensure_ascii=False,
        )


@dataclass(frozen=True)
class RunOutcome:
    status: Literal["completed", "yielded", "failed", "cancelled", "interrupted"]
    text: str = ""
    reason: str = ""
    unresolved_effects: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProtocolReceipt:
    status: Literal["acknowledged", "failed", "unknown"]
    data: JsonObject = field(default_factory=_empty_object)
    message_id: str | None = None
    retcode: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class OutboundIntent:
    address: ConversationAddress
    text: str
    purpose: Literal["final", "message", "inquiry", "control", "progress"] = "final"
    reply_to: str | None = None
    artifact_refs: tuple[str, ...] = ()
    inquiry_id: str | None = None
    presentation: Literal["file", "image", "record", "video"] = "file"
    forward_message_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.forward_message_ids:
            if self.text or self.artifact_refs or self.reply_to or self.inquiry_id:
                raise ValueError("A merged forward cannot mix text, artifacts, or inquiry metadata")
            if self.purpose != "message" or len(self.forward_message_ids) > 100:
                raise ValueError("Invalid merged-forward purpose or item count")
            if len(set(self.forward_message_ids)) != len(self.forward_message_ids):
                raise ValueError("Duplicate merged-forward messages")
            if any(
                type(value) is not str
                or not value.isascii()
                or not value.removeprefix("-").isdigit()
                for value in self.forward_message_ids
            ):
                raise ValueError("Merged forward requires actual numeric message identifiers")


@dataclass(frozen=True)
class DeliveryRef:
    delivery_id: str
    status: DeliveryStatus


@dataclass(frozen=True)
class ControlCommand:
    context_id: str
    actor_id: str
    action: Literal["stop", "resume", "close"]


@dataclass(frozen=True)
class ControlReceipt:
    accepted: bool
    control_epoch: int
    reason: str = ""


class RunHalted(Exception):
    """No further effects may be authorized by this run."""


class StaleView(Exception):
    """New relevant input requires a new model decision before dispatch."""


class StateConflict(Exception):
    """A conditional state transition failed without changing state."""
