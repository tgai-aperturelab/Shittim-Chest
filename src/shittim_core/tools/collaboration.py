from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from shittim_core.delivery.service import Delivery, plain_text
from shittim_core.domain.types import (
    ConversationAddress,
    ExecutionContext,
    RunHalted,
    StaleView,
    StateConflict,
    ToolResult,
)
from shittim_core.state import StateStore
from shittim_core.tools.executor import ToolExecutor, ToolSpec

type RecipientVerifier = Callable[[ConversationAddress, str], Awaitable[bool]]


def observed_recipient(store: StateStore, address: ConversationAddress, recipient_id: str) -> bool:
    """Only actual stored input authors at the exact address count, never quoted names."""
    for context in store.list_contexts():
        for row in store.list_events(str(context["context_id"])):
            event = row.get("event")
            if (
                row["kind"] == "input"
                and event is not None
                and event.address == address
                and event.author_id == recipient_id
            ):
                return True
    return any(
        event.address == address and event.author_id == recipient_id
        for _, event in store.list_unbound_inputs()
    )


def register_collaboration_tools(
    tools: ToolExecutor,
    store: StateStore,
    delivery: Delivery,
    *,
    known_recipient: RecipientVerifier | None = None,
) -> None:
    async def verified(address: ConversationAddress, recipient_id: str) -> bool:
        if recipient_id == address.bot_id:
            return False
        if known_recipient is None:
            return observed_recipient(store, address, recipient_id)
        try:
            return await known_recipient(address, recipient_id)
        except Exception:
            return False

    async def send_question(inquiry_id: str, delivery_id: str) -> ToolResult:
        await delivery.send(delivery_id)
        sent = store.get_delivery(delivery_id)
        if sent["status"] == "superseded":
            return ToolResult(
                "not_executed", "Question was superseded before dispatch", (delivery_id,)
            )
        if sent["status"] != "acknowledged":
            return ToolResult(
                "failed" if sent["status"] == "failed" else "unknown",
                "Question delivery is not confirmed",
                (delivery_id,),
            )
        return ToolResult(
            "succeeded",
            f"Question sent; answer status={store.get_inquiry(inquiry_id)['status']}. "
            f"inquiry_id={inquiry_id}",
            (inquiry_id, delivery_id),
        )

    async def ask(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        address = ConversationAddress(ctx.run.address.bot_id, args["kind"], args["target_id"])
        if (
            address.key != ctx.run.address.key
            and address.key not in ctx.run.access.allowed_recipients
        ):
            return ToolResult("not_executed", "Recipient is outside this request's scope")
        if not await verified(address, str(args["recipient_id"])):
            return ToolResult("not_executed", "Recipient identity or membership is not verified")
        try:
            store.assert_current(ctx.run.run_id, ctx.run.control_epoch, ctx.input_seq)
            inquiry_id, delivery_id = store.create_inquiry(
                ctx.run.run_id, args["recipient_id"], address, args["question"], ctx.input_seq
            )
        except (RunHalted, StaleView, StateConflict):
            return ToolResult("not_executed", "Question was not prepared for dispatch")
        return await send_question(inquiry_id, delivery_id)

    def resources(args: dict[str, Any], ctx: ExecutionContext) -> tuple[str, ...]:
        address = ConversationAddress(ctx.run.address.bot_id, args["kind"], args["target_id"])
        if (
            address.kind == "private"
            and args["recipient_id"] != address.target_id
            or args["recipient_id"] == address.bot_id
            or not plain_text(args["question"])
        ):
            raise ValueError("Question target or text is invalid")
        if address != ctx.run.address and address.key not in ctx.run.access.allowed_recipients:
            raise ValueError("Recipient address is outside this request's scope")
        return ("qq:" + address.key,)

    tools.register(
        ToolSpec(
            "ask_person",
            "向明确成员询问缺少的信息；结果是发送状态，不是真人答案。",
            {
                "type": "object",
                "properties": {
                    "recipient_id": {"type": "string", "pattern": "^[0-9]+$"},
                    "kind": {"enum": ["private", "group"]},
                    "target_id": {"type": "string", "pattern": "^[0-9]+$"},
                    "question": {"type": "string", "minLength": 1, "maxLength": 3000},
                },
                "required": ["recipient_id", "kind", "target_id", "question"],
                "additionalProperties": False,
            },
            ask,
            resources,
        )
    )

    def management_target(args: dict[str, Any], ctx: ExecutionContext):
        try:
            old = store.get_inquiry(str(args["inquiry_id"]))
        except StateConflict as exc:
            raise ValueError("Unknown inquiry") from exc
        if old["context_id"] != ctx.run.context_id:
            raise ValueError("Inquiry belongs to another context")
        old_address: ConversationAddress = old["address"]
        address = ConversationAddress(
            ctx.run.address.bot_id,
            args.get("kind", old_address.kind),
            args.get("target_id", old_address.target_id),
        )
        recipient_id = str(args.get("recipient_id", old["recipient_id"]))
        return old, address, recipient_id

    def management_resources(args: dict[str, Any], ctx: ExecutionContext) -> tuple[str, ...]:
        _old, address, recipient_id = management_target(args, ctx)
        if args["action"] == "cancel":
            # Cancelling validity is local bookkeeping; it must not clear an unknown QQ effect.
            return ()
        return resources(
            {
                "kind": address.kind,
                "target_id": address.target_id,
                "recipient_id": recipient_id,
                "question": args["question"],
            },
            ctx,
        )

    async def manage(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        try:
            _old, address, recipient_id = management_target(args, ctx)
            if args["action"] == "cancel":
                cancelled = store.cancel_inquiry(ctx.run.run_id, args["inquiry_id"], ctx.input_seq)
                return ToolResult(
                    "succeeded",
                    "Question cancelled for future continuation; sent messages and "
                    "uncertain external effects were not reversed",
                    (str(cancelled["inquiry_id"]),),
                )
            if not await verified(address, recipient_id):
                return ToolResult("not_executed", "Replacement recipient is not verified")
            identity = json.dumps(
                [ctx.run.run_id, args["inquiry_id"], recipient_id, address.key, args["question"]],
                ensure_ascii=False,
            )
            replacement_id = "inquiry_" + uuid5(NAMESPACE_URL, identity).hex
            inquiry_id, delivery_id = store.replace_inquiry(
                ctx.run.run_id,
                args["inquiry_id"],
                recipient_id,
                address,
                args["question"],
                ctx.input_seq,
                replacement_id,
            )
        except (ValueError, RunHalted, StaleView, StateConflict):
            return ToolResult(
                "not_executed", "Inquiry change was not authorized or is no longer current"
            )
        return await send_question(inquiry_id, delivery_id)

    tools.register(
        ToolSpec(
            "manage_inquiry",
            "取消当前上下文的未答问题，或替换问题及已验证收件人；不撤回已发送消息，不清除未知效果。",
            {
                "type": "object",
                "properties": {
                    "action": {"enum": ["cancel", "replace"]},
                    "inquiry_id": {"type": "string", "minLength": 1},
                    "question": {"type": "string", "minLength": 1, "maxLength": 3000},
                    "recipient_id": {"type": "string", "pattern": "^[0-9]+$"},
                    "kind": {"enum": ["private", "group"]},
                    "target_id": {"type": "string", "pattern": "^[0-9]+$"},
                },
                "required": ["action", "inquiry_id"],
                "additionalProperties": False,
                "dependentRequired": {"kind": ["target_id"], "target_id": ["kind"]},
                "if": {"properties": {"action": {"const": "replace"}}},
                "then": {"required": ["question"]},
                "else": {"maxProperties": 2},
            },
            manage,
            management_resources,
        )
    )

    async def wait(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        pending = store.list_open_inquiries(ctx.run.context_id)
        wanted = str(args["inquiry_id"])
        matches = [item for item in pending if item["inquiry_id"] == wanted]
        if not matches:
            return ToolResult("not_executed", "No open question with that identity")
        sent = store.get_delivery(matches[0]["delivery_id"])
        if sent["status"] != "acknowledged":
            return ToolResult("not_executed", "Cannot wait for an unconfirmed question")
        return ToolResult("needs_input", "Waiting for the real recipient's answer", (wanted,))

    tools.register(
        ToolSpec(
            "wait_for_input",
            "结束当前执行并等待已发送问题的真实回复，不轮询或自动催问。",
            {
                "type": "object",
                "properties": {"inquiry_id": {"type": "string", "minLength": 1}},
                "required": ["inquiry_id"],
                "additionalProperties": False,
            },
            wait,
            lambda _a, _c: (),
        )
    )
