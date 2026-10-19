from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

from shittim_core.domain.types import (
    AccessContext,
    DeliveryRef,
    ExecutionContext,
    OutboundIntent,
    ProtocolReceipt,
    Segment,
)
from shittim_core.qq.codec import SendItem, plan_forward, plan_send, text_segments
from shittim_core.state import StateStore

type CallAction = Callable[[str, dict[str, Any], str], Awaitable[ProtocolReceipt]]
type FileResolver = Callable[[OutboundIntent, str, AccessContext], Awaitable[Segment]]
type ReceiptListener = Callable[[str, ProtocolReceipt], Awaitable[None]]


def plain_text(text: str) -> str:
    """Render the message body only; never apply this to attached file contents."""
    # Strip standalone opening/closing fences, keeping every content line.
    # Inline fenced snippets lose only their delimiters, not the snippet itself.
    text = re.sub(r"(?m)^[ \t]*(?:`{3,}|~{3,})(?:[A-Za-z0-9_+.-]+)?[ \t]*$", "", text)
    text = re.sub(r"`{3,}|~{3,}", "", text)
    text = re.sub(r"!?\[([^\]]+)\]\(([^)]+)\)", r"\1 \2", text)
    text = re.sub(r"(?m)^\s{0,3}#{1,6}\s+", "", text)
    text = re.sub(r"\*\*([^\n]+?)\*\*|__([^\n]+?)__", lambda m: m[1] or m[2], text)
    text = re.sub(r"`([^`\n]+)`", r"\1", text)
    text = re.sub(r"</?(?:p|div|span|strong|b|em|i|pre|code|h[1-6])(?:\s[^>]*)?>", "", text)
    return re.sub(r"<br\s*/?>", "\n", text).strip()


class Delivery:
    def __init__(
        self,
        store: StateStore,
        call: CallAction,
        *,
        file_resolver: FileResolver | None = None,
        text_limit: int = 1500,
    ) -> None:
        if type(text_limit) is not int or text_limit < 1:
            raise ValueError("Text item limit must be a positive integer")
        self.store, self.call = store, call
        self.file_resolver = file_resolver
        self.text_limit = text_limit
        self._receipt_listeners: list[ReceiptListener] = []
        self.listener_failures: list[str] = []
        self._dispatch_open = True

    def stop_dispatch(self) -> None:
        """Leave unsent outbox items intact for a later, separately validated startup."""
        self._dispatch_open = False

    def add_receipt_listener(self, callback: ReceiptListener) -> None:
        if callback not in self._receipt_listeners:
            self._receipt_listeners.append(callback)

    async def _record_receipt(self, delivery_id: str, receipt: ProtocolReceipt) -> None:
        self.store.finish_delivery(delivery_id, receipt)
        for callback in tuple(self._receipt_listeners):
            try:
                await callback(delivery_id, receipt)
            except Exception as error:
                self.listener_failures.append(type(error).__name__)

    def _items(self, intent: OutboundIntent) -> tuple[OutboundIntent, ...]:
        if intent.artifact_refs or intent.forward_message_ids:
            return (intent,)
        text = plain_text(intent.text)
        if not text:
            raise ValueError("Empty message")
        # An inquiry remains one question/reference (bounded by its tool schema).
        if intent.purpose == "inquiry":
            return (replace(intent, text=text),)
        return tuple(
            replace(
                intent,
                text=text[index : index + self.text_limit],
                reply_to=intent.reply_to if index == 0 else None,
            )
            for index in range(0, len(text), self.text_limit)
        )

    def complete_run(
        self,
        run_id: str,
        status: str,
        text: str = "",
        final_intent: OutboundIntent | None = None,
        *,
        reason: str = "",
        unresolved_effects: tuple[str, ...] = (),
    ) -> str | None:
        """Render once, then commit the outcome and every text item in one state transaction."""
        intents = self._items(final_intent) if final_intent is not None else ()
        ids = self.store.finish_run_items(
            run_id, status, text, intents, reason=reason, unresolved_effects=unresolved_effects
        )
        return ids[0] if ids else None

    async def submit(self, intent: OutboundIntent, ctx: ExecutionContext) -> DeliveryRef:
        ids = self.store.prepare_delivery_items(ctx.run.run_id, self._items(intent), ctx.input_seq)
        delivery_id = ids[0]
        await self.send(delivery_id)
        statuses = {row["status"] for row in self.store.list_delivery_items(delivery_id)}
        if statuses == {"acknowledged"}:
            return DeliveryRef(delivery_id, "acknowledged")
        for status in ("unknown", "attempting", "failed", "superseded", "prepared"):
            if status in statuses:
                return DeliveryRef(delivery_id, status)
        raise ValueError("Invalid delivery batch state")

    async def send(self, delivery_id: str) -> ProtocolReceipt | None:
        """Advance only an acknowledged prefix; no automatic retry of failed or unknown items."""
        last_receipt: ProtocolReceipt | None = None
        for item in self.store.list_delivery_items(delivery_id):
            current = self.store.get_delivery(item["delivery_id"])
            if current["status"] == "acknowledged":
                continue
            if current["status"] != "prepared":
                return current["receipt"] or last_receipt
            receipt = await self._send_item(item["delivery_id"])
            if receipt is not None:
                last_receipt = receipt
            if self.store.get_delivery(item["delivery_id"])["status"] != "acknowledged":
                break
        return last_receipt

    async def _send_item(self, delivery_id: str) -> ProtocolReceipt | None:
        # Format/validate before claiming so an invalid local intent is not an ambiguous send.
        if not self._dispatch_open:
            return None
        row = self.store.get_delivery(delivery_id)
        intent: OutboundIntent = row["intent"]
        if intent.forward_message_ids:
            try:
                context = self.store.get_context(row["context_id"])
                if intent.address != context["address"]:
                    raise PermissionError("Forward destination differs from source conversation")
                self.store.visible_message_sources(
                    row["context_id"], row["input_seq"], intent.forward_message_ids
                )
                items = (plan_forward(intent.address, intent.forward_message_ids),)
            except (PermissionError, ValueError):
                if self.store.claim_delivery(delivery_id) is None:
                    return None
                receipt = ProtocolReceipt(
                    "failed", error="forward_validation_failed_not_dispatched"
                )
                await self._record_receipt(delivery_id, receipt)
                return receipt
        elif intent.artifact_refs:
            if self.file_resolver is None:
                raise ValueError(
                    "Artifact delivery requires an explicitly authorized file resolver"
                )
            try:
                context = self.store.get_context(row["context_id"])
                segment = await self.file_resolver(intent, row["context_id"], context["access"])
                if segment.type not in {"file", "image", "record", "video"}:
                    raise ValueError("File resolver returned an unsupported segment")
                items = plan_send(intent.address, (segment,))
            except Exception:
                # The resolver performs local validation only, so no QQ request has occurred.
                if self.store.claim_delivery(delivery_id) is None:
                    return None
                receipt = ProtocolReceipt("failed", error="file_validation_failed_not_dispatched")
                await self._record_receipt(delivery_id, receipt)
                return receipt
        else:
            # Batch text was rendered before the atomic commit; cleaning each fragment
            # again would delete boundary spaces or reinterpret newly exposed Markdown.
            text = intent.text if "batch_id" in row else plain_text(intent.text)
            if not text:
                raise ValueError("Empty message")
            segments = list(text_segments(text, intent.reply_to))
            if intent.purpose == "inquiry" and intent.address.kind == "group" and intent.inquiry_id:
                question = self.store.get_inquiry(intent.inquiry_id)
                segments.insert(0, Segment("at", {"qq": question["recipient_id"]}))
            items = plan_send(intent.address, segments)
            if not text.strip() and "batch_id" in row:
                # Keep whitespace-only fragments intact; a platform rejection is a real
                # failed item rather than silently removing content from the reply.
                items = (
                    SendItem(
                        f"send_{intent.address.kind}_msg",
                        {
                            "self_id": intent.address.bot_id,
                            "group_id" if intent.address.kind == "group" else "user_id": int(
                                intent.address.target_id
                            ),
                            "message": [{"type": "text", "data": {"text": text}}],
                        },
                    ),
                )
        if len(items) != 1:
            raise ValueError("One delivery record must describe one platform send")
        if not self._dispatch_open:
            return None
        claimed = self.store.claim_delivery(delivery_id)
        if claimed is None:
            return None
        try:
            receipt = await self.call(items[0].action, items[0].params, delivery_id)
        except Exception:
            receipt = ProtocolReceipt("unknown", error="transport_result_unavailable")
        await self._record_receipt(delivery_id, receipt)
        return receipt

    async def drain_prepared(self) -> None:
        for row in self.store.list_deliveries("prepared"):
            await self.send(row["delivery_id"])
