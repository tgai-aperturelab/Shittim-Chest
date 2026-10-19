"""One-level QQ quotations become attributed data, never synthetic human commands."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from shittim_core.domain.types import Acceptance, AccessContext, InputEvent
from shittim_core.qq.codec import ActionCaller, ReplyResolution, resolve_reply
from shittim_core.state import StateStore


class ReferenceIntake:
    def __init__(
        self,
        store: StateStore,
        call: ActionCaller,
        notify: Callable[[str], Awaitable[None]],
        *,
        policy_check: Callable[[Mapping[str, Any]], bool],
        max_concurrent: int = 2,
    ) -> None:
        if max_concurrent < 1:
            raise ValueError("Reference concurrency must be positive")
        self.store, self.call, self.notify = store, call, notify
        self.policy_check = policy_check
        self._capacity = asyncio.Semaphore(max_concurrent)
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self.closed = False
        self.failures: list[str] = []

    @staticmethod
    def result_id(context_id: str, input_id: str) -> str:
        return f"quote:{context_id}:{input_id}"

    def _completed(self, context_id: str, input_id: str) -> bool:
        result_id = "result:" + self.result_id(context_id, input_id)
        return any(row["event_id"] == result_id for row in self.store.list_events(context_id))

    def _allowed(self, context_id: str, event: InputEvent) -> bool:
        context = self.store.get_context(context_id)
        access: AccessContext = context["access"]
        return bool(
            context["kind"] == "work"
            and context["control_state"] != "closed"
            and access.audience == event.address.key
            and self.policy_check(context)
        )

    def submit(self, event: InputEvent, accepted: Acceptance) -> bool:
        """Queue an associated source without waiting for its quoted message."""
        if (
            self.closed
            or accepted.status != "associated"
            or not accepted.input_id
            or not accepted.context_id
            or event.reply_to is None
            or event.text.strip() in {"停止", "/stop", "继续", "/resume", "关闭", "/close"}
        ):
            return False
        context_id, input_id = accepted.context_id, accepted.input_id
        source = self.store.get_input(input_id)
        if source != event:
            raise ValueError("Quotation source differs from the accepted input")
        if not any(
            row["kind"] == "input" and row["input_id"] == input_id
            for row in self.store.list_events(context_id)
        ):
            raise ValueError("Quotation source is not associated with this context")
        if not self._allowed(context_id, source) or self._completed(context_id, input_id):
            return False
        key = self.result_id(context_id, input_id)
        existing = self._tasks.get(key)
        if existing is not None and not existing.done():
            return False
        task = asyncio.create_task(self._process(source, context_id, input_id))
        self._tasks[key] = task
        task.add_done_callback(lambda completed: self._record_failure(key, completed))
        return True

    def _record_failure(self, key: str, task: asyncio.Task[None]) -> None:
        if self._tasks.get(key) is not task:
            return
        self._tasks.pop(key, None)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                self.failures.append(type(error).__name__)

    async def _record(
        self,
        event: InputEvent,
        context_id: str,
        input_id: str,
        resolution: ReplyResolution,
        reason: str = "",
    ) -> None:
        payload: dict[str, Any] = {
            "kind": "quoted_message",
            "status": resolution.status,
            "source_kind": "untrusted_quotation",
            "source_input_id": input_id,
            "source_message_id": event.message_id,
            "provider_id": event.author_id,
            "source_address": event.address.key,
            "quoted_message_id": resolution.reply_id,
            "quoted_author_id": resolution.author_id,
        }
        if reason:
            payload["reason"] = reason
        if resolution.status == "resolved":
            payload["text"] = resolution.text
            # Retain useful identity metadata only. Never expose fetch URLs or file paths,
            # nor recursively fetch quoted replies, forward nodes, or attachments.
            payload["unread_segments"] = [
                {
                    "type": part.type,
                    "id": str(part.data.get("id") or part.data.get("file_id") or ""),
                    "name": str(part.data.get("name") or part.data.get("file_name") or "")[:300],
                    "content_read": False,
                }
                for part in resolution.segments
                if part.type != "text"
            ]
        self.store.append_result_event(
            context_id,
            input_id,
            self.result_id(context_id, input_id),
            payload,
            event.address.key,
        )
        context = self.store.get_context(context_id)
        if not self.closed and context["control_state"] == "active" and self.policy_check(context):
            await self.notify(context_id)

    async def _process(self, event: InputEvent, context_id: str, input_id: str) -> None:
        assert event.reply_to is not None
        try:
            async with self._capacity:
                if not self._allowed(context_id, event):
                    await self._record(
                        event,
                        context_id,
                        input_id,
                        ReplyResolution(event.reply_to, "unavailable"),
                        "current_policy_denied",
                    )
                    return
                known_sent = self.store.resolve_sent(event.address, event.reply_to)
                result = await resolve_reply(
                    event, self.call, event.address if known_sent is not None else None
                )
                assert result is not None
                # A narrowed policy while I/O was pending must not retain newly read text.
                if not self._allowed(context_id, event):
                    result = ReplyResolution(event.reply_to, "unavailable")
                    reason = "current_policy_denied"
                else:
                    reason = ""
                await self._record(event, context_id, input_id, result, reason)
        except asyncio.CancelledError:
            if not self._completed(context_id, input_id):
                await self._record(
                    event,
                    context_id,
                    input_id,
                    ReplyResolution(event.reply_to, "unavailable"),
                    "intake_interrupted",
                )
            raise
        except Exception as error:
            self.failures.append(type(error).__name__)
            if not self._completed(context_id, input_id):
                await self._record(
                    event,
                    context_id,
                    input_id,
                    ReplyResolution(event.reply_to, "unavailable"),
                    "intake_processing_failed",
                )

    def recover(self) -> int:
        """Rebuild pending reads; persisted successes and failures are never fetched again."""
        scheduled = 0
        for context in self.store.list_contexts():
            if context["control_state"] == "closed" or not self.policy_check(context):
                continue
            for row in self.store.list_events(context["context_id"]):
                if row["kind"] == "input":
                    scheduled += self.submit(
                        row["event"],
                        Acceptance("associated", row["input_id"], context["context_id"]),
                    )
        return scheduled

    async def wait_idle(self) -> None:
        while self._tasks:
            tasks = tuple(self._tasks.values())
            await asyncio.gather(*tasks, return_exceptions=True)
            for key, task in tuple(self._tasks.items()):
                if task.done():
                    self._record_failure(key, task)

    async def close(self) -> None:
        self.closed = True
        for task in self._tasks.values():
            if not task.done():
                task.cancel()
        await self.wait_idle()
