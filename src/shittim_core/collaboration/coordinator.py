from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import replace
from typing import Any, Literal

from shittim_core.agent.loop import AgentEngine
from shittim_core.agent.ports import RunControl, RunPorts
from shittim_core.context.compactor import Compactor
from shittim_core.context.runtime_compaction import prepare_run
from shittim_core.delivery.service import Delivery, plain_text
from shittim_core.domain.types import (
    Acceptance,
    AccessContext,
    ControlCommand,
    ControlReceipt,
    InputEvent,
    OutboundIntent,
    ProtocolReceipt,
    RunHalted,
    RunOutcome,
    RunRequest,
    StateConflict,
)
from shittim_core.state import StateStore
from shittim_core.tools.execution import EXECUTION_TOOLS
from shittim_core.tools.history import HISTORY_TOOLS, history_resource


class Coordinator:
    def __init__(
        self,
        store: StateStore,
        engine: AgentEngine,
        ports: RunPorts,
        delivery: Delivery,
        *,
        bot_id: str,
        allowed_users: frozenset[str],
        allowed_groups: frozenset[str],
        access_factory: Callable[[InputEvent], AccessContext],
        max_concurrent: int = 2,
        max_model_calls: int = 8,
        max_tool_calls: int = 12,
        timeout_seconds: float = 120,
        policy_predicate: Callable[[dict[str, Any]], bool] | None = None,
        compactor: Compactor | None = None,
        max_context_chars: int = 32000,
        on_associated: Callable[[InputEvent, Acceptance], None] | None = None,
    ) -> None:
        if max_concurrent < 1 or max_context_chars < 1:
            raise ValueError("Concurrency and context budget must be positive")
        self.store, self.engine, self.ports, self.delivery = store, engine, ports, delivery
        self.bot_id = bot_id
        self.allowed_users, self.allowed_groups = allowed_users, allowed_groups
        self.access_factory = access_factory
        self.policy_predicate: Callable[[dict[str, Any]], bool] = (
            policy_predicate or self._default_policy
        )
        self.compactor, self.max_context_chars = compactor, max_context_chars
        self.on_associated = on_associated
        self.max_model_calls, self.max_tool_calls = max_model_calls, max_tool_calls
        self.timeout_seconds = timeout_seconds
        self._intake = asyncio.Lock()
        self._capacity = asyncio.Semaphore(max_concurrent)
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._controls: dict[str, RunControl] = {}
        self._run_controls: dict[str, RunControl] = {}
        self._notices: set[asyncio.Task[Any]] = set()
        self._resume_requested: dict[str, int] = {}
        self.failures: list[str] = []
        self.closed = False
        self.last_recovery: dict[str, list[str]] | None = None
        self.deferred_contexts: dict[str, str] = {}
        self.deferred_inputs: dict[str, str] = {}
        self.delivery.add_receipt_listener(self._receipt_arrived)

    def _possible_early_answer(self, event: InputEvent) -> bool:
        if not event.reply_to:
            return False
        for delivery in self.store.list_deliveries("attempting"):
            inquiry_id = delivery["intent"].inquiry_id
            if inquiry_id:
                question = self.store.get_inquiry(inquiry_id)
                if (
                    question["address"] == event.address
                    and question["recipient_id"] == event.author_id
                ):
                    return True
        return False

    async def _receipt_arrived(self, delivery_id: str, receipt: ProtocolReceipt) -> None:
        if self.closed:
            return
        intent = self.store.get_delivery(delivery_id)["intent"]
        address = intent.address
        recipient = (
            self.store.get_inquiry(intent.inquiry_id)["recipient_id"] if intent.inquiry_id else None
        )
        for input_id, event in self.store.list_unbound_inputs():
            if event.address == address and (
                (
                    receipt.status == "acknowledged"
                    and receipt.message_id is not None
                    and event.reply_to == receipt.message_id
                )
                or event.author_id == recipient
            ):
                result = await self._accept(event, recover_unbound=True)
                if (
                    result.status in {"associated", "duplicate"}
                    or result.reason == "inactive_inquiry_answer"
                ):
                    self.deferred_inputs.pop(input_id, None)
                elif result.reason:
                    self.deferred_inputs[input_id] = result.reason

    def _default_policy(self, context: dict[str, Any]) -> bool:
        address = context["address"]
        access: AccessContext = context["access"]
        owner = str(context["owner_id"])
        if address.bot_id != self.bot_id or owner not in self.allowed_users:
            return False
        if address.kind == "group" and address.target_id not in self.allowed_groups:
            return False
        expected = self.access_factory(InputEvent(address, owner, "policy-check", ""))
        resources = expected.allowed_resources
        if {"read_forward", "send_forward"}.intersection(expected.allowed_tools):
            resources |= {"qq:forward:" + str(context["context_id"])}
        if HISTORY_TOOLS.intersection(expected.allowed_tools):
            resources |= {history_resource(str(context["context_id"]))}
        if (EXECUTION_TOOLS | {"skill_manage"}).intersection(expected.allowed_tools):
            resources |= {"execution:workspace:" + str(context["context_id"])}
        if {"skills_list", "skill_view", "skill_manage"}.intersection(expected.allowed_tools):
            resources |= {"skills:task:" + str(context["context_id"])}
        if {
            "read_material",
            "send_material",
            "create_material",
            "import_material",
            "export_file",
            "vision_analyze",
        }.intersection(expected.allowed_tools):
            resources |= {"materials:" + str(context["context_id"])}
        return (
            access.actor_id == owner
            and access.audience == address.key
            and access.allowed_tools.issubset(expected.allowed_tools)
            and access.allowed_resources.issubset(resources)
            and access.allowed_recipients.issubset(expected.allowed_recipients)
        )

    async def accept(self, event: InputEvent) -> Acceptance:
        return await self._accept(event)

    async def recover_unbound_inputs(self) -> list[Acceptance]:
        """Recover durable intake gaps without replaying uncertain control commands."""
        results: list[Acceptance] = []
        for input_id, event in self.store.list_unbound_inputs():
            if event.text.strip() in {"停止", "/stop", "继续", "/resume"}:
                self.deferred_inputs[input_id] = "control_input_requires_explicit_continuation"
                continue
            result = await self._accept(event, recover_unbound=True)
            results.append(result)
            if result.status not in {"associated", "duplicate"}:
                self.deferred_inputs[input_id] = result.reason or result.status
        return results

    async def _accept(self, event: InputEvent, *, recover_unbound: bool = False) -> Acceptance:
        result = await self._accept_locked(event, recover_unbound=recover_unbound)
        if (
            result.status == "associated"
            and result.reason != "new_conversation"
            and self.on_associated is not None
        ):
            try:
                self.on_associated(event, result)
            except Exception as error:
                # Intake is already durable; recovery can finish missing enrichment.
                self.failures.append("enrichment:" + type(error).__name__)
        return result

    async def _accept_locked(
        self, event: InputEvent, *, recover_unbound: bool = False
    ) -> Acceptance:
        async with self._intake:
            if self.closed or event.address.bot_id != self.bot_id or event.author_id == self.bot_id:
                return Acceptance("ignored", reason="wrong_account_or_self")
            if event.author_id not in self.allowed_users:
                return Acceptance("rejected", reason="sender_not_allowed")
            if event.address.kind == "group" and event.address.target_id not in self.allowed_groups:
                return Acceptance("rejected", reason="group_not_allowed")
            linked: dict[str, Any] | None = None
            quoted_notice = False
            if event.reply_to:
                linked = self.store.resolve_sent(event.address, event.reply_to)
                if linked is None:
                    linked = self.store.resolve_input(event.address, event.reply_to)
                if linked is not None:
                    quoted_notice = (
                        self.store.get_context(str(linked["context_id"]))["kind"] == "router_notice"
                    )
            late_attachment_reason = ""
            late_attachment_bound = False
            if linked is None:
                linked, late_attachment_reason = self._late_attachment_reference(event)
                late_attachment_bound = linked is not None
            early_answer = linked is None and self._possible_early_answer(event)
            if (
                event.address.kind == "group"
                and not event.mentioned_bot
                and linked is None
                and not early_answer
                and not late_attachment_reason
            ):
                # Persistence or an in-flight inquiry was only a provisional intake
                # reason. Recovery must prove an actual trigger again; it grants none.
                return Acceptance("ignored", reason="not_addressed")
            if quoted_notice:
                linked = None
            input_id, duplicate = self.store.ingest(event)
            if duplicate:
                previous = self.store.resolve_input(event.address, event.message_id)
                if previous is not None or not recover_unbound:
                    return Acceptance(
                        "duplicate", input_id, previous["context_id"] if previous else None
                    )
            if late_attachment_reason:
                self.deferred_inputs[input_id] = late_attachment_reason
                return Acceptance("stored", input_id, reason=late_attachment_reason)
            if early_answer:
                self.deferred_inputs[input_id] = "awaiting_inquiry_receipt_evidence"
                return Acceptance("stored", input_id, reason="awaiting_inquiry_receipt_evidence")
            commands: dict[str, Literal["stop", "resume", "close"]] = {
                "停止": "stop",
                "/stop": "stop",
                "继续": "resume",
                "/resume": "resume",
            }
            command = commands.get(event.text.strip())
            new_conversation = event.text.strip() in {"/new", "新会话"}
            focused = self.store.conversation_focus(event.address, event.author_id)
            if (
                (linked is None or new_conversation)
                and focused is not None
                and focused["control_state"] != "closed"
                and command not in {"stop", "close"}
                and not self.policy_predicate(focused)
            ):
                focus_id = str(focused["context_id"])
                self.deferred_contexts[focus_id] = "current_policy_rejects_saved_authority"
                return Acceptance("rejected", input_id, focus_id, "context_policy_changed")
            if new_conversation:
                context_id = self._new_context(event)
                self._queue_notice(
                    input_id,
                    "已开始新会话。请直接告诉我要处理的事情。",
                    context_id,
                    update_focus=True,
                    start_conversation=True,
                )
                return Acceptance("associated", input_id, context_id, "new_conversation")
            if (
                linked is None
                and focused is not None
                and (focused["control_state"] != "closed" or command)
            ):
                linked = {"context_id": focused["context_id"]}
            if command:
                if linked is None:
                    self._queue_notice(input_id, "请引用要停止或继续的那条请求，避免影响其他事情。")
                    return Acceptance("needs_clarification", input_id, reason="quote_the_target")
                context_id = str(linked["context_id"])
                receipt = await self.control(ControlCommand(context_id, event.author_id, command))
                if receipt.accepted:
                    self._queue_notice(
                        input_id,
                        "已暂停后续执行。已经发出的操作可能仍会返回结果。"
                        if command == "stop"
                        else "已收到继续执行的请求。尚未确认结果的操作不会自动重做。",
                        context_id,
                        update_focus=True,
                    )
                else:
                    self._queue_notice(
                        input_id,
                        "这条请求暂时不能由你控制，请让发起人确认。"
                        if receipt.reason != "context_policy_changed"
                        else "这条请求当前不具备继续执行所需的权限。",
                    )
                return Acceptance(
                    "associated" if receipt.accepted else "rejected",
                    input_id,
                    context_id,
                    receipt.reason,
                )
            if linked is not None:
                context_id = str(linked["context_id"])
                if not self.policy_predicate(self.store.get_context(context_id)):
                    self.deferred_contexts[context_id] = "current_policy_rejects_saved_authority"
                    return Acceptance("rejected", input_id, context_id, "context_policy_changed")
                saved_context = self.store.get_context(context_id)
                update_focus = (
                    not late_attachment_bound
                    and saved_context["control_state"] != "closed"
                    and saved_context["address"] == event.address
                )
                inquiry_id = linked.get("inquiry_id")
                if inquiry_id:
                    question = self.store.get_inquiry(str(inquiry_id))
                    if question["recipient_id"] == event.author_id and question["status"] in {
                        "cancelled",
                        "superseded",
                    }:
                        self.store.record_late_answer(str(inquiry_id), input_id)
                        self._queue_notice(
                            input_id,
                            "这条问题已取消或被替代。答复已保留，不会据此继续旧操作。",
                        )
                        return Acceptance("stored", input_id, context_id, "inactive_inquiry_answer")
                    if question["recipient_id"] == event.author_id and question["status"] == "open":
                        self.store.answer_inquiry(
                            str(inquiry_id), input_id, update_focus=update_focus
                        )
                    else:
                        self.store.attach_input(context_id, input_id, update_focus=update_focus)
                else:
                    self.store.attach_input(context_id, input_id, update_focus=update_focus)
            else:
                context_id = self._new_context(event)
                self.store.attach_input(context_id, input_id, update_focus=True)
            if self.store.get_context(context_id)["control_state"] == "active":
                self._schedule(context_id)
            return Acceptance("associated", input_id, context_id)

    def _new_context(self, event: InputEvent) -> str:
        context_id = uuid.uuid4().hex
        access = self.access_factory(event)
        if {
            "read_material",
            "send_material",
            "create_material",
            "import_material",
            "export_file",
            "vision_analyze",
        }.intersection(access.allowed_tools):
            access = replace(
                access,
                allowed_resources=access.allowed_resources | frozenset({"materials:" + context_id}),
            )
        if {"skills_list", "skill_view", "skill_manage"}.intersection(access.allowed_tools):
            access = replace(
                access,
                allowed_resources=access.allowed_resources | {"skills:task:" + context_id},
            )
        if (EXECUTION_TOOLS | {"skill_manage"}).intersection(access.allowed_tools):
            access = replace(
                access,
                allowed_resources=access.allowed_resources | {"execution:workspace:" + context_id},
            )
        if {"read_forward", "send_forward"}.intersection(access.allowed_tools):
            access = replace(
                access,
                allowed_resources=access.allowed_resources | {"qq:forward:" + context_id},
            )
        if HISTORY_TOOLS.intersection(access.allowed_tools):
            access = replace(
                access,
                allowed_resources=access.allowed_resources | {history_resource(context_id)},
            )
        self.store.create_context(context_id, event, access)
        return context_id

    def _late_attachment_reference(self, event: InputEvent) -> tuple[dict[str, Any] | None, str]:
        """Associate only a unique durable same-author reference to this exact message.

        QQ may report a quoted request before its file message. This is a reverse
        edge lookup, not a same-user/time heuristic; existing contexts are never merged.
        Nonempty text, controls, quoted attachments and non-file segments take the
        normal intake route so a late command cannot be resurrected by this shortcut.
        """
        attachment_types = {"file", "image", "record", "video"}
        if (
            event.reply_to is not None
            or event.text.strip()
            or not any(segment.type in attachment_types for segment in event.segments)
            or any(
                segment.type not in attachment_types | {"text"}
                or segment.type == "text"
                and str(segment.data.get("text", "")).strip()
                for segment in event.segments
            )
        ):
            return None, ""
        controls = {"停止", "/stop", "继续", "/resume", "关闭", "/close"}
        referenced: list[dict[str, Any]] = []
        for context in self.store.list_contexts(event.address):
            if context["kind"] != "work":
                continue
            for row in self.store.list_events(str(context["context_id"])):
                if row["kind"] != "input":
                    continue
                source: InputEvent = row["event"]
                if (
                    source.address == event.address
                    and source.author_id == event.author_id
                    and source.reply_to == event.message_id
                    and source.text.strip()
                    and source.text.strip() not in controls
                ):
                    referenced.append(context)
                    break
        if not referenced:
            return None, ""
        eligible = [
            context
            for context in referenced
            if context["control_state"] in {"active", "paused"} and self.policy_predicate(context)
        ]
        if len(eligible) == 1:
            return {"context_id": eligible[0]["context_id"]}, ""
        if len(eligible) > 1:
            return None, "referenced_attachment_has_multiple_contexts"
        return None, "referenced_attachment_context_inactive_or_policy_denied"

    def _queue_notice(
        self,
        input_id: str,
        text: str,
        context_id: str | None = None,
        *,
        update_focus: bool = False,
        start_conversation: bool = False,
    ) -> None:
        event = self.store.get_input(input_id)
        delivery_id = self.store.prepare_notice(
            input_id,
            text,
            AccessContext(event.author_id, event.address.key),
            context_id,
            update_focus=update_focus,
            start_conversation=start_conversation,
        )
        task = asyncio.create_task(self.delivery.send(delivery_id))
        self._notices.add(task)
        task.add_done_callback(self._notice_done)

    def _notice_done(self, task: asyncio.Task[Any]) -> None:
        self._notices.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self.failures.append(type(task.exception()).__name__)

    async def control(self, command: ControlCommand) -> ControlReceipt:
        context = self.store.get_context(command.context_id)
        if context["kind"] != "work":
            return ControlReceipt(False, context["control_epoch"], "quote_the_target")
        if command.action == "resume" and not self.policy_predicate(context):
            return ControlReceipt(False, context["control_epoch"], "context_policy_changed")
        receipt = self.store.control(command.context_id, command.actor_id, command.action)
        if not receipt.accepted:
            return receipt
        control = self._controls.get(command.context_id)
        if control and command.action in {"stop", "close"}:
            control.stop("user_stop")
        if command.action in {"stop", "close"}:
            self._resume_requested.pop(command.context_id, None)
        if command.action == "resume":
            self._resume_requested[command.context_id] = receipt.control_epoch
            self._schedule(command.context_id)
        return receipt

    def _schedule(self, context_id: str) -> None:
        task = self._tasks.get(context_id)
        if task is None or task.done():
            task = asyncio.create_task(self._run_context(context_id))
            self._tasks[context_id] = task
            task.add_done_callback(lambda done: self._context_done(context_id, done))

    def _context_done(self, context_id: str, task: asyncio.Task[None]) -> None:
        if self._tasks.get(context_id) is task:
            self._tasks.pop(context_id, None)
        if not task.cancelled() and task.exception() is not None:
            self.failures.append(type(task.exception()).__name__)
        if not self.closed and context_id in self._resume_requested:
            self._schedule(context_id)

    def _release_control(self, run_id: str, context_id: str, control: RunControl) -> None:
        if control.inflight:
            for observer in tuple(control.inflight):
                observer.add_done_callback(
                    lambda _done: self._release_control(run_id, context_id, control)
                )
            return
        self._run_controls.pop(run_id, None)
        self.failures.extend("late_result:" + error for error in control.late_errors)
        control.late_errors.clear()
        if self._controls.get(context_id) is control:
            self._controls.pop(context_id, None)

    async def notify_result(self, context_id: str) -> None:
        """Wake already accepted work only after its durable enrichment result."""
        context = self.store.get_context(context_id)
        if (
            not self.closed
            and context["kind"] == "work"
            and context["control_state"] == "active"
            and self.policy_predicate(context)
        ):
            self._schedule(context_id)

    async def _run_context(self, context_id: str) -> None:
        async with self._capacity:
            while not self.closed:
                context = self.store.get_context(context_id)
                if context["control_state"] != "active" or context["kind"] != "work":
                    self._resume_requested.pop(context_id, None)
                    return
                if not self.policy_predicate(context):
                    self.deferred_contexts[context_id] = "current_policy_rejects_saved_authority"
                    self._resume_requested.pop(context_id, None)
                    return
                run_id = uuid.uuid4().hex
                try:
                    epoch = self.store.claim_run(context_id, run_id)
                except (StateConflict, RunHalted):
                    self._resume_requested.pop(context_id, None)
                    return
                self._resume_requested.pop(context_id, None)
                request = RunRequest(
                    run_id,
                    context_id,
                    context["address"],
                    context["access"],
                    epoch,
                    self.max_model_calls,
                    self.max_tool_calls,
                    self.timeout_seconds,
                )
                control = RunControl()
                self._controls[context_id] = control
                self._run_controls[run_id] = control
                try:
                    if self.compactor is not None:
                        request = await prepare_run(
                            request, self.ports, control, self.compactor, self.max_context_chars
                        )
                    outcome = await self.engine.run(request, self.ports, control)
                except Exception as exc:
                    self.failures.append(type(exc).__name__)
                    outcome = RunOutcome("failed", reason="execution_failed")
                if control.reason == "shutdown" and outcome.status == "cancelled":
                    outcome = replace(outcome, status="interrupted")
                if outcome.status == "completed" and not plain_text(outcome.text):
                    outcome = RunOutcome("failed", reason="empty_visible_output")
                final = (
                    OutboundIntent(request.address, outcome.text)
                    if outcome.status == "completed" and outcome.text
                    else None
                )
                if outcome.status == "yielded" and outcome.reason == "unknown":
                    final = OutboundIntent(
                        request.address,
                        "有一项操作的结果尚未确认，相关操作已暂缓，不会自动重复执行。",
                        "message",
                    )
                elif outcome.status == "failed":
                    final = OutboundIntent(
                        request.address,
                        "本次处理未能完成。已确认的结果已保留，尚未确认的操作不会自动重做。",
                        "message",
                    )
                try:
                    delivery_id = self.delivery.complete_run(
                        run_id,
                        outcome.status,
                        outcome.text,
                        final,
                        reason=outcome.reason,
                        unresolved_effects=outcome.unresolved_effects,
                    )
                    if delivery_id:
                        await self.delivery.send(delivery_id)
                except Exception as exc:
                    self.failures.append(type(exc).__name__)
                    self._release_control(run_id, context_id, control)
                    return
                self._release_control(run_id, context_id, control)
                latest = self.store.get_context(context_id)
                run = self.store.get_run(run_id)
                if latest["control_state"] != "active" or latest["input_seq"] <= run["input_seq"]:
                    return
                # New accepted input gets its own continuation; old tool calls are never replayed.
                # Release the concurrency slot before the next run so queued contexts get a turn.
                self._resume_requested[context_id] = int(latest["control_epoch"])
                return

    async def wait_idle(self) -> None:
        while True:
            tasks = [task for task in self._tasks.values() if not task.done()]
            tasks.extend(task for task in self._notices if not task.done())
            if not tasks:
                return
            await asyncio.gather(*tasks, return_exceptions=True)

    def begin_shutdown(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.delivery.stop_dispatch()
        for control in tuple(self._run_controls.values()):
            control.stop("shutdown")
        try:
            self.store.revoke_running_authority()
        except StateConflict:
            # StateStore also revokes these run IDs in memory before attempting its write.
            self.failures.append("shutdown_authority_persistence_failed")

    async def close(self) -> bool:
        self.begin_shutdown()
        await self.wait_idle()
        drained = True
        for control in tuple(self._run_controls.values()):
            drained = await control.drain_inflight(timeout=5.0) and drained
        return drained

    def recover(self) -> dict[str, list[str]]:
        # Recovery is deliberately separate from starting new model work.
        self.last_recovery = self.store.recover()
        return self.last_recovery

    def resume_pending(self) -> list[str]:
        """Resume a once-captured startup report after transport readiness, never all history."""
        report = self.last_recovery
        if report is None or self.closed:
            return []
        blocked: set[str] = set()
        for effect in self.store.list_effects():
            if effect["status"] in {"attempting", "unknown"}:
                blocked.add(str(self.store.get_run(effect["run_id"])["context_id"]))
        for delivery in self.store.list_deliveries():
            if delivery["status"] in {"attempting", "unknown"}:
                blocked.add(str(delivery["context_id"]))
        scheduled: list[str] = []
        for context_id in dict.fromkeys(
            [*report.get("active_contexts", []), *report.get("interrupted_contexts", [])]
        ):
            if context_id in blocked:
                self.deferred_contexts[context_id] = "unresolved_external_effect"
                continue
            context = self.store.get_context(context_id)
            if not self.policy_predicate(context):
                self.deferred_contexts[context_id] = "current_policy_rejects_saved_authority"
                continue
            if context["control_state"] == "active":
                self._schedule(context_id)
                scheduled.append(context_id)
        self.last_recovery = None
        return scheduled
