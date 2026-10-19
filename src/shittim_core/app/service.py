"""Explicit composition root for the independent candidate application."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any
from uuid import uuid4

import httpx

from shittim_core.agent.loop import AgentEngine
from shittim_core.agent.model import OpenAIModel
from shittim_core.agent.ports import AsyncModel
from shittim_core.app.config import Settings
from shittim_core.app.lease import DataDirectoryLease
from shittim_core.app.material_intake import MaterialIntake
from shittim_core.app.progress import ProgressConfig, ProgressReporter
from shittim_core.app.reference_intake import ReferenceIntake
from shittim_core.app.run_ports import RuntimePorts
from shittim_core.collaboration.coordinator import Coordinator
from shittim_core.context.builder import ContextBuilder
from shittim_core.context.compactor import Compactor
from shittim_core.delivery.service import Delivery
from shittim_core.domain.types import (
    Acceptance,
    AccessContext,
    ConversationAddress,
    InputEvent,
    OutboundIntent,
    Segment,
)
from shittim_core.engine.mcp_client import MCPRuntime
from shittim_core.engine.processes import ExecutionRuntime
from shittim_core.materials.store import MaterialStore
from shittim_core.qq.codec import media_segment
from shittim_core.qq.transport import QQTransport
from shittim_core.state import StateStore
from shittim_core.tools.collaboration import observed_recipient, register_collaboration_tools
from shittim_core.tools.execution import (
    EXECUTION_TOOLS,
    HOST_RESOURCE,
    MATERIAL_TOOLS,
    register_execution_tools,
)
from shittim_core.tools.executor import ToolExecutor
from shittim_core.tools.history import HISTORY_TOOLS, history_resource, register_history_tools
from shittim_core.tools.material_tools import register_material_tools
from shittim_core.tools.mcp_tools import register_mcp_tools
from shittim_core.tools.qq_forward import (
    QQ_FORWARD_TOOLS,
    forward_resource,
    register_qq_forward_tools,
)
from shittim_core.tools.runtime_skills import (
    RUNTIME_SKILL_TOOLS,
    register_runtime_skills,
    task_skill_resource,
)
from shittim_core.tools.vision import VISION_TOOLS, VisionConfig, register_vision_tool
from shittim_core.tools.web import WEB_RESOURCE, WEB_TOOLS, WebRuntime, register_web_tools


@dataclass(frozen=True)
class ShutdownResult:
    drained: bool
    released: bool
    reason: str


class Application:
    def __init__(
        self,
        settings: Settings,
        *,
        model: AsyncModel | None = None,
        environ: Mapping[str, str] | None = None,
        download_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings
        self._injected_model = model
        self._environ = environ
        self._injected_download_client = download_client
        self._owned_download_client: httpx.AsyncClient | None = None
        self.material_intake: MaterialIntake | None = None
        self.reference_intake: ReferenceIntake | None = None
        self._owned_model: OpenAIModel | None = None
        self._owned_vision_model: OpenAIModel | None = None
        self.lease = DataDirectoryLease(settings.data_dir)
        self.store: StateStore | None = None
        self.context: ContextBuilder | None = None
        self.tools: ToolExecutor | None = None
        self.execution: ExecutionRuntime | None = None
        self.web: WebRuntime | None = None
        self.material_web: WebRuntime | None = None
        self.mcp: list[MCPRuntime] = []
        self.mcp_failures: list[dict[str, str]] = []
        self.mcp_granted_tools: frozenset[str] = frozenset()
        self.mcp_granted_resources: frozenset[str] = frozenset()
        self.ports: RuntimePorts | None = None
        self.engine: AgentEngine | None = None
        self.transport: QQTransport | None = None
        self.delivery: Delivery | None = None
        self.progress: ProgressReporter | None = None
        self.coordinator: Coordinator | None = None
        self.recovery: dict[str, list[str]] = {}
        self.port: int | None = None
        self._accepting = False
        self._started = False
        self._finished = False
        self._activation: asyncio.Task[None] | None = None
        self.recovery_failures: list[str] = []
        self.deferred_policy: dict[str, str] = {}
        self.recovery_complete = asyncio.Event()

    def _host_mutation_ready(self) -> None:
        assert self.store is not None
        if any(row["status"] == "unknown" for row in self.store.list_processes()):
            raise ValueError("Host has an execution process with unknown outcome")
        if any(
            row["status"] == "unknown" and HOST_RESOURCE in row["resource_keys"]
            for row in self.store.list_effects()
        ):
            raise ValueError("Host has an unresolved effect")

    def _host_start_ready(self) -> None:
        self._host_mutation_ready()
        assert self.store is not None
        if any(
            row["status"] == "attempting" and HOST_RESOURCE in row["resource_keys"]
            for row in self.store.list_effects()
        ):
            raise ValueError("Host has an effect in flight")

    def access_for(self, event: InputEvent) -> AccessContext:
        """Grant only configured capabilities and the current QQ conversation resource."""
        return AccessContext(
            actor_id=event.author_id,
            audience=event.address.key,
            allowed_tools=self.settings.allowed_tools | self.mcp_granted_tools,
            allowed_resources=self.settings.allowed_resources
            | {"qq:" + event.address.key}
            | {root.resource for root in self.settings.runtime_skill_roots}
            | self.mcp_granted_resources
            | (
                {WEB_RESOURCE}
                if self.settings.allowed_tools.intersection(WEB_TOOLS)
                else set[str]()
            )
            | (
                {HOST_RESOURCE}
                if self.settings.execution_backend == "local_host"
                or self.settings.execution_backend == "bubblewrap"
                and self.settings.execution_network == "host"
                else set[str]()
            ),
            allowed_recipients=frozenset(),
        )

    def context_allowed(self, context: Mapping[str, Any]) -> bool:
        """Persisted grants must remain a subset of this explicitly loaded startup policy."""
        address: ConversationAddress = context["address"]
        access: AccessContext = context["access"]
        owner = str(context["owner_id"])
        if address.bot_id != self.settings.bot_id or owner not in self.settings.allowed_users:
            return False
        if address.kind == "group" and address.target_id not in self.settings.allowed_groups:
            return False
        if address.kind == "private" and address.target_id != owner:
            return False
        resources = self.settings.allowed_resources | {"qq:" + address.key}
        resources |= {root.resource for root in self.settings.runtime_skill_roots}
        resources |= self.mcp_granted_resources
        if self.settings.allowed_tools.intersection(WEB_TOOLS):
            resources |= {WEB_RESOURCE}
        if self.settings.allowed_tools.intersection(QQ_FORWARD_TOOLS):
            resources |= {forward_resource(str(context["context_id"]))}
        if self.settings.allowed_tools.intersection(HISTORY_TOOLS):
            resources |= {history_resource(str(context["context_id"]))}
        if self.settings.allowed_tools.intersection(RUNTIME_SKILL_TOOLS):
            resources |= {task_skill_resource(str(context["context_id"]))}
        if (
            self.settings.execution_backend == "local_host"
            or self.settings.execution_backend == "bubblewrap"
            and self.settings.execution_network == "host"
        ):
            resources |= {HOST_RESOURCE}
        if self.settings.allowed_tools.intersection(EXECUTION_TOOLS | {"skill_manage"}):
            resources |= {"execution:workspace:" + str(context["context_id"])}
        if self.settings.allowed_tools.intersection(MATERIAL_TOOLS):
            resources |= {"materials:" + str(context["context_id"])}
        return (
            access.actor_id == owner
            and access.audience == address.key
            and access.allowed_tools.issubset(self.settings.allowed_tools | self.mcp_granted_tools)
            and access.allowed_resources.issubset(resources)
            and not access.allowed_recipients
        )

    async def _known_recipient(self, address: ConversationAddress, recipient_id: str) -> bool:
        """Configured human IDs plus observed identity or a matching read-only QQ lookup."""
        if (
            recipient_id not in self.settings.allowed_users
            or address.bot_id != self.settings.bot_id
            or address.kind == "group"
            and address.target_id not in self.settings.allowed_groups
            or address.kind == "private"
            and address.target_id != recipient_id
        ):
            return False
        assert self.store is not None
        if observed_recipient(self.store, address, recipient_id):
            return True
        if self.transport is None:
            return False
        action = "get_group_member_info" if address.kind == "group" else "get_stranger_info"
        params: dict[str, Any] = {
            "self_id": address.bot_id,
            "user_id": int(recipient_id),
            "no_cache": True,
        }
        if address.kind == "group":
            params["group_id"] = int(address.target_id)
        result = await self.transport.call(action, params, "recipient-check:" + uuid4().hex)
        if result.status != "acknowledged" or str(result.data.get("user_id", "")) != recipient_id:
            return False
        if address.kind == "group" and str(result.data.get("group_id", "")) != address.target_id:
            return False
        if "self_id" in result.data and str(result.data["self_id"]) != address.bot_id:
            return False
        return True

    async def _resolve_output_file(
        self,
        intent: OutboundIntent,
        context_id: str,
        access: AccessContext,
    ) -> Segment:
        """Resolve only verified immutable local artifacts for an explicitly shared filesystem."""
        if (
            not self.settings.napcat_shared_local_files
            or "send_material" not in access.allowed_tools
            or len(intent.artifact_refs) != 1
            or intent.text
            or intent.reply_to is not None
            or intent.address.key != access.audience
        ):
            raise PermissionError("Local artifact delivery is not authorized")
        assert self.store is not None
        if not self.context_allowed(self.store.get_context(context_id)):
            raise PermissionError("Current application policy rejects this context")
        artifact_id, separator, raw_version = intent.artifact_refs[0].rpartition("@")
        if not separator or not raw_version.isdigit() or not artifact_id:
            raise ValueError("Delivery artifact reference must pin its version")
        materials = MaterialStore(
            self.settings.data_dir / "materials",
            self.store,
            context_id,
            max_bytes=self.settings.max_attachment_bytes,
        )
        result = await materials.read(artifact_id, access, version=int(raw_version))
        if result.status != "read" or result.ref is None:
            raise PermissionError("Artifact scope, integrity or availability validation failed")
        meta = self.store.get_artifact(artifact_id, version=result.ref.version)
        if intent.presentation != "file" and result.data is not None:
            return media_segment(intent.presentation, result.data, str(meta["path"]))
        return Segment("file", {"file": str(meta["path"]), "name": result.ref.name})

    def _apply_startup_policy(self) -> None:
        assert self.store is not None
        for context in self.store.list_contexts():
            if self.context_allowed(context):
                continue
            context_id = str(context["context_id"])
            self.deferred_policy[context_id] = "persisted_grant_outside_current_settings"
            # Preserve closed state; invalidate prepared sends even when already paused.
            self.store.control(
                context_id,
                str(context["owner_id"]),
                "close" if context["control_state"] == "closed" else "stop",
            )

    async def _receive(self, event: InputEvent) -> Acceptance:
        if not self._accepting or self.coordinator is None:
            return Acceptance("ignored", reason="application_stopping")
        return await self.coordinator.accept(event)

    def _after_associated(self, event: InputEvent, accepted: Acceptance) -> None:
        if self.reference_intake is not None:
            self.reference_intake.submit(event, accepted)
        if self.material_intake is not None:
            self.material_intake.submit(event, accepted)

    async def _activate_recovery(self) -> None:
        """Old work waits for an authenticated transport; prepared sends aren't failed offline."""
        assert self.transport is not None and self.coordinator is not None
        assert self.delivery is not None and self.store is not None
        await self.transport.ready.wait()
        if not self._accepting:
            return
        try:
            await self.coordinator.recover_unbound_inputs()
            if self.reference_intake is not None:
                self.reference_intake.recover()
            if self.material_intake is not None:
                self.material_intake.recover()
            if self.execution is not None:
                await self.execution.recover_completions()
            self.coordinator.resume_pending()
            for row in self.store.list_deliveries("prepared"):
                if not self._accepting:
                    break
                await self.delivery.send(row["delivery_id"])
        except Exception as error:
            self.recovery_failures.append(type(error).__name__)
        finally:
            self.recovery_complete.set()

    async def start(self) -> int:
        """Acquire ownership, recover records, build dependencies, then listen explicitly.

        Returns:
            Bound candidate port. No upstream service configuration is altered.
        """
        if self._started or self._finished:
            raise RuntimeError("Application instances cannot be started twice")
        environment = os.environ if self._environ is None else self._environ
        token = environment.get(self.settings.token_env, "")
        if not token:
            raise ValueError(f"Missing credential reference: {self.settings.token_env}")
        api_key = environment.get(self.settings.api_key_env, "")
        if self._injected_model is None and not api_key:
            raise ValueError(f"Missing credential reference: {self.settings.api_key_env}")
        self.lease.acquire()
        try:
            self.store = StateStore(self.settings.data_dir / "state.sqlite3")
            self.context = ContextBuilder(self.store)
            self.tools = ToolExecutor(self.store)
            self.transport = QQTransport(
                self.settings.bot_id,
                token,
                self._receive,
                timeout_seconds=self.settings.protocol_timeout_seconds,
            )
            self.delivery = Delivery(
                self.store,
                self.transport.call,
                file_resolver=self._resolve_output_file
                if self.settings.napcat_shared_local_files
                else None,
            )
            self.progress = ProgressReporter(
                self.store,
                self.delivery,
                config=ProgressConfig(
                    enabled=self.settings.progress_enabled,
                    min_interval_seconds=self.settings.progress_min_interval_seconds,
                ),
            )
            register_collaboration_tools(
                self.tools, self.store, self.delivery, known_recipient=self._known_recipient
            )
            register_qq_forward_tools(self.tools, self.store, self.transport.call, self.delivery)
            register_history_tools(self.tools, self.store, policy_check=self.context_allowed)
            register_material_tools(
                self.tools,
                self.store,
                self.settings.data_dir / "materials",
                max_bytes=self.settings.max_attachment_bytes,
                delivery=self.delivery,
                enable_send=self.settings.napcat_shared_local_files,
            )
            if self.settings.execution_backend != "disabled":
                self.execution = ExecutionRuntime(
                    self.settings.data_dir / "workspaces",
                    self.store,
                    timeout=self.settings.execution_timeout_seconds,
                    backend=self.settings.execution_backend,
                    bwrap_path=self.settings.execution_bwrap_path,
                    python_runtime=self.settings.execution_python_runtime,
                    network=self.settings.execution_network,
                )
                register_execution_tools(
                    self.tools,
                    self.execution,
                    self.settings.data_dir / "materials",
                    max_material_bytes=self.settings.max_attachment_bytes,
                )
            execution = self.execution
            grants = dict(self.settings.mcp_tool_grants)
            for configured in self.settings.mcp_servers:
                try:
                    server = replace(
                        configured,
                        env={key: environment[ref] for key, ref in configured.env.items()},
                        headers={key: environment[ref] for key, ref in configured.headers.items()},
                    )
                    runtime = MCPRuntime((server,), stdio_start_guard=self._host_start_ready)
                    self.mcp.append(runtime)
                    await runtime.start()
                    self.mcp_failures.extend(
                        {"server": str(row["server"]), "error": str(row["error"])}
                        for row in runtime.status()
                        if row["state"] != "ready"
                    )
                    definitions = register_mcp_tools(
                        self.tools,
                        runtime,
                        host_resource=HOST_RESOURCE,
                        host_mutation_guard=self._host_mutation_ready,
                    )
                    enabled = grants.get(server.name, ())
                    for definition in definitions:
                        if "*" in enabled or definition.remote_name in enabled:
                            self.mcp_granted_tools |= {definition.name}
                            self.mcp_granted_resources |= {definition.resource_key}
                            if server.transport == "stdio":
                                self.mcp_granted_resources |= {HOST_RESOURCE}
                except Exception as error:
                    self.mcp_failures.append(
                        {"server": configured.name, "error": type(error).__name__}
                    )
            if self.settings.allowed_tools.intersection(WEB_TOOLS):
                self.web = WebRuntime(self.settings.web_config)
                register_web_tools(self.tools, self.web)
            register_runtime_skills(
                self.tools,
                self.settings.runtime_skill_roots,
                workspace_for=(lambda context_id: execution.workspace(context_id).root)
                if execution
                else None,
                available_env=frozenset(environment),
                workspace_display_root="/workspace"
                if self.settings.execution_backend == "bubblewrap"
                else None,
                context_mutation_guard=(
                    lambda ctx: execution.assert_mutation_ready(ctx.run.context_id)
                )
                if execution
                else None,
                mutation_resources=(lambda ctx: (execution.resource_key(ctx.run.context_id),))
                if execution
                else (),
            )
            self.ports = RuntimePorts(
                self.store,
                self.context,
                self.tools,
                policy_check=self.context_allowed,
                progress=self.progress,
            )
            model = self._injected_model
            if model is None:
                self._owned_model = OpenAIModel(
                    model=self.settings.model,
                    base_url=self.settings.base_url,
                    api_key=api_key,
                    max_tokens=self.settings.max_output_tokens,
                    timeout=self.settings.model_timeout_seconds,
                    reasoning_echo=self.settings.reasoning_echo,
                )
                model = self._owned_model
            if self.settings.allowed_tools.intersection(VISION_TOOLS):
                vision_model = model
                if self.settings.vision_model and self.settings.vision_model != self.settings.model:
                    self._owned_vision_model = OpenAIModel(
                        model=self.settings.vision_model,
                        base_url=self.settings.base_url,
                        api_key=api_key,
                        max_tokens=self.settings.max_output_tokens,
                        timeout=self.settings.model_timeout_seconds,
                        reasoning_echo=self.settings.reasoning_echo,
                    )
                    vision_model = self._owned_vision_model
                register_vision_tool(
                    self.tools,
                    self.store,
                    self.settings.data_dir / "materials",
                    vision_model,
                    config=VisionConfig(
                        state="ready",
                        model_label=self.settings.vision_model or self.settings.model,
                        max_bytes=self.settings.max_attachment_bytes,
                        timeout_seconds=self.settings.model_timeout_seconds,
                    ),
                )
            self.engine = AgentEngine(model)
            self.coordinator = Coordinator(
                self.store,
                self.engine,
                self.ports,
                self.delivery,
                bot_id=self.settings.bot_id,
                allowed_users=self.settings.allowed_users,
                allowed_groups=self.settings.allowed_groups,
                access_factory=self.access_for,
                max_concurrent=self.settings.max_concurrent,
                max_model_calls=self.settings.max_model_calls,
                max_tool_calls=self.settings.max_tool_calls,
                timeout_seconds=self.settings.timeout_seconds,
                policy_predicate=self.context_allowed,
                compactor=Compactor(
                    self.context, model, timeout_seconds=self.settings.model_timeout_seconds
                ),
                max_context_chars=self.settings.max_context_chars,
                on_associated=self._after_associated,
            )
            if self.execution is not None:
                self.execution.notify = self.coordinator.notify_result
            self.reference_intake = ReferenceIntake(
                self.store,
                self.transport.call,
                self.coordinator.notify_result,
                policy_check=self.context_allowed,
                max_concurrent=self.settings.max_concurrent,
            )
            if "read_material" in self.settings.allowed_tools and (
                self.settings.allowed_download_hosts or self.settings.platform_public_files
            ):
                client = self._injected_download_client
                if client is None:
                    self._owned_download_client = httpx.AsyncClient(trust_env=False)
                    client = self._owned_download_client
                if self.settings.platform_public_files:
                    self.material_web = WebRuntime(
                        replace(
                            self.settings.web_config,
                            max_download_bytes=self.settings.max_attachment_bytes,
                        ),
                        client=client,
                    )
                self.material_intake = MaterialIntake(
                    self.store,
                    self.settings.data_dir / "materials",
                    self.transport.call,
                    self.coordinator.notify_result,
                    client=client,
                    allowed_hosts=self.settings.allowed_download_hosts,
                    max_bytes=self.settings.max_attachment_bytes,
                    policy_check=self.context_allowed,
                    max_concurrent=self.settings.max_concurrent,
                    public_fetch=self.material_web.fetch_bytes if self.material_web else None,
                )
            # Recovery classifies unknown effects; it never blindly replays every active context.
            self.recovery = self.coordinator.recover()
            self._apply_startup_policy()
            self._accepting = True
            self.port = await self.transport.start(self.settings.host, self.settings.port)
            self._activation = asyncio.create_task(self._activate_recovery())
            self._started = True
            return self.port
        except BaseException:
            self._accepting = False
            for mcp in self.mcp:
                await mcp.close()
            if self.web is not None:
                await self.web.close()
            if self.material_web is not None:
                await self.material_web.close()
            if self.execution is not None:
                await self.execution.close()
            if self.transport is not None:
                await self.transport.close()
            if self._owned_model is not None:
                await self._owned_model.close()
            if self._owned_vision_model is not None:
                await self._owned_vision_model.close()
            if self._owned_download_client is not None:
                await self._owned_download_client.aclose()
            if self.store is not None:
                self.store.close()
            self.lease.release()
            self._finished = True
            raise

    async def close(self) -> ShutdownResult:
        """Stop ingress, drain runs, and retain storage ownership if effects remain alive."""
        self._accepting = False
        if self.coordinator is not None:
            self.coordinator.begin_shutdown()
        if self._activation is not None and not self._activation.done():
            self._activation.cancel()
            await asyncio.gather(self._activation, return_exceptions=True)
        if self.transport is not None:
            await self.transport.close()
        if self.reference_intake is not None:
            await self.reference_intake.close()
        if self.material_intake is not None:
            await self.material_intake.close()
        for mcp in self.mcp:
            await mcp.close()
        if self.execution is not None:
            await self.execution.close()
        if self.web is not None:
            await self.web.close()
        if self.material_web is not None:
            await self.material_web.close()
        if self.coordinator is not None:
            drained = await self.coordinator.close()
            if not drained:
                return ShutdownResult(False, False, "inflight_effects_unsettled_lease_retained")
        if self._owned_model is not None:
            await self._owned_model.close()
            self._owned_model = None
        if self._owned_vision_model is not None:
            await self._owned_vision_model.close()
            self._owned_vision_model = None
        if self._owned_download_client is not None:
            await self._owned_download_client.aclose()
            self._owned_download_client = None
        if self.store is not None:
            self.store.close()
        self.lease.release()
        self._finished = True
        return ShutdownResult(True, True, "stopped_without_claiming_external_rollback")
