"""Opt-in Q04/Q05/Q06 driver with an actual in-process candidate and scripted model.

No production configuration is written. A separately configured S reverse connection
and P forward endpoint are prerequisites, not created by this development program.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from websockets.asyncio.client import connect

from shittim_core.app.config import Settings
from shittim_core.app.service import Application
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import ConversationAddress, ProtocolReceipt

if __package__:
    from .qq_live_probe import (
        PeerClient,
        ProbeProfile,
        ProbeStopped,
        emit_report,
        message_ref,
        output_refusal,
        reserve_report_output,
    )
else:
    from qq_live_probe import (
        PeerClient,
        ProbeProfile,
        ProbeStopped,
        emit_report,
        message_ref,
        output_refusal,
        reserve_report_output,
    )

type Record = dict[str, Any]
type StartHook = Callable[[Application], Awaitable[None]]


@dataclass(frozen=True)
class CandidateProbeProfile:
    peer: ProbeProfile
    candidate_config: Path | None = None
    isolation_config: Path | None = None
    peer_shared_dir: Path | None = None
    cases: tuple[str, ...] = ("Q04", "Q06")

    def __post_init__(self) -> None:
        if (
            not self.cases
            or len(set(self.cases)) != len(self.cases)
            or any(case not in {"Q04", "Q05", "Q06"} for case in self.cases)
        ):
            raise ValueError("Select unique Q04/Q05/Q06 cases; Q07 requires a third identity")

    @classmethod
    def load(cls, path: Path) -> CandidateProbeProfile:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or "peer" not in data:
            raise ValueError("Candidate probe profile requires its peer profile")
        peer = dict(data.pop("peer"))
        peer["cases"] = ("Q01",)  # Reuse the bounded peer reader, not its Q01 scenario.
        peer["sut_engine"], peer["model_mode"] = "fusion_candidate", "scripted"
        data["peer"] = ProbeProfile(**peer)
        if "cases" in data:
            data["cases"] = tuple(data["cases"])
        for name in ("candidate_config", "isolation_config", "peer_shared_dir"):
            if data.get(name) is not None:
                location = Path(data[name])
                data[name] = (
                    location if location.is_absolute() else (path.parent / location).resolve()
                )
        return cls(**data)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _identity(value: Any, expected: str) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, str)) and str(value) == expected


def _preflight(
    profile: CandidateProbeProfile | None,
    allow_live: bool,
    environment: Mapping[str, str],
    simulation: bool,
) -> tuple[Settings | None, str, Record]:
    if not allow_live or profile is None:
        return None, "explicit_profile_and_opt_in_required", {}
    peer = profile.peer
    if not peer.forward_ws_url or not environment.get(peer.token_env):
        return None, "peer_endpoint_or_token_not_configured", {}
    if profile.candidate_config is None or not profile.candidate_config.is_file():
        return None, "candidate_application_config_missing", {}
    settings = Settings.load(profile.candidate_config)
    if not environment.get(settings.token_env):
        return None, "candidate_token_not_configured", {}
    if settings.bot_id != peer.sut_bot_id or settings.allowed_users != frozenset(
        {peer.test_member_id}
    ):
        return None, "candidate_identity_or_sender_scope_mismatch", {}
    expected_groups = frozenset({peer.group_id}) if peer.destination == "group" else frozenset()
    if settings.allowed_groups != expected_groups:
        return None, "candidate_group_scope_mismatch", {}
    required = {"ask_person", "wait_for_input"} if set(profile.cases) & {"Q04", "Q06"} else set()
    if "Q05" in profile.cases:
        required |= {"read_material", "create_material", "send_material"}
        directory = profile.peer_shared_dir
        if (
            not settings.napcat_shared_local_files
            or not settings.allowed_download_hosts
            or directory is None
            or not directory.is_dir()
            or directory.is_symlink()
            or directory.is_junction()
        ):
            return None, "shared_files_or_download_allowlist_missing", {}
        if peer.destination != "private":
            return None, "q05_group_upload_not_implemented", {}
    if not required.issubset(settings.allowed_tools):
        return None, "candidate_required_tools_not_enabled", {}
    if profile.isolation_config is None or not profile.isolation_config.is_file():
        return None, "single_consumer_configuration_evidence_missing", {}
    raw = profile.isolation_config.read_bytes()
    if len(raw) > 1024 * 1024:
        return None, "isolation_configuration_too_large", {}
    decoded = json.loads(raw)
    if not isinstance(decoded, dict) or not isinstance(decoded.get("network"), dict):
        return None, "isolation_configuration_shape_not_supported", {}
    network = decoded["network"]
    kinds = {"websocketClients", "websocketServers", "httpClients", "httpServers", "httpSseServers"}
    if set(network) - kinds or any(
        not isinstance(network.get(kind, []), list)
        or any(not isinstance(item, dict) for item in network.get(kind, []))
        for kind in kinds
    ):
        return None, "isolation_configuration_shape_not_supported", {}
    clients = [item for item in network.get("websocketClients", []) if item.get("enable") is True]
    other = [
        item
        for kind in ("websocketServers", "httpClients", "httpServers", "httpSseServers")
        for item in network.get(kind, [])
        if item.get("enable") is True
    ]
    if len(clients) != 1 or other:
        return None, "single_enabled_consumer_not_established", {}
    endpoint = urlsplit(str(clients[0].get("url", "")))
    allowed_hosts = {settings.host}
    if settings.host in {"127.0.0.1", "localhost", "0.0.0.0", "::", "::1"}:
        allowed_hosts |= {"127.0.0.1", "localhost", "::1"}
    if (
        endpoint.scheme not in {"ws", "wss"}
        or endpoint.hostname not in allowed_hosts
        or endpoint.port != settings.port
        or endpoint.path != "/onebot"
        or endpoint.query
        or endpoint.fragment
        or endpoint.username
        or endpoint.password
        or (settings.port == 0 and not simulation)
    ):
        return None, "isolation_configuration_not_for_candidate_endpoint", {}
    if settings.data_dir.is_symlink() or settings.data_dir.is_junction():
        return None, "candidate_storage_root_is_linked", {}
    if settings.data_dir.parent == settings.data_dir or not settings.data_dir.parent.is_dir():
        return None, "dedicated_probe_parent_not_prepared", {}
    return (
        settings,
        "",
        {
            "configuration_sha256": _sha(raw),
            "basis": "supplied_single_consumer_config_and_live_account_checks",
            "external_consumer_absence_proven": False,
        },
    )


class Budget:
    def __init__(self, maximum: int):
        self.maximum = maximum
        self.peer_sends = 0
        self.sut_sends = 0
        self.stop_reason: str | None = None

    def claim(self, side: str) -> None:
        if self.stop_reason or self.peer_sends + self.sut_sends >= self.maximum:
            self.stop_reason = self.stop_reason or "combined_outbound_budget_exhausted"
            raise ProbeStopped(self.stop_reason)
        if side == "peer":
            self.peer_sends += 1
        else:
            self.sut_sends += 1


def _source_fingerprint() -> str:
    root = Path(__file__).resolve().parents[1]
    paths = sorted((root / "src").rglob("*.py"))
    paths += [Path(__file__).resolve(), root / "tools" / "qq_live_probe.py"]
    return _sha(
        "\n".join(
            path.relative_to(root).as_posix() + ":" + _sha(path.read_bytes()) for path in paths
        ).encode()
    )


class FilePeer(PeerClient):
    def __init__(self, socket, profile: ProbeProfile) -> None:
        super().__init__(socket, profile)
        self.stimulus_receipts: list[Record] = []

    async def file_url(self, file_id: str) -> dict[str, Any]:
        """One narrowly scoped extra read action; normal sends still use PeerClient."""
        if self.stop_reason:
            raise ProbeStopped(self.stop_reason)
        action = "get_private_file_url"
        echo = uuid4().hex
        future: asyncio.Future[Record] = asyncio.get_running_loop().create_future()
        self.pending[echo] = future
        try:
            await self.socket.send(
                json.dumps(
                    {
                        "action": action,
                        "params": {"file_id": file_id, "self_id": self.profile.test_member_id},
                        "echo": echo,
                    }
                )
            )
            receipt = await asyncio.wait_for(future, self.profile.request_timeout_seconds)
            if (
                receipt.get("status") != "ok"
                or type(receipt.get("retcode")) is not int
                or receipt.get("retcode") != 0
            ):
                raise ProbeStopped("peer_file_location_unconfirmed")
            data = receipt.get("data")
            if not isinstance(data, dict) or not isinstance(data.get("url"), str):
                raise ProbeStopped("peer_file_location_unconfirmed")
            return data
        finally:
            self.pending.pop(echo, None)

    async def wait_file(self, nonce: str) -> tuple[Record, Record]:
        deadline = time.monotonic() + self.profile.case_timeout_seconds
        while time.monotonic() < deadline:
            if self.stop_reason:
                raise ProbeStopped(self.stop_reason)
            try:
                message = await asyncio.wait_for(self.inbound.get(), 0.05)
            except TimeoutError:
                continue
            for segment in message.get("message", []):
                if segment.get("type") != "file":
                    continue
                data = segment.get("data", {})
                name = str(data.get("name") or data.get("file_name") or data.get("file") or "")
                if nonce in name and message.get("message_id") is not None:
                    return message, data
        raise ProbeStopped("peer_file_not_observed")


class ScriptModel:
    """Deterministic decisions and explicit model-entry barriers, never a second Agent."""

    def __init__(self, peer_id: str, destination: str, target: str):
        self.peer_id, self.destination, self.target = peer_id, destination, target
        self.case = ""
        self.nonce = ""
        self.entries: dict[int, asyncio.Event] = {}
        self.release_correction = asyncio.Event()
        self.material_ready = asyncio.Event()
        self.requests: list[Record] = []
        self.counter = 0
        self.request_count = 0
        self.source_sha: str | None = None
        self.created: Record | None = None
        self.expected_source = b""

    def select(self, case: str, nonce: str) -> None:
        self.case, self.nonce, self.counter = case, nonce, 0
        self.entries = {index: asyncio.Event() for index in range(1, 8)}
        self.release_correction = asyncio.Event()
        self.material_ready = asyncio.Event()
        self.source_sha, self.created = None, None

    def _call(self, name: str, arguments: Record, label: str) -> NormalizedResponse:
        return NormalizedResponse(
            None,
            [ToolCall(self.nonce + ":" + label, name, json.dumps(arguments, ensure_ascii=False))],
            "tool_calls",
        )

    @staticmethod
    def _tool_results(messages: list[Record]) -> list[Record]:
        result: list[Record] = []
        for message in messages:
            if message.get("role") != "tool":
                continue
            outer = json.loads(message["content"])
            try:
                inner = json.loads(outer["content"])
            except (ValueError, TypeError):
                continue
            if isinstance(inner, dict):
                result.append(inner)
        return result

    async def complete(self, messages: list[Record], tools: list[Record]) -> NormalizedResponse:
        self.counter += 1
        self.request_count += 1
        index = self.counter
        self.requests.append(
            {
                "case": self.case,
                "stage": index,
                "input_ids": [m.get("_input_id") for m in messages if m.get("_input_id")],
                "correction_seen": any(
                    self.nonce + ":correction" in str(m.get("content")) for m in messages
                ),
            }
        )
        self.entries.setdefault(index, asyncio.Event()).set()
        if index > 7:
            raise ValueError("Scripted probe model budget exceeded")
        if self.case in {"Q04", "Q06"}:
            if index == 1:
                return self._call(
                    "ask_person",
                    {
                        "recipient_id": self.peer_id,
                        "kind": self.destination,
                        "target_id": self.target,
                        "question": self.nonce + ":question",
                    },
                    "ask",
                )
            if self.case == "Q04":
                if index == 2:
                    await self.release_correction.wait()
                    return self._call(
                        "ask_person",
                        {
                            "recipient_id": self.peer_id,
                            "kind": self.destination,
                            "target_id": self.target,
                            "question": self.nonce + ":obsolete-must-not-send",
                        },
                        "obsolete",
                    )
                await asyncio.Event().wait()  # Only the real stop path releases this invocation.
                raise AssertionError("Unreachable")
            if index == 2:
                for message in messages:
                    if message.get("role") == "tool":
                        outer = json.loads(message["content"])
                        inquiry = next(
                            (
                                str(ref)
                                for ref in outer.get("evidence_refs", [])
                                if str(ref).startswith("inquiry_")
                            ),
                            None,
                        )
                        if inquiry:
                            return self._call("wait_for_input", {"inquiry_id": inquiry}, "wait")
                raise ValueError("No actual persisted inquiry reference")
            return NormalizedResponse(self.nonce + ":continued", None, "stop")

        results = self._tool_results(messages)
        sent = any(
            message.get("role") == "tool"
            and "acknowledged the file" in str(json.loads(message["content"]).get("content", ""))
            for message in messages
        )
        if sent:
            return NormalizedResponse(self.nonce + ":file-complete", None, "stop")
        created = next((r for r in results if r.get("status") == "created"), None)
        if created:
            self.created = created
            return self._call(
                "send_material",
                {"artifact_id": created["artifact_id"], "version": created["version"]},
                "send",
            )
        read = next((r for r in results if r.get("content_read") and r.get("parsed_as_text")), None)
        if read:
            if (
                read["sha256"] != _sha(self.expected_source)
                or read["text"].encode() != self.expected_source
            ):
                raise ValueError("Actual S material read does not match synthetic source")
            self.source_sha = read["sha256"]
            return self._call(
                "create_material",
                {
                    "name": self.nonce + "-output.txt",
                    "content": read["text"] + "\nverified:" + self.nonce,
                    "artifact_sources": [
                        {"artifact_id": read["artifact_id"], "version": read["version"]}
                    ],
                },
                "create",
            )
        for message in messages:
            if not str(message.get("_event_id", "")).startswith("result:"):
                continue
            content = str(message.get("content", ""))
            if "\n" not in content:
                continue
            payload = json.loads(content.split("\n", 1)[1])
            if payload.get("status") == "stored":
                return self._call(
                    "read_material",
                    {"artifact_id": payload["artifact_id"], "version": payload["version"]},
                    "read",
                )
        await self.material_ready.wait()
        # New material input invalidates this earlier view, so this text must not be delivered.
        return NormalizedResponse(self.nonce + ":stale-material-view", None, "stop")


async def _wait(
    predicate: Callable[[], Any], timeout: float, budget: Budget, peer: FilePeer
) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if budget.stop_reason or peer.stop_reason:
            raise ProbeStopped(budget.stop_reason or peer.stop_reason or "observer_stopped")
        result = predicate()
        if result:
            return result
        await asyncio.sleep(0.01)  # Poll a concrete state predicate, never guess execution timing.
    raise ProbeStopped("target_stage_not_exercised")


def _source_context(application: Application, nonce: str, peer_id: str) -> Record | None:
    assert application.store is not None
    for context in application.store.list_contexts():
        if context.get("kind") != "work":
            continue
        for row in application.store.list_events(context["context_id"]):
            event = row.get("event")
            if event is None or event.author_id != peer_id:
                continue
            if nonce in event.text or any(nonce in str(segment.data) for segment in event.segments):
                return {"context": context, "input_id": row["input_id"], "event": event}
    return None


async def _send(peer: FilePeer, budget: Budget, parts: list[Record]) -> Record:
    budget.claim("peer")
    params: Record = {"message": parts}
    if peer.profile.destination == "group":
        params["group_id"] = int(peer.profile.group_id or "0")
        parts = [{"type": "at", "data": {"qq": peer.profile.sut_bot_id}}, *parts]
        params["message"] = parts
    else:
        params["user_id"] = int(peer.profile.sut_bot_id)
    result = await peer.call("send_" + peer.profile.destination + "_msg", params)
    peer.stimulus_receipts.append(
        {
            "status": result["status"],
            "message": message_ref(peer.profile, result["data"]["message_id"], "send_receipt")
            if result.get("data", {}).get("message_id") is not None
            else None,
        }
    )
    if result["status"] != "acknowledged":
        raise ProbeStopped("peer_send_unconfirmed_no_retry")
    return message_ref(peer.profile, result["data"]["message_id"], "send_receipt")


async def _separate_case_conversation(
    app: Application,
    peer: FilePeer,
    budget: Budget,
    model: ScriptModel,
    previous_context_id: str,
    record: Record,
) -> None:
    """Independent probe cases use the real explicit new-conversation protocol.

    Completing or pausing Q04 does not close the user's conversation. Never edit
    focus/contexts directly here or silently resume the previous test's task.
    """
    store = app.store
    assert store is not None and app.coordinator is not None
    timeout = peer.profile.case_timeout_seconds
    # A quote/file lookup from the preceding case must settle while its scripted
    # model selection is still active, not wake it after the next case is selected.
    if app.reference_intake is not None:
        await asyncio.wait_for(app.reference_intake.wait_idle(), timeout)
    if app.material_intake is not None:
        await asyncio.wait_for(app.material_intake.wait_idle(), timeout)
    await asyncio.wait_for(app.coordinator.wait_idle(), timeout)
    before_calls = model.request_count
    previous_state = store.get_context(previous_context_id)["control_state"]
    reset: Record = {
        "status": "attempting",
        "previous_context_id": previous_context_id,
        "previous_control_state": previous_state,
    }
    record["conversation_reset"] = reset
    command = await _send(peer, budget, [{"type": "text", "data": {"text": "/new"}}])
    record["peer_messages"].append(command)
    reset["peer_command"] = command
    address = ConversationAddress(
        peer.profile.sut_bot_id,
        peer.profile.destination,
        peer.profile.group_id or peer.profile.test_member_id,
    )

    def new_focus() -> Record | None:
        focus = store.conversation_focus(address, peer.profile.test_member_id)
        if not focus or focus["context_id"] == previous_context_id:
            return None
        if focus["address"] != address or focus["owner_id"] != peer.profile.test_member_id:
            raise AssertionError("New probe conversation has a different identity or scope")
        return focus

    focus = await _wait(new_focus, timeout, budget, peer)
    context_id = str(focus["context_id"])
    commands = [
        row
        for row in store.list_events(context_id)
        if row["kind"] == "input"
        and row["event"].author_id == peer.profile.test_member_id
        and row["event"].text.strip() == "/new"
    ]
    if len(commands) != 1 or focus["control_state"] != "active":
        raise AssertionError("Explicit new-conversation command was not durably associated")
    source = commands[0]
    reset.update(
        context_id=context_id,
        source_input_id=source["input_id"],
        source_message_id=source["event"].message_id,
    )
    confirmation = await peer.wait_reply("已开始新会话")
    if confirmation is None:
        raise ProbeStopped("new_conversation_confirmation_not_observed")
    record["peer_messages"].append(
        message_ref(peer.profile, confirmation["message_id"], "new_conversation_received")
    )
    acknowledgements = await _wait(
        lambda: [
            row
            for row in store.list_deliveries("acknowledged", context_id=context_id)
            if row["intent"].purpose == "control"
            and row["intent"].reply_to == source["event"].message_id
        ],
        timeout,
        budget,
        peer,
    )
    await asyncio.wait_for(app.coordinator.wait_idle(), timeout)
    if model.request_count != before_calls:
        raise AssertionError("New-conversation routing unexpectedly invoked the scripted model")
    if store.get_context(previous_context_id)["control_state"] != previous_state:
        raise AssertionError("Scenario reset changed the prior task's control state")
    reset.update(
        status="acknowledged",
        delivery_id=acknowledgements[0]["delivery_id"],
        additional_model_calls=0,
    )


async def _download_peer_file(url: str, client: httpx.AsyncClient, hosts: frozenset[str]) -> bytes:
    parts = urlsplit(url)
    if (
        parts.scheme not in {"http", "https"}
        or parts.hostname not in hosts
        or parts.username
        or parts.password
        or parts.fragment
        or parts.port not in (None, 80, 443)
    ):
        raise ProbeStopped("peer_file_url_outside_allowlist")
    async with client.stream(
        "GET", url, follow_redirects=False, timeout=10, headers={"Accept-Encoding": "identity"}
    ) as response:
        if (
            response.status_code != 200
            or response.headers.get("Content-Encoding", "identity") != "identity"
        ):
            raise ProbeStopped("peer_file_download_unconfirmed")
        data = b""
        async for chunk in response.aiter_bytes():
            data += chunk
            if len(data) > 65536:
                raise ProbeStopped("peer_file_size_exceeded")
        return data


async def _case(
    case: str,
    nonce: str,
    app: Application,
    peer: FilePeer,
    budget: Budget,
    model: ScriptModel,
    profile: CandidateProbeProfile,
    client: httpx.AsyncClient,
    record: Record | None = None,
) -> Record:
    store = app.store
    assert store is not None and app.coordinator is not None
    model.select(case, nonce)
    result: Record = (
        record
        if record is not None
        else {"case_id": case, "nonce": nonce, "status": "inconclusive", "peer_messages": []}
    )
    timeout = profile.peer.case_timeout_seconds
    if case == "Q05":
        assert profile.peer_shared_dir is not None
        folder = profile.peer_shared_dir / ("qq-probe-" + nonce)
        folder.mkdir(exist_ok=False)
        path = folder / (nonce + "-input.txt")
        model.expected_source = ("synthetic-input:" + nonce + "\nsource payload").encode()
        path.write_bytes(model.expected_source)
        result["source_sha256"] = _sha(model.expected_source)
        result["peer_messages"].append(
            await _send(
                peer,
                budget,
                [{"type": "file", "data": {"file": path.resolve().as_uri(), "name": path.name}}],
            )
        )
    else:
        result["peer_messages"].append(
            await _send(
                peer, budget, [{"type": "text", "data": {"text": "controlled probe " + nonce}}]
            )
        )
    source = await _wait(
        lambda: _source_context(app, nonce, profile.peer.test_member_id), timeout, budget, peer
    )
    context_id = source["context"]["context_id"]
    reset = result.get("conversation_reset")
    if reset and (reset.get("status") != "acknowledged" or reset.get("context_id") != context_id):
        raise AssertionError("Case stimulus did not enter its confirmed new conversation")
    result["source_input"] = {
        "input_id": source["input_id"],
        "context_id": context_id,
        "address": source["event"].address.key,
        "author_id": source["event"].author_id,
        "message_id": source["event"].message_id,
    }
    if case == "Q05":
        events = await _wait(
            lambda: [
                row for row in store.list_events(context_id) if row["kind"] == "material_result"
            ],
            timeout,
            budget,
            peer,
        )
        if not any(row["payload"].get("status") == "stored" for row in events):
            raise ProbeStopped("source_file_not_obtained")
        model.material_ready.set()
        message, file = await peer.wait_file(nonce)
        url = file.get("url")
        if not isinstance(url, str):
            location = await peer.file_url(str(file.get("file_id", "")))
            url = location["url"]
        downloaded = await _download_peer_file(url, client, app.settings.allowed_download_hosts)
        await _wait(lambda: model.created, timeout, budget, peer)
        expected = model.expected_source + ("\nverified:" + nonce).encode()
        if downloaded != expected or model.source_sha != result["source_sha256"]:
            raise AssertionError("File content hash/actual-read mismatch")
        created = model.created or {}
        metadata = store.get_artifact(created["artifact_id"], created["version"])
        lineage = json.loads(metadata["source_ref"])
        if (
            source["input_id"] not in lineage["input_ids"]
            or not lineage["artifact_sources"]
            or lineage["artifact_sources"][0]["sha256"] != result["source_sha256"]
        ):
            raise AssertionError("Derived source lineage is incomplete")
        result.update(
            s_read_sha256=model.source_sha,
            output_sha256=_sha(downloaded),
            s_output_sha256=created["sha256"],
            artifact_id=created["artifact_id"],
            artifact_version=created["version"],
            source_input_ids=lineage["input_ids"],
        )
        if created["sha256"] != result["output_sha256"]:
            raise AssertionError("P downloaded different bytes from S's registered artifact")
        result["peer_messages"].append(
            message_ref(profile.peer, message["message_id"], "file_received")
        )
        await asyncio.wait_for(app.coordinator.wait_idle(), timeout)
        output_deliveries = [
            row
            for row in store.list_deliveries()
            if row["context_id"] == context_id and row["intent"].artifact_refs
        ]
        if not output_deliveries or any(
            row["status"] != "acknowledged" for row in output_deliveries
        ):
            raise ProbeStopped("source_file_delivery_receipt_unconfirmed")
        result["delivery_ids"] = [row["delivery_id"] for row in output_deliveries]
    else:
        anchor = await peer.wait_reply(nonce + ":question")
        if anchor is None:
            raise ProbeStopped("question_not_observed_by_peer")
        result["peer_messages"].append(
            message_ref(profile.peer, anchor["message_id"], "question_received")
        )
        await _wait(lambda: model.entries[2].is_set(), timeout, budget, peer)
        inquiry = await _wait(lambda: store.list_open_inquiries(context_id), timeout, budget, peer)
        inquiry_id = inquiry[0]["inquiry_id"]
        first_run = inquiry[0]["run_id"]
        result.update(inquiry_id=inquiry_id, first_run_id=first_run)
        quote = {"type": "reply", "data": {"id": str(anchor["message_id"])}}
        if case == "Q04":
            correction_basis = store.get_context(context_id)["input_seq"]
            result["peer_messages"].append(
                await _send(
                    peer, budget, [quote, {"type": "text", "data": {"text": nonce + ":correction"}}]
                )
            )
            await _wait(
                lambda: (
                    store.get_context(context_id)["input_seq"] > correction_basis
                    and any(
                        row["kind"] == "input"
                        and row["event"].author_id == profile.peer.test_member_id
                        and row["event"].text.strip() == nonce + ":correction"
                        for row in store.list_events(context_id)
                    )
                ),
                timeout,
                budget,
                peer,
            )
            model.release_correction.set()
            await _wait(lambda: model.entries[3].is_set(), timeout, budget, peer)
            if not model.requests[-1]["correction_seen"]:
                raise AssertionError("Correction did not reach S's actual model input")
            result["peer_messages"].append(
                await _send(peer, budget, [quote, {"type": "text", "data": {"text": "停止"}}])
            )
            await _wait(
                lambda: store.get_context(context_id)["control_state"] == "paused",
                timeout,
                budget,
                peer,
            )
            await asyncio.wait_for(app.coordinator.wait_idle(), timeout)
            effects = store.list_effects(context_id)
            if any(nonce + ":obsolete-must-not-send" in str(effect["args"]) for effect in effects):
                raise AssertionError("Stale tool decision was dispatched")
            history = store.history(context_id)
            if not any(
                m.get("role") == "tool"
                and m.get("tool_call_id") == nonce + ":obsolete"
                and json.loads(m["content"]).get("status") == "not_executed"
                for m in history
            ):
                raise AssertionError("Stale tool sequence did not close")
            if store.get_run(first_run)["status"] != "cancelled":
                raise AssertionError("Stopped model invocation did not produce cancelled run")
            result.update(
                correction_consumed=True,
                model_stop_stage=3,
                run_status=store.get_run(first_run)["status"],
                paused=True,
            )
        else:
            await _wait(
                lambda: store.get_run(first_run)["status"] == "yielded", timeout, budget, peer
            )
            if model.counter != 2:
                raise AssertionError("Waiting unexpectedly consumed additional model calls")
            result["peer_messages"].append(
                await _send(
                    peer, budget, [quote, {"type": "text", "data": {"text": nonce + ":answer"}}]
                )
            )
            reply = await peer.wait_reply(nonce + ":continued")
            if reply is None:
                raise ProbeStopped("continued_result_not_observed")
            await asyncio.wait_for(app.coordinator.wait_idle(), timeout)
            answered = store.get_inquiry(inquiry_id)
            answer = store.get_input(answered["answer_input_id"])
            finals = [
                row
                for row in store.list_deliveries()
                if row["context_id"] == context_id and row["intent"].purpose == "final"
            ]
            if (
                answered["status"] != "answered"
                or answer.author_id != profile.peer.test_member_id
                or not finals
                or finals[-1]["run_id"] == first_run
            ):
                raise AssertionError("Inquiry was not answered by P in a new run")
            if finals[-1]["status"] != "acknowledged":
                raise ProbeStopped("continuation_delivery_receipt_unconfirmed")
            result.update(
                answer_input_id=answered["answer_input_id"],
                continued_run_id=finals[-1]["run_id"],
                answer_author_id=answer.author_id,
            )
            result["peer_messages"].append(
                message_ref(profile.peer, reply["message_id"], "continued_received")
            )
    result["source_inputs"] = [
        {
            "input_id": row["input_id"],
            "message_id": row["event"].message_id,
            "author_id": row["event"].author_id,
            "address": row["event"].address.key,
        }
        for row in store.list_events(context_id)
        if row["kind"] == "input"
    ]
    result["status"] = "pass"
    return result


async def run_candidate_probe(
    profile: CandidateProbeProfile | None,
    *,
    allow_live: bool = False,
    environ: Mapping[str, str] | None = None,
    simulation: bool = False,
    on_sut_started: StartHook | None = None,
    download_client: httpx.AsyncClient | None = None,
) -> Record:
    environment = os.environ if environ is None else environ
    report: Record = {
        "status": "not_run",
        "transport": "simulated" if simulation else "qq_live",
        "engine": "fusion_candidate",
        "model": "scripted",
        "cases": [],
        "full_chain_verified": False,
        "qq_live_verified": False,
        "production_configuration_changed": False,
    }
    if not simulation and (on_sut_started is not None or download_client is not None):
        return {**report, "reason": "test_injection_forbidden_in_live_mode"}
    try:
        settings, reason, isolation = _preflight(profile, allow_live, environment, simulation)
    except (ValueError, TypeError, OSError, KeyError):
        return {**report, "reason": "invalid_preflight_configuration"}
    if settings is None or profile is None:
        return {**report, "reason": reason}
    batch_id = uuid4().hex[:16]
    report.update(batch_id=batch_id, isolation=isolation, status="inconclusive")
    budget = Budget(profile.peer.max_outbound)
    model = ScriptModel(
        profile.peer.test_member_id,
        profile.peer.destination,
        profile.peer.group_id or profile.peer.test_member_id,
    )
    source_root = Path(__file__).resolve().parents[1] / "src"
    imported = Path(sys.modules[Application.__module__].__file__ or "").resolve()
    if not source_root.is_dir() or not imported.is_relative_to(source_root.resolve()):
        return {
            **report,
            "status": "not_run",
            "reason": "candidate_import_not_from_inspected_source_tree",
        }
    fingerprint = _source_fingerprint()
    report["candidate_source_sha256"] = fingerprint
    # Create a fresh sibling, never open or modify the configured candidate/production data directory.
    probe_data = settings.data_dir.parent / ("qq-probe-data-" + batch_id)
    if probe_data.exists():
        return {**report, "status": "not_run", "reason": "probe_directory_collision"}
    settings = replace(settings, data_dir=probe_data)
    app = Application(settings, model=model, environ=environment, download_client=download_client)
    owned_client = download_client is None
    client = download_client or httpx.AsyncClient(trust_env=False)
    peer: FilePeer | None = None
    try:
        async with asyncio.timeout(profile.peer.batch_timeout_seconds):
            await app.start()
            assert app.delivery is not None and app.transport is not None
            original_call = app.delivery.call

            async def guarded_call(action: str, params: Record, attempt_id: str) -> ProtocolReceipt:
                if action.startswith("send_"):
                    try:
                        budget.claim("sut")
                    except ProbeStopped:
                        return ProtocolReceipt("failed", error="probe_budget_not_dispatched")
                return await original_call(action, params, attempt_id)

            app.delivery.call = guarded_call
            if on_sut_started is not None:
                await on_sut_started(app)
            try:
                await asyncio.wait_for(
                    app.transport.ready.wait(), profile.peer.request_timeout_seconds
                )
            except TimeoutError:
                report.update(status="not_run", reason="candidate_s_account_not_connected")
                return report
            identity = await app.transport.call("get_login_info", {}, "probe-login:" + batch_id)
            if identity.status != "acknowledged" or not _identity(
                identity.data.get("user_id"), settings.bot_id
            ):
                report.update(status="not_run", reason="candidate_live_identity_not_confirmed")
                return report
            await asyncio.wait_for(
                app.recovery_complete.wait(), profile.peer.request_timeout_seconds
            )
            report["sut_evidence"] = {
                "construction": "in_process_Application",
                "login_id": settings.bot_id,
                "fresh_data_directory": True,
                "scripted_model_class": type(model).__name__,
            }
            async with connect(
                profile.peer.forward_ws_url or "",
                additional_headers={
                    "Authorization": "Bearer " + environment[profile.peer.token_env]
                },
                open_timeout=profile.peer.request_timeout_seconds,
                max_size=2 * 1024 * 1024,
            ) as socket:
                peer = FilePeer(socket, profile.peer)
                await peer.start()
                login = await peer.call("get_login_info", {})
                if login["status"] != "acknowledged" or not _identity(
                    login.get("data", {}).get("user_id"), profile.peer.test_member_id
                ):
                    report.update(status="not_run", reason="peer_live_identity_not_confirmed")
                    return report
                if profile.peer.destination == "group":
                    for user in (profile.peer.sut_bot_id, profile.peer.test_member_id):
                        membership = await peer.call(
                            "get_group_member_info",
                            {"group_id": int(profile.peer.group_id or "0"), "user_id": int(user)},
                        )
                        if (
                            membership["status"] != "acknowledged"
                            or not _identity(membership.get("data", {}).get("user_id"), user)
                            or not _identity(
                                membership.get("data", {}).get("group_id"),
                                profile.peer.group_id or "",
                            )
                        ):
                            report.update(status="not_run", reason="group_membership_not_confirmed")
                            return report
                previous_context_id: str | None = None
                for case in profile.cases:
                    nonce = case + "-" + batch_id
                    record: Record = {
                        "case_id": case,
                        "nonce": nonce,
                        "status": "inconclusive",
                        "peer_messages": [],
                    }
                    report["cases"].append(record)
                    try:
                        async with asyncio.timeout(profile.peer.case_timeout_seconds):
                            if previous_context_id is not None:
                                await _separate_case_conversation(
                                    app, peer, budget, model, previous_context_id, record
                                )
                            await _case(
                                case, nonce, app, peer, budget, model, profile, client, record
                            )
                            previous_context_id = str(record["source_input"]["context_id"])
                    except TimeoutError:
                        record.update(
                            status="inconclusive"
                            if record.get("source_input")
                            else "not_exercised",
                            reason="case_deadline_exceeded",
                        )
                        raise ProbeStopped("case_deadline_exceeded") from None
                    except ProbeStopped as error:
                        record["reason"] = str(error)
                        if str(error) == "target_stage_not_exercised":
                            record["status"] = "not_exercised"
                        raise
                    except Exception as error:
                        record.update(status="fail", failure_class=type(error).__name__)
                        raise
                report.update(
                    status="pass",
                    full_chain_verified=not simulation,
                    qq_live_verified=not simulation,
                    offline_chain_verified=simulation,
                )
    except ProbeStopped as error:
        report.update(status="inconclusive", reason=str(error))
    except TimeoutError:
        report.update(status="inconclusive", reason="bounded_phase_or_batch_timeout")
    except Exception as error:
        report.update(
            status="fail", reason="probe_or_candidate_failure", failure_class=type(error).__name__
        )
    finally:
        if peer is not None:
            report["peer_stimulus_receipts"] = peer.stimulus_receipts
            await peer.close()
        model.release_correction.set()
        model.material_ready.set()
        shutdown = await app.close()
        if not shutdown.drained:
            report.update(status="inconclusive", reason="candidate_effects_unsettled")
        if owned_client:
            await client.aclose()
        report["counts"] = {
            "peer_send_attempts": budget.peer_sends,
            "candidate_send_attempts": budget.sut_sends,
            "model_calls": model.request_count,
        }
        report["model_stage_observations"] = model.requests
        report["q07"] = "not_implemented_requires_third_real_identity"
        if _source_fingerprint() != fingerprint:
            report.update(status="inconclusive", reason="candidate_source_changed_during_probe")
        if report["status"] != "pass":
            report["full_chain_verified"] = report["qq_live_verified"] = False
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Opt-in in-process candidate QQ lifecycle probe")
    parser.add_argument("--allow-live", action="store_true")
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        output = reserve_report_output(args.output)
    except OSError as error:
        emit_report(output_refusal(error), None)
        return 2
    try:
        profile = CandidateProbeProfile.load(args.profile) if args.profile else None
        report = asyncio.run(run_candidate_probe(profile, allow_live=args.allow_live))
    except Exception as error:
        report = {
            "status": "not_run",
            "reason": "invalid_configuration",
            "failure_class": type(error).__name__,
            "full_chain_verified": False,
        }
    saved = emit_report(report, output)
    return 0 if report["status"] == "pass" and saved else 2


if __name__ == "__main__":
    raise SystemExit(main())
