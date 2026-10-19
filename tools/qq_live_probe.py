"""Opt-in deterministic QQ peer probe; never changes NapCat or starts an Agent.

Run without --allow-live and --profile to get a machine-readable not_run result.
Credentials are resolved only from the environment variable named by the profile.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping, TextIO, cast
from urllib.parse import urlsplit
from uuid import uuid4

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus


def _valid_id(value: object) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"[0-9]+", value))


@dataclass(frozen=True)
class ProbeProfile:
    test_member_id: str
    sut_bot_id: str
    forward_ws_url: str | None = None
    token_env: str = "QQ_PROBE_PEER_TOKEN"
    sut_engine: Literal["pi_baseline", "fusion_candidate"] = "pi_baseline"
    model_mode: Literal["none", "scripted", "live"] = "live"
    destination: Literal["private", "group"] = "private"
    group_id: str | None = None
    cases: tuple[str, ...] = ("Q01",)
    max_outbound: int = 10
    case_timeout_seconds: float = 60.0
    batch_timeout_seconds: float = 600.0
    request_timeout_seconds: float = 15.0

    def __post_init__(self) -> None:
        if not _valid_id(self.test_member_id) or not _valid_id(self.sut_bot_id):
            raise ValueError("QQ identities must be explicit numeric strings")
        if self.test_member_id == self.sut_bot_id:
            raise ValueError("Peer and robot must be distinct accounts")
        if self.sut_engine not in ("pi_baseline", "fusion_candidate"):
            raise ValueError("Unknown engine label")
        if self.model_mode not in ("none", "scripted", "live"):
            raise ValueError("Unknown model label")
        if self.destination not in ("private", "group"):
            raise ValueError("Explicit private/group destination required")
        if self.destination == "group" and not _valid_id(self.group_id):
            raise ValueError("Group probe requires an explicitly authorized group_id")
        if self.destination == "private" and self.group_id is not None:
            raise ValueError("Private profile must not contain group_id")
        if self.cases not in (("Q01",), ("Q01", "Q03")):
            raise ValueError("Only Q01 or Q01 followed by Q03 is implemented")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.token_env):
            raise ValueError("token_env must name an environment variable, not a credential")
        if not 1 <= self.max_outbound <= 40:
            raise ValueError("Outbound budget must be 1..40")
        for value, limit in (
            (self.case_timeout_seconds, 60),
            (self.batch_timeout_seconds, 600),
            (self.request_timeout_seconds, 15),
        ):
            if not math.isfinite(value) or not 0 < value <= limit:
                raise ValueError("Timeout is outside the bounded probe limits")
        if self.forward_ws_url:
            endpoint = urlsplit(self.forward_ws_url)
            if (
                endpoint.scheme not in ("ws", "wss")
                or not endpoint.hostname
                or endpoint.username
                or endpoint.password
                or endpoint.query
                or endpoint.fragment
            ):
                raise ValueError("Use an explicit WS endpoint without URL credentials/query")

    @classmethod
    def load(cls, path: Path) -> ProbeProfile:
        """Read a non-secret profile; unknown keys are rejected rather than ignored."""
        decoded: object = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(decoded, dict):
            raise ValueError("Profile must be an object")
        data = cast(dict[str, Any], decoded)
        if "cases" in data:
            data["cases"] = tuple(data["cases"])
        return cls(**data)


class ProbeStopped(Exception):
    """No further scripted stimulus is allowed in this batch."""


class PeerClient:
    """One reader observes P's account and pairs API responses before any request is sent."""

    def __init__(self, socket: ClientConnection, profile: ProbeProfile) -> None:
        self.socket = socket
        self.profile = profile
        self.pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self.inbound: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=128)
        self.reader_ready = asyncio.Event()
        self.stop_reason: str | None = None
        self.observed_outbound = 0
        self.reader_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.reader_task = asyncio.create_task(self._read())
        await self.reader_ready.wait()

    def _stop(self, reason: str) -> None:
        self.stop_reason = reason
        for future in self.pending.values():
            if not future.done():
                future.set_result({"status": "unknown", "reason": reason})

    async def _read(self) -> None:
        self.reader_ready.set()
        try:
            async for raw in self.socket:
                try:
                    item: Any = json.loads(raw)
                except (ValueError, UnicodeDecodeError):
                    continue
                if not isinstance(item, dict):
                    continue
                item = cast(dict[str, Any], item)
                echo = item.get("echo")
                if isinstance(echo, str) and echo in self.pending:
                    future = self.pending[echo]
                    if not future.done():
                        future.set_result(item)
                    continue
                if "post_type" not in item:
                    continue
                if str(item.get("self_id")) != self.profile.test_member_id:
                    self._stop("peer_event_account_mismatch")
                    break
                if item.get("post_type") == "meta_event":
                    status = item.get("status")
                    if (
                        isinstance(status, dict)
                        and cast(dict[str, Any], status).get("online") is False
                    ):
                        self._stop("peer_offline")
                        break
                    continue
                if item.get("post_type") != "message":
                    continue
                if str(item.get("user_id")) != self.profile.sut_bot_id:
                    continue
                if self.profile.destination == "private":
                    in_scope = item.get("message_type") == "private" and not item.get("group_id")
                else:
                    in_scope = (
                        item.get("message_type") == "group"
                        and str(item.get("group_id")) == self.profile.group_id
                    )
                if not in_scope:
                    continue
                self.observed_outbound += 1
                if self.observed_outbound > self.profile.max_outbound:
                    self._stop("outbound_budget_exceeded")
                    break
                try:
                    self.inbound.put_nowait(item)
                except asyncio.QueueFull:
                    self._stop("observation_queue_full")
                    break
        except ConnectionClosed:
            self._stop("peer_connection_closed")
        finally:
            if self.stop_reason is None:
                self._stop("peer_observer_stopped")

    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        if self.stop_reason:
            raise ProbeStopped(self.stop_reason)
        if action not in {
            "get_login_info",
            "get_group_member_info",
            "send_private_msg",
            "send_group_msg",
        }:
            raise ValueError("Probe action not permitted")
        if action.startswith("send_"):
            if self.observed_outbound >= self.profile.max_outbound:
                raise ProbeStopped("outbound_budget_exhausted")
            self.observed_outbound += 1
        echo = uuid4().hex
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self.pending[echo] = future
        try:
            await self.socket.send(
                json.dumps({"action": action, "params": dict(params), "echo": echo})
            )
            response = await asyncio.wait_for(future, self.profile.request_timeout_seconds)
            if (
                response.get("status") == "ok"
                and type(response.get("retcode")) is int
                and response.get("retcode") == 0
            ):
                data = response.get("data")
                if not isinstance(data, dict):
                    return {"status": "unknown", "reason": "invalid_response_data"}
                data = cast(dict[str, Any], data)
                if action.startswith("send_") and data.get("message_id") is None:
                    return {"status": "unknown", "reason": "send_without_message_id"}
                return {"status": "acknowledged", "data": data}
            if response.get("status") == "failed":
                return {"status": "failed", "retcode": response.get("retcode")}
            return {"status": "unknown", "reason": "unrecognized_receipt"}
        except (TimeoutError, ConnectionClosed, OSError):
            return {"status": "unknown", "reason": "no_definitive_receipt_no_retry"}
        finally:
            self.pending.pop(echo, None)

    async def wait_reply(self, nonce: str) -> dict[str, Any] | None:
        deadline = time.monotonic() + self.profile.case_timeout_seconds
        while not self.stop_reason:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                item = await asyncio.wait_for(self.inbound.get(), min(remaining, 0.1))
            except TimeoutError:
                continue
            segments = item.get("message")
            if not isinstance(segments, list) or item.get("message_id") is None:
                continue
            texts: list[str] = []
            for raw_part in cast(list[object], segments):
                if not isinstance(raw_part, dict):
                    continue
                part = cast(dict[str, Any], raw_part)
                data = part.get("data")
                if part.get("type") == "text" and isinstance(data, dict):
                    texts.append(str(cast(dict[str, Any], data).get("text", "")))
            text = "".join(texts)
            if nonce in text:
                return item
        raise ProbeStopped(self.stop_reason or "observer_stopped")

    async def close(self) -> None:
        await self.socket.close()
        if self.reader_task is not None:
            await self.reader_task


def message_ref(profile: ProbeProfile, message_id: object, direction: str) -> dict[str, str]:
    return {
        "self_id": profile.test_member_id,
        "conversation_type": profile.destination,
        "conversation_target": profile.group_id or profile.sut_bot_id,
        "message_id": str(message_id),
        "direction": direction,
    }


async def run_probe(
    profile: ProbeProfile | None,
    *,
    allow_live: bool = False,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Execute a bounded scripted batch only after both explicit opt-ins are present."""
    report: dict[str, Any] = {
        "status": "not_run",
        "transport": "not_exercised",
        "cases": [],
        "scope": "peer_observed_platform_roundtrip",
        "full_chain_verified": False,
        "sut_internal_trace": "not_observed",
        "two_independent_members": "not_exercised",
        "batch_id": uuid4().hex,
    }
    if not allow_live or profile is None:
        report["reason"] = "explicit_allow_live_and_profile_required"
        return report
    report.update(
        {
            "engine": profile.sut_engine,
            "model": profile.model_mode,
            "test_member_id": profile.test_member_id,
            "sut_bot_id": profile.sut_bot_id,
        }
    )
    if not profile.forward_ws_url:
        report["reason"] = "peer_onebot_endpoint_not_configured"
        return report
    token = (os.environ if environ is None else environ).get(profile.token_env)
    if not token:
        report["reason"] = "peer_token_reference_unavailable"
        return report

    client: PeerClient | None = None
    try:
        async with asyncio.timeout(profile.batch_timeout_seconds):
            async with connect(
                profile.forward_ws_url,
                additional_headers={"Authorization": f"Bearer {token}"},
                open_timeout=profile.request_timeout_seconds,
                max_size=2 * 1024 * 1024,
            ) as socket:
                client = PeerClient(socket, profile)
                await client.start()
                login = await client.call("get_login_info", {})
                if login.get("status") != "acknowledged":
                    report.update(status="inconclusive", reason="peer_identity_query_unconfirmed")
                    return report
                if str(login["data"].get("user_id")) != profile.test_member_id:
                    report.update(status="fail", reason="peer_identity_mismatch_zero_stimulus")
                    return report
                report["peer_identity_verified"] = True
                if profile.destination == "group":
                    for member in (profile.test_member_id, profile.sut_bot_id):
                        membership = await client.call(
                            "get_group_member_info",
                            {
                                "group_id": int(profile.group_id or "0"),
                                "user_id": int(member),
                                "no_cache": True,
                            },
                        )
                        if (
                            membership.get("status") != "acknowledged"
                            or str(membership["data"].get("user_id")) != member
                            or str(membership["data"].get("group_id")) != profile.group_id
                        ):
                            report.update(
                                status="inconclusive",
                                reason="group_membership_unconfirmed_zero_stimulus",
                            )
                            return report
                report["transport"] = "qq_live"
                last_received_id: str | None = None
                for case_id in profile.cases:
                    nonce = f"QQPROBE-{report['batch_id'][:12]}-{case_id}-{uuid4().hex[:8]}"
                    segments: list[dict[str, Any]] = []
                    if case_id == "Q03":
                        if last_received_id is None:
                            raise ProbeStopped("no_peer_received_quote_id")
                        segments.append({"type": "reply", "data": {"id": last_received_id}})
                    if profile.destination == "group":
                        segments.extend(
                            [
                                {"type": "at", "data": {"qq": profile.sut_bot_id}},
                                {"type": "text", "data": {"text": " "}},
                            ]
                        )
                    segments.append(
                        {"type": "text", "data": {"text": f"测试消息，请只回复：{nonce}"}}
                    )
                    params: dict[str, Any] = {"message": segments}
                    params["group_id" if profile.destination == "group" else "user_id"] = int(
                        profile.group_id or profile.sut_bot_id
                    )
                    result: dict[str, Any] = {
                        "case_id": case_id,
                        "nonce": nonce,
                        "status": "inconclusive",
                    }
                    report["cases"].append(result)
                    receipt = await client.call(f"send_{profile.destination}_msg", params)
                    result["send_status"] = receipt["status"]
                    if receipt["status"] == "acknowledged":
                        result["peer_send_receipt"] = message_ref(
                            profile, receipt["data"]["message_id"], "sent"
                        )
                    elif receipt["status"] == "failed":
                        report.update(status="fail", reason="stimulus_rejected_no_retry")
                        return report
                    reply = await client.wait_reply(nonce)
                    if reply is None:
                        report.update(
                            status="inconclusive", reason="no_matching_peer_observation_no_retry"
                        )
                        return report
                    last_received_id = str(reply["message_id"])
                    result["peer_received_event"] = message_ref(
                        profile, last_received_id, "received"
                    )
                    result["sender_id"] = str(reply["user_id"])
                    result["status"] = "pass"
                    result["meaning"] = (
                        "P observed an actual platform event from S containing this nonce"
                    )
                    if receipt["status"] == "unknown":
                        report.update(
                            status="inconclusive",
                            reason="observed_reply_but_send_receipt_unknown_batch_stopped",
                        )
                        return report
                report["status"] = "pass"
                report["reason"] = (
                    "peer_roundtrip_only_not_internal_engine_or_collaboration_verification"
                )
    except InvalidStatus:
        report.update(status="fail", reason="peer_websocket_handshake_rejected_zero_stimulus")
    except ProbeStopped as error:
        report.update(status="inconclusive", reason=str(error))
    except (TimeoutError, ConnectionClosed, OSError):
        report.update(status="inconclusive", reason="connection_or_batch_timeout_no_retry")
    finally:
        if client is not None:
            report["observed_outbound_total"] = client.observed_outbound
            await client.close()
    return report


def reserve_report_output(path: Path | None) -> TextIO | None:
    """Reserve a new report before any connection; existing evidence is immutable."""
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("x", encoding="utf-8")


def output_refusal(error: OSError) -> dict[str, Any]:
    return {
        "status": "not_run",
        "reason": "output_already_exists"
        if isinstance(error, FileExistsError)
        else "output_unavailable",
        "cases": [],
        "full_chain_verified": False,
        "qq_live_verified": False,
        "production_configuration_changed": False,
    }


def emit_report(report: dict[str, Any], output: TextIO | None) -> bool:
    """Always retain the complete result on stdout, including file-write failures."""
    encoded = json.dumps(report, ensure_ascii=False, indent=2)
    saved = True
    if output is not None:
        try:
            with output:
                output.write(encoded + "\n")
                output.flush()
        except OSError as error:
            saved = False
            encoded = json.dumps(
                {
                    **report,
                    "report_output": {"status": "failed", "failure_class": type(error).__name__},
                },
                ensure_ascii=False,
                indent=2,
            )
    print(encoded)
    return saved


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
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
        profile = ProbeProfile.load(args.profile) if args.profile else None
        report = asyncio.run(run_probe(profile, allow_live=args.allow_live))
    except (ValueError, TypeError, OSError, KeyError):
        report = {"status": "not_run", "reason": "invalid_profile", "full_chain_verified": False}
    saved = emit_report(report, output)
    return 1 if report["status"] == "fail" or not saved else 0


if __name__ == "__main__":
    raise SystemExit(main())
