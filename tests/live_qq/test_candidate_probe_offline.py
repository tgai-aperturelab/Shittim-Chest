"""A loopback QQ-like platform connecting P to an actual in-process candidate S.

The relay is a test fixture, not an HTTP/WS bridge in the product architecture.
Every result is tagged simulated and cannot satisfy qq_live acceptance.
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import unquote, urlsplit

import httpx
import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from tools.qq_candidate_probe import CandidateProbeProfile, run_candidate_probe
from tools.qq_live_probe import ProbeProfile


class Platform:
    def __init__(self, root: Path, *, mode: str = "normal"):
        self.root, self.mode = root, mode
        self.peer_socket = None
        self.source_socket = None
        self.source_reader = None
        self.peer_connections = 0
        self.peer_requests = []
        self.source_requests = []
        self.source_messages = {}
        self.peer_quote_to_source = {}
        self.files = {}
        self.peer_count = 0
        self.source_count = 0
        self.app = None

    async def ack(self, socket, request, data):
        await socket.send(
            json.dumps({"echo": request["echo"], "status": "ok", "retcode": 0, "data": data})
        )

    def read_fixture(self, uri: str) -> bytes:
        raw = unquote(urlsplit(uri).path) if uri.startswith("file:") else uri
        if os.name == "nt" and raw.startswith("/") and len(raw) > 2 and raw[2] == ":":
            raw = raw[1:]
        path = Path(raw).resolve()
        assert path.is_relative_to(self.root.resolve())
        return path.read_bytes()

    async def peer_handler(self, socket):
        self.peer_connections += 1
        self.peer_socket = socket
        try:
            async for raw in socket:
                request = json.loads(raw)
                self.peer_requests.append(request)
                action, params = request["action"], request["params"]
                if action == "get_login_info":
                    await self.ack(
                        socket, request, {"user_id": 201 if self.mode == "wrong_p" else 200}
                    )
                elif action == "get_group_member_info":
                    await self.ack(
                        socket,
                        request,
                        {"user_id": params["user_id"], "group_id": params["group_id"]},
                    )
                elif action == "get_private_file_url":
                    await self.ack(
                        socket, request, {"url": "http://files.local/" + params["file_id"]}
                    )
                else:
                    self.peer_count += 1
                    segments = json.loads(json.dumps(params["message"]))
                    for segment in segments:
                        if segment["type"] == "reply":
                            p_id = int(segment["data"]["id"])
                            assert p_id in self.peer_quote_to_source
                            segment["data"]["id"] = self.peer_quote_to_source[p_id]
                        if segment["type"] == "file":
                            file_id = f"input{self.peer_count}"
                            self.files[file_id] = self.read_fixture(segment["data"]["file"])
                            segment["data"] = {"file_id": file_id, "name": segment["data"]["name"]}
                    message_id = 3000 + self.peer_count
                    event = {
                        "post_type": "message",
                        "self_id": 100,
                        "user_id": 200,
                        "sender": {"user_id": 200, "nickname": "fixture P"},
                        "message_type": "group" if action == "send_group_msg" else "private",
                        "message_id": message_id,
                        "message": segments,
                    }
                    if action == "send_group_msg":
                        event["group_id"] = params["group_id"]
                    self.source_messages[message_id] = event
                    if self.mode != "silent_source":
                        await self.source_socket.send(json.dumps(event))
                    reset = any(
                        s["type"] == "text" and s["data"].get("text") == "/new" for s in segments
                    )
                    if self.mode != "unknown_p_send" and not (
                        self.mode == "unknown_reset_send" and reset
                    ):
                        await self.ack(socket, request, {"message_id": 2000 + self.peer_count})
        except ConnectionClosed:
            pass

    async def attach_source(self, app):
        self.app = app
        if self.mode == "no_s":
            return
        self.source_socket = await connect(
            f"ws://127.0.0.1:{app.port}/onebot",
            additional_headers={"Authorization": "Bearer source-secret", "X-Self-ID": "100"},
        )
        self.source_reader = asyncio.create_task(self.read_source())

    async def read_source(self):
        try:
            async for raw in self.source_socket:
                request = json.loads(raw)
                self.source_requests.append(request)
                action, params = request["action"], request["params"]
                if action == "get_login_info":
                    await self.ack(
                        self.source_socket,
                        request,
                        {"user_id": 101 if self.mode == "wrong_s" else 100},
                    )
                elif action == "get_group_member_info":
                    await self.ack(
                        self.source_socket,
                        request,
                        {"user_id": params["user_id"], "group_id": params["group_id"]},
                    )
                elif action == "get_msg":
                    await self.ack(
                        self.source_socket, request, self.source_messages[int(params["message_id"])]
                    )
                elif action == "get_private_file_url":
                    await self.ack(
                        self.source_socket,
                        request,
                        {
                            "url": "http://files.local/" + params["file_id"],
                            "file_name": "source.txt",
                        },
                    )
                elif action.startswith("send_"):
                    self.source_count += 1
                    source_id, peer_id = 1000 + self.source_count, 5000 + self.source_count
                    self.peer_quote_to_source[peer_id] = source_id
                    segments = json.loads(json.dumps(params["message"]))
                    source_event = {
                        "post_type": "message",
                        "self_id": 100,
                        "user_id": 100,
                        "sender": {"user_id": 100},
                        "message_id": source_id,
                        "message_type": "group" if action == "send_group_msg" else "private",
                        "message": segments,
                    }
                    if action == "send_group_msg":
                        source_event["group_id"] = params["group_id"]
                    self.source_messages[source_id] = source_event
                    for segment in segments:
                        if segment["type"] == "file":
                            file_id = f"output{self.source_count}"
                            self.files[file_id] = self.read_fixture(segment["data"]["file"])
                            segment["data"] = {"file_id": file_id, "name": segment["data"]["name"]}
                    event = {**source_event, "self_id": 200, "message_id": peer_id}
                    # P observes before S's API receipt: this is deliberately not a sleep-based test.
                    reset_confirmation = any(
                        s["type"] == "text" and "已开始新会话" in s["data"].get("text", "")
                        for s in segments
                    )
                    if self.mode != "suppress_reset_confirmation" or not reset_confirmation:
                        await self.peer_socket.send(json.dumps(event))
                    await self.ack(self.source_socket, request, {"message_id": source_id})
        except ConnectionClosed:
            pass

    async def http(self, request):
        key = request.url.path.removeprefix("/")
        data = self.files.get(key)
        if data is None:
            return httpx.Response(404)
        if self.mode == "tamper_output" and key.startswith("output"):
            data = b"different content"
        return httpx.Response(200, content=data)

    async def close(self):
        if self.source_socket is not None:
            await self.source_socket.close()
        if self.source_reader is not None:
            await self.source_reader


@asynccontextmanager
async def environment(
    tmp_path: Path, *, mode="normal", cases=("Q04", "Q06"), max_outbound=40, destination="private"
):
    platform = Platform(tmp_path, mode=mode)
    shared = tmp_path / "shared"
    shared.mkdir()
    candidate = tmp_path / "candidate.json"
    candidate.write_text(
        json.dumps(
            {
                "bot_id": "100",
                "token_env": "SOURCE_TOKEN",
                "model": "scripted-only",
                "base_url": "https://example.invalid/v1",
                "api_key_env": "UNUSED_KEY",
                "data_dir": str(tmp_path / "candidate-data"),
                "port": 0,
                "allowed_users": ["200"],
                "allowed_groups": ["10"] if destination == "group" else [],
                "allowed_tools": [
                    "ask_person",
                    "wait_for_input",
                    "read_material",
                    "create_material",
                    "send_material",
                ],
                "allowed_download_hosts": ["files.local"],
                "napcat_shared_local_files": True,
                "protocol_timeout_seconds": 0.5,
                "timeout_seconds": 10,
            }
        ),
        encoding="utf-8",
    )
    isolation = tmp_path / "onebot.json"
    isolation.write_text(
        json.dumps(
            {
                "network": {
                    "websocketClients": [
                        {"enable": True, "url": "ws://127.0.0.1:0/onebot", "token": "source-secret"}
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    async with serve(platform.peer_handler, "127.0.0.1", 0) as server:
        peer = ProbeProfile(
            "200",
            "100",
            forward_ws_url=f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}",
            token_env="PEER_TOKEN",
            max_outbound=max_outbound,
            case_timeout_seconds=1.5,
            request_timeout_seconds=0.15,
            batch_timeout_seconds=6,
            destination=destination,
            group_id="10" if destination == "group" else None,
        )
        profile = CandidateProbeProfile(peer, candidate, isolation, shared, cases)
        async with httpx.AsyncClient(transport=httpx.MockTransport(platform.http)) as client:
            try:
                yield platform, profile, client
            finally:
                await platform.close()


async def run(platform, profile, client, *, allow_live=True, simulation=True):
    return await run_candidate_probe(
        profile,
        allow_live=allow_live,
        environ={"SOURCE_TOKEN": "source-secret", "PEER_TOKEN": "peer-secret"},
        simulation=simulation,
        on_sut_started=platform.attach_source,
        download_client=client,
    )


async def test_q04_and_q06_use_actual_stages_and_account_scoped_platform_quotes(tmp_path):
    async with environment(tmp_path) as (platform, profile, client):
        report = await run(platform, profile, client)
        assert report["status"] == "pass", report
        assert report["offline_chain_verified"] is True
        assert report["full_chain_verified"] is False and report["qq_live_verified"] is False
        q04, q06 = report["cases"]
        assert q04["correction_consumed"] and q04["run_status"] == "cancelled"
        assert q06["continued_run_id"] != q06["first_run_id"]
        assert q06["answer_author_id"] == "200"
        quoted_ids = [
            segment["data"]["id"]
            for request in platform.peer_requests
            if request["action"].startswith("send_")
            for segment in request["params"]["message"]
            if segment["type"] == "reply"
        ]
        assert quoted_ids and all(int(value) >= 5000 for value in quoted_ids)
        assert report["counts"]["peer_send_attempts"] == 6
        reset = q06["conversation_reset"]
        assert reset["status"] == "acknowledged" and reset["additional_model_calls"] == 0
        assert reset["previous_control_state"] == "paused"
        assert reset["previous_context_id"] == q04["source_input"]["context_id"]
        assert (
            reset["context_id"] == q06["source_input"]["context_id"] != reset["previous_context_id"]
        )


@pytest.mark.parametrize("cases", [("Q06", "Q04"), ("Q04", "Q05", "Q06")])
async def test_independent_cases_use_explicit_new_and_preserve_case_assertions(tmp_path, cases):
    async with environment(tmp_path, cases=cases) as (platform, profile, client):
        report = await run(platform, profile, client)
        assert report["status"] == "pass", report
        assert len({case["source_input"]["context_id"] for case in report["cases"]}) == len(cases)
        resets = [case["conversation_reset"] for case in report["cases"][1:]]
        assert all(
            reset["status"] == "acknowledged" and reset["additional_model_calls"] == 0
            for reset in resets
        )
        sent_new = [
            request
            for request in platform.peer_requests
            if request["action"].startswith("send_")
            and any(
                segment["type"] == "text" and segment["data"].get("text") == "/new"
                for segment in request["params"]["message"]
            )
        ]
        assert len(sent_new) == len(cases) - 1
        q04 = next(case for case in report["cases"] if case["case_id"] == "Q04")
        q06 = next(case for case in report["cases"] if case["case_id"] == "Q06")
        assert q04["correction_consumed"] and q04["model_stop_stage"] == 3
        assert q06["continued_run_id"] != q06["first_run_id"]


@pytest.mark.parametrize("mode", ["unknown_reset_send", "suppress_reset_confirmation"])
async def test_unconfirmed_new_conversation_stops_before_next_case_stimulus(tmp_path, mode):
    async with environment(tmp_path, mode=mode) as (platform, profile, client):
        report = await run(platform, profile, client)
        assert report["status"] == "inconclusive"
        assert report["cases"][0]["status"] == "pass"
        assert report["cases"][1]["conversation_reset"]["status"] != "acknowledged"
        assert not report["full_chain_verified"] and not report["qq_live_verified"]
        stimuli = json.dumps(platform.peer_requests)
        assert "Q06-" not in stimuli
        assert all(row["case"] == "Q04" for row in report["model_stage_observations"])


async def test_q05_compares_actual_s_read_and_p_download_with_derived_sources(tmp_path):
    async with environment(tmp_path, cases=("Q05",)) as (platform, profile, client):
        report = await run(platform, profile, client)
        assert report["status"] == "pass", report
        result = report["cases"][0]
        assert result["source_sha256"] == result["s_read_sha256"]
        assert result["output_sha256"] == result["s_output_sha256"]
        assert result["source_input"]["input_id"] in result["source_input_ids"]
        assert any(r["action"] == "get_private_file_url" for r in platform.peer_requests)
        assert any(r["action"] == "get_private_file_url" for r in platform.source_requests)


async def test_q05_wrong_file_bytes_fail_even_when_filename_and_receipts_match(tmp_path):
    async with environment(tmp_path, cases=("Q05",), mode="tamper_output") as (
        platform,
        profile,
        client,
    ):
        report = await run(platform, profile, client)
        assert report["status"] == "fail" and report["cases"][0]["status"] == "fail"
        assert not report["full_chain_verified"]


@pytest.mark.parametrize(
    "mode,reason",
    [
        ("no_s", "candidate_s_account_not_connected"),
        ("wrong_s", "candidate_live_identity_not_confirmed"),
        ("wrong_p", "peer_live_identity_not_confirmed"),
    ],
)
async def test_missing_or_wrong_live_identity_sends_no_stimulus(tmp_path, mode, reason):
    async with environment(tmp_path, mode=mode) as (platform, profile, client):
        report = await run(platform, profile, client)
        assert report["status"] == "not_run" and report["reason"] == reason
        assert platform.peer_count == 0 and platform.source_count == 0


async def test_opt_in_and_shared_file_prerequisites_fail_before_any_connection(tmp_path):
    async with environment(tmp_path, cases=("Q05",)) as (platform, profile, client):
        report = await run(platform, profile, client, allow_live=False)
        assert report["status"] == "not_run"
        profile = CandidateProbeProfile(
            profile.peer, profile.candidate_config, profile.isolation_config, None, ("Q05",)
        )
        report = await run(platform, profile, client)
        assert report["reason"] == "shared_files_or_download_allowlist_missing"
        assert platform.peer_connections == 0 and not platform.source_requests


async def test_old_or_multiple_consumers_are_not_accepted_as_isolation_evidence(tmp_path):
    async with environment(tmp_path) as (platform, profile, client):
        data = json.loads(profile.isolation_config.read_text())
        data["network"]["websocketClients"].append(
            {"enable": True, "url": "ws://localhost:3001/onebot"}
        )
        profile.isolation_config.write_text(json.dumps(data))
        report = await run(platform, profile, client)
        assert report["status"] == "not_run"
        assert platform.peer_connections == 0 and not platform.source_requests


async def test_existing_configured_data_directory_is_never_opened_or_modified(tmp_path):
    async with environment(tmp_path, cases=("Q06",)) as (platform, profile, client):
        original = tmp_path / "candidate-data"
        original.mkdir()
        (original / "state.sqlite3").write_bytes(b"NOT A SQLITE DB: must never be opened")
        (original / "private.txt").write_text("synthetic pre-existing data", encoding="utf-8")
        before = {p.name: p.read_bytes() for p in original.iterdir()}
        report = await run(platform, profile, client)
        assert report["status"] == "pass", report
        assert {p.name: p.read_bytes() for p in original.iterdir()} == before
        assert platform.app.settings.data_dir.parent == original.parent
        assert platform.app.settings.data_dir != original


async def test_unrecognized_or_enabled_sse_consumer_configuration_is_not_used(tmp_path):
    async with environment(tmp_path) as (platform, profile, client):
        data = json.loads(profile.isolation_config.read_text())
        data["network"]["httpSseServers"] = [{"enable": True}]
        profile.isolation_config.write_text(json.dumps(data))
        assert (await run(platform, profile, client))["status"] == "not_run"
        assert platform.peer_connections == 0 and not platform.source_requests


async def test_silence_is_not_exercised_not_a_stage_pass(tmp_path):
    async with environment(tmp_path, mode="silent_source", cases=("Q04",)) as (
        platform,
        profile,
        client,
    ):
        report = await run(platform, profile, client)
        assert report["status"] == "inconclusive"
        assert report["cases"][0]["status"] == "not_exercised"
        assert platform.peer_count == 1


async def test_combined_budget_stops_before_next_stimulus_and_keeps_partial_evidence(tmp_path):
    async with environment(tmp_path, cases=("Q04",), max_outbound=3) as (platform, profile, client):
        report = await run(platform, profile, client)
        assert report["status"] == "inconclusive"
        assert (
            sum(report["counts"][key] for key in ("peer_send_attempts", "candidate_send_attempts"))
            <= 3
        )
        assert report["cases"][0]["peer_messages"] and report["peer_stimulus_receipts"]


async def test_unknown_peer_send_is_not_retried(tmp_path):
    async with environment(tmp_path, cases=("Q06",), mode="unknown_p_send") as (
        platform,
        profile,
        client,
    ):
        report = await run(platform, profile, client)
        assert report["status"] == "inconclusive" and platform.peer_count == 1
        assert report["peer_stimulus_receipts"][0]["status"] == "unknown"


async def test_simulation_injection_cannot_be_labeled_live(tmp_path):
    async with environment(tmp_path) as (platform, profile, client):
        report = await run(platform, profile, client, simulation=False)
        assert report["status"] == "not_run"
        assert report["reason"] == "test_injection_forbidden_in_live_mode"
        assert platform.peer_connections == 0 and not platform.source_requests


async def test_group_q04_uses_real_membership_preflight_and_mentions(tmp_path):
    async with environment(tmp_path, cases=("Q04",), destination="group") as (
        platform,
        profile,
        client,
    ):
        report = await run(platform, profile, client)
        assert report["status"] == "pass", report
        memberships = [r for r in platform.peer_requests if r["action"] == "get_group_member_info"]
        assert {str(r["params"]["user_id"]) for r in memberships} == {"100", "200"}
        initial = next(r for r in platform.peer_requests if r["action"] == "send_group_msg")
        assert initial["params"]["message"][0] == {"type": "at", "data": {"qq": "100"}}


def test_q07_is_explicitly_out_of_two_account_driver_scope():
    with pytest.raises(ValueError):
        CandidateProbeProfile(ProbeProfile("200", "100"), cases=("Q07",))
