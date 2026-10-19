"""Loopback-only validation of the optional peer driver; these are never qq_live evidence."""

import asyncio
import json
from contextlib import asynccontextmanager
from http import HTTPStatus

import pytest
from websockets.asyncio.server import serve

from tools.qq_live_probe import ProbeProfile, run_probe


@asynccontextmanager
async def peer_server(*, login_id="200", mode="normal"):
    requests = []
    connections = []

    def authorize(connection, request):
        connections.append(request.headers.get("Authorization"))
        if request.headers.get("Authorization") != "Bearer fake-secret":
            return connection.respond(HTTPStatus.UNAUTHORIZED, "Denied")
        return None

    async def handler(socket):
        index = 0
        async for raw in socket:
            request = json.loads(raw)
            requests.append(request)
            action = request["action"]
            if action == "get_login_info":
                await socket.send(
                    json.dumps(
                        {
                            "echo": request["echo"],
                            "status": "ok",
                            "retcode": 0,
                            "data": {"user_id": login_id},
                        }
                    )
                )
            elif action == "get_group_member_info":
                await socket.send(
                    json.dumps(
                        {
                            "echo": request["echo"],
                            "status": "ok",
                            "retcode": 0,
                            "data": request["params"],
                        }
                    )
                )
            else:
                index += 1
                nonce = request["params"]["message"][-1]["data"]["text"].split("：")[-1]
                if mode == "foreign_account":
                    self_id = 999
                else:
                    self_id = 200
                response_event = {
                    "post_type": "message",
                    "self_id": self_id,
                    "user_id": 100,
                    "message_type": "group" if action == "send_group_msg" else "private",
                    "message_id": 9000 + index,
                    "message": [{"type": "text", "data": {"text": nonce}}],
                }
                if action == "send_group_msg":
                    response_event["group_id"] = request["params"]["group_id"]
                # Observe before the send receipt to exercise fast-event buffering.
                if mode != "silent":
                    await socket.send(json.dumps(response_event))
                if mode != "missing_receipt":
                    await socket.send(
                        json.dumps(
                            {
                                "echo": request["echo"],
                                "status": "ok",
                                "retcode": 0,
                                "data": {"message_id": 1000 + index},
                            }
                        )
                    )

    async with serve(handler, "127.0.0.1", 0, process_request=authorize) as server:
        yield f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}", requests, connections


def profile(url=None, **kwargs):
    return ProbeProfile(
        "200",
        "100",
        forward_ws_url=url,
        case_timeout_seconds=0.15,
        request_timeout_seconds=0.05,
        batch_timeout_seconds=2,
        **kwargs,
    )


async def test_no_network_without_both_opt_ins():
    async with peer_server() as (url, requests, connections):
        assert (await run_probe(profile(url), environ={"QQ_PROBE_PEER_TOKEN": "fake-secret"}))[
            "status"
        ] == "not_run"
        assert (await run_probe(None, allow_live=True))["status"] == "not_run"
        assert requests == [] and connections == []


async def test_missing_endpoint_and_token_are_not_run_not_pass():
    missing_endpoint = await run_probe(profile(), allow_live=True, environ={})
    assert missing_endpoint["status"] == "not_run"
    assert missing_endpoint["reason"] == "peer_onebot_endpoint_not_configured"
    async with peer_server() as (url, requests, connections):
        result = await run_probe(profile(url), allow_live=True, environ={})
        assert result["status"] == "not_run" and requests == [] and connections == []


async def test_token_auth_failure_does_not_send_stimulus():
    async with peer_server() as (url, requests, _):
        result = await run_probe(
            profile(url), allow_live=True, environ={"QQ_PROBE_PEER_TOKEN": "wrong"}
        )
        assert result["status"] == "fail"
        assert requests == []
        assert "wrong" not in json.dumps(result)


async def test_login_identity_mismatch_stops_before_send():
    async with peer_server(login_id="100") as (url, requests, _):
        result = await run_probe(
            profile(url), allow_live=True, environ={"QQ_PROBE_PEER_TOKEN": "fake-secret"}
        )
        assert result["status"] == "fail"
        assert result["reason"] == "peer_identity_mismatch_zero_stimulus"
        assert [r["action"] for r in requests] == ["get_login_info"]


async def test_fast_observation_and_account_scoped_quote_ids():
    async with peer_server() as (url, requests, connections):
        result = await run_probe(
            profile(url, cases=("Q01", "Q03")),
            allow_live=True,
            environ={"QQ_PROBE_PEER_TOKEN": "fake-secret"},
        )
        assert connections == ["Bearer fake-secret"]
        assert result["status"] == "pass" and not result["full_chain_verified"]
        assert result["two_independent_members"] == "not_exercised"
        first, second = result["cases"]
        assert first["peer_send_receipt"]["message_id"] == "1001"
        assert first["peer_received_event"]["message_id"] == "9001"
        assert second["nonce"] != first["nonce"]
        assert requests[2]["params"]["message"][0] == {"type": "reply", "data": {"id": "9001"}}
        assert first["peer_received_event"]["self_id"] == "200"
        assert result["observed_outbound_total"] == 4


async def test_group_is_explicit_membership_verified_and_bot_mentioned():
    async with peer_server() as (url, requests, _):
        result = await run_probe(
            profile(url, destination="group", group_id="300"),
            allow_live=True,
            environ={"QQ_PROBE_PEER_TOKEN": "fake-secret"},
        )
        assert result["status"] == "pass"
        assert [r["action"] for r in requests[:3]] == [
            "get_login_info",
            "get_group_member_info",
            "get_group_member_info",
        ]
        send = requests[3]
        assert send["action"] == "send_group_msg" and send["params"]["group_id"] == 300
        assert send["params"]["message"][0] == {"type": "at", "data": {"qq": "100"}}


async def test_unknown_send_is_never_retried_and_stops_next_case():
    async with peer_server(mode="missing_receipt") as (url, requests, _):
        result = await run_probe(
            profile(url, cases=("Q01", "Q03")),
            allow_live=True,
            environ={"QQ_PROBE_PEER_TOKEN": "fake-secret"},
        )
        assert result["status"] == "inconclusive"
        assert result["cases"][0]["send_status"] == "unknown"
        assert result["cases"][0]["peer_received_event"]["message_id"] == "9001"
        assert len([r for r in requests if r["action"].startswith("send_")]) == 1


async def test_silence_and_wrong_observation_account_do_not_pass():
    for mode in ("silent", "foreign_account"):
        async with peer_server(mode=mode) as (url, requests, _):
            result = await run_probe(
                profile(url), allow_live=True, environ={"QQ_PROBE_PEER_TOKEN": "fake-secret"}
            )
            assert result["status"] == "inconclusive"
            assert len([r for r in requests if r["action"].startswith("send_")]) == 1


async def test_message_budget_stops_next_stimulus():
    async with peer_server() as (url, requests, _):
        result = await run_probe(
            profile(url, cases=("Q01", "Q03"), max_outbound=2),
            allow_live=True,
            environ={"QQ_PROBE_PEER_TOKEN": "fake-secret"},
        )
        assert result["status"] == "inconclusive"
        assert result["reason"] == "outbound_budget_exhausted"
        assert len([r for r in requests if r["action"].startswith("send_")]) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"test_member_id": "100"},
        {"destination": "group"},
        {"forward_ws_url": "ws://token:secret@example.invalid"},
        {"forward_ws_url": "ws://example.invalid?token=secret"},
        {"cases": ("Q07",)},
        {"max_outbound": 41},
        {"batch_timeout_seconds": float("inf")},
    ],
)
def test_invalid_profiles_reject_scope_and_unbounded_parameters(changes):
    values = {"test_member_id": "200", "sut_bot_id": "100", **changes}
    with pytest.raises(ValueError):
        ProbeProfile(**values)


async def test_batch_timeout_is_bounded():
    async with peer_server(mode="silent") as (url, requests, _):
        config = ProbeProfile(
            "200",
            "100",
            forward_ws_url=url,
            batch_timeout_seconds=0.05,
            request_timeout_seconds=0.05,
            case_timeout_seconds=0.2,
        )
        result = await asyncio.wait_for(
            run_probe(config, allow_live=True, environ={"QQ_PROBE_PEER_TOKEN": "fake-secret"}), 1
        )
        assert result["status"] == "inconclusive"
        assert len([r for r in requests if r["action"].startswith("send_")]) <= 1
