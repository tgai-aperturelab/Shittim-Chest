import asyncio
import json

import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from shittim_core.qq.transport import QQTransport


def headers(bot="123", token="test-secret"):
    return {"X-Self-ID": bot, "Authorization": f"Bearer {token}", "X-Client-Role": "universal"}


def message(self_id=123, user_id=456, message_id=10):
    return {
        "post_type": "message",
        "message_type": "private",
        "self_id": self_id,
        "user_id": user_id,
        "message_id": message_id,
        "message": [{"type": "text", "data": {"text": "hello"}}],
    }


async def acknowledge(ws, request, message_id=99):
    await ws.send(
        json.dumps(
            {
                "echo": request["echo"],
                "status": "ok",
                "retcode": 0,
                "data": {"message_id": message_id},
            }
        )
    )


@pytest.mark.parametrize("message_id", [True, {}, "", "not-an-id", 1.5, "--1", "١٢"])
async def test_malformed_send_message_identity_is_unknown(setup_transport, message_id):
    transport, url, _ = setup_transport
    async with connect(url, additional_headers=headers()) as ws:
        task = asyncio.create_task(transport.call("send_private_msg", {"user_id": 456}, "bad-id"))
        request = json.loads(await ws.recv())
        await acknowledge(ws, request, message_id)
        result = await task
        assert result.status == "unknown" and result.message_id is None


@pytest.fixture
async def setup_transport():
    events = asyncio.Queue()
    transport = QQTransport("123", "test-secret", events.put, timeout_seconds=0.15)
    port = await transport.start()
    yield transport, f"ws://127.0.0.1:{port}/onebot", events
    await transport.close()


@pytest.mark.parametrize("role", ["Universal", "universal"])
async def test_napcat_native_universal_role_receives_input(setup_transport, role):
    _, url, events = setup_transport
    native_headers = {**headers(), "X-Client-Role": role}
    async with connect(url, additional_headers=native_headers) as ws:
        await ws.send(json.dumps(message()))
        event = await asyncio.wait_for(events.get(), 1)
        assert event.message_id == "10"


@pytest.mark.parametrize("role", ["API", "Event", "universal-extra"])
async def test_split_or_unknown_client_role_is_rejected(setup_transport, role):
    _, url, events = setup_transport
    with pytest.raises(InvalidStatus):
        async with connect(url, additional_headers={**headers(), "X-Client-Role": role}):
            pytest.fail("Non-universal socket admitted")
    assert events.empty()


@pytest.mark.parametrize("account,token", [("123", "wrong"), ("999", "test-secret")])
async def test_connection_rejects_token_or_account(setup_transport, account, token):
    transport, url, events = setup_transport
    with pytest.raises(InvalidStatus):
        async with connect(url, additional_headers=headers(account, token)):
            pytest.fail("Unauthorized connection accepted")
    assert events.empty()
    assert (await transport.call("send_private_msg", {}, "absent")).status == "failed"


async def test_real_socket_receives_input_and_correlates_api_receipt(setup_transport):
    transport, url, events = setup_transport
    async with connect(url, additional_headers=headers()) as ws:
        await ws.send(json.dumps(message(self_id=999)))
        await ws.send(json.dumps(message(user_id=123)))
        await ws.send(json.dumps(message()))
        received = await asyncio.wait_for(events.get(), 1)
        assert received.author_id == "456" and received.message_id == "10"
        assert events.empty()
        pending = asyncio.create_task(transport.call("send_private_msg", {"user_id": 456}, "one"))
        request = json.loads(await ws.recv())
        assert request["params"]["self_id"] == "123"
        await ws.send(
            json.dumps(
                {
                    "echo": "not-the-request",
                    "status": "ok",
                    "retcode": 0,
                    "data": {"message_id": 777},
                }
            )
        )
        await acknowledge(ws, request)
        result = await pending
        assert result.status == "acknowledged" and result.message_id == "99"
        assert (await transport.call("send_private_msg", {}, "one")).status == "failed"


async def test_replacement_resolves_old_pending_unknown_without_clearing_new_socket(
    setup_transport,
):
    transport, url, _ = setup_transport
    assert not transport.ready.is_set()
    async with connect(url, additional_headers=headers()) as old:
        await asyncio.wait_for(transport.ready.wait(), 1)
        old_call = asyncio.create_task(transport.call("send_private_msg", {}, "old"))
        old_request = json.loads(await old.recv())
        async with connect(url, additional_headers=headers()) as new:
            await old.wait_closed()
            assert (await old_call).status == "unknown"
            new_call = asyncio.create_task(transport.call("send_private_msg", {}, "new"))
            request = json.loads(await new.recv())
            # An old receipt replayed on a new connection cannot resolve the new call.
            await acknowledge(new, old_request, 100)
            await acknowledge(new, request, 101)
            assert (await new_call).message_id == "101"
            assert transport.ready.is_set()


async def test_disconnect_and_timeout_are_unknown_without_resend(setup_transport):
    transport, url, _ = setup_transport
    async with connect(url, additional_headers=headers()) as ws:
        pending = asyncio.create_task(transport.call("send_private_msg", {}, "disconnect"))
        await ws.recv()
        await ws.close()
        assert (await pending).status == "unknown"
    async with connect(url, additional_headers=headers()) as ws:
        pending = asyncio.create_task(transport.call("send_private_msg", {}, "timeout"))
        request = json.loads(await ws.recv())
        assert (await pending).status == "unknown"
        await acknowledge(ws, request)  # Late response is harmless, not a new attempt.
        probe = asyncio.create_task(transport.call("get_status", {}, "probe"))
        next_request = json.loads(await ws.recv())
        assert next_request["action"] == "get_status"
        await acknowledge(ws, next_request)
        assert (await probe).status == "acknowledged"


@pytest.mark.parametrize(
    "response,expected",
    [
        ({"status": "failed", "retcode": 100, "data": None}, "failed"),
        ({"status": "async", "retcode": 1, "data": {}}, "unknown"),
        ({"status": "ok", "retcode": 0, "data": None}, "unknown"),
    ],
)
async def test_protocol_classification_preserves_uncertainty(setup_transport, response, expected):
    transport, url, _ = setup_transport
    async with connect(url, additional_headers=headers()) as ws:
        pending = asyncio.create_task(transport.call("send_private_msg", {}, "classification"))
        request = json.loads(await ws.recv())
        await ws.send(json.dumps({**response, "echo": request["echo"]}))
        assert (await pending).status == expected


async def test_slow_intake_callback_does_not_block_protocol_receipt():
    entered = asyncio.Event()
    release = asyncio.Event()

    async def on_event(event):
        entered.set()
        await release.wait()

    transport = QQTransport("123", "test-secret", on_event)
    port = await transport.start()
    try:
        async with connect(f"ws://127.0.0.1:{port}/onebot", additional_headers=headers()) as ws:
            await ws.send(json.dumps(message()))
            await asyncio.wait_for(entered.wait(), 1)
            pending = asyncio.create_task(transport.call("get_status", {}, "slow-callback"))
            request = json.loads(await ws.recv())
            await acknowledge(ws, request)
            assert (await asyncio.wait_for(pending, 1)).status == "acknowledged"
    finally:
        release.set()
        await transport.close()


async def test_cancel_after_dispatch_is_unknown_and_late_receipt_is_ignored(setup_transport):
    transport, url, _ = setup_transport
    async with connect(url, additional_headers=headers()) as ws:
        pending = asyncio.create_task(transport.call("send_private_msg", {}, "cancelled"))
        request = json.loads(await ws.recv())
        pending.cancel()
        assert (await pending).status == "unknown"
        await acknowledge(ws, request)
        probe = asyncio.create_task(transport.call("get_status", {}, "after-cancel"))
        request = json.loads(await ws.recv())
        assert request["action"] == "get_status"
        await acknowledge(ws, request)
        assert (await probe).status == "acknowledged"


async def test_send_without_message_id_is_unknown_but_query_dict_is_valid(setup_transport):
    transport, url, _ = setup_transport
    async with connect(url, additional_headers=headers()) as ws:
        for action, status in [("send_private_msg", "unknown"), ("get_status", "acknowledged")]:
            pending = asyncio.create_task(transport.call(action, {}, action))
            request = json.loads(await ws.recv())
            await ws.send(
                json.dumps({"echo": request["echo"], "status": "ok", "retcode": 0, "data": {}})
            )
            assert (await pending).status == status
