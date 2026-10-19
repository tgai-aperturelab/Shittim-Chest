"""Public-platform attachment fetches retain source authority and WebRuntime bounds."""

import asyncio
import hashlib
from dataclasses import replace

import httpx
import pytest

from shittim_core.app.config import Settings
from shittim_core.app.material_intake import MaterialIntake
from shittim_core.app.service import Application
from shittim_core.domain.model import NormalizedResponse
from shittim_core.domain.types import (
    Acceptance,
    AccessContext,
    ConversationAddress,
    InputEvent,
    ProtocolReceipt,
    Segment,
)
from shittim_core.materials.store import MaterialStore
from shittim_core.state import StateStore
from shittim_core.tools.web import WebConfig, WebRuntime


def source(store, *, resources=True, direct_url=None):
    address = ConversationAddress("123", "private", "456")
    event = InputEvent(
        address,
        "456",
        "1",
        "read attachment",
        (
            Segment(
                "file",
                {
                    "file_id": "source-file",
                    "name": "notes.txt",
                    **({"url": direct_url} if direct_url else {}),
                },
            ),
        ),
    )
    access = AccessContext(
        "456",
        address.key,
        frozenset({"read_material"}),
        frozenset({"materials:ctx"}) if resources else frozenset(),
    )
    input_id, _ = store.ingest(event)
    store.create_context("ctx", event, access)
    store.attach_input("ctx", input_id)
    return event, Acceptance("associated", input_id, "ctx"), access


async def public_dns(host, port):
    return ["93.184.215.14"]


async def notify(context_id):
    return None


async def test_public_fetch_uses_pinned_http_and_retains_source(tmp_path):
    store = StateStore(tmp_path / "state.sqlite")
    event, accepted, access = source(store)
    calls, requests = [], []
    content = b"actual attachment bytes"

    async def locate(action, params, attempt):
        calls.append(action)
        return ProtocolReceipt("acknowledged", {"url": "https://files.example.org/notes"})

    def serve(request):
        requests.append(request)
        return httpx.Response(200, content=content)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as client:
        runtime = WebRuntime(WebConfig(max_download_bytes=1000), client=client, resolver=public_dns)
        intake = MaterialIntake(
            store,
            tmp_path / "materials",
            locate,
            notify,
            client=client,
            allowed_hosts=frozenset(),
            max_bytes=1000,
            policy_check=lambda _: True,
            public_fetch=runtime.fetch_bytes,
        )
        try:
            assert intake.submit(event, accepted)
            await intake.wait_idle()
            result = store.list_events("ctx")[-1]["payload"]
            assert result["status"] == "stored" and result["source_input_id"] == accepted.input_id
            assert result["provider_id"] == "456" and result["source_message_id"] == "1"
            assert result["sha256"] == hashlib.sha256(content).hexdigest()
            assert result["content_read"] is False and "url" not in result
            stored = await MaterialStore(tmp_path / "materials", store, "ctx").read(
                result["artifact_id"], access
            )
            assert stored.data == content
            assert store.get_artifact(result["artifact_id"])["source_ref"] == accepted.input_id
            assert calls == ["get_private_file_url"]
            assert (
                requests[0].url.host == "93.184.215.14"
                and requests[0].headers["host"] == "files.example.org"
            )
        finally:
            await intake.close()
            await runtime.close()
    store.close()


@pytest.mark.parametrize("denial", ["scope", "revoked_policy"])
async def test_authority_is_checked_before_locator_and_http(tmp_path, denial):
    store = StateStore(tmp_path / "state.sqlite")
    event, accepted, _ = source(store, resources=denial != "scope")
    policy = {"allow": True}

    async def forbidden(*args):
        pytest.fail("Denied authority reached a network boundary")

    async with httpx.AsyncClient() as client:
        intake = MaterialIntake(
            store,
            tmp_path / "materials",
            forbidden,
            notify,
            client=client,
            allowed_hosts=frozenset(),
            max_bytes=1000,
            policy_check=lambda _: policy["allow"],
            public_fetch=forbidden,
        )
        assert intake.submit(event, accepted)
        if denial == "revoked_policy":
            policy["allow"] = False
        await intake.wait_idle()
        assert store.list_events("ctx")[-1]["payload"]["reason"] == (
            "material_scope_denied" if denial == "scope" else "current_policy_denied"
        )
        await intake.close()
    store.close()


async def test_caller_cannot_replace_accepted_attachment_url_or_context(tmp_path):
    store = StateStore(tmp_path / "state.sqlite")
    event, accepted, access = source(store, direct_url="https://files.example.org/original")
    other = InputEvent(event.address, "456", "2", "other")
    store.create_context("other", other, access)

    async def forbidden(*args):
        pytest.fail("Unbound or altered source reached network")

    async with httpx.AsyncClient() as client:
        intake = MaterialIntake(
            store,
            tmp_path / "materials",
            forbidden,
            notify,
            client=client,
            allowed_hosts=frozenset(),
            max_bytes=1000,
            policy_check=lambda _: True,
            public_fetch=forbidden,
        )
        with pytest.raises(ValueError, match="accepted record"):
            intake.submit(
                replace(event, segments=(Segment("file", {"url": "http://127.0.0.1/secret"}),)),
                accepted,
            )
        with pytest.raises(ValueError, match="associated"):
            intake.submit(event, replace(accepted, context_id="other"))
        await intake.close()
    store.close()


@pytest.mark.parametrize("mode", ["private", "oversize"])
async def test_public_fetch_private_and_size_failures_record_unavailable(tmp_path, mode):
    store = StateStore(tmp_path / "state.sqlite")
    event, accepted, _ = source(store, direct_url="https://files.example.org/data")
    requests = []

    async def resolver(host, port):
        return ["127.0.0.1"] if mode == "private" else ["93.184.215.14"]

    def serve(request):
        requests.append(request)
        return httpx.Response(200, content=b"x" * 120)

    async def forbidden(*args):
        pytest.fail("Direct platform URL should not need locator")

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as client:
        runtime = WebRuntime(WebConfig(max_download_bytes=64), client=client, resolver=resolver)
        intake = MaterialIntake(
            store,
            tmp_path / "materials",
            forbidden,
            notify,
            client=client,
            allowed_hosts=frozenset(),
            max_bytes=64,
            policy_check=lambda _: True,
            public_fetch=runtime.fetch_bytes,
        )
        assert intake.submit(event, accepted)
        await intake.wait_idle()
        result = store.list_events("ctx")[-1]["payload"]
        assert result["status"] == "unavailable" and "artifact_id" not in result
        assert result["reason"] == (
            "platform_file_network_denied" if mode == "private" else "platform_file_download_failed"
        )
        assert len(requests) == (0 if mode == "private" else 1)
        assert list((tmp_path / "materials").iterdir()) == []
        await intake.close()
        await runtime.close()
    store.close()


async def test_public_fetch_close_cancels_download_and_records_interruption(tmp_path):
    store = StateStore(tmp_path / "state.sqlite")
    event, accepted, _ = source(store, direct_url="https://files.example.org/data")
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def fetch(url):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def forbidden(*args):
        pytest.fail("Direct source needs no platform locator")

    async with httpx.AsyncClient() as client:
        intake = MaterialIntake(
            store,
            tmp_path / "materials",
            forbidden,
            notify,
            client=client,
            allowed_hosts=frozenset(),
            max_bytes=64,
            policy_check=lambda _: True,
            public_fetch=fetch,
        )
        assert intake.submit(event, accepted)
        await entered.wait()
        await intake.close()
        assert cancelled.is_set() and not intake._tasks
        assert store.list_events("ctx")[-1]["payload"]["reason"] == "intake_interrupted"
        assert list((tmp_path / "materials").iterdir()) == []
    store.close()


class IdleModel:
    async def complete(self, messages, tools):
        return NormalizedResponse("done", None, "stop")


def settings(tmp_path, **kwargs):
    return Settings(
        "123",
        "QQ_TOKEN",
        "fixture",
        "https://model.invalid/v1",
        "MODEL_KEY",
        tmp_path / "app",
        port=0,
        allowed_users=frozenset({"456"}),
        allowed_tools=frozenset({"read_material"}),
        platform_public_files=True,
        max_attachment_bytes=1234,
        **kwargs,
    )


async def test_application_public_fetch_wiring_and_client_ownership(tmp_path):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: pytest.fail("No network expected"))
    ) as client:
        app = Application(
            settings(tmp_path),
            model=IdleModel(),
            environ={"QQ_TOKEN": "fixture"},
            download_client=client,
        )
        await app.start()
        try:
            assert app.material_web is not None and app.material_intake is not None
            assert app.material_web.client is client
            assert app.material_web.config.max_download_bytes == 1234
            assert app.material_intake.public_fetch == app.material_web.fetch_bytes
        finally:
            await app.close()
        assert not client.is_closed  # injected client remains owned by its caller


async def test_application_closes_owned_public_fetch_client(tmp_path):
    app = Application(settings(tmp_path), model=IdleModel(), environ={"QQ_TOKEN": "fixture"})
    await app.start()
    client = app.material_web.client
    await app.close()
    assert client.is_closed


async def test_application_start_failure_closes_public_fetch_client(tmp_path, monkeypatch):
    from shittim_core.qq.transport import QQTransport

    async def fail_start(self, host, port):
        raise OSError("simulated listener startup failure")

    monkeypatch.setattr(QQTransport, "start", fail_start)
    app = Application(settings(tmp_path), model=IdleModel(), environ={"QQ_TOKEN": "fixture"})
    with pytest.raises(OSError, match="listener"):
        await app.start()
    assert app.material_web.client.is_closed
    assert app._finished


@pytest.mark.parametrize("failure", ["size", "policy"])
async def test_public_fetch_result_rechecked_before_storage(tmp_path, failure):
    store = StateStore(tmp_path / "state.sqlite")
    event, accepted, _ = source(store, direct_url="https://files.example.org/data")
    policy = {"allowed": True}

    async def fetch(url):
        if failure == "policy":
            policy["allowed"] = False
        return b"x" * 65

    async def forbidden(*args):
        pytest.fail("Direct attachment needs no locator")

    async with httpx.AsyncClient() as client:
        intake = MaterialIntake(
            store,
            tmp_path / "materials",
            forbidden,
            notify,
            client=client,
            allowed_hosts=frozenset(),
            max_bytes=64,
            policy_check=lambda _: policy["allowed"],
            public_fetch=fetch,
        )
        assert intake.submit(event, accepted)
        await intake.wait_idle()
        result = store.list_events("ctx")[-1]["payload"]
        assert result["status"] == "unavailable"
        assert result["reason"] == (
            "material_too_large" if failure == "size" else "current_policy_denied"
        )
        assert list((tmp_path / "materials").iterdir()) == []
        await intake.close()
    store.close()


@pytest.mark.parametrize("value", ["true", 1, None])
def test_public_file_switch_requires_actual_boolean(tmp_path, value):
    with pytest.raises(ValueError, match="JSON boolean"):
        replace(settings(tmp_path), platform_public_files=value)
