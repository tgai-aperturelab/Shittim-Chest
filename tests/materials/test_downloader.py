from dataclasses import replace

import httpx
import pytest

from shittim_core.domain.types import AccessContext, ConversationAddress, InputEvent
from shittim_core.materials import MaterialStore
from shittim_core.materials.downloader import download_material
from shittim_core.state import StateStore


@pytest.fixture
def env(tmp_path):
    store = StateStore(tmp_path / "state.db")
    address = ConversationAddress("bot", "private", "owner")
    access = AccessContext("owner", address.key, allowed_resources=frozenset({"materials:c"}))
    store.create_context("c", InputEvent(address, "owner", "1", "fixture"), access)
    yield MaterialStore(tmp_path / "materials", store, "c", max_bytes=32), access
    store.close()


async def retrieve(env, client, url="https://files.example/fixture", **kwargs):
    materials, access = env
    return await download_material(
        url,
        client=client,
        allowed_hosts=frozenset({"files.example"}),
        materials=materials,
        access=access,
        name="fixture.txt",
        **kwargs,
    )


async def test_mock_http_download_registers_checked_bytes(env):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"fixture"))
    ) as client:
        result = await retrieve(env, client, source_ref="input:1")
    assert result.status == "stored"
    material, access = env
    assert (await material.read(result.ref.artifact_id, access)).data == b"fixture"


@pytest.mark.parametrize(
    "url",
    [
        "https://other.example/a",
        "file:///secret",
        "https://name:password@files.example/a",
        "https://files.example:8080/a",
        "https://files.example/a#fragment",
    ],
)
async def test_url_rejection_does_not_issue_request(env, url):
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: calls.append(req))
    ) as client:
        result = await retrieve(env, client, url)
    assert result.status == "unavailable"
    assert not calls


async def test_redirect_is_not_followed_even_if_client_default_is_enabled(env):
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(302, headers={"Location": "https://other.example/private"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), follow_redirects=True
    ) as client:
        result = await retrieve(env, client)
    assert result.status == "unavailable"
    assert len(calls) == 1
    assert not list(env[0].root.iterdir())


@pytest.mark.parametrize("headers", [{"Content-Length": "33"}, {"Content-Length": "1"}])
async def test_size_limit_covers_declared_and_actual_bytes(env, headers):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, headers=headers, content=b"x" * 33)
        )
    ) as client:
        result = await retrieve(env, client)
    assert result.status == "unavailable"
    assert not list(env[0].root.iterdir())


async def test_no_capability_prevents_network_read(env):
    material, access = env
    calls = []
    denied = (material, replace(access, allowed_resources=frozenset()))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: calls.append(req))
    ) as client:
        result = await retrieve(denied, client)
    assert result.status == "unavailable"
    assert not calls
