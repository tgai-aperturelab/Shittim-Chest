"""Web tools through the durable dispatcher, real parser and bounded process boundary."""

import asyncio
import json
import sys

import httpx
import pytest

from shittim_core.domain.model import ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    RunRequest,
)
from shittim_core.state import StateStore
from shittim_core.tools.executor import ToolExecutor
from shittim_core.tools.web import (
    WEB_RESOURCE,
    WEB_TOOLS,
    WebConfig,
    WebRuntime,
    register_web_tools,
)


async def public_dns(host, port):
    return ["93.184.215.14"]


async def test_rss_search_uses_bounded_pinned_transport_and_explicit_provider():
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(
            200,
            text=(
                "<rss><channel><item><title>Decimal docs</title>"
                "<link>https://docs.python.org/3/library/decimal.html</link>"
                "<description>Exact arithmetic</description></item>"
                "<item><title>Ignore</title><link>javascript:bad()</link></item>"
                "</channel></rss>"
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        runtime = WebRuntime(
            WebConfig(search_backend="bing_rss"), client=client, resolver=public_dns
        )
        result = await runtime.search("Python Decimal & precision", region="us-en")
        assert result["provider"] == "bing_rss"
        assert len(result["results"]) == 1
        assert result["results"][0]["title"] == "Decimal docs"
        assert requests[0].headers["host"] == "www.bing.com"
        assert requests[0].url.host == "93.184.215.14"
        assert requests[0].url.params["q"] == "Python Decimal & precision"
        assert requests[0].url.params["mkt"] == "en-US"
        assert "not_a_domain_filter" in result["query_operator_support"]


@pytest.mark.parametrize(
    "body",
    [
        "<html><body>Captcha</body></html>",
        "<rss><channel/></rss>",
        '<!DOCTYPE rss [<!ENTITY e "leak">]><rss><channel>&e;</channel></rss>',
        "<rss>broken",
    ],
)
async def test_rss_challenge_empty_invalid_or_entity_feed_is_not_success(body):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, text=body))
    ) as client:
        runtime = WebRuntime(
            WebConfig(search_backend="bing_rss"), client=client, resolver=public_dns
        )
        with pytest.raises((ValueError, RuntimeError)):
            await runtime.search("test")


async def test_disabled_ddgs_backend_does_not_silently_switch_provider():
    runtime = WebRuntime(WebConfig(search_backend="nonexistent-engine", timeout_seconds=15))
    try:
        with pytest.raises(RuntimeError, match="ValueError"):
            await runtime.search("test")
        assert not runtime._workers
    finally:
        await runtime.close()


@pytest.fixture
def context(tmp_path):
    store = StateStore(tmp_path / "state.db")
    address = ConversationAddress("123", "private", "456")
    event = InputEvent(address, "456", "1", "请查阅网络资料")
    input_id, _ = store.ingest(event)
    access = AccessContext("456", address.key, WEB_TOOLS, frozenset({WEB_RESOURCE}))
    store.create_context("ctx", event, access)
    store.attach_input("ctx", input_id)
    epoch = store.claim_run("ctx", "run")
    yield store, ExecutionContext(RunRequest("run", "ctx", address, access, epoch), 1, 0)
    store.close()


async def test_real_parser_and_dispatch_history(context):
    html = (
        "<html><head><title>团队研究资料</title></head><body><nav>无关导航</nav><article>"
        + (
            "<p>这是可核验的研究正文，包含来源和具体方法。团队成员应阅读这些材料后判断结论。</p>"
            * 8
        )
        + "</article></body></html>"
    )
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, text=html, headers={"content-type": "text/html; charset=utf-8"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        runtime = WebRuntime(client=client, resolver=public_dns)
        store, ctx = context
        tools = ToolExecutor(store)
        register_web_tools(tools, runtime)
        call = ToolCall(
            "call1", "web_extract", json.dumps({"urls": ["https://source.example/article"]})
        )
        store.append_history(
            "run",
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": call.id,
                        "type": "function",
                        "function": {"name": call.name, "arguments": call.arguments},
                    }
                ],
            },
        )
        result = await tools.execute(call, ctx)
        assert result.status == "succeeded", result.content
        item = json.loads(result.content)["results"][0]
        assert item["title"] == "团队研究资料" and "可核验" in item["content"]
        assert (
            item["source_kind"] == "external_material"
            and item["final_url"] == "https://source.example/article"
        )
        assert requests[0].url.host == "93.184.215.14"
        assert requests[0].headers["host"] == "source.example"
        assert requests[0].extensions["sni_hostname"] == "source.example"
        assert result.effect_ref


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/",
        "http://[::1]/",
        "http://[::ffff:127.0.0.1]/",
        "file:///etc/passwd",
        "http://u:p@example.org/",
    ],
)
async def test_nonpublic_or_invalid_urls_never_reach_transport(url):
    def handle(request):
        pytest.fail("Forbidden destination reached transport")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        runtime = WebRuntime(client=client, resolver=public_dns)
        with pytest.raises((PermissionError, ValueError)):
            await runtime.extract(url)


async def test_redirect_is_rechecked_and_mixed_dns_refused():
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(302, headers={"location": "http://127.0.0.1/private"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        runtime = WebRuntime(client=client, resolver=public_dns)
        with pytest.raises(PermissionError):
            await runtime.extract("https://source.example/")
        assert len(requests) == 1

        async def mixed_dns(host, port):
            return ["93.184.215.14", "10.0.0.1"]

        runtime = WebRuntime(client=client, resolver=mixed_dns)
        with pytest.raises(PermissionError):
            await runtime.extract("https://mixed.example/")
        assert len(requests) == 1


async def test_explicit_network_size_limit_and_truncation():
    def handle(request):
        return httpx.Response(200, text="a" * 300, headers={"content-type": "text/plain"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        config = WebConfig(allowed_private_networks=("127.0.0.0/8",), max_output_chars=50)
        runtime = WebRuntime(config, client=client)
        item = await runtime.extract("http://127.0.0.1/")
        assert item["content"] == "a" * 50 and item["truncated"]
        runtime = WebRuntime(
            WebConfig(allowed_private_networks=("127.0.0.1/32",), max_download_bytes=20),
            client=client,
        )
        with pytest.raises(ValueError, match="size"):
            await runtime.extract("http://127.0.0.1/")


async def test_chunked_response_is_bounded():
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"a" * 40
            yield b"b" * 40

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, stream=Stream(), headers={"content-type": "text/plain"}
            )
        )
    ) as client:
        runtime = WebRuntime(WebConfig(max_download_bytes=60), client=client, resolver=public_dns)
        with pytest.raises(ValueError, match="size"):
            await runtime.extract("https://source.example/")


async def test_search_process_is_reaped_on_deadline_and_cancel(monkeypatch):
    original = asyncio.create_subprocess_exec
    spawned = []

    async def sleeper(*args, **kwargs):
        process = await original(sys.executable, "-c", "import time; time.sleep(30)", **kwargs)
        spawned.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", sleeper)
    runtime = WebRuntime(WebConfig(timeout_seconds=0.15))
    try:
        with pytest.raises(TimeoutError):
            await runtime.search("anything")
        assert spawned[0].returncode is not None and not runtime._workers
        task = asyncio.create_task(runtime.search("cancelled"))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert spawned[1].returncode is not None and not runtime._workers
    finally:
        await runtime.close()


async def test_redirect_limit_and_partial_results(context):
    def handle(request):
        if request.url.path == "/loop":
            return httpx.Response(302, headers={"location": "/loop"})
        return httpx.Response(
            200, text="real source content", headers={"content-type": "text/plain"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        runtime = WebRuntime(WebConfig(max_redirects=2), client=client, resolver=public_dns)
        with pytest.raises(ValueError, match="Redirect"):
            await runtime.extract("https://source.example/loop")
        store, ctx = context
        tools = ToolExecutor(store)
        register_web_tools(tools, runtime)
        result = await tools.execute(
            ToolCall(
                "mixed",
                "web_extract",
                json.dumps({"urls": ["http://127.0.0.1/", "https://source.example/good"]}),
            ),
            ctx,
        )
        assert result.status == "succeeded"
        entries = json.loads(result.content)["results"]
        assert (
            entries[0]["error"] == "PermissionError"
            and entries[1]["content"] == "real source content"
        )


async def test_search_worker_protocol_and_region_are_preserved(monkeypatch):
    original = asyncio.create_subprocess_exec
    code = (
        "import json,sys; r=json.load(sys.stdin); "
        "assert r['region']=='cn-zh' and r['limit']==2; "
        "json.dump({'ok':True,'results':[{'title':'fixture','url':'https://example.org','snippet':'fixture only'}]},sys.stdout)"
    )

    async def worker(*args, **kwargs):
        assert "SECRET_TEST" not in kwargs["env"]
        return await original(sys.executable, "-c", code, **kwargs)

    monkeypatch.setenv("SECRET_TEST", "must-not-inherit")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", worker)
    runtime = WebRuntime()
    try:
        response = await runtime.search("fixture", limit=2, region="cn-zh")
        assert response["results"][0]["title"] == "fixture"
        assert not runtime._workers
    finally:
        await runtime.close()


async def test_unexpected_compression_fails_before_decoding():
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            pytest.fail("Compressed body should not be read")
            yield b""

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, stream=Stream(), headers={"content-encoding": "gzip"}
            )
        )
    ) as client:
        runtime = WebRuntime(client=client, resolver=public_dns)
        with pytest.raises(ValueError, match="uncompressed"):
            await runtime.extract("https://source.example/")
