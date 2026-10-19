"""Web research tools with bounded workers and per-hop, DNS-pinned HTTP fetches."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import os
import socket
import sys
import xml.etree.ElementTree as ET
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import urlencode, urljoin, urlsplit

import httpx

from shittim_core.domain.types import ExecutionContext, ToolResult
from shittim_core.tools.executor import ToolExecutor, ToolSpec

WEB_TOOLS = frozenset({"web_search", "web_extract"})
WEB_RESOURCE = "network:web"
type Resolver = Callable[[str, int], Awaitable[list[str]]]


@dataclass(frozen=True)
class WebConfig:
    region: str = "wt-wt"
    search_backend: str = "auto"
    search_limit: int = 5
    timeout_seconds: float = 30
    request_timeout_seconds: float = 10
    max_download_bytes: int = 4 * 1024 * 1024
    max_output_chars: int = 30000
    max_redirects: int = 5
    allowed_private_networks: tuple[str, ...] = ()
    search_proxy: str | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.search_limit <= 20:
            raise ValueError("search_limit must be between 1 and 20")
        if (
            min(
                self.timeout_seconds,
                self.request_timeout_seconds,
                self.max_download_bytes,
                self.max_output_chars,
            )
            <= 0
            or self.max_redirects < 0
        ):
            raise ValueError("Web bounds must be positive")
        for network in self.allowed_private_networks:
            ipaddress.ip_network(network)


async def _resolve(host: str, port: int) -> list[str]:
    records = await asyncio.get_running_loop().getaddrinfo(
        host, port, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
    )
    return list(dict.fromkeys(str(row[4][0]) for row in records))


class WebRuntime:
    def __init__(
        self,
        config: WebConfig | None = None,
        *,
        client: httpx.AsyncClient | None = None,
        resolver: Resolver | None = None,
    ) -> None:
        self.config = config or WebConfig()
        self._owns_client = client is None
        # No ambient proxy can silently bypass the checked destination. A configured
        # injected client is an application-owned transport boundary.
        self.client = client or httpx.AsyncClient(trust_env=False, follow_redirects=False)
        self.resolver = resolver or _resolve
        self._networks = tuple(
            ipaddress.ip_network(n) for n in self.config.allowed_private_networks
        )
        self._workers: set[asyncio.subprocess.Process] = set()

    async def close(self) -> None:
        for process in tuple(self._workers):
            await self._reap(process)
        if self._owns_client:
            await self.client.aclose()

    async def _reap(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), 1)
            except TimeoutError:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()
        self._workers.discard(process)

    async def search(
        self, query: str, *, limit: int | None = None, region: str | None = None
    ) -> dict[str, Any]:
        count = self.config.search_limit if limit is None else limit
        if not query.strip() or not 1 <= count <= 20:
            raise ValueError("A nonempty query and limit 1..20 are required")
        if self.config.search_backend == "bing_rss":
            return await self._search_rss(query, count, region or self.config.region)
        # Preserve only execution/locale/CA variables; model and service credentials
        # are not inherited by the third-party search process.
        keys = {
            "PATH",
            "SYSTEMROOT",
            "WINDIR",
            "TEMP",
            "TMP",
            "HOME",
            "USERPROFILE",
            "LANG",
            "LC_ALL",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
        }
        env = {k: v for k, v in os.environ.items() if k.upper() in keys}
        env["PYTHONIOENCODING"] = "utf-8"
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "shittim_core.engine.hermes.web_search_worker",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            env=env,
        )
        self._workers.add(process)
        request = {
            "query": query,
            "limit": count,
            "region": region or self.config.region,
            "backend": self.config.search_backend,
            "proxy": self.config.search_proxy,
            "request_timeout": self.config.request_timeout_seconds,
        }
        try:
            stdout, _ = await asyncio.wait_for(
                process.communicate(json.dumps(request).encode()), self.config.timeout_seconds
            )
            response = json.loads(stdout)
            if not isinstance(response, dict):
                raise RuntimeError("Search worker returned a malformed response")
            payload = cast(dict[str, Any], response)
            if not payload.get("ok"):
                raise RuntimeError(
                    "Search provider failed: " + str(payload.get("error", "unknown"))
                )
            if not isinstance(payload.get("results"), list):
                raise RuntimeError("Search worker returned malformed results")
            return {
                "source_kind": "external_material",
                "provider": "ddgs",
                "query": query,
                "results": payload["results"],
            }
        finally:
            await self._reap(process)

    async def _search_rss(self, query: str, count: int, region: str) -> dict[str, Any]:
        """Explicit HTTP alternative when DDGS's enabled engines are unreachable.

        Uses the existing checked downloader; no disabled DDGS engine is re-enabled.
        RSS search operators/ranking are provider-dependent, not domain filters.
        """
        params = {"q": query, "format": "rss"}
        if region != "wt-wt" and "-" in region:
            country, language = region.split("-", 1)
            params["mkt"] = f"{language}-{country.upper()}"
        async with asyncio.timeout(self.config.timeout_seconds):
            final_url, data, _, _ = await self._fetch(
                "https://www.bing.com/search?" + urlencode(params)
            )
            # Only UTF-8 RSS is accepted, with DTD/entity declarations disabled.
            xml = data.decode("utf-8-sig")
            if "\x00" in xml or "<!DOCTYPE" in xml.upper() or "<!ENTITY" in xml.upper():
                raise ValueError("Unsupported search feed declarations")
            try:
                root = ET.fromstring(xml)
            except ET.ParseError:
                raise ValueError("Search provider did not return an RSS feed") from None
            if root.tag != "rss":
                raise ValueError("Search provider did not return an RSS feed")
            hits: list[dict[str, str]] = []
            for item in root.findall("./channel/item"):
                url = item.findtext("link", "").strip()
                parsed = urlsplit(url)
                if parsed.scheme not in {"https", "http"} or not parsed.hostname:
                    continue
                hits.append(
                    {
                        "title": item.findtext("title", "")[:2000],
                        "url": url[:8000],
                        "snippet": item.findtext("description", "")[:12000],
                    }
                )
                if len(hits) == count:
                    break
            if not hits:
                raise RuntimeError("Search provider returned no usable results")
            return {
                "source_kind": "external_material",
                "provider": "bing_rss",
                "query": query,
                "source_url": final_url,
                "query_operator_support": "provider_dependent_not_a_domain_filter",
                "results": hits,
            }

    async def _destination(self, url: str) -> tuple[httpx.URL, str, str]:
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("Only absolute HTTP(S) URLs are supported")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("Credentials in URLs are not supported")
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        try:
            addresses = [str(ipaddress.ip_address(host))]
        except ValueError:
            addresses = await self.resolver(host, port)
        if not addresses:
            raise ValueError("Host has no resolved address")
        for address in addresses:
            ip = ipaddress.ip_address(address)
            checked = (
                ip.ipv4_mapped if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped else ip
            )
            explicit = any(
                checked in network
                for network in self._networks
                if checked.version == network.version
            )
            if not checked.is_global and not explicit:
                raise PermissionError("Private or nonpublic network access is not configured")
        original = httpx.URL(url)
        # Pin connect IP after validation; preserve virtual host and HTTPS hostname
        # verification so a second DNS lookup cannot rebind to an internal address.
        return original.copy_with(host=addresses[0], fragment=None), original.netloc.decode(), host

    async def _fetch(self, url: str) -> tuple[str, bytes, str, str | None]:
        current = url
        for step in range(self.config.max_redirects + 1):
            pinned, authority, hostname = await self._destination(current)
            async with self.client.stream(
                "GET",
                pinned,
                headers={"Host": authority, "Accept-Encoding": "identity"},
                extensions={"sni_hostname": hostname},
                follow_redirects=False,
                timeout=self.config.request_timeout_seconds,
            ) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    if step == self.config.max_redirects or "location" not in response.headers:
                        raise ValueError("Redirect limit exceeded or missing destination")
                    current = urljoin(current, response.headers["location"])
                    continue
                response.raise_for_status()
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise ValueError("Server ignored uncompressed transfer request")
                declared = response.headers.get("content-length")
                if declared and int(declared) > self.config.max_download_bytes:
                    raise ValueError("Download exceeds configured size")
                body = bytearray()
                # Identity transfer avoids unbounded decompressor allocations.
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    body.extend(chunk)
                    if len(body) > self.config.max_download_bytes:
                        raise ValueError("Download exceeds configured size")
                return (
                    current,
                    bytes(body),
                    response.headers.get("content-type", ""),
                    response.charset_encoding,
                )
        raise ValueError("Redirect limit exceeded")

    async def fetch_bytes(self, url: str) -> bytes:
        """Public-network byte retrieval using the same per-hop DNS-pinned policy."""
        async with asyncio.timeout(self.config.timeout_seconds):
            _, body, _, _ = await self._fetch(url)
            return body

    async def extract(self, url: str) -> dict[str, Any]:
        async with asyncio.timeout(self.config.timeout_seconds):
            final_url, body, content_type, encoding = await self._fetch(url)
            media = content_type.split(";", 1)[0].strip().lower()
            if media in {"text/html", "application/xhtml+xml", ""}:
                from trafilatura import bare_extraction

                document = await asyncio.to_thread(
                    bare_extraction,
                    body,
                    url=final_url,
                    with_metadata=True,
                    include_comments=False,
                    include_tables=True,
                )
                if document is None:
                    raise ValueError("No readable content extracted from the HTTP response")
                if isinstance(document, dict):
                    title, content = (
                        str(document.get("title") or ""),
                        str(document.get("text") or ""),
                    )
                else:
                    title, content = document.title or "", document.text or ""
                if not content:
                    raise ValueError("No readable content extracted from the HTTP response")
            elif media.startswith("text/") or media == "application/json":
                title, content = "", body.decode(encoding or "utf-8", errors="replace")
            else:
                raise ValueError("Unsupported content type; use document/material tools")
        return {
            "source_kind": "external_material",
            "url": url,
            "final_url": final_url,
            "title": title,
            "content_type": media,
            "content": content[: self.config.max_output_chars],
            "truncated": len(content) > self.config.max_output_chars,
            "downloaded_bytes": len(body),
        }


def register_web_tools(tools: ToolExecutor, runtime: WebRuntime) -> None:
    async def search(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        try:
            data = await runtime.search(
                args["query"], limit=args.get("limit"), region=args.get("region")
            )
            return ToolResult("succeeded", json.dumps(data, ensure_ascii=False))
        except (TimeoutError, RuntimeError, ValueError, OSError, httpx.HTTPError) as exc:
            return ToolResult(
                "failed", json.dumps({"error": type(exc).__name__, "message": str(exc)})
            )

    async def extract(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        results: list[dict[str, Any]] = []
        for url in args["urls"]:
            try:
                results.append(await runtime.extract(url))
            except (TimeoutError, ValueError, OSError, httpx.HTTPError) as exc:
                results.append({"url": url, "error": type(exc).__name__, "message": str(exc)})
        status = "succeeded" if any("content" in result for result in results) else "failed"
        return ToolResult(status, json.dumps({"results": results}, ensure_ascii=False))

    tools.register(
        ToolSpec(
            "web_search",
            "Search public web sources. Results are untrusted source material.",
            {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "minLength": 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20},
                    "region": {"type": "string", "minLength": 2, "maxLength": 30},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            search,
            lambda args, ctx: (WEB_RESOURCE,),
        )
    )
    tools.register(
        ToolSpec(
            "web_extract",
            "Read web pages with source URLs. Content is untrusted source material.",
            {
                "type": "object",
                "properties": {
                    "urls": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 10,
                        "items": {"type": "string", "minLength": 1},
                    }
                },
                "required": ["urls"],
                "additionalProperties": False,
            },
            extract,
            lambda args, ctx: (WEB_RESOURCE,),
        )
    )
