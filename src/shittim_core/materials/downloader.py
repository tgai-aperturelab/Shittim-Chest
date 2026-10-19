"""Explicit HTTP material retrieval. No redirect, implicit client, or automatic retry."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit

import httpx

from shittim_core.domain.types import AccessContext
from shittim_core.materials.store import ArtifactRef, MaterialStore


@dataclass(frozen=True)
class DownloadResult:
    status: Literal["stored", "unavailable"]
    ref: ArtifactRef | None = None
    reason: str = ""


async def _fetch_bytes(url: str, client: httpx.AsyncClient, limit: int) -> bytes:
    async with client.stream(
        "GET", url, follow_redirects=False, timeout=30.0, headers={"Accept-Encoding": "identity"}
    ) as response:
        if response.status_code != 200:
            raise ValueError("http_not_success")
        if response.headers.get("Content-Encoding", "identity").lower() not in {"", "identity"}:
            raise ValueError("compressed_transfer_not_supported")
        length = response.headers.get("Content-Length")
        if length is not None and (int(length) < 0 or int(length) > limit):
            raise ValueError("material_too_large")
        chunks: list[bytes] = []
        count = 0
        async for chunk in response.aiter_bytes(chunk_size=min(65536, limit + 1)):
            count += len(chunk)
            if count > limit:
                raise ValueError("material_too_large")
            chunks.append(chunk)
        return b"".join(chunks)


async def download_material(
    url: str,
    *,
    client: httpx.AsyncClient,
    allowed_hosts: frozenset[str],
    materials: MaterialStore,
    access: AccessContext,
    name: str,
    source_ref: str | None = None,
    max_bytes: int | None = None,
) -> DownloadResult:
    """A trusted caller supplies an already checked URL and exact host allowlist.

    This is not DNS-rebinding protection. Production must trust its allowlisted source
    and resolver; arbitrary model-generated hosts/URLs must never populate the allowlist.
    """
    limit = min(materials.max_bytes, max_bytes if max_bytes is not None else materials.max_bytes)
    try:
        parts = urlsplit(url)
        if (
            parts.scheme not in {"http", "https"}
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.fragment
            or parts.hostname.lower() not in allowed_hosts
            or parts.port not in (None, 80, 443)
            or limit < 1
        ):
            return DownloadResult("unavailable", reason="url_not_allowed")
        # Refuse before network activity if this caller cannot create scoped materials.
        materials.authorize_write(access)
        # Bound the whole request, not only each socket read: a trickling peer cannot run forever.
        data = await asyncio.wait_for(_fetch_bytes(url, client, limit), timeout=30.0)
        ref = await materials.add_bytes(data, name, access, source_ref=source_ref)
        return DownloadResult("stored", ref)
    except Exception:
        return DownloadResult("unavailable", reason="material_download_failed")
