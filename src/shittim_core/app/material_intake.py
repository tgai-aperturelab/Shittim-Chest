"""Asynchronous enrichment of accepted QQ files; results are not synthetic human messages."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx

from shittim_core.domain.types import Acceptance, AccessContext, InputEvent, Segment
from shittim_core.materials.downloader import DownloadResult, download_material
from shittim_core.materials.store import MaterialStore
from shittim_core.qq.codec import ActionCaller, resolve_files
from shittim_core.state import StateStore


class MaterialIntake:
    def __init__(
        self,
        store: StateStore,
        root: Path,
        call: ActionCaller,
        notify: Callable[[str], Awaitable[None]],
        *,
        client: httpx.AsyncClient,
        allowed_hosts: frozenset[str],
        max_bytes: int,
        policy_check: Callable[[Mapping[str, Any]], bool],
        max_concurrent: int = 2,
        public_fetch: Callable[[str], Awaitable[bytes]] | None = None,
    ) -> None:
        self.store, self.root, self.call, self.notify = store, root, call, notify
        self.client, self.allowed_hosts, self.max_bytes = client, allowed_hosts, max_bytes
        self.policy_check = policy_check
        self.public_fetch = public_fetch
        self._capacity = asyncio.Semaphore(max_concurrent)
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self.closed = False
        self.failures: list[str] = []

    @staticmethod
    def result_id(context_id: str, input_id: str, index: int) -> str:
        return f"material:{context_id}:{input_id}:{index}"

    def _completed(self, context_id: str) -> set[str]:
        return {
            str(row["event_id"]).removeprefix("result:")
            for row in self.store.list_events(context_id)
            if row["kind"] == "material_result"
        }

    def submit(self, event: InputEvent, accepted: Acceptance) -> bool:
        """Schedule only an accepted source; persisted results and duplicate inputs aren't replayed."""
        if (
            self.closed
            or accepted.status != "associated"
            or not accepted.input_id
            or not accepted.context_id
            or event.text.strip() in {"停止", "/stop", "继续", "/resume", "关闭", "/close"}
        ):
            return False
        context_id, input_id = accepted.context_id, accepted.input_id
        context = self.store.get_context(context_id)
        access: AccessContext = context["access"]
        if (
            "read_material" not in access.allowed_tools
            or (not self.allowed_hosts and self.public_fetch is None)
            or not self.policy_check(context)
        ):
            return False
        if self.store.get_input(input_id) != event:
            raise ValueError("Enrichment input does not match the accepted record")
        if not any(
            row["kind"] == "input" and row["input_id"] == input_id
            for row in self.store.list_events(context_id)
        ):
            raise ValueError("Enrichment input is not associated with this context")
        done = self._completed(context_id)
        indices = [
            index
            for index, segment in enumerate(event.segments)
            if segment.type in {"file", "image", "record", "video"}
            and self.result_id(context_id, input_id, index) not in done
        ]
        if not indices:
            return False
        key = f"{context_id}:{input_id}"
        existing = self._tasks.get(key)
        if existing is not None and not existing.done():
            return False
        self._tasks[key] = asyncio.create_task(self._process(event, context_id, input_id, indices))
        self._tasks[key].add_done_callback(lambda task: self._record_failure(key, task))
        return True

    def _record_failure(self, key: str, task: asyncio.Task[None]) -> None:
        if self._tasks.get(key) is task:
            self._tasks.pop(key, None)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                self.failures.append(type(error).__name__)

    async def _record(
        self,
        event: InputEvent,
        context_id: str,
        input_id: str,
        index: int,
        payload: dict[str, Any],
    ) -> None:
        payload.update(
            {
                "source_input_id": input_id,
                "source_message_id": event.message_id,
                "provider_id": event.author_id,
                "segment_index": index,
            }
        )
        self.store.append_result_event(
            context_id,
            input_id,
            self.result_id(context_id, input_id, index),
            payload,
            event.address.key,
        )
        if not self.closed:
            await self.notify(context_id)

    async def _process(
        self, event: InputEvent, context_id: str, input_id: str, indices: list[int]
    ) -> None:
        try:
            async with self._capacity:
                context = self.store.get_context(context_id)
                access: AccessContext = context["access"]
                if self.closed or not self.policy_check(context):
                    for index in indices:
                        await self._record(
                            event,
                            context_id,
                            input_id,
                            index,
                            {"status": "unavailable", "reason": "current_policy_denied"},
                        )
                    return
                if event.address.key != access.audience:
                    for index in indices:
                        await self._record(
                            event,
                            context_id,
                            input_id,
                            index,
                            {"status": "unavailable", "reason": "source_scope_differs"},
                        )
                    return
                # Provider identity comes from the accepted QQ event, not model arguments.
                writer_access = replace(access, actor_id=event.author_id)
                materials = MaterialStore(
                    self.root, self.store, context_id, max_bytes=self.max_bytes
                )
                try:
                    materials.authorize_write(writer_access)
                except PermissionError:
                    for index in indices:
                        await self._record(
                            event,
                            context_id,
                            input_id,
                            index,
                            {"status": "unavailable", "reason": "material_scope_denied"},
                        )
                    return
                pending_event = replace(
                    event,
                    segments=tuple(
                        segment if index in indices else Segment("already_resolved", {})
                        for index, segment in enumerate(event.segments)
                    ),
                )
                located = await resolve_files(pending_event, self.call)
                for item in located:
                    payload: dict[str, Any] = {
                        "media_type": item.media_type,
                        "file_id": item.file_id,
                        "name": item.name[:300],
                        "status": "unavailable",
                        "content_read": False,
                    }
                    if self.closed:
                        payload["reason"] = "intake_interrupted"
                    elif not self.policy_check(self.store.get_context(context_id)):
                        payload["reason"] = "current_policy_denied"
                    elif item.status != "located" or item.url is None:
                        payload["reason"] = "file_location_unavailable"
                    else:
                        result = await self._download(
                            item.url, materials, writer_access, item.name, input_id
                        )
                        if result.status == "stored" and result.ref is not None:
                            payload.update(
                                status="stored",
                                artifact_id=result.ref.artifact_id,
                                name=result.ref.name,
                                version=result.ref.version,
                                sha256=result.ref.sha256,
                                content_read=False,
                            )
                        else:
                            payload["reason"] = result.reason
                    await self._record(event, context_id, input_id, item.index, payload)
        except asyncio.CancelledError:
            completed = self._completed(context_id)
            for index in indices:
                if self.result_id(context_id, input_id, index) not in completed:
                    await self._record(
                        event,
                        context_id,
                        input_id,
                        index,
                        {
                            "status": "unavailable",
                            "content_read": False,
                            "reason": "intake_interrupted",
                        },
                    )
            raise
        except Exception as error:
            self.failures.append(type(error).__name__)
            completed = self._completed(context_id)
            for index in indices:
                if self.result_id(context_id, input_id, index) not in completed:
                    await self._record(
                        event,
                        context_id,
                        input_id,
                        index,
                        {
                            "status": "unavailable",
                            "content_read": False,
                            "reason": "intake_processing_failed",
                        },
                    )

    async def _download(
        self, url: str, materials: MaterialStore, access: AccessContext, name: str, source_ref: str
    ) -> DownloadResult:
        if self.public_fetch is None:
            return await download_material(
                url,
                client=self.client,
                allowed_hosts=self.allowed_hosts,
                materials=materials,
                access=access,
                name=name,
                source_ref=source_ref,
                max_bytes=self.max_bytes,
            )
        # The caller supplied a locator from an accepted platform attachment, not a
        # model argument. Permission is the configured read_material capability;
        # public_fetch separately enforces IP/redirect/size/timeout policy.
        try:
            materials.authorize_write(access)
            data = await self.public_fetch(url)
            if self.closed or not self.policy_check(self.store.get_context(materials.context_id)):
                return DownloadResult("unavailable", reason="current_policy_denied")
            if len(data) > self.max_bytes:
                return DownloadResult("unavailable", reason="material_too_large")
            return DownloadResult(
                "stored", await materials.add_bytes(data, name, access, source_ref=source_ref)
            )
        except PermissionError:
            return DownloadResult("unavailable", reason="platform_file_network_denied")
        except Exception:
            return DownloadResult("unavailable", reason="platform_file_download_failed")

    def recover(self) -> int:
        """Reconstruct unfinished downloads from accepted inputs, never retry recorded failures."""
        scheduled = 0
        for context in self.store.list_contexts():
            if context["control_state"] == "closed" or not self.policy_check(context):
                continue
            for row in self.store.list_events(context["context_id"]):
                if row["kind"] != "input":
                    continue
                scheduled += self.submit(
                    row["event"], Acceptance("associated", row["input_id"], context["context_id"])
                )
        return scheduled

    async def wait_idle(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    async def close(self) -> None:
        self.closed = True
        for task in self._tasks.values():
            if not task.done():
                task.cancel()
        await self.wait_idle()
