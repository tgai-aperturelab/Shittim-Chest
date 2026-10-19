"""Account-bound OneBot transport replacing aiocqhttp's process-global result store."""

from __future__ import annotations

import asyncio
import hmac
import json
import math
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, cast
from uuid import uuid4

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response

from shittim_core.domain.types import InputEvent, JsonObject, ProtocolReceipt

from .codec import decode_event


@dataclass
class _Pending:
    socket: ServerConnection
    future: asyncio.Future[ProtocolReceipt]


class QQTransport:
    """One bot per listener; replacement never lends an old connection new authority."""

    def __init__(
        self,
        bot_id: str,
        token: str,
        on_event: Callable[[InputEvent], Awaitable[object]],
        *,
        timeout_seconds: float = 15.0,
    ) -> None:
        if not bot_id.isdigit() or not token:
            raise ValueError("Explicit bot ID and nonempty token required")
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("Timeout must be finite and positive")
        self.bot_id = bot_id
        self._token = token
        self._on_event = on_event
        self._timeout = timeout_seconds
        self._server: Server | None = None
        self._socket: ServerConnection | None = None
        self._pending: dict[str, _Pending] = {}
        self._events: set[asyncio.Task[object]] = set()
        self._attempts: set[str] = set()
        self.intake_failures = 0
        self.ready = asyncio.Event()

    def _authorize(self, connection: ServerConnection, request: Request) -> Response | None:
        try:
            auth = request.headers.get("Authorization", "")
            account = request.headers.get("X-Self-ID", "")
            # NapCat's native reverse client sends "Universal"; test/other clients
            # may use lowercase. This is a role token, not an authentication secret.
            role = request.headers.get("X-Client-Role", "universal").casefold()
        except Exception:
            return connection.respond(HTTPStatus.UNAUTHORIZED, "Unauthorized\n")
        if (
            request.path != "/onebot"
            or account != self.bot_id
            or role != "universal"
            or not hmac.compare_digest(auth.encode(), f"Bearer {self._token}".encode())
        ):
            return connection.respond(HTTPStatus.UNAUTHORIZED, "Unauthorized\n")
        return None

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> int:
        """Start an explicitly requested listener and return its bound port."""
        if self._server is not None:
            raise RuntimeError("Transport already started")
        self._server = await serve(
            self._handle, host, port, process_request=self._authorize, max_size=2 * 1024 * 1024
        )
        return int(self._server.sockets[0].getsockname()[1])

    def _fail_pending(self, socket: ServerConnection) -> None:
        for key, pending in list(self._pending.items()):
            if pending.socket is socket:
                self._pending.pop(key, None)
                if not pending.future.done():
                    pending.future.set_result(ProtocolReceipt("unknown", error="Connection lost"))

    def _event_done(self, task: asyncio.Task[object]) -> None:
        self._events.discard(task)
        # Retrieve failures so no task exception leaks user message content to stderr.
        if not task.cancelled():
            if task.exception() is not None:
                self.intake_failures += 1

    async def _accept_event(self, event: InputEvent) -> object:
        return await self._on_event(event)

    async def _handle(self, socket: ServerConnection) -> None:
        previous = self._socket
        self._socket = socket
        self.ready.set()
        if previous is not None:
            self._fail_pending(previous)
            await previous.close(code=1000, reason="Connection replaced")
        try:
            async for raw in socket:
                if self._socket is not socket:
                    continue
                try:
                    decoded: object = json.loads(raw)
                except (ValueError, UnicodeDecodeError):
                    continue
                if not isinstance(decoded, dict):
                    continue
                payload = cast(JsonObject, decoded)
                if "echo" in payload:
                    echo = payload["echo"]
                    if not isinstance(echo, str):
                        continue
                    pending = self._pending.get(echo)
                    if pending is None or pending.socket is not socket or pending.future.done():
                        continue
                    data = payload.get("data")
                    retcode = payload.get("retcode")
                    if payload.get("status") == "ok" and type(retcode) is int and retcode == 0:
                        if not isinstance(data, dict):
                            pending.future.set_result(
                                ProtocolReceipt("unknown", error="Malformed API response")
                            )
                        else:
                            response_data = cast(JsonObject, data)
                            message_id = response_data.get("message_id")
                            valid_message_id = (
                                not isinstance(message_id, bool)
                                and isinstance(message_id, (str, int))
                                and str(message_id).isascii()
                                and str(message_id).removeprefix("-").isdigit()
                            )
                            pending.future.set_result(
                                ProtocolReceipt(
                                    "acknowledged",
                                    dict(response_data),
                                    str(message_id) if valid_message_id else None,
                                    retcode,
                                )
                            )
                    elif payload.get("status") == "failed" and type(retcode) is int:
                        pending.future.set_result(
                            ProtocolReceipt(
                                "failed", retcode=retcode, error="OneBot rejected action"
                            )
                        )
                    else:
                        pending.future.set_result(
                            ProtocolReceipt("unknown", error="Unrecognized API response")
                        )
                    continue
                event = decode_event(payload, self.bot_id)
                if event is not None:
                    task = asyncio.create_task(self._accept_event(event))
                    self._events.add(task)
                    task.add_done_callback(self._event_done)
        except ConnectionClosed:
            pass
        finally:
            if self._socket is socket:
                self._socket = None
                self.ready.clear()
            self._fail_pending(socket)

    async def call(
        self, action: str, params: Mapping[str, Any], attempt_id: str
    ) -> ProtocolReceipt:
        """Send once and distinguish acknowledged, rejected and uncertain outcomes.

        Args:
            action: OneBot action chosen by the authorized caller.
            params: Explicit account/destination parameters.
            attempt_id: Unique local attempt ID; transport does not provide remote idempotency.

        Returns:
            Actual protocol receipt. Timeout or disconnect after dispatch remains unknown.
        """
        if str(params.get("self_id", self.bot_id)) != self.bot_id:
            return ProtocolReceipt("failed", error="Wrong account; not dispatched")
        if not attempt_id or attempt_id in self._attempts:
            return ProtocolReceipt("failed", error="Duplicate/invalid attempt; not dispatched")
        socket = self._socket
        if socket is None:
            return ProtocolReceipt("failed", error="Not connected; not dispatched")
        envelope = {
            "action": action,
            "params": {**params, "self_id": self.bot_id},
            "echo": uuid4().hex,
        }
        try:
            encoded = json.dumps(envelope, ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError):
            return ProtocolReceipt("failed", error="Invalid parameters; not dispatched")
        self._attempts.add(attempt_id)
        echo = str(envelope["echo"])
        future: asyncio.Future[ProtocolReceipt] = asyncio.get_running_loop().create_future()
        self._pending[echo] = _Pending(socket, future)
        try:
            await socket.send(encoded)
            receipt = await asyncio.wait_for(future, timeout=self._timeout)
            if (
                action
                in {
                    "send_private_msg",
                    "send_group_msg",
                    "send_private_forward_msg",
                    "send_group_forward_msg",
                }
                and receipt.status == "acknowledged"
                and receipt.message_id is None
            ):
                return ProtocolReceipt(
                    "unknown",
                    receipt.data,
                    retcode=receipt.retcode,
                    error="Send response omitted message ID",
                )
            return receipt
        except (TimeoutError, ConnectionClosed, OSError):
            return ProtocolReceipt("unknown", error="No definitive receipt; do not auto-retry")
        except asyncio.CancelledError:
            return ProtocolReceipt("unknown", error="Cancelled after dispatch may have begun")
        finally:
            self._pending.pop(echo, None)

    async def close(self) -> None:
        """Close protocol connections, resolve pending calls and drain accepted callbacks."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        if self._events:
            await asyncio.gather(*self._events, return_exceptions=True)
