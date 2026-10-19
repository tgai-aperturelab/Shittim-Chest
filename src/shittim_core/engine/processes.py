"""Application-owned process lifetimes and durable spawn intents.

The local_host backend is a real host capability, not a filesystem sandbox. Workspaces
organize tasks; shell commands have the service account's operating-system permissions.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import shlex
import shutil
import signal
import subprocess
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from shittim_core.domain.types import ExecutionContext
from shittim_core.engine.bubblewrap import BubblewrapBackend
from shittim_core.engine.files import WorkspaceFiles
from shittim_core.engine.hermes.tool_output_truncate import truncate_head_tail
from shittim_core.engine.output import read_utf8_page
from shittim_core.engine.process_tree import ProcessTree
from shittim_core.state import StateStore


@dataclass
class LiveProcess:
    process: asyncio.subprocess.Process
    task: asyncio.Task[None]
    context_id: str
    tree: ProcessTree


class ExecutionRuntime:
    def __init__(
        self,
        root: Path,
        store: StateStore,
        *,
        timeout: float = 300.0,
        backend: Literal["local_host", "bubblewrap"] = "local_host",
        bwrap_path: str | Path | None = None,
        python_runtime: str | Path | None = None,
        network: Literal["isolated", "host"] = "isolated",
    ) -> None:
        self.root, self.store, self.timeout = root.resolve(), store, timeout
        if backend not in {"local_host", "bubblewrap"}:
            raise ValueError("Unknown execution backend")
        if network not in {"isolated", "host"}:
            raise ValueError("Unknown execution network mode")
        self.backend = backend
        self.network = network if backend == "bubblewrap" else "host"
        self.sandbox = (
            BubblewrapBackend(
                self.root, bwrap_path=bwrap_path, python_runtime=python_runtime, network=network
            )
            if backend == "bubblewrap"
            else None
        )
        self.python_executable = (
            self.sandbox.python_executable if self.sandbox is not None else sys.executable
        )
        self.root.mkdir(parents=True, exist_ok=True)
        self.live: dict[str, LiveProcess] = {}
        self.stop_requests: set[str] = set()
        self.notify: Callable[[str], Awaitable[None]] | None = None
        self.closed = False
        self.shell = shutil.which("pwsh") if os.name == "nt" else shutil.which("bash")
        if os.name == "nt" and not self.shell:
            self.shell = shutil.which("powershell")
        if not self.shell:
            raise ValueError("Execution backend requires PowerShell on Windows or bash on POSIX")
        # Old process PIDs cannot be trusted after a restart. Never replay or kill them blindly.
        self.recovered_unknown = self.store.recover_processes()

    def workspace(self, context_id: str) -> WorkspaceFiles:
        return WorkspaceFiles(
            self.root / hashlib.sha256(context_id.encode()).hexdigest(),
            virtual_root="/workspace" if self.sandbox is not None else None,
        )

    def resource_key(self, context_id: str) -> str:
        return (
            "execution:workspace:" + context_id
            if self.sandbox is not None and self.network == "isolated"
            else "execution:local_host"
        )

    def assert_mutation_ready(self, context_id: str | None = None) -> None:
        if self.closed:
            raise ValueError("Execution runtime is stopping")
        uncertain_process = any(
            row["status"] == "unknown"
            and (
                self.sandbox is None
                or self.network == "host"
                or context_id is None
                or row["details"].get("backend", "local_host") == "local_host"
                or row["details"].get("network", "isolated") == "host"
                or row["context_id"] == context_id
            )
            for row in self.store.list_processes()
        )
        uncertain_effect = any(
            effect["status"] == "unknown"
            and any(
                key == "execution:local_host"
                or (
                    key.startswith("execution:workspace:")
                    and (
                        self.sandbox is None
                        or self.network == "host"
                        or context_id is None
                        or key == self.resource_key(context_id)
                    )
                )
                for key in effect["resource_keys"]
            )
            for effect in self.store.list_effects()
        )
        if uncertain_process or uncertain_effect:
            raise ValueError(
                "A conflicting process has an unknown outcome; reconcile it before new mutations"
            )

    @staticmethod
    async def terminate(
        process: asyncio.subprocess.Process, tree: ProcessTree | None = None
    ) -> None:
        if tree is not None:
            # The shell can exit first; its descendants still belong to the job/group.
            tree.terminate()
            await asyncio.wait_for(process.wait(), 10)
            deadline = asyncio.get_running_loop().time() + 10
            while tree.alive():
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError("Process tree termination could not be confirmed")
                await asyncio.sleep(0.01)
            return
        if process.returncode is not None:
            return
        try:
            if os.name == "nt":
                killer = await asyncio.create_subprocess_exec(
                    "taskkill.exe",
                    "/PID",
                    str(process.pid),
                    "/T",
                    "/F",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )
                await killer.wait()
            else:
                os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await asyncio.wait_for(process.wait(), 10)

    async def start(
        self, command: str, ctx: ExecutionContext, *, cwd: str = ".", timeout: float | None = None
    ) -> str:
        self.assert_mutation_ready(ctx.run.context_id)
        workspace = self.workspace(ctx.run.context_id)
        directory = workspace.path(cwd)
        if not directory.is_dir():
            raise ValueError("Working directory does not exist")
        process_id = uuid4().hex
        logs = self.root / ".process-output"
        logs.mkdir(exist_ok=True)
        output = logs / (process_id + ".log")
        control_directory = (
            self.root / ".process-control" / process_id if self.sandbox is not None else logs
        )
        control_directory.mkdir(parents=True, exist_ok=True)
        gate = control_directory / (process_id + ".gate")
        gate_visible = (
            Path("/run/execution-control") / gate.name if self.sandbox is not None else gate
        )
        details: dict[str, Any] = {
            "command": command,
            "cwd": self.sandbox.virtual_cwd(workspace.root, directory)
            if self.sandbox is not None
            else str(directory),
            "output": str(output),
            "exit_code": None,
            "backend": self.backend,
            "network": self.network,
            "ownership": "bubblewrap_pid_namespace"
            if self.sandbox is not None
            else ("windows_job" if os.name == "nt" else "posix_process_group"),
            "ownership_limit": "Windows job covers CreateProcess descendants; external service/WMI launched work is not contained"
            if os.name == "nt"
            else (
                "PID namespace and parent-death protection; other task mounts hidden; "
                + (
                    "shared host network permits private-network access"
                    if self.network == "host"
                    else "network isolated"
                )
                if self.sandbox is not None
                else "Explicit setsid/setpgid can escape the process group"
            ),
            "exit_code_scope": "root_process",
        }
        self.store.create_process(process_id, ctx.run.run_id, details)
        # Deliberately no model/provider keys, SSH agent or arbitrary service env inheritance.
        keep = {
            "path",
            "systemroot",
            "windir",
            "comspec",
            "pathext",
            "temp",
            "tmp",
            "home",
            "userprofile",
            "lang",
            "lc_all",
            "systemdrive",
        }
        env = {k: v for k, v in os.environ.items() if k.lower() in keep}
        env["PYTHONIOENCODING"] = "utf-8"
        if self.sandbox is None:
            path_key = next((key for key in env if key.lower() == "path"), "PATH")
            env[path_key] = str(Path(sys.executable).parent) + os.pathsep + env.get(path_key, "")
        assert self.shell is not None
        # No user command runs until process-tree ownership and its durable record
        # are established. If the owner dies before assignment, the gated root exits
        # by itself instead of waiting forever outside the job.
        if os.name == "nt":
            quoted_gate = "'" + str(gate).replace("'", "''") + "'"
            guarded_command = (
                "$__executionGateWait = [System.Diagnostics.Stopwatch]::StartNew(); "
                f"while (-not [System.IO.File]::Exists({quoted_gate})) {{ "
                "if ($__executionGateWait.Elapsed.TotalSeconds -ge 30) { exit 125 }; "
                "Start-Sleep -Milliseconds 10 }; " + command
            )
            argv = [self.shell, "-NoProfile", "-NonInteractive", "-Command", guarded_command]
        else:
            quoted_gate = shlex.quote(str(gate_visible))
            guarded_command = (
                f"for ((i=0; i<3000; i++)); do [[ -f {quoted_gate} ]] && break; sleep 0.01; done; "
                f"[[ -f {quoted_gate} ]] || exit 125; " + command
            )
            argv = [self.shell, "--noprofile", "--norc", "-c", guarded_command]
            if self.sandbox is not None:
                argv = self.sandbox.argv(
                    workspace.root, directory, control_directory, guarded_command
                )
        options: dict[str, Any] = (
            {"creationflags": subprocess.CREATE_NO_WINDOW}
            if os.name == "nt"
            else {"start_new_session": True}
        )
        try:
            tree = ProcessTree()
        except OSError as exc:
            details["error"] = str(exc)
            self.store.update_process(process_id, "failed", details)
            raise
        process: asyncio.subprocess.Process | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=directory,
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                **options,
            )
            tree.assign(process.pid)
            details["pid"] = process.pid
            self.store.update_process(process_id, "running", details)
            # Spawn is awaited: a stop or newer input can arrive while the gated
            # shell starts. Recheck authority at the actual command-release boundary.
            self.store.assert_current(ctx.run.run_id, ctx.run.control_epoch, ctx.input_seq)
            gate.touch(exist_ok=False)
        except OSError as exc:
            try:
                if process is not None:
                    await self.terminate(process, tree if tree.alive() else None)
            finally:
                tree.close()
            details["error"] = str(exc)
            self.store.update_process(process_id, "failed", details)
            raise
        except BaseException:
            try:
                if process is not None:
                    await self.terminate(process, tree if tree.alive() else None)
            finally:
                tree.close()
            try:
                details["stop_reason"] = "command_gate_not_released"
                self.store.update_process(process_id, "failed", details)
            except Exception:
                # The original failure wins; a durable nonterminal intent remains
                # recoverable as unknown if the store itself is unavailable.
                pass
            raise
        assert process is not None
        worker = asyncio.create_task(
            self._supervise(
                process_id, process, tree, gate, output, details, ctx, timeout or self.timeout
            )
        )
        self.live[process_id] = LiveProcess(process, worker, ctx.run.context_id, tree)
        return process_id

    async def _supervise(
        self,
        process_id: str,
        process: asyncio.subprocess.Process,
        tree: ProcessTree,
        gate: Path,
        output: Path,
        details: dict[str, Any],
        ctx: ExecutionContext,
        timeout: float,
    ) -> None:
        assert process.stdout is not None
        reader = process.stdout

        async def drain() -> None:
            with output.open("xb") as stream:
                while data := await reader.read(65536):
                    stream.write(data)
                    stream.flush()

        draining = asyncio.create_task(drain())
        deadline = asyncio.get_running_loop().time() + timeout
        reason: str | None = None
        try:
            while process.returncode is None or tree.alive():
                context = self.store.get_context(ctx.run.context_id)
                if (
                    self.closed
                    or context["control_state"] != "active"
                    or context["control_epoch"] != ctx.run.control_epoch
                ):
                    reason = "control_stopped"
                elif asyncio.get_running_loop().time() >= deadline:
                    reason = "process_timeout"
                elif draining.done() and draining.exception() is not None:
                    reason = "output_capture_failed"
                if reason:
                    await self.terminate(process, tree)
                    break
                await asyncio.sleep(0.05)
            await process.wait()
            await asyncio.wait_for(asyncio.shield(draining), 5)
            persisted = next(
                row
                for row in self.store.list_processes(ctx.run.context_id)
                if row["process_id"] == process_id
            )
            details.update(persisted["details"])
            reason = reason or ("tool_stop_requested" if process_id in self.stop_requests else None)
            details.update(exit_code=process.returncode, stop_reason=reason)
            self.store.update_process(process_id, "terminated" if reason else "exited", details)
            await self.publish_completion(process_id, ctx.run.context_id)
        except BaseException as exc:
            try:
                await self.terminate(process, tree)
            except Exception:
                pass
            details["error"] = type(exc).__name__
            self.store.update_process(process_id, "unknown", details)
        finally:
            try:
                tree.close()
                gate.unlink(missing_ok=True)
                if self.sandbox is not None:
                    gate.parent.rmdir()
            finally:
                if not draining.done():
                    draining.cancel()
                await asyncio.gather(draining, return_exceptions=True)

    async def publish_completion(self, process_id: str, context_id: str) -> bool:
        row = next(
            row for row in self.store.list_processes(context_id) if row["process_id"] == process_id
        )
        details = row["details"]
        if row["status"] not in {"exited", "terminated", "failed"} or not details.get(
            "wait_source"
        ):
            return False
        self.store.append_result_event(
            context_id,
            details["wait_source"],
            "process-complete:" + process_id,
            {
                "kind": "process_completion",
                "process_id": process_id,
                "status": row["status"],
                "exit_code": details.get("exit_code"),
                "stop_reason": details.get("stop_reason"),
                "exit_code_scope": "root_process",
            },
            details["wait_audience"],
        )
        if self.notify is not None and not self.closed:
            await self.notify(context_id)
        return True

    async def recover_completions(self) -> None:
        # Repairs a crash after saving the process result but before publishing its event.
        # append_result_event uses a stable identity: replay never adds a second input.
        for row in self.store.list_processes():
            await self.publish_completion(row["process_id"], row["context_id"])

    def await_completion(self, process_id: str, ctx: ExecutionContext) -> bool:
        self.record(process_id, ctx.run.context_id)  # scope check before looking up process details
        row = next(
            row
            for row in self.store.list_processes(ctx.run.context_id)
            if row["process_id"] == process_id
        )
        if row["status"] not in {"starting", "running"}:
            return False
        events = [
            event
            for event in self.store.list_events(ctx.run.context_id)
            if event["seq"] <= ctx.input_seq and event["audience"] == ctx.run.access.audience
        ]
        if not events:
            raise ValueError("A process wait requires an authorized source input")
        details = row["details"]
        details.update(wait_source=events[-1]["input_id"], wait_audience=ctx.run.access.audience)
        self.store.update_process(process_id, row["status"], details)
        return True

    def record(self, process_id: str, context_id: str) -> dict[str, Any]:
        for row in self.store.list_processes(context_id):
            if row["process_id"] == process_id:
                details = row["details"]
                output = Path(details["output"])
                # The log path is generated by this runtime and never accepts model paths.
                content = ""
                if output.exists():
                    with output.open("rb") as stream:
                        size = output.stat().st_size
                        if size > 32000:
                            head = stream.read(12800)
                            stream.seek(max(0, size - 19200))
                            data = (
                                head
                                + b"\n[output omitted; use process_manage output pagination]\n"
                                + stream.read(19200)
                            )
                        else:
                            data = stream.read()
                    content = data.decode("utf-8", errors="replace")
                return {
                    "process_id": process_id,
                    "status": row["status"],
                    "exit_code": details.get("exit_code"),
                    "output": truncate_head_tail(content, 32000),
                    "stop_reason": details.get("stop_reason"),
                    "cwd": details["cwd"],
                    "backend": details.get("backend", "local_host"),
                    "network": details.get(
                        "network",
                        "host"
                        if details.get("backend", "local_host") == "local_host"
                        else "isolated",
                    ),
                    "ownership": details.get("ownership"),
                    "ownership_limit": details.get("ownership_limit"),
                    "exit_code_scope": details.get("exit_code_scope", "root_process"),
                }
        raise PermissionError("Process does not belong to this task")

    async def manage(
        self,
        process_id: str,
        context_id: str,
        action: str,
        *,
        wait_seconds: float = 0,
        data: str = "",
        offset: int = 0,
    ) -> dict[str, Any]:
        current = self.record(process_id, context_id)
        live = self.live.get(process_id)
        if action == "stop":
            if live and (live.process.returncode is None or live.tree.alive()):
                self.stop_requests.add(process_id)
                await self.terminate(live.process, live.tree)
                await asyncio.shield(live.task)
            elif current["status"] == "unknown":
                return {
                    **current,
                    "error": "Cannot terminate a recovered PID without identity proof",
                }
        elif action == "write":
            if not live or live.process.returncode is not None or live.process.stdin is None:
                raise ValueError("Process has no active standard input")
            live.process.stdin.write(data.encode("utf-8"))
            await live.process.stdin.drain()
        elif action == "wait" and live:
            await asyncio.wait({live.task}, timeout=wait_seconds)
        elif action == "output":
            row = next(
                r for r in self.store.list_processes(context_id) if r["process_id"] == process_id
            )
            path = Path(row["details"]["output"])
            with path.open("rb") as stream:
                stream.seek(offset)
                page, next_offset = read_utf8_page(stream, offset)
            return {
                **current,
                "output": page,
                "next_offset": next_offset,
                "byte_count": next_offset - offset,
            }
        return self.record(process_id, context_id)

    async def close(self) -> None:
        self.closed = True
        await asyncio.gather(*(p.task for p in self.live.values()), return_exceptions=True)
