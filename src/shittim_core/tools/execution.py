"""Executable tools, joined to the existing effect/history and material delivery path."""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import subprocess
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from shittim_core.domain.types import ExecutionContext, ToolResult
from shittim_core.engine.output import read_utf8_page
from shittim_core.engine.processes import ExecutionRuntime
from shittim_core.materials.store import MaterialStore
from shittim_core.tools.executor import Handler, ResourceResolver, ToolExecutor, ToolSpec

EXECUTION_TOOLS = frozenset(
    {
        "terminal",
        "process_manage",
        "read_file",
        "write_file",
        "patch",
        "search_files",
        "import_material",
        "export_file",
    }
)
MATERIAL_TOOLS = frozenset(
    {
        "read_material",
        "create_material",
        "send_material",
        "import_material",
        "export_file",
        "vision_analyze",
    }
)
HOST_RESOURCE = "execution:local_host"


def _result(data: dict[str, Any], *, failed: bool = False) -> ToolResult:
    return ToolResult("failed" if failed else "succeeded", json.dumps(data, ensure_ascii=False))


def register_execution_tools(
    tools: ToolExecutor, runtime: ExecutionRuntime, material_root: Path, *, max_material_bytes: int
) -> None:
    async def terminal(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        try:
            process_id = await runtime.start(
                args["command"], ctx, cwd=args.get("cwd", "."), timeout=args.get("timeout_seconds")
            )
        except (ValueError, PermissionError, OSError) as exc:
            return ToolResult("not_executed", str(exc))
        result = await runtime.manage(
            process_id, ctx.run.context_id, "wait", wait_seconds=args.get("yield_seconds", 1.0)
        )
        return _result(
            result,
            failed=result["status"] in {"failed", "terminated"}
            or result["exit_code"] not in (None, 0),
        )

    async def process(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        action = args["action"]
        if action == "list":
            return _result(
                {
                    "processes": [
                        runtime.record(r["process_id"], ctx.run.context_id)
                        for r in runtime.store.list_processes(ctx.run.context_id)
                    ]
                }
            )
        if not args.get("process_id"):
            return ToolResult("not_executed", "process_id is required for this action")
        try:
            if action == "await_completion":
                if runtime.await_completion(args["process_id"], ctx):
                    return ToolResult(
                        "needs_input",
                        json.dumps(
                            {
                                "waiting_for_process": args["process_id"],
                                "resume_on_completion": True,
                            }
                        ),
                    )
                return _result(runtime.record(args["process_id"], ctx.run.context_id))
            if action == "write":
                runtime.assert_mutation_ready(ctx.run.context_id)
            data = await runtime.manage(
                args["process_id"],
                ctx.run.context_id,
                action,
                wait_seconds=args.get("wait_seconds", 1),
                data=args.get("data", ""),
                offset=args.get("offset", 0),
            )
            return _result(data, failed=bool(data.get("error")))
        except (ValueError, PermissionError, FileNotFoundError) as exc:
            return ToolResult("not_executed", str(exc))

    async def read(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        files = runtime.workspace(ctx.run.context_id)
        value = await asyncio.to_thread(files.read_file_raw, args["path"])
        if value.error:
            return _result({"error": value.error}, failed=True)
        lines = value.content.splitlines(keepends=True)
        offset, limit = args.get("offset", 1), args.get("limit", 200)
        selected = lines[offset - 1 : offset - 1 + limit]
        text = "".join(f"{i}: {line}" for i, line in enumerate(selected, start=offset))
        # Byte/character pagination in addition to lines retains access to a very long line.
        char_offset = args.get("character_offset", 0)
        page = text[char_offset : char_offset + 32000]
        return _result(
            {
                "path": args["path"],
                "content": page,
                "total_lines": len(lines),
                "sha256": asdict(value)["_content_sha256"],
                "byte_count": value.file_size,
                "next_offset": offset
                if char_offset + len(page) < len(text)
                else offset + len(selected),
                "next_character_offset": char_offset + len(page)
                if char_offset + len(page) < len(text)
                else 0,
                "truncated": offset - 1 + len(selected) < len(lines)
                or char_offset + len(page) < len(text),
            }
        )

    async def write(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        try:
            runtime.assert_mutation_ready(ctx.run.context_id)
            files = runtime.workspace(ctx.run.context_id)
            # Explicit hash allows callers to prevent stale full-file overwrites.
            if args.get("expected_sha256"):
                try:
                    prior = await asyncio.to_thread(files.bytes, args["path"])
                except FileNotFoundError:
                    # This read is before write_file: a missing optimistic-check
                    # source proves no mutation was attempted. Do not catch the
                    # same error around the later write/result observation.
                    return ToolResult(
                        "not_executed", "Expected file does not exist; no write attempted"
                    )
                if hashlib.sha256(prior).hexdigest() != args["expected_sha256"]:
                    return ToolResult("not_executed", "File changed since it was read")
            value = await asyncio.to_thread(files.write_file, args["path"], args["content"])
            return _result(
                cast(dict[str, Any], cast(Any, value).to_dict()), failed=bool(value.error)
            )
        except (ValueError, PermissionError) as exc:
            return ToolResult("not_executed", str(exc))

    async def patch(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        try:
            runtime.assert_mutation_ready(ctx.run.context_id)
            result = await asyncio.to_thread(
                runtime.workspace(ctx.run.context_id).patch, args["patch"]
            )
            return _result(result, failed=not result["success"])
        except (ValueError, PermissionError) as exc:
            return ToolResult("not_executed", str(exc))

    def search_sync(args: dict[str, Any], ctx: ExecutionContext) -> dict[str, Any]:
        executable = shutil.which("rg")
        if not executable:
            return {"error": "ripgrep is not installed; terminal and file tools remain available"}
        root = runtime.workspace(ctx.run.context_id).path(args.get("path", "."))
        argv = [executable, "--color", "never"]
        if args.get("pattern") is None:
            argv += ["--files"]
        else:
            argv += ["--line-number", "--no-heading", "--max-count", "100", "-e", args["pattern"]]
        if args.get("glob"):
            argv += ["--glob", args["glob"]]
        argv += [str(root)]
        # stdout goes to disk, so a large repository cannot allocate unbounded capture RAM.
        with tempfile.TemporaryFile() as output:
            result = subprocess.run(
                argv,
                stdout=output,
                stderr=subprocess.PIPE,
                timeout=30,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            length = output.tell()
            text, next_offset = read_utf8_page(output, args.get("offset", 0))
        return {
            "output": text,
            "exit_code": result.returncode,
            "error": result.stderr.decode("utf-8", errors="replace")
            if result.returncode > 1
            else "",
            "next_offset": next_offset,
            "truncated": next_offset < length,
        }

    async def search(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        try:
            data = await asyncio.to_thread(search_sync, args, ctx)
            return _result(data, failed=bool(data.get("error")))
        except (ValueError, PermissionError, subprocess.TimeoutExpired) as exc:
            return ToolResult("not_executed", str(exc))

    async def import_material(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        try:
            runtime.assert_mutation_ready(ctx.run.context_id)
            files = runtime.workspace(ctx.run.context_id)
            path = files.path(args["path"])
            if path.exists():
                return ToolResult("not_executed", "Working file exists; choose another destination")
        except (ValueError, PermissionError) as exc:
            return ToolResult("not_executed", str(exc))
        materials = MaterialStore(
            material_root, runtime.store, ctx.run.context_id, max_bytes=max_material_bytes
        )
        item = await materials.read(
            args["artifact_id"], ctx.run.access, version=args.get("version")
        )
        if item.status != "read" or item.data is None or item.ref is None:
            return ToolResult("not_executed", "Material is unavailable or outside this task")
        path.parent.mkdir(parents=True, exist_ok=True)

        # Never overwrite an existing working copy on a repeated import.
        def save() -> None:
            with path.open("xb") as stream:
                stream.write(item.data or b"")

        await asyncio.to_thread(save)
        return _result({"path": args["path"], "source": asdict(item.ref), "copied": True})

    async def export_file(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        files = runtime.workspace(ctx.run.context_id)
        try:
            data = await asyncio.to_thread(files.bytes, args["path"])
        except (OSError, ValueError) as exc:
            return ToolResult("not_executed", str(exc))
        lineage = {
            "kind": "execution_artifact",
            "run_id": ctx.run.run_id,
            "input_seq": ctx.input_seq,
            "workspace_file": args["path"],
            "audience": ctx.run.access.audience,
            "input_ids": [
                r["input_id"]
                for r in runtime.store.list_events(ctx.run.context_id)
                if r["seq"] <= ctx.input_seq and r["audience"] == ctx.run.access.audience
            ],
        }
        materials = MaterialStore(
            material_root, runtime.store, ctx.run.context_id, max_bytes=max_material_bytes
        )
        ref = await materials.add_bytes(
            data,
            args.get("name") or Path(args["path"]).name,
            ctx.run.access,
            source_ref=json.dumps(lineage, ensure_ascii=False),
        )
        return ToolResult(
            "succeeded",
            json.dumps(asdict(ref), ensure_ascii=False),
            artifact_refs=(ref.artifact_id,),
        )

    string = {"type": "string"}
    path = {"type": "string", "minLength": 1}
    positive = {"type": "integer", "minimum": 1}
    nonnegative = {"type": "integer", "minimum": 0}
    definitions: list[tuple[str, str, Handler, dict[str, Any], list[str], bool]] = [
        (
            "terminal",
            "在当前任务工作目录执行终端命令。返回进程ID；running不是完成，使用process_manage等候、查看输出或停止。",
            terminal,
            {
                "command": path,
                "cwd": string,
                "yield_seconds": {"type": "number", "minimum": 0, "maximum": 30},
                "timeout_seconds": {"type": "number", "exclusiveMinimum": 0, "maximum": 3600},
            },
            ["command"],
            True,
        ),
        (
            "process_manage",
            "管理本任务进程：list/status/wait/output/write/stop。长任务可await_completion让出执行，完成后自动接续；不需用户再发消息。output用字节offset分页。",
            process,
            {
                "action": {
                    "enum": [
                        "list",
                        "status",
                        "wait",
                        "output",
                        "write",
                        "stop",
                        "await_completion",
                    ]
                },
                "process_id": string,
                "wait_seconds": {"type": "number", "minimum": 0, "maximum": 30},
                "data": string,
                "offset": nonnegative,
            },
            ["action"],
            False,
        ),
        (
            "read_file",
            "读取任务工作目录中的UTF-8文本，保留行号、哈希和分页位置。二进制文件应使用对应工具或终端解析。",
            read,
            {
                "path": path,
                "offset": positive,
                "limit": {"type": "integer", "minimum": 1, "maximum": 2000},
                "character_offset": nonnegative,
            },
            ["path"],
            False,
        ),
        (
            "write_file",
            "写入完整UTF-8文件并核验哈希。修改已有文件可提供expected_sha256防止过期覆盖；局部修改优先patch。",
            write,
            {"path": path, "content": string, "expected_sha256": string},
            ["path", "content"],
            True,
        ),
        (
            "patch",
            "应用*** Begin Patch / Add File / Update File / Delete File / Move File格式补丁。先校验所有操作，再写入并返回差异。",
            patch,
            {"patch": path},
            ["patch"],
            True,
        ),
        (
            "search_files",
            "使用ripgrep在工作目录搜索文本或文件。省略pattern列文件；可提供glob和输出字节offset。",
            search,
            {"path": string, "pattern": string, "glob": string, "offset": nonnegative},
            [],
            False,
        ),
        (
            "import_material",
            "把已取得且当前任务可见的附件复制到工作目录以便处理；保留原件，不覆盖已有工作文件。",
            import_material,
            {"artifact_id": path, "version": positive, "path": path},
            ["artifact_id", "path"],
            True,
        ),
        (
            "export_file",
            "把任务工作目录的实际文件登记成不可变成果。返回artifact_id/version，可交给send_material发送；文件格式保持原样。",
            export_file,
            {"path": path, "name": string},
            ["path"],
            True,
        ),
    ]

    def process_resources(args: dict[str, Any], ctx: ExecutionContext) -> tuple[str, ...]:
        return (runtime.resource_key(ctx.run.context_id),) if args.get("action") == "write" else ()

    for name, description, handler, properties, required, mutation in definitions:
        resources: ResourceResolver = (
            (lambda _args, ctx: (runtime.resource_key(ctx.run.context_id),))
            if mutation
            else (lambda _args, _ctx: ())
        )
        if name == "process_manage":
            resources = process_resources
        tools.register(
            ToolSpec(
                name,
                description,
                {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                    "additionalProperties": False,
                },
                handler,
                resources,
            )
        )
