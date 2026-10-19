"""Scoped reading of explicitly registered material; no arbitrary path or URL tool."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shittim_core.delivery.service import Delivery
from shittim_core.domain.types import (
    ExecutionContext,
    OutboundIntent,
    RunHalted,
    StaleView,
    StateConflict,
    ToolResult,
)
from shittim_core.materials.documents import DOCUMENT_SUFFIXES, read_document
from shittim_core.materials.names import sanitize_name
from shittim_core.materials.store import MaterialStore
from shittim_core.state import StateStore
from shittim_core.tools.executor import ToolExecutor, ToolSpec

TEXT_SUFFIXES = frozenset(
    {
        "",
        ".txt",
        ".md",
        ".csv",
        ".tsv",
        ".json",
        ".yaml",
        ".yml",
        ".log",
        ".py",
        ".js",
        ".ts",
        ".html",
        ".css",
        ".xml",
        ".toml",
        ".ini",
        ".cfg",
        ".sql",
        ".rst",
    }
)


def register_material_tools(
    tools: ToolExecutor,
    store: StateStore,
    root: Path,
    *,
    max_bytes: int = 4 * 1024 * 1024,
    delivery: Delivery | None = None,
    enable_send: bool = False,
) -> None:
    """Register scoped handlers; registration alone never grants a run any capability."""

    async def read(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        materials = MaterialStore(root, store, ctx.run.context_id, max_bytes=max_bytes)
        result = await materials.read(
            str(args["artifact_id"]), ctx.run.access, version=args.get("version")
        )
        if result.status != "read" or result.ref is None or result.data is None:
            return ToolResult(
                "not_executed",
                json.dumps(
                    {"status": result.status, "reason": result.reason, "content_read": False}
                ),
            )
        ref, data = result.ref, result.data
        info: dict[str, Any] = {
            "artifact_id": ref.artifact_id,
            "version": ref.version,
            "sha256": ref.sha256,
            "name": ref.name,
            "byte_count": len(data),
            "content_read": True,
            "source_kind": "untrusted_material",
            "parsed_as_text": False,
        }
        suffix = Path(ref.name).suffix.lower()
        if suffix in DOCUMENT_SUFFIXES:
            parsed = await read_document(
                data,
                suffix,
                offset=args.get("offset", 0),
                limit=args.get("limit", 50),
                character_offset=args.get("character_offset", 0),
            )
            info.update(format=suffix.removeprefix("."), parsed_document=parsed)
            if not parsed.get("parsed"):
                info["reason"] = parsed.get("error", "document_parse_failed")
        elif suffix in TEXT_SUFFIXES:
            try:
                decoded = data.decode("utf-8-sig", errors="strict")
                if any(ord(char) < 32 and char not in "\n\r\t" for char in decoded):
                    raise ValueError("Binary control characters")
                info.update(
                    parsed_as_text=True,
                    encoding="utf-8",
                    text=decoded[
                        args.get("character_offset", 0) : args.get("character_offset", 0) + 16000
                    ],
                    truncated=len(decoded) > args.get("character_offset", 0) + 16000,
                    next_character_offset=min(
                        len(decoded), args.get("character_offset", 0) + 16000
                    ),
                    character_count=len(decoded),
                )
            except (UnicodeDecodeError, ValueError):
                info["reason"] = "binary_or_unsupported_encoding_no_parser_configured"
        else:
            info["reason"] = "document_format_not_parsed_no_parser_configured"
        return ToolResult(
            "succeeded", json.dumps(info, ensure_ascii=False), artifact_refs=(ref.artifact_id,)
        )

    tools.register(
        ToolSpec(
            "read_material",
            "分页读取当前任务材料的源内容：UTF-8文本、PDF可提取文字、Word段落和表格、Excel单元格值及保存的公式。返回解析状态、来源和next_offset/next_character_offset续读位置。",
            {
                "type": "object",
                "properties": {
                    "offset": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                    "character_offset": {"type": "integer", "minimum": 0},
                    "artifact_id": {"type": "string", "pattern": "^[A-Za-z0-9_-]{1,128}$"},
                    "version": {"type": "integer", "minimum": 1},
                },
                "required": ["artifact_id"],
                "additionalProperties": False,
            },
            read,
            lambda _args, ctx: ("materials:" + ctx.run.context_id,),
        )
    )

    async def create(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        name = str(args["name"])
        if name != sanitize_name(name) or "/" in name or "\\" in name or name in {".", ".."}:
            return ToolResult("not_executed", "A material name must be a filename, not a path")
        artifact_id = args.get("artifact_id")
        expected_version = args.get("expected_version")
        if (artifact_id is None) != (expected_version is None):
            return ToolResult(
                "not_executed", "Updating requires both artifact_id and expected_version"
            )
        try:
            data = str(args["content"]).encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            return ToolResult("not_executed", "Content is not valid UTF-8 text")
        if len(data) > max_bytes:
            return ToolResult("not_executed", "UTF-8 content exceeds the material byte limit")
        materials = MaterialStore(root, store, ctx.run.context_id, max_bytes=max_bytes)
        lineage: dict[str, Any] = {
            "kind": "derived_material",
            "run_id": ctx.run.run_id,
            "input_seq": ctx.input_seq,
            "audience": ctx.run.access.audience,
            "input_ids": [],
            "events": [],
            "artifact_sources": [],
        }
        source_ids: set[str] = set()
        for row in store.list_events(ctx.run.context_id):
            if (
                int(row["seq"]) > ctx.input_seq
                or row["audience"] != ctx.run.access.audience
                or row.get("handled_by_notice", False)
            ):
                continue
            source_ids.update(str(value) for value in row.get("source_ids", [row["input_id"]]))
            lineage["events"].append(
                {"event_id": row["event_id"], "seq": row["seq"], "kind": row["kind"]}
            )
        lineage["input_ids"] = sorted(source_ids)
        if artifact_id is not None:
            previous = await materials.read(
                str(artifact_id), ctx.run.access, version=expected_version
            )
            if previous.status != "read" or previous.ref is None:
                return ToolResult(
                    "not_executed", "Previous version is unavailable or outside this context"
                )
            lineage["previous_version"] = {
                "artifact_id": previous.ref.artifact_id,
                "version": previous.ref.version,
                "sha256": previous.ref.sha256,
            }
        seen_sources: set[tuple[str, int]] = set()
        for source in args.get("artifact_sources", []):
            source_key = (str(source["artifact_id"]), int(source["version"]))
            if source_key in seen_sources:
                continue
            seen_sources.add(source_key)
            checked = await materials.read(source_key[0], ctx.run.access, version=source_key[1])
            if checked.status != "read" or checked.ref is None:
                return ToolResult(
                    "not_executed", "A declared source is unavailable or outside this scope"
                )
            lineage["artifact_sources"].append(
                {
                    "artifact_id": checked.ref.artifact_id,
                    "version": checked.ref.version,
                    "sha256": checked.ref.sha256,
                }
            )
        try:
            # MaterialStore writes a complete exclusive file, then registers its immutable
            # version. A competing version registration discards only our unregistered file.
            store.assert_current(ctx.run.run_id, ctx.run.control_epoch, ctx.input_seq)
            created = await materials.add_bytes(
                data,
                name,
                ctx.run.access,
                artifact_id=artifact_id,
                expected_version=expected_version,
                source_ref=json.dumps(lineage, ensure_ascii=False, sort_keys=True),
            )
        except (PermissionError, StateConflict, ValueError, RunHalted, StaleView):
            return ToolResult("not_executed", "Material scope or expected version changed")
        ref = f"{created.artifact_id}@{created.version}"
        return ToolResult(
            "succeeded",
            json.dumps(
                {
                    "status": "created",
                    "artifact_ref": ref,
                    "artifact_id": created.artifact_id,
                    "version": created.version,
                    "name": created.name,
                    "sha256": created.sha256,
                    "byte_count": len(data),
                    "encoding": "utf-8",
                    "source_input_ids": lineage["input_ids"],
                },
                ensure_ascii=False,
            ),
            artifact_refs=(ref,),
        )

    tools.register(
        ToolSpec(
            "create_material",
            "在当前协作上下文创建UTF-8文本成果文件或生成新版本，保留来源和范围；不能指定目录。",
            {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "minLength": 1, "maxLength": 120},
                    "content": {"type": "string", "maxLength": max_bytes},
                    "artifact_id": {"type": "string", "pattern": "^[A-Za-z0-9_-]{1,128}$"},
                    "expected_version": {"type": "integer", "minimum": 1},
                    "artifact_sources": {
                        "type": "array",
                        "maxItems": 16,
                        "items": {
                            "type": "object",
                            "properties": {
                                "artifact_id": {
                                    "type": "string",
                                    "pattern": "^[A-Za-z0-9_-]{1,128}$",
                                },
                                "version": {"type": "integer", "minimum": 1},
                            },
                            "required": ["artifact_id", "version"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": ["name", "content"],
                "additionalProperties": False,
            },
            create,
            lambda _args, ctx: ("materials:" + ctx.run.context_id,),
        )
    )
    if not enable_send:
        return
    if delivery is None:
        raise ValueError("Material sending requires the application's sole delivery service")

    async def send(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        # Pin the exact readable version before creating the durable outbox intent.
        materials = MaterialStore(root, store, ctx.run.context_id, max_bytes=max_bytes)
        material = await materials.read(
            str(args["artifact_id"]), ctx.run.access, version=args.get("version")
        )
        if material.status != "read" or material.ref is None:
            return ToolResult("not_executed", "Artifact is unavailable or outside this context")
        reference = f"{material.ref.artifact_id}@{material.ref.version}"
        intent = OutboundIntent(
            ctx.run.address,
            "",
            purpose="message",
            artifact_refs=(reference,),
            presentation=args.get("presentation", "file"),
        )
        assert delivery is not None
        delivered = await delivery.submit(intent, ctx)
        if delivered.status == "acknowledged":
            return ToolResult(
                "succeeded",
                "QQ acknowledged the file send; this does not prove it was read",
                (delivered.delivery_id,),
                (reference,),
            )
        if delivered.status == "superseded":
            return ToolResult(
                "not_executed",
                "File send was superseded before dispatch",
                (delivered.delivery_id,),
                (reference,),
            )
        return ToolResult(
            "failed" if delivered.status == "failed" else "unknown",
            "File delivery is not confirmed; do not automatically resend",
            (delivered.delivery_id,),
            (reference,),
        )

    tools.register(
        ToolSpec(
            "send_material",
            "将当前任务已核验版本的成果发送到当前QQ会话；presentation选择file、image、record或video，默认文件。不能指定任意路径或其它收件人。",
            {
                "type": "object",
                "properties": {
                    "presentation": {"enum": ["file", "image", "record", "video"]},
                    "artifact_id": {"type": "string", "pattern": "^[A-Za-z0-9_-]{1,128}$"},
                    "version": {"type": "integer", "minimum": 1},
                },
                "required": ["artifact_id"],
                "additionalProperties": False,
            },
            send,
            lambda _args, ctx: ("materials:" + ctx.run.context_id, "qq:" + ctx.run.address.key),
        )
    )
