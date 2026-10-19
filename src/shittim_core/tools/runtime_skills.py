"""Explicit runtime skill libraries and task-local authoring.

Skills are tool data, never authority. No Codex/global home discovery, shell
preprocessing, credential capture, hook loading or installation occurs on view.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import stat
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, cast

import yaml

from shittim_core.domain.types import ExecutionContext, ToolResult
from shittim_core.engine.hermes.skill_metadata import (
    EXCLUDED_SKILL_DIRS,
    SKILL_SUPPORT_DIRS,
    parse_frontmatter,
    required_environment_variables,
    skill_matches_platform,
)
from shittim_core.tools.executor import ToolExecutor, ToolSpec

RUNTIME_SKILL_TOOLS = frozenset({"skills_list", "skill_view", "skill_manage"})
_ID = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$")


class SkillCopyUncertain(RuntimeError):
    """A task-local import started but its complete publication was not confirmed."""


@dataclass(frozen=True)
class SkillRoot:
    """An application-selected, read-only library. ID is also its resource key."""

    id: str
    path: Path
    source: str = "operator_configured"

    def __post_init__(self) -> None:
        if not _ID.fullmatch(self.id) or self.id == "task":
            raise ValueError("Invalid or reserved skill root ID")

    @property
    def resource(self) -> str:
        return f"skills:root:{self.id}"


def task_skill_resource(context_id: str) -> str:
    return f"skills:task:{context_id}"


def _relative(value: str) -> str:
    if not value or "\\" in value or ":" in value or "\x00" in value:
        raise ValueError("Expected a safe relative POSIX path")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or PureWindowsPath(value).drive
        or any(p in {"", ".", ".."} for p in value.split("/"))
    ):
        raise ValueError("Expected a safe relative POSIX path")
    return path.as_posix()


def _under(root: Path, name: str) -> Path:
    root = root.absolute()
    candidate = root / _relative(name)
    # Reject links at every level, including the configured root and its ancestors.
    for part in (candidate, *candidate.parents):
        if part.is_symlink() or part.is_junction():
            raise PermissionError("Linked skill paths are not supported")
    if not candidate.resolve().is_relative_to(root.resolve()):
        raise PermissionError("Skill path escapes its root")
    return candidate


def _bytes(path: Path, limit: int) -> bytes:
    if not stat.S_ISREG(path.stat().st_mode):
        raise ValueError("Skill resource must be a regular file")
    with path.open("rb") as stream:
        value = stream.read(limit + 1)
    if len(value) > limit:
        raise ValueError("Skill file exceeds configured byte limit")
    return value


def _json(data: dict[str, Any]) -> ToolResult:
    return ToolResult("succeeded", json.dumps(data, ensure_ascii=False, default=str))


class RuntimeSkills:
    def __init__(
        self,
        roots: Sequence[SkillRoot],
        *,
        workspace_for: Callable[[str], Path] | None = None,
        max_file_bytes: int = 1024 * 1024,
        available_env: frozenset[str] = frozenset(),
        mutation_guard: Callable[[], None] | None = None,
        context_mutation_guard: Callable[[ExecutionContext], None] | None = None,
        workspace_display_root: str | Callable[[str], str] | None = None,
        max_package_files: int = 512,
        max_package_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if len({r.id for r in roots}) != len(roots):
            raise ValueError("Duplicate skill root IDs")
        if max_file_bytes < 1:
            raise ValueError("Skill byte limit must be positive")
        if max_package_files < 1 or max_package_bytes < 1:
            raise ValueError("Skill package bounds must be positive")
        self.roots = tuple(roots)
        self.workspace_for = workspace_for
        self.max_file_bytes = max_file_bytes
        self.available_env = available_env
        self.mutation_guard = mutation_guard
        self.context_mutation_guard = context_mutation_guard
        self.workspace_display_root = workspace_display_root
        self.max_package_files = max_package_files
        self.max_package_bytes = max_package_bytes

    def display_directory(self, root_id: str, directory: Path, ctx: ExecutionContext) -> str:
        if root_id == "task" and self.workspace_for and self.workspace_display_root is not None:
            display = (
                self.workspace_display_root(ctx.run.context_id)
                if callable(self.workspace_display_root)
                else self.workspace_display_root
            )
            relative = (
                directory.resolve()
                .relative_to(self.workspace_for(ctx.run.context_id).resolve())
                .as_posix()
            )
            return str(PurePosixPath(display) / relative)
        return str(directory.resolve())

    def _roots(self, ctx: ExecutionContext) -> list[tuple[str, Path, str]]:
        allowed = ctx.run.access.allowed_resources
        values = [(r.id, r.path, r.source) for r in self.roots if r.resource in allowed]
        if self.workspace_for and task_skill_resource(ctx.run.context_id) in allowed:
            values.append(
                (
                    "task",
                    self.workspace_for(ctx.run.context_id) / ".runtime-skills",
                    f"task:{ctx.run.context_id}",
                )
            )
        return values

    def _catalog(self, ctx: ExecutionContext) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        rows: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        for root_id, root, source in self._roots(ctx):
            try:
                _under(root, "SKILL.md")
                if not root.exists():
                    if root_id != "task":
                        errors.append({"root": root_id, "error": "Configured root does not exist"})
                    continue
                for folder, dirs, files in os.walk(root, followlinks=False):
                    here = Path(folder)
                    dirs[:] = sorted(
                        d
                        for d in dirs
                        if d not in EXCLUDED_SKILL_DIRS
                        and not (here / d).is_symlink()
                        and not (here / d).is_junction()
                    )
                    if "SKILL.md" not in files:
                        continue
                    # Package support directories are references, not standalone skills.
                    dirs[:] = [d for d in dirs if d not in SKILL_SUPPORT_DIRS]
                    relative = (here / "SKILL.md").relative_to(root).as_posix()
                    try:
                        path = _under(root, relative)
                        raw = _bytes(path, self.max_file_bytes)
                        meta, body = parse_frontmatter(raw.decode("utf-8-sig"))
                        name = meta.get("name", here.name)
                        if not isinstance(name, str) or not name.strip():
                            raise ValueError("Skill name must be non-empty text")
                        description = meta.get("description", "")
                        if not isinstance(description, str):
                            raise ValueError("Skill description must be text")
                        if not description:
                            description = next(
                                (
                                    s.strip()
                                    for s in body.splitlines()
                                    if s.strip() and not s.startswith("#")
                                ),
                                "",
                            )
                        key = here.relative_to(root).as_posix()
                        rows.append(
                            {
                                "name": name,
                                "id": f"{root_id}:{key}",
                                "root": root_id,
                                "relative_path": relative,
                                "description": description,
                                "category": key.split("/")[0] if "/" in key else None,
                                "source": source,
                                "sha256": hashlib.sha256(raw).hexdigest(),
                                "platform_supported": skill_matches_platform(meta),
                            }
                        )
                    except (OSError, ValueError, yaml.YAMLError) as exc:
                        errors.append({"root": root_id, "path": relative, "error": str(exc)})
            except (OSError, ValueError) as exc:
                errors.append({"root": root_id, "error": str(exc)})
        return sorted(rows, key=lambda s: s["id"]), errors

    def list(self, args: dict[str, Any], ctx: ExecutionContext) -> dict[str, Any]:
        rows, errors = self._catalog(ctx)
        category = args.get("category")
        if category is not None:
            rows = [r for r in rows if r["category"] == category]
        offset, limit = args.get("offset", 0), args.get("limit", 100)
        return {
            "skills": rows[offset : offset + limit],
            "total": len(rows),
            "next_offset": offset + limit if offset + limit < len(rows) else None,
            "errors": errors,
            "hint": "Use exact id with skill_view; duplicate names are ambiguous",
        }

    def resolve(self, name: str, ctx: ExecutionContext) -> tuple[dict[str, Any], Path, Path]:
        rows, _ = self._catalog(ctx)
        matches = [r for r in rows if r["id"] == name]
        if not matches:
            matches = [r for r in rows if r["name"] == name]
        if len(matches) != 1:
            raise ValueError(
                "Skill unavailable or ambiguous; use an authorized exact id from skills_list"
            )
        row = matches[0]
        root = next(p for ident, p, _ in self._roots(ctx) if ident == row["root"])
        path = _under(root, row["relative_path"])
        return row, root, path

    def view(self, args: dict[str, Any], ctx: ExecutionContext) -> dict[str, Any]:
        row, _, main = self.resolve(args["name"], ctx)
        original = _bytes(main, self.max_file_bytes)
        meta, _ = parse_frontmatter(original.decode("utf-8-sig"))
        path = main
        if args.get("file_path"):
            relative = _relative(args["file_path"])
            path = _under(main.parent, relative)
        data = _bytes(path, self.max_file_bytes)
        offset, limit = args.get("offset", 0), args.get("limit", 32000)
        try:
            content = data.decode("utf-8-sig")
            encoding = "utf-8"
        except UnicodeError:
            content = base64.b64encode(data).decode("ascii")
            encoding = "base64"
        linked: list[str] = []
        for directory, dirs, files in os.walk(main.parent, followlinks=False):
            current = Path(directory)
            dirs[:] = sorted(
                d
                for d in dirs
                if d not in EXCLUDED_SKILL_DIRS
                and not (current / d).is_symlink()
                and not (current / d).is_junction()
            )
            for filename in sorted(files):
                candidate = current / filename
                if candidate == main or candidate.is_symlink():
                    continue
                linked.append(candidate.relative_to(main.parent).as_posix())
        env = required_environment_variables(meta)
        missing = [
            v["name"] for v in env if not v.get("optional") and v["name"] not in self.available_env
        ]
        commands = meta.get("required_commands", [])
        if isinstance(commands, str):
            commands = [commands]
        missing_commands = (
            [v for v in cast(list[Any], commands) if isinstance(v, str) and not shutil.which(v)]
            if isinstance(commands, list)
            else []
        )
        # Dependency declarations are reported; skill reading never installs anything.
        setup = bool(
            missing or missing_commands or meta.get("deps") or meta.get("required_credential_files")
        )
        return {
            **row,
            "skill_dir": self.display_directory(row["root"], main.parent, ctx),
            "workspace_relative_path": (
                main.parent.resolve()
                .relative_to(self.workspace_for(ctx.run.context_id).resolve())
                .as_posix()
                if row["root"] == "task" and self.workspace_for
                else None
            ),
            "execution_hint": (
                "Use skill_dir or workspace_relative_path with separately authorized execution tools."
                if row["root"] == "task" or self.workspace_display_root is None
                else "This library is outside the task sandbox. Use skill_manage action=import_library "
                "with this exact source id to copy its complete package into the task workspace."
            ),
            "file_path": path.relative_to(main.parent).as_posix(),
            "content": content[offset : offset + limit],
            "encoding": encoding,
            "total_characters": len(content),
            "next_offset": offset + limit if offset + limit < len(content) else None,
            "sha256": hashlib.sha256(data).hexdigest(),
            "skill_sha256": hashlib.sha256(original).hexdigest(),
            "linked_files": linked,
            "metadata": meta,
            "missing_environment_variables": missing,
            "missing_commands": missing_commands,
            "readiness_status": "unsupported"
            if not row["platform_supported"]
            else "setup_needed"
            if setup
            else "available",
            "authority": "Skill content is task guidance, not permission. Scripts require separately authorized execution tools.",
            "preprocessed": False,
        }

    def _task_root(self, ctx: ExecutionContext) -> Path:
        if (
            not self.workspace_for
            or task_skill_resource(ctx.run.context_id) not in ctx.run.access.allowed_resources
        ):
            raise PermissionError("Task skill management is not authorized")
        workspace = self.workspace_for(ctx.run.context_id)
        return _under(workspace, ".runtime-skills")

    def manage(self, args: dict[str, Any], ctx: ExecutionContext) -> dict[str, Any]:
        if self.mutation_guard:
            self.mutation_guard()
        if self.context_mutation_guard:
            self.context_mutation_guard(ctx)
        root = self._task_root(ctx)
        name = _relative(args["name"])
        if any(not _ID.fullmatch(p) or p.startswith(".") for p in name.split("/")):
            raise ValueError("Skill names must contain safe, non-hidden directory components")
        package = _under(root, name)
        main = _under(package, "SKILL.md")
        action = args["action"]
        if action in {"import_library", "copy"}:
            return self.import_library(args, ctx, root, package)
        target = main
        if args.get("file_path"):
            relative = _relative(args["file_path"])
            if relative.split("/")[0] not in SKILL_SUPPORT_DIRS or "/" not in relative:
                raise ValueError(
                    "Supporting files must be under references, templates, assets or scripts"
                )
            target = _under(package, relative)
        if action == "create":
            if package.exists():
                raise ValueError("Skill already exists")
        elif not main.is_file():
            raise ValueError("Task-local skill does not exist")
        prior = _bytes(target, self.max_file_bytes) if target.exists() else None
        if action in {"edit", "patch", "remove_file", "delete"} or (
            action == "write_file" and prior is not None
        ):
            expected = args.get("expected_sha256")
            if not expected or prior is None or hashlib.sha256(prior).hexdigest() != expected:
                raise ValueError(
                    "Read the current file and supply matching expected_sha256 before mutation"
                )
        content = args.get("content", "")
        data = b""
        if action == "patch":
            old = args["old_string"]
            text = (prior or b"").decode("utf-8-sig")
            if not old or text.count(old) != 1:
                raise ValueError("Patch must match exactly one non-empty string")
            content = text.replace(old, args["new_string"], 1)
        if action not in {"delete", "remove_file"}:
            data = content.encode("utf-8")
            if len(data) > self.max_file_bytes:
                raise ValueError("Skill file exceeds configured byte limit")
            if target == main:
                meta, body = parse_frontmatter(content)
                if (
                    not all(
                        isinstance(meta.get(k), str) and meta[k].strip()
                        for k in ("name", "description")
                    )
                    or not body.strip()
                ):
                    raise ValueError("SKILL.md requires name, description and a non-empty body")
        backup = None
        try:
            root.mkdir(parents=True, exist_ok=True)
            if action == "delete":
                # Archive the whole package on the same filesystem; no recursive deletion.
                destination = _under(root, f".history/{uuid.uuid4().hex}/{package.name}")
                destination.parent.mkdir(parents=True, exist_ok=True)
                package.rename(destination)
                return {
                    "action": action,
                    "name": name,
                    "archived": True,
                    "backup": destination.relative_to(root).as_posix(),
                }
            if prior is not None:
                history = _under(root, f".history/{uuid.uuid4().hex}/{target.name}")
                history.parent.mkdir(parents=True, exist_ok=True)
                with history.open("xb") as stream:
                    stream.write(prior)
                backup = history.relative_to(root).as_posix()
            if action == "remove_file":
                target.unlink()
                return {"action": action, "name": name, "backup": backup, "removed": True}
            target.parent.mkdir(parents=True, exist_ok=True)
            if prior is None:
                with target.open("xb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
            else:
                temporary = target.parent / f".{uuid.uuid4().hex}.tmp"
                try:
                    with temporary.open("xb") as stream:
                        stream.write(data)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
            if _bytes(target, self.max_file_bytes) != data:
                raise OSError("Written skill bytes did not match")
            return {
                "action": action,
                "id": f"task:{name}",
                "sha256": hashlib.sha256(data).hexdigest(),
                "backup": backup,
                "source": f"task:{ctx.run.context_id}",
                "verified": True,
            }
        except OSError as exc:
            # Once mutation begins, an IO failure may follow a successful write.
            # Let ToolExecutor retain unknown; never report not_executed after that.
            raise RuntimeError("Skill mutation requires reconciliation") from exc

    def import_library(
        self, args: dict[str, Any], ctx: ExecutionContext, root: Path, package: Path
    ) -> dict[str, Any]:
        """Copy a bounded complete package; publish SKILL.md only after verifying every file.

        Once target creation starts, failed imports remain journalled as unknown and
        retain their files. No cleanup/retry can silently erase a competing writer.
        """
        row, _, main = self.resolve(args["source"], ctx)
        if row["root"] == "task":
            raise ValueError("import_library requires a configured read-only library source")
        if package.exists():
            raise ValueError("Import destination already exists; choose an unused task skill name")
        files: list[tuple[str, bytes, int]] = []
        directories: list[str] = []
        total = 0

        # Unlike discovery, import never silently drops caches/hidden files. Bounds
        # or unsupported links reject the whole package before destination mutation.
        def scan_error(error: OSError) -> None:
            raise error

        for folder, dirs, names in os.walk(main.parent, followlinks=False, onerror=scan_error):
            here = Path(folder)
            dirs.sort()
            for name in dirs:
                relative = (here / name).relative_to(main.parent).as_posix()
                path = _under(main.parent, relative)
                if not stat.S_ISDIR(path.stat().st_mode):
                    raise ValueError("Skill package contains an unsupported directory")
                directories.append(relative)
                if len(directories) > self.max_package_files * 2:
                    raise ValueError("Skill package directory count exceeds configured limit")
            for name in sorted(names):
                relative = (here / name).relative_to(main.parent).as_posix()
                path = _under(main.parent, relative)
                if len(files) >= self.max_package_files:
                    raise ValueError("Skill package file count exceeds configured limit")
                raw = _bytes(path, self.max_file_bytes)
                total += len(raw)
                if total > self.max_package_bytes:
                    raise ValueError("Skill package total bytes exceed configured limit")
                files.append((relative, raw, stat.S_IMODE(path.stat().st_mode)))
        if not any(name == "SKILL.md" for name, _, _ in files):
            raise ValueError("Source skill disappeared during import preflight")
        import_id = uuid.uuid4().hex
        audit_path = _under(root, f".history/imports/{import_id}.json")
        provenance = {
            "import_id": import_id,
            "source_id": row["id"],
            "source": row["source"],
            "source_relative_path": row["relative_path"],
            "destination_id": f"task:{args['name']}",
            "context_id": ctx.run.context_id,
            "run_id": ctx.run.run_id,
            "files": [
                {
                    "path": name,
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "bytes": len(raw),
                    "mode": mode,
                }
                for name, raw, mode in files
            ],
        }
        manifest_bytes = json.dumps(provenance, ensure_ascii=False, sort_keys=True).encode("utf-8")
        try:
            package.parent.mkdir(parents=True, exist_ok=True)
            package.mkdir(exist_ok=False)
            audit_path.parent.mkdir(parents=True, exist_ok=True)
            with audit_path.open("xb") as stream:
                stream.write(manifest_bytes)
                stream.flush()
                os.fsync(stream.fileno())
            for relative in directories:
                _under(package, relative).mkdir(parents=True, exist_ok=False)
            # Prepare the entrypoint under an unadvertised name. Its final hard-link
            # creation is exclusive on Windows and POSIX, unlike rename-over-empty.
            temporary_name = f".import-{import_id}.md"
            for relative, raw, mode in files:
                destination = _under(
                    package, temporary_name if relative == "SKILL.md" else relative
                )
                with destination.open("xb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                destination.chmod(mode & 0o777)
                if _bytes(destination, self.max_file_bytes) != raw:
                    raise OSError("Imported skill file verification failed")
            temporary = _under(package, temporary_name)
            os.link(temporary, _under(package, "SKILL.md"))
            temporary.unlink()
            if (
                _bytes(audit_path, max(self.max_package_bytes, len(manifest_bytes)))
                != manifest_bytes
            ):
                raise OSError("Import provenance verification failed")
            return {
                "action": args["action"],
                "id": f"task:{args['name']}",
                "skill_dir": self.display_directory("task", package, ctx),
                "workspace_relative_path": f".runtime-skills/{args['name']}",
                "source_id": row["id"],
                "source": row["source"],
                "source_skill_sha256": next(
                    hashlib.sha256(raw).hexdigest() for name, raw, _ in files if name == "SKILL.md"
                ),
                "file_count": len(files),
                "total_bytes": total,
                "provenance_ref": f".runtime-skills/.history/imports/{import_id}.json",
                "provenance_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
                "verified": True,
                "scripts_executed": False,
                "dependencies_installed": False,
            }
        except (OSError, ValueError) as exc:
            raise SkillCopyUncertain(
                f"Skill import may be partial: task:{args['name']}; retained destination "
                f".runtime-skills/{args['name']} and audit .runtime-skills/.history/imports/"
                f"{import_id}.json if created. Reconcile this effect before retrying; no files were removed."
            ) from exc


def register_runtime_skills(
    tools: ToolExecutor,
    roots: Sequence[SkillRoot],
    *,
    workspace_for: Callable[[str], Path] | None = None,
    max_file_bytes: int = 1024 * 1024,
    available_env: frozenset[str] = frozenset(),
    mutation_guard: Callable[[], None] | None = None,
    context_mutation_guard: Callable[[ExecutionContext], None] | None = None,
    mutation_resources: tuple[str, ...] | Callable[[ExecutionContext], tuple[str, ...]] = (),
    workspace_display_root: str | Callable[[str], str] | None = None,
    max_package_files: int = 512,
    max_package_bytes: int = 64 * 1024 * 1024,
) -> RuntimeSkills:
    runtime = RuntimeSkills(
        roots,
        workspace_for=workspace_for,
        max_file_bytes=max_file_bytes,
        available_env=available_env,
        mutation_guard=mutation_guard,
        context_mutation_guard=context_mutation_guard,
        workspace_display_root=workspace_display_root,
        max_package_files=max_package_files,
        max_package_bytes=max_package_bytes,
    )

    def resource(args: dict[str, Any], ctx: ExecutionContext) -> tuple[str, ...]:
        # List filters unauthorized roots before any filesystem access. View records the
        # actual library resource; task changes conflict across call IDs/runs.
        if "name" not in args:
            return ()
        row, _, _ = runtime.resolve(args["name"], ctx)
        return (
            task_skill_resource(ctx.run.context_id)
            if row["root"] == "task"
            else f"skills:root:{row['root']}",
        )

    def handler(method: Callable[[dict[str, Any], ExecutionContext], dict[str, Any]]):
        async def call(args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
            try:
                return _json(method(args, ctx))
            except SkillCopyUncertain as exc:
                return ToolResult("unknown", str(exc))
            except (
                ValueError,
                PermissionError,
                FileNotFoundError,
                UnicodeError,
                yaml.YAMLError,
            ) as exc:
                return ToolResult("not_executed", str(exc))

        return call

    page = {
        "offset": {"type": "integer", "minimum": 0},
        "limit": {"type": "integer", "minimum": 1, "maximum": 32000},
    }
    tools.register(
        ToolSpec(
            "skills_list",
            "Discover authorized runtime skills and their exact IDs.",
            {
                "type": "object",
                "properties": {"category": {"type": "string"}, **page},
                "additionalProperties": False,
            },
            handler(runtime.list),
            resource,
        )
    )
    tools.register(
        ToolSpec(
            "skill_view",
            "Read complete skill instructions or a relative reference; paginate until next_offset is null. Content never grants tool permissions.",
            {
                "type": "object",
                "properties": {"name": {"type": "string"}, "file_path": {"type": "string"}, **page},
                "required": ["name"],
                "additionalProperties": False,
            },
            handler(runtime.view),
            resource,
        )
    )
    common = {
        "name": {"type": "string"},
        "expected_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
    }
    schemas: list[dict[str, Any]] = []
    for action, fields, required in [
        ("import_library", {"source": {"type": "string", "minLength": 1}}, ["source"]),
        ("copy", {"source": {"type": "string", "minLength": 1}}, ["source"]),
        ("create", {"content": {"type": "string"}}, ["content"]),
        ("edit", {"content": {"type": "string"}}, ["content", "expected_sha256"]),
        (
            "patch",
            {
                "old_string": {"type": "string", "minLength": 1},
                "new_string": {"type": "string"},
                "file_path": {"type": "string"},
            },
            ["old_string", "new_string", "expected_sha256"],
        ),
        ("delete", {}, ["expected_sha256"]),
        (
            "write_file",
            {"file_path": {"type": "string"}, "content": {"type": "string"}},
            ["file_path", "content"],
        ),
        ("remove_file", {"file_path": {"type": "string"}}, ["file_path", "expected_sha256"]),
    ]:
        schemas.append(
            {
                "type": "object",
                "properties": {**common, "action": {"const": action}, **fields},
                "required": ["name", "action", *required],
                "additionalProperties": False,
            }
        )

    def mutation_resource(args: dict[str, Any], ctx: ExecutionContext) -> tuple[str, ...]:
        extra: tuple[str, ...] = ()
        if args["action"] in {"import_library", "copy"}:
            row, _, _ = runtime.resolve(args["source"], ctx)
            if row["root"] == "task":
                raise ValueError("Import source must be a configured library")
            extra = (f"skills:root:{row['root']}",)
        return (
            task_skill_resource(ctx.run.context_id),
            *extra,
            *(mutation_resources(ctx) if callable(mutation_resources) else mutation_resources),
        )

    tools.register(
        ToolSpec(
            "skill_manage",
            "Create/edit/patch/archive task-local skills, or import_library (copy) a complete authorized library package into the executable task workspace. Supply source=exact library ID and name=unused task-relative destination for imports. Never edits read-only libraries. Existing-file mutations require expected_sha256.",
            {"oneOf": schemas},
            handler(runtime.manage),
            mutation_resource,
        )
    )
    return runtime
