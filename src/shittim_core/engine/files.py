"""Workspace host for Hermes file and patch algorithms, with verified result bytes."""

from __future__ import annotations

import hashlib
import stat
from pathlib import Path
from typing import Any, cast

from shittim_core.engine.hermes import file_operations_native as native
from shittim_core.engine.hermes import patch_parser
from shittim_core.engine.hermes.file_operations_common import ReadResult, WriteResult


class WorkspaceFiles:
    def __init__(
        self, root: Path, max_bytes: int = 64 * 1024 * 1024, *, virtual_root: str | None = None
    ) -> None:
        self.root = root.resolve()
        self.max_bytes = max_bytes
        self.virtual_root = Path(virtual_root) if virtual_root is not None else None
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, value: str) -> Path:
        candidate = Path(value)
        if self.virtual_root is not None and candidate.is_relative_to(self.virtual_root):
            candidate = candidate.relative_to(self.virtual_root)
        if not candidate.is_absolute():
            candidate = self.root / candidate
        resolved = candidate.resolve()
        if not resolved.is_relative_to(self.root):
            raise PermissionError("Path is outside this task workspace")
        current = candidate
        while current != self.root:
            if current.is_symlink() or current.is_junction():
                raise PermissionError("Linked workspace paths are not supported")
            if current == current.parent:
                raise PermissionError("Path has no workspace ancestor")
            current = current.parent
        return resolved

    def bytes(self, name: str) -> bytes:
        path = self.path(name)
        info = path.stat()
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("Expected a regular file")
        with path.open("rb") as stream:
            data = stream.read(self.max_bytes + 1)
        if len(data) > self.max_bytes:
            raise ValueError("File exceeds the configured byte limit")
        return data

    def read_file_raw(self, name: str) -> ReadResult:
        try:
            data = self.bytes(name)
            content = data.decode("utf-8-sig")
            return ReadResult(
                content=content,
                total_lines=len(content.splitlines()),
                file_size=len(data),
                _content_sha256=hashlib.sha256(data).hexdigest(),
            )
        except FileNotFoundError:
            return ReadResult(error="File does not exist", not_found=True)
        except (OSError, ValueError, UnicodeError) as exc:
            return ReadResult(error=str(exc))

    def write_file(self, name: str, content: str, *, pre_content: str | None = None) -> WriteResult:
        path = self.path(name)
        before = self.read_file_raw(name)
        if before.error and not before.not_found:
            return WriteResult(error=before.error)
        if pre_content is not None and before.content != pre_content:
            return WriteResult(error="File changed since patch read; read the current file again")
        # Preserve the source file's newline convention and BOM, as Hermes does.
        prior = self.bytes(name) if not before.not_found else b""
        normalized = content.replace("\r\n", "\n")
        if b"\r\n" in prior:
            normalized = normalized.replace("\n", "\r\n")
        data = (b"\xef\xbb\xbf" if prior.startswith(b"\xef\xbb\xbf") else b"") + normalized.encode(
            "utf-8"
        )
        if len(data) > self.max_bytes:
            return WriteResult(error="File exceeds the configured byte limit")
        error = native.atomic_write(str(path), data)
        if error:
            return WriteResult(error=error)
        digest = hashlib.sha256(data).hexdigest()
        if native.sha256_of(str(path)) != digest:
            return WriteResult(error="Written content could not be verified")
        return WriteResult(bytes_written=len(data), verified=True, _content_sha256=digest)

    def delete_file(self, name: str) -> WriteResult:
        path = self.path(name)
        if not path.is_file():
            return WriteResult(error="Only regular files can be deleted")
        path.unlink()
        return WriteResult(verified=not path.exists())

    def move_file(self, name: str, destination: str) -> WriteResult:
        source, target = self.path(name), self.path(destination)
        if target.exists() or not source.is_file():
            return WriteResult(error="Move requires a regular source and unused destination")
        target.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive copy avoids the POSIX rename-overwrite race. Retain source on any failure.
        data = self.bytes(name)
        with target.open("xb") as stream:
            stream.write(data)
        if self.bytes(destination) != data:
            return WriteResult(error="Move copy verification failed; original retained")
        source.unlink()
        return WriteResult(bytes_written=len(data), verified=True)

    def patch(self, text: str) -> dict[str, Any]:
        # Preflight EVERY path before the upstream apply phase can mutate any file.
        operations, error = patch_parser.parse_v4a_patch(text)
        if error:
            return {"success": False, "error": error}
        for operation in operations:
            self.path(operation.file_path)
            if operation.new_path:
                self.path(operation.new_path)
        result = patch_parser.apply_v4a_operations(operations, self)
        return cast(dict[str, Any], cast(Any, result).to_dict())
