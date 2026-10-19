from __future__ import annotations

import asyncio
import hashlib
import os
import stat
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from shittim_core.domain.types import AccessContext, StateConflict
from shittim_core.materials.names import sanitize_name
from shittim_core.state import StateStore


@dataclass(frozen=True)
class ArtifactRef:
    artifact_id: str
    context_id: str
    audience: str
    version: int
    sha256: str
    name: str


@dataclass(frozen=True)
class MaterialReadResult:
    status: Literal["read", "denied", "unavailable", "integrity_error"]
    ref: ArtifactRef | None = None
    data: bytes | None = field(default=None, repr=False)
    reason: str = ""


class MaterialStore:
    """Each handle is bound to one collaboration context, not a global file capability.

    The root must be local and app-controlled. Metadata paths cannot escape it. Version files
    are immutable; this does not promise protection from a hostile OS administrator racing
    directory replacement. No network download or document parsing happens implicitly.
    """

    def __init__(
        self,
        root: Path | str,
        store: StateStore,
        context_id: str,
        *,
        max_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        if max_bytes < 1 or not context_id:
            raise ValueError("A scoped context and positive material size limit are required")
        self._requested_root = Path(root).absolute()
        if self._requested_root.is_symlink() or self._requested_root.is_junction():
            raise ValueError("Material root must not be a link")
        self._requested_root.mkdir(parents=True, exist_ok=True)
        self.root = self._requested_root.resolve(strict=True)
        self.store, self.context_id, self.max_bytes = store, context_id, max_bytes

    def _scope(self, access: AccessContext, artifact_id: str | None = None) -> None:
        context = self.store.get_context(self.context_id)
        if (
            access.audience != context["address"].key
            or access.policy_version != context["policy_version"]
        ):
            raise PermissionError("Material scope does not match current authority")
        resources = {f"materials:{self.context_id}"}
        if artifact_id:
            resources.add(f"artifact:{artifact_id}")
        if not resources.intersection(access.allowed_resources):
            raise PermissionError("No material resource capability")

    def _root_current(self) -> None:
        if (
            self._requested_root.is_symlink()
            or self._requested_root.is_junction()
            or self._requested_root.resolve(strict=True) != self.root
        ):
            raise PermissionError("Material root changed")

    def authorize_write(self, access: AccessContext) -> None:
        """Check the trusted caller's context capability before retrieving bytes."""
        self._scope(access)

    def _checked_path(self, raw: str) -> Path:
        self._root_current()
        path = Path(raw)
        if not path.is_absolute() or path.parent != self.root:
            raise PermissionError("Material path outside the controlled root")
        if path.is_symlink() or path.is_junction() or path.resolve(strict=True).parent != self.root:
            raise PermissionError("Linked material path is not readable")
        return path

    def _read_bytes(self, raw: str) -> bytes:
        path = self._checked_path(raw)
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise PermissionError("Material is not a regular file")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
        with os.fdopen(fd, "rb") as stream:
            opened = os.fstat(stream.fileno())
            # Recheck after open before reading; do not follow a swapped final-component link.
            current = path.lstat()
            if (
                not stat.S_ISREG(current.st_mode)
                or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
                or (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                raise PermissionError("Material identity changed during open")
            if opened.st_size > self.max_bytes:
                raise ValueError("Material exceeds size limit")
            data = stream.read(self.max_bytes + 1)
        if len(data) > self.max_bytes:
            raise ValueError("Material exceeds size limit")
        return data

    def _write_bytes(self, path: Path, data: bytes) -> None:
        self._root_current()
        # Random filename + exclusive creation; never overwrite an original or previous version.
        with path.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())

    @staticmethod
    def _ref(meta: dict[str, Any]) -> ArtifactRef:
        stored_name = Path(meta["path"]).name
        prefix, separator, label = stored_name.partition("-")
        name = label if separator and len(prefix) == 32 else stored_name
        return ArtifactRef(
            str(meta["artifact_id"]),
            str(meta["context_id"]),
            str(meta["audience"]),
            int(meta["version"]),
            str(meta["sha256"]),
            name,
        )

    async def add_bytes(
        self,
        data: bytes,
        name: str,
        access: AccessContext,
        *,
        artifact_id: str | None = None,
        expected_version: int | None = None,
        source_ref: str | None = None,
    ) -> ArtifactRef:
        if len(data) > self.max_bytes:
            raise ValueError("Material exceeds size limit")
        self._scope(access, artifact_id)
        version, owner = 1, access.actor_id
        if artifact_id is None:
            if expected_version is not None:
                raise ValueError("Cannot version an unidentified material")
            artifact_id = "artifact_" + uuid.uuid4().hex
        else:
            old = self.store.get_artifact(artifact_id)
            if old["context_id"] != self.context_id or old["audience"] != access.audience:
                raise PermissionError("Material belongs to a different context")
            if expected_version is None or old["version"] != expected_version:
                raise StateConflict("Material base version changed")
            version, owner = expected_version + 1, str(old["owner_id"])
        label = sanitize_name(name)
        path = self.root / (uuid.uuid4().hex + "-" + label)
        digest = hashlib.sha256(data).hexdigest()
        writer = asyncio.create_task(asyncio.to_thread(self._write_bytes, path, data))
        try:
            await asyncio.shield(writer)
            self._scope(access, artifact_id)
            self.store.register_artifact(
                artifact_id,
                self.context_id,
                owner,
                access.audience,
                str(path),
                digest,
                version=version,
                source_ref=source_ref,
            )
        except BaseException:
            # Finish our own local write before removing only its unregistered file.
            await asyncio.gather(writer, return_exceptions=True)
            if path.exists() and not path.is_symlink():
                path.unlink()
            raise
        return ArtifactRef(artifact_id, self.context_id, access.audience, version, digest, label)

    async def read(
        self,
        artifact_id: str,
        access: AccessContext,
        *,
        version: int | None = None,
    ) -> MaterialReadResult:
        try:
            self._scope(access, artifact_id)
            meta = self.store.get_artifact(artifact_id, version=version)
            if meta["context_id"] != self.context_id or meta["audience"] != access.audience:
                raise PermissionError("Material belongs to a different context")
            data = await asyncio.to_thread(self._read_bytes, str(meta["path"]))
            self._scope(access, artifact_id)
            if hashlib.sha256(data).hexdigest() != meta["sha256"]:
                return MaterialReadResult("integrity_error", reason="checksum_mismatch")
            return MaterialReadResult("read", self._ref(meta), data)
        except PermissionError:
            return MaterialReadResult("denied", reason="material_scope_or_path_denied")
        except (OSError, StateConflict, ValueError):
            return MaterialReadResult("unavailable", reason="material_unavailable")
