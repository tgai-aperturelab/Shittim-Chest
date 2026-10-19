"""Scoped immutable material files; SQLite metadata remains owned by StateStore."""

from .store import ArtifactRef, MaterialReadResult, MaterialStore

__all__ = ["ArtifactRef", "MaterialReadResult", "MaterialStore"]
