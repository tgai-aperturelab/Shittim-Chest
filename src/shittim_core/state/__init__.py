"""Explicitly constructed persistence; importing this package opens no resources."""

from .store import StateStore

__all__ = ["StateStore"]
