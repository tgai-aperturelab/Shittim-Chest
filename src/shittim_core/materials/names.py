"""Filename projection adapted from AstrBot components.py:757-767.

Source: AstrBotDevs/AstrBot @ e5e21b71aec5642f110eb115eb8e9a0d8028b7e5.
AGPL-3.0-or-later. Labels follow a random storage prefix; they do not choose directories.
"""

from pathlib import PurePosixPath

_MAX_NAME_CHARACTERS = 120
# Storage adds a 32-character UUID plus '-'. Leave space within a 255-byte
# filesystem component while retaining the existing 120-character label bound.
_MAX_NAME_BYTES = 220


def _clip_name(value: str, characters: int, byte_limit: int) -> str:
    return value[:characters].encode("utf-8")[:byte_limit].decode("utf-8", errors="ignore")


def sanitize_name(name: str | None) -> str:
    if not name:
        return "file"
    normalized = str(name).replace("\\", "/")
    basename = PurePosixPath(normalized).name.replace("\x00", "").strip()
    for char in ':*?"<>|':
        basename = basename.replace(char, "_")
    if basename in {"", ".", ".."}:
        return "file"
    # A display label must not introduce control characters or unbounded metadata.
    basename = "".join(c for c in basename if ord(c) >= 32).rstrip(". ")
    suffix = PurePosixPath(basename).suffix
    # A lone leading dot is a hidden name, not a format suffix. Multi-dot names
    # retain their final suffix, which is also the document-parser dispatch key.
    if (
        suffix
        and len(suffix) < _MAX_NAME_CHARACTERS
        and len(suffix.encode("utf-8")) < _MAX_NAME_BYTES
    ):
        stem = _clip_name(
            basename[: -len(suffix)],
            _MAX_NAME_CHARACTERS - len(suffix),
            _MAX_NAME_BYTES - len(suffix.encode("utf-8")),
        )
        basename = (stem or "f") + suffix
    else:
        # An extension that alone exceeds the label budget cannot be preserved;
        # treat it as an ordinary bounded label, never leave a trailing dot.
        basename = _clip_name(basename, _MAX_NAME_CHARACTERS, _MAX_NAME_BYTES)
    return basename.rstrip(". ") or "file"
