"""Incremental UTF-8 pagination: never split a valid character between returned pages."""

import codecs
from typing import Protocol


class PageStream(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...
    def seek(self, offset: int, whence: int = 0, /) -> int: ...


def read_utf8_page(stream: PageStream, offset: int, limit: int = 32000) -> tuple[str, int]:
    if offset < 0 or limit < 4:
        raise ValueError("Output offset must be nonnegative and page capacity at least four bytes")
    stream.seek(offset)
    raw = stream.read(limit)
    eof = not stream.read(1)
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    text = decoder.decode(raw, final=eof)
    pending, _ = decoder.getstate()
    return text, offset + len(raw) - len(pending)
