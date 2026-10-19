"""An OS-held exclusive candidate-directory lease; stale PID text is never a lock."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import BinaryIO


class AlreadyRunning(RuntimeError):
    """Another process or application instance owns this exact data directory."""


class DataDirectoryLease:
    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir.resolve() / ".instance.lock"
        self._file: BinaryIO | None = None

    @property
    def acquired(self) -> bool:
        return self._file is not None

    def acquire(self) -> None:
        """Acquire without waiting; create the directory only at explicit startup."""
        if self._file is not None:
            raise AlreadyRunning("This application already owns the data directory")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file = self.path.open("a+b")
        try:
            file.seek(0, os.SEEK_END)
            if file.tell() == 0:
                file.write(b"\0")
                file.flush()
            file.seek(0)
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            file.close()
            raise AlreadyRunning("Candidate data directory is already locked") from error
        except BaseException:
            file.close()
            raise
        self._file = file

    def release(self) -> None:
        """Release ownership without unlinking the lock inode or touching the state database."""
        file, self._file = self._file, None
        if file is None:
            return
        try:
            file.seek(0)
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
        finally:
            file.close()
