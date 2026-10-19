"""Operating-system process-tree ownership, independent from workspace path policy.

Windows jobs prohibit breakaway and kill their members on handle close. Assign the
gated root process before releasing its command. POSIX process groups cover ordinary
children but a child can explicitly escape with setsid/setpgid: this is not a sandbox.
"""

from __future__ import annotations

import ctypes
import os
import signal
from pathlib import Path
from typing import Any

if os.name == "nt":
    from ctypes import wintypes

    class _BasicLimit(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _IoCounters(ctypes.Structure):
        _fields_ = [
            (name, ctypes.c_ulonglong)
            for name in (
                "ReadOperationCount",
                "WriteOperationCount",
                "OtherOperationCount",
                "ReadTransferCount",
                "WriteTransferCount",
                "OtherTransferCount",
            )
        ]

    class _ExtendedLimit(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimit),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    class _BasicAccounting(ctypes.Structure):
        _fields_ = [
            ("TotalUserTime", ctypes.c_longlong),
            ("TotalKernelTime", ctypes.c_longlong),
            ("ThisPeriodTotalUserTime", ctypes.c_longlong),
            ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
            ("TotalPageFaultCount", wintypes.DWORD),
            ("TotalProcesses", wintypes.DWORD),
            ("ActiveProcesses", wintypes.DWORD),
            ("TotalTerminatedProcesses", wintypes.DWORD),
        ]

    _kernel: Any = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    _kernel.CreateJobObjectW.restype = wintypes.HANDLE
    _kernel.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    _kernel.SetInformationJobObject.restype = wintypes.BOOL
    _kernel.QueryInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_void_p,
    ]
    _kernel.QueryInformationJobObject.restype = wintypes.BOOL
    _kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _kernel.OpenProcess.restype = wintypes.HANDLE
    _kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    _kernel.AssignProcessToJobObject.restype = wintypes.BOOL
    _kernel.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    _kernel.TerminateJobObject.restype = wintypes.BOOL
    _kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    _kernel.CloseHandle.restype = wintypes.BOOL


class ProcessTree:
    """One root and its tracked descendants. All handles are owned by this object."""

    def __init__(self) -> None:
        self._handle: Any = None
        self._pgid: int | None = None
        self._closed = False
        if os.name == "nt":
            self._handle = _kernel.CreateJobObjectW(None, None)
            if not self._handle:
                raise ctypes.WinError(ctypes.get_last_error())
            limits = _ExtendedLimit()
            limits.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            if not _kernel.SetInformationJobObject(
                self._handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)
            ):
                error = ctypes.WinError(ctypes.get_last_error())
                self.close()
                raise error

    def assign(self, pid: int) -> None:
        if self._closed or self._pgid is not None:
            raise RuntimeError("Process tree is closed or already assigned")
        if os.name == "nt":
            # PROCESS_SET_QUOTA | PROCESS_TERMINATE; close the temporary process handle.
            handle = _kernel.OpenProcess(0x0100 | 0x0001, False, pid)
            if not handle:
                raise ctypes.WinError(ctypes.get_last_error())
            try:
                if not _kernel.AssignProcessToJobObject(self._handle, handle):
                    raise ctypes.WinError(ctypes.get_last_error())
            finally:
                _kernel.CloseHandle(handle)
        else:
            # ExecutionRuntime starts the gated root with start_new_session=True.
            if os.getpgid(pid) != pid:
                raise RuntimeError("Execution root does not own its process group")
        self._pgid = pid

    def alive(self) -> bool:
        if self._closed or self._pgid is None:
            return False
        if os.name == "nt":
            account = _BasicAccounting()
            if not _kernel.QueryInformationJobObject(
                self._handle, 1, ctypes.byref(account), ctypes.sizeof(account), None
            ):
                raise ctypes.WinError(ctypes.get_last_error())
            return bool(account.ActiveProcesses)
        try:
            os.killpg(self._pgid, 0)
        except ProcessLookupError:
            return False
        # Linux leaves orphan zombies in the group until init reaps them; they cannot
        # perform more work. Other POSIX platforms conservatively keep the group alive.
        proc = Path("/proc")
        if proc.is_dir():
            uncertain = False
            for entry in proc.iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                    if int(fields[2]) == self._pgid and fields[0] != "Z":
                        return True
                except (FileNotFoundError, ProcessLookupError):
                    continue
                except (PermissionError, ValueError, IndexError):
                    uncertain = True
            return uncertain
        return True

    def terminate(self) -> None:
        if self._closed or self._pgid is None:
            return
        if os.name == "nt":
            if not _kernel.TerminateJobObject(self._handle, 1):
                raise ctypes.WinError(ctypes.get_last_error())
        else:
            try:
                os.killpg(self._pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def close(self) -> None:
        if self._closed:
            return
        if os.name == "nt":
            if self._handle:
                if not _kernel.CloseHandle(self._handle):
                    raise ctypes.WinError(ctypes.get_last_error())
                self._handle = None
        else:
            self.terminate()
        self._closed = True
