"""Explicit Linux filesystem, process and network isolation for task execution.

Policy is project-owned: bubblewrap itself is a mechanism, not a complete policy.
Only the selected task directory is writable on the host. No host /home, /root,
state directory or filesystem sockets are exposed. Networking is isolated unless
the application explicitly configures host mode, which also permits private-network
access. This does not impose
CPU/memory/disk quotas or protect against kernel vulnerabilities.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Literal


class BubblewrapBackend:
    executable: str
    python_runtime: Path
    python_relative: Path | None
    network: Literal["isolated", "host"]

    def __init__(
        self,
        workspace_root: Path,
        *,
        bwrap_path: str | Path | None = None,
        python_runtime: str | Path | None = None,
        network: Literal["isolated", "host"] = "isolated",
    ) -> None:
        if network not in {"isolated", "host"}:
            raise ValueError("Unknown bubblewrap network mode")
        self.network = network
        if sys.platform != "linux":
            raise ValueError("bubblewrap execution is supported only on Linux; no host fallback")
        executable = str(bwrap_path) if bwrap_path is not None else shutil.which("bwrap")
        if (
            executable is None
            or not Path(executable).is_file()
            or not os.access(executable, os.X_OK)
        ):
            raise ValueError("An executable bubblewrap binary must be configured")
        self.executable = str(Path(executable).resolve())
        self.python_runtime = Path(python_runtime or sys.prefix).resolve(strict=True)
        if workspace_root.resolve().is_relative_to(
            self.python_runtime
        ) or self.python_runtime.is_relative_to(workspace_root.resolve()):
            raise ValueError("Python runtime mount must not overlap the task workspace store")
        if self.python_runtime in {
            Path("/"),
            Path("/home"),
            Path("/root"),
            Path("/tmp"),
            Path("/var"),
        }:
            raise ValueError("Python runtime must be a dedicated interpreter prefix")
        self.python_relative = next(
            (
                Path("bin") / name
                for name in ("python", "python3")
                if (self.python_runtime / "bin" / name).is_file()
            ),
            None,
        )
        if self.python_relative is None or not (self.python_runtime / "lib").is_dir():
            raise ValueError(
                "Python runtime requires bin/python or bin/python3 and a lib directory"
            )
        if not Path("/bin/bash").is_file():
            raise ValueError("bubblewrap execution requires /bin/bash")

    @property
    def python_executable(self) -> str:
        return str(Path("/runtime") / (self.python_relative or "bin/python"))

    @staticmethod
    def virtual_cwd(workspace: Path, directory: Path) -> str:
        relative = directory.resolve().relative_to(workspace.resolve())
        return str(Path("/workspace") / relative)

    def argv(
        self, workspace: Path, directory: Path, control_directory: Path, command: str
    ) -> list[str]:
        argv = [
            self.executable,
            "--unshare-all",
            "--unshare-user",
            "--unshare-pid",
            "--share-net" if self.network == "host" else "--unshare-net",
            "--die-with-parent",
            "--new-session",
            "--disable-userns",
            "--cap-drop",
            "ALL",
            "--clearenv",
        ]
        # Bind only executable/library directories, not /usr/local configuration,
        # /etc as a whole, or host /dev. Preserve merged-/usr symlink layouts.
        for name in ("/usr/bin", "/usr/lib", "/usr/lib64", "/bin", "/lib", "/lib64"):
            path = Path(name)
            if path.is_symlink():
                argv += ["--symlink", os.readlink(path), name]
            elif path.is_dir():
                argv += ["--ro-bind", name, name]
        if Path("/etc/ld.so.cache").is_file():
            argv += ["--ro-bind", "/etc/ld.so.cache", "/etc/ld.so.cache"]
        if self.network == "host":
            # Selected DNS/public trust infrastructure only. In particular, do
            # not mount /etc as a whole, /etc/ssl/private, proxy credentials or home.
            for name in (
                "/etc/resolv.conf",
                "/etc/hosts",
                "/etc/ssl/certs",
                "/usr/share/ca-certificates",
            ):
                if Path(name).exists():
                    argv += ["--ro-bind", name, name]
        argv += [
            "--ro-bind",
            str(self.python_runtime),
            "/runtime",
            "--bind",
            str(workspace),
            "/workspace",
            "--proc",
            "/proc",
            "--dev",
            "/dev",
            "--tmpfs",
            "/tmp",
            "--dir",
            "/tmp/home",
            "--dir",
            "/run",
            "--ro-bind",
            str(control_directory),
            "/run/execution-control",
            "--setenv",
            "HOME",
            "/tmp/home",
            "--setenv",
            "TMPDIR",
            "/tmp",
            "--setenv",
            "PATH",
            "/runtime/bin:/usr/bin:/bin",
            "--setenv",
            "LANG",
            "C.UTF-8",
            "--setenv",
            "PYTHONIOENCODING",
            "utf-8",
            "--setenv",
            "PYTHONDONTWRITEBYTECODE",
            "1",
            "--chdir",
            self.virtual_cwd(workspace, directory),
            "--",
            "/bin/bash",
            "--noprofile",
            "--norc",
            "-c",
            command,
        ]
        return argv
