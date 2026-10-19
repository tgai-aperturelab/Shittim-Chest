"""Import behavior in a fresh interpreter, not proof of a general security sandbox."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

CHECK = r"""
import asyncio, importlib, importlib.abc, json, logging, os, pathlib, sqlite3
import subprocess, sys, threading
source, module = sys.argv[1:]
sys.path.insert(0, source)
for key in ('HOME', 'USERPROFILE', 'XDG_CONFIG_HOME', 'HERMES_HOME'):
    os.environ[key] = os.getcwd()
before_env = dict(os.environ)
before_cwd = os.getcwd()
before_threads = tuple(threading.enumerate())
root = logging.getLogger()
before_logging = (tuple(root.handlers), tuple(root.filters), root.level)
forbidden = {'astrbot', 'agent', 'run_agent', 'model_tools', 'hermes_cli', 'gateway', 'tools'}
class NoUpstream(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in forbidden:
            raise AssertionError('legacy host import: ' + fullname)
        return None
sys.meta_path.insert(0, NoUpstream())
def audit(event, args):
    if event in {'socket.connect','socket.bind','socket.getaddrinfo','sqlite3.connect',
                 'subprocess.Popen','os.mkdir','os.remove','os.rename','os.rmdir','os.chdir',
                 'os.putenv','os.unsetenv','os.system'}:
        raise AssertionError('import side effect: ' + event)
    if event == 'open':
        path, mode, flags = args
        if (isinstance(mode, str) and any(ch in mode for ch in 'wax+')) or (
                isinstance(flags, int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC)):
            raise AssertionError('import write: ' + str(path))
        if isinstance(path, (str, bytes)) and os.fsdecode(path).endswith('.env'):
            raise AssertionError('import reads environment file')
def no_thread(*args, **kwargs):
    raise AssertionError('import starts thread')
def no_task(*args, **kwargs):
    raise AssertionError('import starts async task')
threading.Thread.start = no_thread
asyncio.BaseEventLoop.create_task = no_task
sys.addaudithook(audit)
importlib.import_module(module)
assert dict(os.environ) == before_env
assert os.getcwd() == before_cwd
assert tuple(threading.enumerate()) == before_threads
assert (tuple(root.handlers), tuple(root.filters), root.level) == before_logging
print(json.dumps({'module': module, 'import_ok': True}))
"""


@pytest.mark.parametrize(
    "module",
    [
        "shittim_core",
        "shittim_core.domain.types",
        "shittim_core.state",
        "shittim_core.agent.loop",
        "shittim_core.agent.model",
        "shittim_core.qq.codec",
        "shittim_core.qq.transport",
        "shittim_core.context.builder",
        "shittim_core.tools.executor",
        "shittim_core.delivery.service",
        "shittim_core.collaboration.coordinator",
        "shittim_core.app.run_ports",
        "shittim_core.app.service",
        "shittim_core.app.config",
        "shittim_core.app.reference_intake",
        "shittim_core.app.material_intake",
        "shittim_core.context.compactor",
        "shittim_core.context.runtime_compaction",
        "shittim_core.materials.store",
        "shittim_core.materials.downloader",
        "shittim_core.tools.collaboration",
        "shittim_core.tools.material_tools",
        "shittim_core.tools.execution",
        "shittim_core.engine.files",
        "shittim_core.engine.processes",
        "shittim_core.engine.hermes.patch_parser",
        "shittim_core.engine.hermes.fuzzy_match",
        "shittim_core.engine.hermes.reasoning_carriers",
        "shittim_core.engine.hermes.tool_call_ids",
        "shittim_core.__main__",
    ],
)
def test_public_import_has_no_runtime_side_effects(tmp_path: Path, module: str):
    source = Path(__file__).resolve().parents[2] / "src"
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", CHECK, str(source), module],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert '"import_ok": true' in result.stdout
    assert not list(tmp_path.iterdir())
