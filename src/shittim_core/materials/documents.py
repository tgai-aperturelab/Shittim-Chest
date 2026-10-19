"""Isolated parsing lifecycle; timeout actually terminates the parser process."""

import asyncio
import base64
import json
import os
import subprocess
import sys
from typing import Any, cast

DOCUMENT_SUFFIXES = frozenset({".pdf", ".docx", ".xlsx"})


async def read_document(
    data: bytes,
    suffix: str,
    *,
    offset: int = 0,
    limit: int = 50,
    character_offset: int = 0,
    timeout: float = 30,
) -> dict[str, Any]:
    keep = {"PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "HOME", "USERPROFILE"}
    env = {key: value for key, value in os.environ.items() if key.upper() in keep}
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "shittim_core.materials.document_worker",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        env=env,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    request = json.dumps(
        {
            "data": base64.b64encode(data).decode("ascii"),
            "suffix": suffix,
            "offset": offset,
            "limit": limit,
            "character_offset": character_offset,
        }
    ).encode()
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(request), timeout)
        if process.returncode != 0 or len(stdout) > 1024 * 1024:
            return {"parsed": False, "error": "parser_process_failed"}
        return cast(dict[str, Any], json.loads(stdout))
    except TimeoutError:
        return {"parsed": False, "error": "parser_timeout"}
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()
