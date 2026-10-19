"""Explicit command-line entry point; help never starts a network listener."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
from pathlib import Path

from shittim_core.app.config import Settings
from shittim_core.app.diagnostics import inspect_configuration
from shittim_core.app.service import Application


async def _serve(settings: Settings) -> None:
    application = Application(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered: list[signal.Signals] = []
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
                registered.append(sig)
            except (NotImplementedError, RuntimeError):
                pass
        port = await application.start()
        print(json.dumps({"status": "listening", "host": settings.host, "port": port}), flush=True)
        await stop.wait()
    finally:
        result = await application.close()
        print(
            json.dumps(
                {
                    "status": "stopped" if result.drained else "unsettled",
                    "released": result.released,
                    "reason": result.reason,
                }
            ),
            flush=True,
        )
        # Keep the state writer and OS lease alive until retained effects actually settle.
        while not result.drained:
            result = await application.close()
        for sig in registered:
            loop.remove_signal_handler(sig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the explicitly configured candidate service")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--serve", action="store_true")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Inspect capability configuration without starting services",
    )
    args = parser.parse_args()
    if not args.serve and not args.check:
        parser.print_help()
        return 0
    if args.config is None:
        parser.error("--serve or --check requires --config")
    try:
        settings = Settings.load(args.config)
        if args.check:
            print(
                json.dumps(
                    inspect_configuration(settings, os.environ), ensure_ascii=False, indent=2
                )
            )
            return 0
        asyncio.run(_serve(settings))
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        # Configuration and client exceptions can embed credentials; never print their text.
        print(json.dumps({"status": "failed", "error_type": type(error).__name__}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
