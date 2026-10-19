"""Isolated DDGS worker; adapted from Hermes' bounded DDGS provider/worker.

Hermes 9e01b44dd23978f9ff40b7837467f3a9f1582130, plugins/web/ddgs.
Native search engines may hold the GIL; enforce the deadline in the parent process.
"""
from __future__ import annotations

import json
import sys


def main() -> int:
    try:
        request = json.load(sys.stdin)
        from ddgs import DDGS
        from ddgs.ddgs import ENGINES

        selected = {item.strip() for item in request["backend"].split(",")}
        if not selected.intersection({"auto", "all"}) and not selected.issubset(ENGINES["text"]):
            raise ValueError("Requested search engine is disabled or unavailable")

        with DDGS(timeout=request["request_timeout"], proxy=request.get("proxy")) as search:
            hits = search.text(
                request["query"], max_results=request["limit"],
                region=request["region"], backend=request["backend"],
            )
            result = [
                {"title": str(hit.get("title", ""))[:2000],
                 "url": str(hit.get("href") or hit.get("url") or "")[:8000],
                 "snippet": str(hit.get("body", ""))[:12000]}
                for hit in hits[:request["limit"]]
            ]
        json.dump({"ok": True, "results": result}, sys.stdout, ensure_ascii=False)
        return 0
    except Exception as exc:
        json.dump({"ok": False, "error": type(exc).__name__}, sys.stdout)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
