"""Reproduce the reviewed Hermes execution leaf-module migration, without importing it."""

import argparse
import ast
import hashlib
import json
import subprocess
from pathlib import Path

COMMIT = "9e01b44dd23978f9ff40b7837467f3a9f1582130"
MODULES = (
    "file_operations_native",
    "file_operations_common",
    "patch_parser",
    "fuzzy_match",
    "escape_drift_autocorrect",
    "tool_output_truncate",
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    args = parser.parse_args()
    head = subprocess.check_output(
        ["git", "-C", str(args.source), "rev-parse", "HEAD"], text=True
    ).strip()
    if head != COMMIT:
        raise SystemExit("Source commit does not match the reviewed baseline")
    project = Path(__file__).resolve().parents[1]
    target = project / "src/shittim_core/engine/hermes"
    target.mkdir(parents=True, exist_ok=True)
    records = []
    for name in MODULES:
        source = args.source / "tools" / (name + ".py")
        raw = source.read_bytes()
        text = raw.decode("utf-8")
        changes = ["Relocate internal imports; retain upstream algorithms"]
        for module in MODULES:
            text = text.replace("from tools." + module, "from shittim_core.engine.hermes." + module)
        text = text.replace(
            "from tools.tool_output_limits", "from shittim_core.engine.hermes.output_limits"
        )
        if name == "fuzzy_match":
            tree = ast.parse(text)
            function = next(
                n
                for n in tree.body
                if isinstance(n, ast.FunctionDef) and n.name == "_note_edit_match"
            )
            lines = text.splitlines(keepends=True)
            lines[function.lineno - 1 : function.end_lineno] = [
                "def _note_edit_match(strategy: Optional[str], miss: Optional[str] = None) -> None:\n",
                '    """The application owns observability; never import a CLI metrics host."""\n',
                "    return None\n",
            ]
            text = "".join(lines)
            changes.append("Remove optional CLI metrics import; no telemetry")
        if name == "file_operations_native":
            text = text.replace(
                "if mode is not None:\n            try:",
                'if mode is not None and hasattr(os, "fchmod"):\n            try:',
            )
            changes.append("Guard POSIX-only fchmod on Windows")
        text = f"# Source: NousResearch/hermes-agent {COMMIT}, tools/{name}.py (MIT).\n" + text
        ast.parse(text)
        destination = target / (name + ".py")
        destination.write_text(text, encoding="utf-8", newline="\n")
        records.append(
            {
                "source": "tools/" + name + ".py",
                "source_sha256": hashlib.sha256(raw).hexdigest(),
                "target": str(destination.relative_to(project)).replace("\\", "/"),
                "target_sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
                "changes": changes,
            }
        )
    path = project / "third_party/hermes-execution-manifest.json"
    path.write_text(
        json.dumps(
            {
                "repository": "NousResearch/hermes-agent",
                "commit": COMMIT,
                "license": "MIT",
                "files": records,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"Migrated {len(records)} reviewed modules; no upstream host imported")


if __name__ == "__main__":
    main()
