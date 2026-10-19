"""Opt-in real-model execution probe. Synthetic files only; no QQ message or deployment."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import os
import platform
import sys
from pathlib import Path

from shittim_core.agent.loop import AgentEngine
from shittim_core.agent.model import OpenAIModel
from shittim_core.agent.ports import RunControl
from shittim_core.app.run_ports import RuntimePorts
from shittim_core.context.builder import ContextBuilder
from shittim_core.domain.types import AccessContext, ConversationAddress, InputEvent, RunRequest
from shittim_core.engine.processes import ExecutionRuntime
from shittim_core.materials.store import MaterialStore
from shittim_core.state import StateStore
from shittim_core.tools.execution import EXECUTION_TOOLS, HOST_RESOURCE, register_execution_tools
from shittim_core.tools.executor import ToolExecutor


async def run_probe(
    directory: Path,
    *,
    base_url: str,
    model_name: str,
    api_key: str,
    backend: str = "local_host",
    bwrap_path: Path | None = None,
    python_runtime: Path | None = None,
) -> dict:
    directory.mkdir(parents=True, exist_ok=False)
    store = StateStore(directory / "state.sqlite3")
    address = ConversationAddress("123", "private", "456")
    request_text = (
        "工作目录里的汇总工具 summarize.py 跑不通。请查明原因并修好，用 sales.csv 的真实内容运行，"
        "生成 result.csv，核对各部门金额汇总准确到分，保留原输入，并把实际结果文件登记为成果供交付。"
        "完成后简短说明修了什么及结果。不要只给操作建议。"
        f"当前操作系统是{platform.system()}，Python解释器路径是{'/runtime/bin/python' if backend == 'bubblewrap' else sys.executable}。"
    )
    event = InputEvent(address, "456", "1", request_text)
    input_id, _ = store.ingest(event)
    access = AccessContext(
        "456",
        address.key,
        EXECUTION_TOOLS,
        frozenset({HOST_RESOURCE, "execution:workspace:probe", "materials:probe"}),
    )
    store.create_context("probe", event, access)
    store.attach_input("probe", input_id)
    epoch = store.claim_run("probe", "probe-run")
    runtime = ExecutionRuntime(
        directory / "workspaces",
        store,
        timeout=60,
        backend=backend,
        bwrap_path=bwrap_path,
        python_runtime=python_runtime,
    )
    workspace = runtime.workspace("probe")
    sample = "department,amount\n研发,0.10\n研发,0.20\n运营,2.50\n"
    (workspace.root / "sales.csv").write_text(sample, encoding="utf-8", newline="")
    (workspace.root / "summarize.py").write_text(
        "import csv\nfrom collections import defaultdict\n"
        "totals = defaultdict(int)\n"
        "with open('sales.csv', encoding='utf-8') as source:\n"
        "    for row in csv.DictReader(source):\n"
        "        totals[row['department']] += row['amount']\n"
        "with open('result.csv', 'w', encoding='utf-8', newline='') as output:\n"
        "    writer = csv.writer(output)\n"
        "    writer.writerow(['department', 'total'])\n"
        "    writer.writerows(totals.items())\n",
        encoding="utf-8",
    )
    original_hash = hashlib.sha256((workspace.root / "sales.csv").read_bytes()).hexdigest()
    tools = ToolExecutor(store)
    register_execution_tools(
        tools, runtime, directory / "materials", max_material_bytes=1024 * 1024
    )
    model = OpenAIModel(
        model=model_name,
        base_url=base_url,
        api_key=api_key,
        max_tokens=4096,
        timeout=90,
        reasoning_echo=True,
    )
    result: dict = {
        "transport": "none",
        "model": "live",
        "qq_live": False,
        "requested_model": model_name,
        "runtime": platform.system(),
        "execution_backend": backend,
        "result": "fail",
    }
    try:
        run = RunRequest(
            "probe-run",
            "probe",
            address,
            access,
            epoch,
            max_model_calls=20,
            max_tool_calls=50,
            timeout_seconds=600,
        )
        outcome = await AgentEngine(model).run(
            run, RuntimePorts(store, ContextBuilder(store), tools), RunControl()
        )
        effects = store.list_effects()
        produced = workspace.root / "result.csv"
        rows = []
        if produced.exists():
            with produced.open(encoding="utf-8-sig", newline="") as stream:
                rows = list(csv.DictReader(stream))
        expected = {"研发": "0.30", "运营": "2.50"}
        actual = {r.get("department"): r.get("total") for r in rows}
        artifact_verified = False
        for effect in effects:
            if effect["tool"] == "export_file" and effect["status"] == "succeeded":
                ref = json.loads(effect["result"].content)
                material = await MaterialStore(directory / "materials", store, "probe").read(
                    ref["artifact_id"], access
                )
                if (
                    material.data is not None
                    and produced.exists()
                    and material.data == produced.read_bytes()
                ):
                    artifact_verified = True
        result.update(
            outcome=outcome.status,
            reason=outcome.reason,
            final_text=outcome.text,
            tools=[{"tool": e["tool"], "status": e["status"]} for e in effects],
            processes=[
                {"status": p["status"], "exit_code": p["details"].get("exit_code")}
                for p in store.list_processes()
            ],
            actual_totals=actual,
            artifact_verified=artifact_verified,
            source_preserved=hashlib.sha256((workspace.root / "sales.csv").read_bytes()).hexdigest()
            == original_hash,
        )
        result["result"] = (
            "pass"
            if (
                outcome.status == "completed"
                and actual == expected
                and artifact_verified
                and result["source_preserved"]
                and any(e["tool"] == "terminal" for e in effects)
            )
            else "fail"
        )
    finally:
        await runtime.close()
        await model.close()
        store.close()
    (directory / "report.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result


if __name__ == "__main__":
    result = asyncio.run(
        run_probe(
            Path(sys.argv[1]),
            base_url=os.environ["PROBE_BASE_URL"],
            model_name=os.environ["PROBE_MODEL"],
            api_key=os.environ["PROBE_API_KEY"],
        )
    )
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(0 if result["result"] == "pass" else 1)
