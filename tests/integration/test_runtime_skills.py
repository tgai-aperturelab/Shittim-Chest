"""Actual task skill files through the durable dispatcher, without global profiles."""

import base64
import hashlib
import json
import subprocess
import sys
from dataclasses import replace

import pytest

from shittim_core.domain.model import ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    RunRequest,
)
from shittim_core.state import StateStore
from shittim_core.tools import runtime_skills as module
from shittim_core.tools.executor import ToolExecutor
from shittim_core.tools.runtime_skills import (
    RUNTIME_SKILL_TOOLS,
    SkillRoot,
    register_runtime_skills,
    task_skill_resource,
)


def skill(name="analyze", text="Read all the materials.", extra=""):
    return f"---\nname: {name}\ndescription: |\n  Analyze materials.\n  Preserve sources.\n{extra}---\n# Instructions\n{text}\n"


@pytest.fixture
def setup(tmp_path):
    root = tmp_path / "library"
    (root / "analyze" / "references").mkdir(parents=True)
    (root / "analyze" / "SKILL.md").write_text(skill(), encoding="utf-8", newline="")
    (root / "analyze" / "references" / "guide.md").write_text(
        "Reference 中文", encoding="utf-8", newline=""
    )
    workspaces = tmp_path / "tasks"
    store = StateStore(tmp_path / "state.db")
    address = ConversationAddress("bot", "private", "alice")
    event = InputEvent(address, "alice", "1", "Inspect and maintain task skills")
    input_id, _ = store.ingest(event)
    access = AccessContext(
        "alice",
        address.key,
        RUNTIME_SKILL_TOOLS,
        frozenset({"skills:root:library", task_skill_resource("ctx")}),
    )
    store.create_context("ctx", event, access)
    store.attach_input("ctx", input_id)
    epoch = store.claim_run("ctx", "run")
    ctx = ExecutionContext(RunRequest("run", "ctx", address, access, epoch), 1, 0)
    tools = ToolExecutor(store)
    runtime = register_runtime_skills(
        tools,
        [SkillRoot("library", root, "pinned:test")],
        workspace_for=lambda cid: workspaces / cid,
    )
    count = 0

    async def invoke(name, args, context=None):
        nonlocal count
        count += 1
        call = ToolCall(f"call{count}", name, json.dumps(args, ensure_ascii=False))
        store.append_history(
            "run",
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": call.id,
                        "type": "function",
                        "function": {"name": name, "arguments": call.arguments},
                    }
                ],
            },
        )
        return await tools.execute(call, context or ctx)

    yield root, workspaces, store, runtime, ctx, invoke
    store.close()


async def test_list_view_reference_provenance_effect_history(setup):
    _, _, store, _, _, invoke = setup
    listed = await invoke("skills_list", {})
    row = json.loads(listed.content)["skills"][0]
    assert row["id"] == "library:analyze"
    assert row["source"] == "pinned:test"
    viewed = await invoke("skill_view", {"name": row["id"]})
    value = json.loads(viewed.content)
    assert value["content"] == skill()
    assert value["metadata"]["description"] == "Analyze materials.\nPreserve sources.\n"
    assert value["linked_files"] == ["references/guide.md"]
    reference = await invoke("skill_view", {"name": row["id"], "file_path": "references/guide.md"})
    assert json.loads(reference.content)["content"] == "Reference 中文"
    assert len(store.list_effects()) == 3
    assert all(e["status"] == "succeeded" for e in store.list_effects())


async def test_full_text_pagination_never_silent_truncation(setup):
    root, _, _, _, _, invoke = setup
    content = skill(text="大" * 80000)
    (root / "analyze" / "SKILL.md").write_text(content, encoding="utf-8", newline="")
    chunks, offset = [], 0
    while offset is not None:
        result = await invoke("skill_view", {"name": "analyze", "offset": offset})
        value = json.loads(result.content)
        chunks.append(value["content"])
        offset = value["next_offset"]
    assert "".join(chunks) == content


async def test_binary_assets_read_without_execution(setup):
    root, _, _, _, _, invoke = setup
    target = root / "analyze" / "assets"
    target.mkdir()
    (target / "sample.bin").write_bytes(b"\xff\xfe\x00\x01")
    result = await invoke("skill_view", {"name": "analyze", "file_path": "assets/sample.bin"})
    data = json.loads(result.content)
    assert data["encoding"] == "base64"
    assert base64.b64decode(data["content"]) == b"\xff\xfe\x00\x01"


async def test_duplicate_names_require_exact_library_id(setup):
    root, _, _, _, _, invoke = setup
    (root / "other").mkdir()
    (root / "other" / "SKILL.md").write_text(skill(), encoding="utf-8", newline="")
    result = await invoke("skill_view", {"name": "analyze"})
    assert result.status == "not_executed"
    assert (await invoke("skill_view", {"name": "library:other"})).status == "succeeded"


@pytest.mark.parametrize(
    "path", ["../secret", "/etc/passwd", "C:/secret", "a\\..\\secret", "a/../../secret"]
)
async def test_reference_traversal_denied(setup, path):
    _, _, _, _, _, invoke = setup
    result = await invoke("skill_view", {"name": "analyze", "file_path": path})
    assert result.status == "not_executed"


async def test_unauthorized_libraries_are_not_scanned_or_disclosed(setup):
    _, _, _, _, ctx, invoke = setup
    hidden = replace(
        ctx, run=replace(ctx.run, access=replace(ctx.run.access, allowed_resources=frozenset()))
    )
    result = await invoke("skills_list", {}, hidden)
    assert json.loads(result.content)["skills"] == []
    assert (await invoke("skill_view", {"name": "analyze"}, hidden)).status == "not_executed"
    assert (
        await invoke(
            "skill_manage", {"action": "create", "name": "new", "content": skill("new")}, hidden
        )
    ).status == "not_executed"


async def test_management_roundtrip_with_backups_and_stale_hash_refusal(setup):
    root, workspaces, _, _, _, invoke = setup
    original = (root / "analyze" / "SKILL.md").read_bytes()
    created = await invoke(
        "skill_manage", {"action": "create", "name": "research/new", "content": skill("new")}
    )
    assert created.status == "succeeded", created.content
    data = json.loads(created.content)
    bad = await invoke(
        "skill_manage",
        {
            "action": "edit",
            "name": "research/new",
            "content": skill("new", "Changed"),
            "expected_sha256": "0" * 64,
        },
    )
    assert bad.status == "not_executed"
    patched = await invoke(
        "skill_manage",
        {
            "action": "patch",
            "name": "research/new",
            "old_string": "Read all the materials.",
            "new_string": "Verify the result.",
            "expected_sha256": data["sha256"],
        },
    )
    assert patched.status == "succeeded", patched.content
    backup = json.loads(patched.content)["backup"]
    task = workspaces / "ctx" / ".runtime-skills"
    assert (task / backup).read_text(encoding="utf-8") == skill("new")
    write = await invoke(
        "skill_manage",
        {
            "action": "write_file",
            "name": "research/new",
            "file_path": "references/notes.md",
            "content": "notes",
        },
    )
    assert write.status == "succeeded"
    remove = await invoke(
        "skill_manage",
        {
            "action": "remove_file",
            "name": "research/new",
            "file_path": "references/notes.md",
            "expected_sha256": json.loads(write.content)["sha256"],
        },
    )
    assert remove.status == "succeeded"
    archive = await invoke(
        "skill_manage",
        {
            "action": "delete",
            "name": "research/new",
            "expected_sha256": json.loads(patched.content)["sha256"],
        },
    )
    assert json.loads(archive.content)["archived"]
    assert not (task / "research" / "new").exists()
    assert (task / json.loads(archive.content)["backup"] / "SKILL.md").is_file()
    assert (root / "analyze" / "SKILL.md").read_bytes() == original
    assert json.loads((await invoke("skills_list", {})).content)["total"] == 1


async def test_no_global_discovery_and_task_isolation(setup, monkeypatch, tmp_path):
    _, _, _, runtime, ctx, invoke = setup
    fake_home = tmp_path / "home"
    global_skill = fake_home / ".agents" / "skills" / "global"
    global_skill.mkdir(parents=True)
    (global_skill / "SKILL.md").write_text(skill("global"), encoding="utf-8", newline="")
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv("HERMES_HOME", str(fake_home))
    await invoke(
        "skill_manage", {"action": "create", "name": "private", "content": skill("private")}
    )
    other = replace(
        ctx,
        run=replace(
            ctx.run,
            context_id="another",
            access=replace(
                ctx.run.access, allowed_resources=frozenset({task_skill_resource("another")})
            ),
        ),
    )
    assert runtime.list({}, other)["skills"] == []
    assert all(r["name"] != "global" for r in runtime.list({}, ctx)["skills"])


async def test_read_reports_setup_without_running_commands_or_expanding_shell(setup):
    root, _, _, _, _, invoke = setup
    content = skill(
        text="!`echo must-not-run`",
        extra="deps: [not-installed]\nrequired_environment_variables:\n  - name: NEVER_READ_VALUE\nrequired_commands: [command-which-does-not-exist]\n",
    )
    (root / "analyze" / "SKILL.md").write_text(content, encoding="utf-8", newline="")
    result = json.loads((await invoke("skill_view", {"name": "analyze"})).content)
    assert result["content"] == content
    assert result["readiness_status"] == "setup_needed"
    assert result["preprocessed"] is False
    assert result["missing_environment_variables"] == ["NEVER_READ_VALUE"]


async def test_symlink_reference_denied(setup, tmp_path):
    root, _, _, _, _, invoke = setup
    outside = tmp_path / "secret.txt"
    outside.write_text("secret", encoding="utf-8", newline="")
    link = root / "analyze" / "references" / "link.txt"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("Host does not allow symlinks")
    result = await invoke("skill_view", {"name": "analyze", "file_path": "references/link.txt"})
    assert result.status == "not_executed"


async def test_malformed_skill_reported_without_hiding_good_skills(setup):
    root, _, _, _, _, invoke = setup
    (root / "broken").mkdir()
    (root / "broken" / "SKILL.md").write_text(
        "---\nname: [unterminated\n---\nbody", encoding="utf-8", newline=""
    )
    result = json.loads((await invoke("skills_list", {})).content)
    assert result["total"] == 1
    assert result["errors"][0]["path"] == "broken/SKILL.md"


async def test_unknown_effect_blocks_fresh_call_id(setup):
    _, _, store, _, ctx, invoke = setup
    store.begin_effect(
        "run", "crashed", "skill_manage", {}, (task_skill_resource("ctx"),), ctx.input_seq
    )
    result = await invoke(
        "skill_manage", {"action": "create", "name": "new", "content": skill("new")}
    )
    assert result.status == "not_executed"


async def test_support_skill_md_not_auto_discovered(setup):
    root, _, _, _, _, invoke = setup
    (root / "analyze" / "references" / "SKILL.md").write_text(
        skill("hidden"), encoding="utf-8", newline=""
    )
    result = json.loads((await invoke("skills_list", {})).content)
    assert result["total"] == 1


async def test_existing_supporting_file_requires_hash(setup):
    _, _, _, _, _, invoke = setup
    await invoke("skill_manage", {"action": "create", "name": "new", "content": skill("new")})
    args = {
        "action": "write_file",
        "name": "new",
        "file_path": "scripts/run.py",
        "content": "print('one')",
    }
    assert (await invoke("skill_manage", args)).status == "succeeded"

    args["content"] = "print('two')"
    assert (await invoke("skill_manage", args)).status == "not_executed"
    args["expected_sha256"] = hashlib.sha256(b"print('one')").hexdigest()
    assert (await invoke("skill_manage", args)).status == "succeeded"


async def test_write_then_verification_io_failure_remains_unknown(setup, monkeypatch):
    _, workspaces, _, _, _, invoke = setup
    original = module._bytes

    def fail_verification(path, limit):
        if ".runtime-skills" in str(path) and path.name == "SKILL.md":
            raise PermissionError("Synthetic read failure after write")
        return original(path, limit)

    monkeypatch.setattr(module, "_bytes", fail_verification)
    result = await invoke(
        "skill_manage", {"action": "create", "name": "new", "content": skill("new")}
    )
    assert result.status == "unknown"
    assert (workspaces / "ctx" / ".runtime-skills" / "new" / "SKILL.md").exists()
    monkeypatch.setattr(module, "_bytes", original)
    blocked = await invoke(
        "skill_manage", {"action": "create", "name": "another", "content": skill("another")}
    )
    assert blocked.status == "not_executed"


async def test_readiness_optional_and_legacy_declarations(setup):
    root, _, _, _, _, invoke = setup
    extra = "required_environment_variables:\n  - name: OPTIONAL\n    optional: true\nprerequisites:\n  env_vars: LEGACY_KEY\nsetup:\n  collect_secrets:\n    env_var: SETUP_KEY\n"
    (root / "analyze" / "SKILL.md").write_text(skill(extra=extra), encoding="utf-8", newline="")
    data = json.loads((await invoke("skill_view", {"name": "analyze"})).content)
    assert data["missing_environment_variables"] == ["SETUP_KEY", "LEGACY_KEY"]


async def test_mutation_guard_and_shared_resource_prevent_execution_bypass(setup):
    _, workspaces, store, _, ctx, _ = setup
    tools = ToolExecutor(store)
    called = []

    def guard():
        called.append(True)
        raise PermissionError("Execution requires reconciliation")

    register_runtime_skills(
        tools,
        [],
        workspace_for=lambda cid: workspaces / cid,
        mutation_guard=guard,
        mutation_resources=("execution:local_host",),
    )
    args = json.dumps({"action": "create", "name": "new", "content": skill("new")})
    denied = await tools.execute(ToolCall("no-resource", "skill_manage", args), ctx)
    assert denied.status == "not_executed" and not called
    authorized = replace(
        ctx,
        run=replace(
            ctx.run,
            access=replace(
                ctx.run.access,
                allowed_resources=ctx.run.access.allowed_resources | {"execution:local_host"},
            ),
        ),
    )
    denied = await tools.execute(ToolCall("guarded", "skill_manage", args), authorized)
    assert denied.status == "not_executed" and called == [True]

    assert not (workspaces / "ctx" / ".runtime-skills" / "new").exists()
    store.begin_effect(
        "run", "terminal-crash", "terminal", {}, ("execution:local_host",), ctx.input_seq
    )
    denied = await tools.execute(ToolCall("after-terminal-crash", "skill_manage", args), authorized)
    assert denied.status == "not_executed" and called == [True]


async def test_library_import_and_real_script_execution_in_task_workspace(setup):
    root, workspaces, _, runtime, _, invoke = setup
    runtime.workspace_display_root = "/workspace"
    package = root / "analyze"
    (package / "scripts").mkdir()
    (package / "assets").mkdir()
    (package / "assets" / "input.txt").write_text("真实执行结果", encoding="utf-8")
    (package / "assets" / "raw.bin").write_bytes(b"\x00\xff\x01")
    (package / "scripts" / "run.py").write_text(
        "from pathlib import Path\n"
        "text=(Path(__file__).resolve().parent.parent/'assets/input.txt').read_text(encoding='utf-8')\n"
        "Path('result.txt').write_text(text,encoding='utf-8')\n"
        "print('executed')\n",
        encoding="utf-8",
    )
    result = await invoke(
        "skill_manage", {"action": "import_library", "source": "library:analyze", "name": "copied"}
    )
    assert result.status == "succeeded", result.content
    data = json.loads(result.content)
    assert data["skill_dir"] == "/workspace/.runtime-skills/copied"
    assert data["source_id"] == "library:analyze" and data["source"] == "pinned:test"
    workspace = workspaces / "ctx"
    assert not (workspace / "result.txt").exists()
    audit = json.loads((workspace / data["provenance_ref"]).read_text(encoding="utf-8"))
    assert audit["context_id"] == "ctx"
    for item in audit["files"]:
        copied = workspace / data["workspace_relative_path"] / item["path"]
        assert copied.read_bytes() == (package / item["path"]).read_bytes()
        assert hashlib.sha256(copied.read_bytes()).hexdigest() == item["sha256"]
    process = subprocess.run(
        [sys.executable, ".runtime-skills/copied/scripts/run.py"],
        cwd=workspace,
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    assert process.stdout.strip() == "executed"
    assert (workspace / "result.txt").read_text(encoding="utf-8") == "真实执行结果"
    viewed = json.loads((await invoke("skill_view", {"name": "task:copied"})).content)
    assert viewed["skill_dir"] == "/workspace/.runtime-skills/copied"
    assert viewed["workspace_relative_path"] == ".runtime-skills/copied"
    library = json.loads((await invoke("skill_view", {"name": "library:analyze"})).content)
    assert "import_library" in library["execution_hint"]


async def test_task_display_root_callback_preserves_source_hash(setup):
    _, _, _, runtime, _, invoke = setup
    runtime.workspace_display_root = lambda cid: "/visible/" + cid
    await invoke("skill_manage", {"action": "create", "name": "new", "content": skill("new")})
    result = json.loads((await invoke("skill_view", {"name": "task:new"})).content)
    assert result["skill_dir"] == "/visible/ctx/.runtime-skills/new"
    assert result["source"] == "task:ctx"
    assert result["sha256"] == hashlib.sha256(skill("new").encode()).hexdigest()


@pytest.mark.parametrize("resources", [{"skills:root:library"}, {"skills:task:ctx"}])
async def test_import_requires_source_and_destination_grants(setup, resources):
    _, workspaces, _, _, ctx, invoke = setup
    restricted = replace(
        ctx,
        run=replace(
            ctx.run, access=replace(ctx.run.access, allowed_resources=frozenset(resources))
        ),
    )
    result = await invoke(
        "skill_manage", {"action": "copy", "source": "library:analyze", "name": "new"}, restricted
    )
    assert result.status == "not_executed"
    assert not (workspaces / "ctx" / ".runtime-skills" / "new").exists()


@pytest.mark.parametrize("kind", ["count", "total"])
async def test_import_limits_refuse_whole_package_before_mutation(setup, kind):
    _, workspaces, _, runtime, _, invoke = setup
    if kind == "count":
        runtime.max_package_files = 1
    else:
        runtime.max_package_bytes = 1
    result = await invoke(
        "skill_manage", {"action": "copy", "source": "library:analyze", "name": "new"}
    )
    assert result.status == "not_executed"
    assert not (workspaces / "ctx" / ".runtime-skills" / "new").exists()


async def test_import_refuses_linked_package_reference(setup, tmp_path):
    root, workspaces, _, _, _, invoke = setup
    outside = tmp_path / "outside"
    outside.write_text("outside", encoding="utf-8")
    try:
        (root / "analyze" / "references" / "linked").symlink_to(outside)
    except OSError:
        pytest.skip("Host does not allow symlinks")
    result = await invoke(
        "skill_manage", {"action": "import_library", "source": "library:analyze", "name": "new"}
    )
    assert result.status == "not_executed"
    assert not (workspaces / "ctx" / ".runtime-skills" / "new").exists()


async def test_import_refuses_existing_target_even_when_empty(setup):
    _, workspaces, _, _, _, invoke = setup
    target = workspaces / "ctx" / ".runtime-skills" / "existing"
    target.mkdir(parents=True)
    result = await invoke(
        "skill_manage",
        {"action": "import_library", "source": "library:analyze", "name": "existing"},
    )
    assert result.status == "not_executed" and list(target.iterdir()) == []


async def test_partial_import_keeps_unknown_evidence_and_blocks_replay(setup, monkeypatch):
    _, workspaces, store, _, _, invoke = setup

    def fail_publication(*args, **kwargs):
        raise OSError("Synthetic publication IO failure")

    monkeypatch.setattr(module.os, "link", fail_publication)
    result = await invoke(
        "skill_manage", {"action": "import_library", "source": "library:analyze", "name": "partial"}
    )
    assert result.status == "unknown"
    assert "partial" in result.content and "Reconcile" in result.content
    target = workspaces / "ctx" / ".runtime-skills" / "partial"
    assert (target / "references" / "guide.md").exists()
    assert not (target / "SKILL.md").exists()
    assert list((target.parent / ".history" / "imports").glob("*.json"))
    assert store.list_effects()[-1]["status"] == "unknown"
    repeat = await invoke(
        "skill_manage", {"action": "copy", "source": "library:analyze", "name": "new-name"}
    )
    assert repeat.status == "not_executed"
