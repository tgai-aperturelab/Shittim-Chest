from dataclasses import replace
from pathlib import Path

import pytest

from shittim_core.domain.types import AccessContext, ConversationAddress, InputEvent, StateConflict
from shittim_core.materials import MaterialStore
from shittim_core.materials.names import sanitize_name
from shittim_core.state import StateStore


@pytest.fixture
def setup(tmp_path):
    db = StateStore(tmp_path / "state.db")
    address = ConversationAddress("bot", "group", "g")
    access = AccessContext("owner", address.key, allowed_resources=frozenset({"materials:c"}))
    event = InputEvent(address, "owner", "m", "fixture")
    db.create_context("c", event, access)
    material = MaterialStore(tmp_path / "materials", db, "c", max_bytes=1024)
    yield db, material, access
    db.close()


async def test_new_version_preserves_original_bytes_and_sources(setup):
    db, material, access = setup
    first = await material.add_bytes(b"first", "../a.txt", access, source_ref="input:m")
    second = await material.add_bytes(
        b"second",
        "a.txt",
        access,
        artifact_id=first.artifact_id,
        expected_version=1,
        source_ref="input:n",
    )
    assert second.version == 2
    assert (await material.read(first.artifact_id, access, version=1)).data == b"first"
    assert (await material.read(first.artifact_id, access)).data == b"second"
    assert db.get_artifact(first.artifact_id, version=1)["source_ref"] == "input:m"
    assert len(list(material.root.iterdir())) == 2


async def test_denied_scope_and_capability_never_returns_bytes(setup):
    _, material, access = setup
    ref = await material.add_bytes(b"private", "secret.txt", access)
    for denied in (
        replace(access, audience="another-group"),
        replace(access, allowed_resources=frozenset()),
        replace(access, policy_version=99),
    ):
        result = await material.read(ref.artifact_id, denied)
        assert result.status == "denied"
        assert result.data is None
    specific = replace(access, allowed_resources=frozenset({"artifact:" + ref.artifact_id}))
    assert (await material.read(ref.artifact_id, specific)).data == b"private"


async def test_other_context_cannot_read_even_with_same_audience(setup):
    db, material, access = setup
    ref = await material.add_bytes(b"private", "a.txt", access)
    event = InputEvent(ConversationAddress("bot", "group", "g"), "owner", "m2", "other")
    other_access = replace(access, allowed_resources=frozenset({"materials:other"}))
    db.create_context("other", event, other_access)
    other = MaterialStore(material.root, db, "other")
    assert (await other.read(ref.artifact_id, other_access)).status == "denied"


async def test_changed_content_is_integrity_failure(setup):
    db, material, access = setup
    ref = await material.add_bytes(b"original", "a.txt", access)
    Path(db.get_artifact(ref.artifact_id)["path"]).write_bytes(b"tampered")
    result = await material.read(ref.artifact_id, access)
    assert result.status == "integrity_error"
    assert result.data is None


async def test_metadata_outside_root_is_not_a_file_capability(setup, tmp_path):
    import hashlib

    db, material, access = setup
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"outside")
    db.register_artifact(
        "outside",
        "c",
        "owner",
        access.audience,
        str(outside),
        hashlib.sha256(b"outside").hexdigest(),
    )
    result = await material.read("outside", access)
    assert result.status == "denied"
    assert result.data is None


async def test_final_symlink_is_refused(setup, tmp_path):
    import hashlib

    db, material, access = setup
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"outside")
    link = material.root / "linked.txt"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("Host does not grant symlink creation; run this case on capable host")
    db.register_artifact(
        "linked", "c", "owner", access.audience, str(link), hashlib.sha256(b"outside").hexdigest()
    )
    assert (await material.read("linked", access)).status == "denied"


async def test_stale_version_does_not_write_or_overwrite(setup):
    _, material, access = setup
    first = await material.add_bytes(b"first", "a.txt", access)
    await material.add_bytes(
        b"second", "a.txt", access, artifact_id=first.artifact_id, expected_version=1
    )
    with pytest.raises(StateConflict):
        await material.add_bytes(
            b"stale", "a.txt", access, artifact_id=first.artifact_id, expected_version=1
        )
    assert (await material.read(first.artifact_id, access)).data == b"second"
    assert len(list(material.root.iterdir())) == 2


async def test_oversize_rejected_before_file_or_metadata(setup):
    _, material, access = setup
    with pytest.raises(ValueError):
        await material.add_bytes(b"x" * 1025, "a.txt", access)
    assert list(material.root.iterdir()) == []


async def test_missing_file_returns_unavailable_not_read(setup):
    db, material, access = setup
    ref = await material.add_bytes(b"first", "a.txt", access)
    Path(db.get_artifact(ref.artifact_id)["path"]).unlink()
    result = await material.read(ref.artifact_id, access)
    assert result.status == "unavailable"
    assert result.data is None


async def test_concurrent_updates_only_commit_one_successor(setup):
    import asyncio

    _, material, access = setup
    first = await material.add_bytes(b"original", "a.txt", access)
    results = await asyncio.gather(
        material.add_bytes(
            b"one", "a.txt", access, artifact_id=first.artifact_id, expected_version=1
        ),
        material.add_bytes(
            b"two", "a.txt", access, artifact_id=first.artifact_id, expected_version=1
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(result, StateConflict) for result in results) == 1
    assert (await material.read(first.artifact_id, access, version=1)).data == b"original"
    assert len(list(material.root.iterdir())) == 2


async def test_registration_failure_cleans_only_its_new_file(setup, monkeypatch):
    db, material, access = setup
    first = await material.add_bytes(b"original", "a.txt", access)
    original_path = Path(db.get_artifact(first.artifact_id)["path"])

    def fail(*args, **kwargs):
        raise StateConflict("fixture database failure")

    monkeypatch.setattr(db, "register_artifact", fail)
    with pytest.raises(StateConflict):
        await material.add_bytes(
            b"candidate", "a.txt", access, artifact_id=first.artifact_id, expected_version=1
        )
    assert original_path.read_bytes() == b"original"
    assert list(material.root.iterdir()) == [original_path]


async def test_read_keeps_display_name_and_revalidates_size(setup):
    db, material, access = setup
    first = await material.add_bytes(b"original", "../../a.txt", access)
    result = await material.read(first.artifact_id, access)
    assert result.ref.name == "a.txt"
    Path(db.get_artifact(first.artifact_id)["path"]).write_bytes(b"x" * 1025)
    assert (await material.read(first.artifact_id, access)).status == "unavailable"


@pytest.mark.parametrize("name", ["../../a.txt", r"C:\outside\a.txt", "\x00", "..", "a:b?.txt"])
def test_astrbot_filename_projection_is_only_a_safe_label(name):
    result = sanitize_name(name)
    assert result not in {"", ".", ".."}
    assert not any(c in result for c in '\\/:*?"<>|\x00')
