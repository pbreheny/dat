"""Tests for the pure-logic functions (no S3 / no network)."""
import copy
from pathlib import Path

import pytest

import dat as dat_module
from dat import (
    needs_kill,
    needs_pull,
    needs_purge,
    needs_push,
    read_inventory,
    resolve_kill_conflicts,
    resolve_pull_conflicts,
    resolve_purge_conflicts,
    resolve_push_conflicts,
    take_inventory,
)

OBJ = Path(__file__).resolve().parent / "obj"


@pytest.fixture(scope="module")
def inventories():
    current = take_inventory({}, root=OBJ / "1")
    local   = read_inventory(OBJ / "local.txt")
    master  = take_inventory({}, root=OBJ / "2")
    return current, local, master


# ---------------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------------

def test_needs_push(inventories):
    current, local, _ = inventories
    result = sorted(needs_push(current, local))
    assert result == ["a.txt", "c.txt", "d.txt", "g.txt", "h.txt", "j.txt", "k.txt"]


def test_needs_pull(inventories):
    _, local, master = inventories
    result = sorted(needs_pull(master, local))
    assert result == ["b.txt", "c.txt", "d.txt", "f.txt", "g.txt", "h.txt", "m.txt"]


def test_needs_purge(inventories):
    current, local, _ = inventories
    result = sorted(needs_purge(current, local))
    assert result == ["e.txt", "f.txt", "l.txt"]


def test_needs_kill(inventories):
    _, local, master = inventories
    result = sorted(needs_kill(master, local))
    assert result == ["i.txt", "j.txt", "l.txt"]


# ---------------------------------------------------------------------------
# Conflict resolution  (copies protect the shared fixture from mutation)
# ---------------------------------------------------------------------------

def test_resolve_push_conflicts(inventories):
    current, local, master = inventories
    push = needs_push(current, local)
    conflict, resolved = resolve_push_conflicts(
        current, copy.copy(local), copy.copy(master), push
    )
    assert sorted(conflict) == ["c.txt", "h.txt"]
    assert sorted(resolved) == ["d.txt", "g.txt"]


def test_resolve_purge_conflicts(inventories):
    current, local, master = inventories
    purge = needs_purge(current, local)
    conflict, resolved = resolve_purge_conflicts(
        copy.copy(master), copy.copy(local), purge
    )
    assert sorted(conflict) == ["f.txt"]
    assert sorted(resolved) == ["l.txt"]


def test_resolve_pull_conflicts(inventories):
    current, local, master = inventories
    pull = needs_pull(master, local)
    conflict, resolved = resolve_pull_conflicts(
        current, copy.copy(local), master, pull
    )
    assert sorted(conflict) == ["c.txt", "f.txt", "h.txt"]
    assert sorted(resolved) == ["d.txt", "g.txt"]


def test_resolve_kill_conflicts(inventories):
    current, local, master = inventories
    kill = needs_kill(master, local)
    conflict, resolved = resolve_kill_conflicts(
        current, copy.copy(local), kill
    )
    assert sorted(conflict) == ["j.txt"]
    assert sorted(resolved) == ["l.txt"]


# ---------------------------------------------------------------------------
# _is_ignored
# ---------------------------------------------------------------------------

_is_ignored = dat_module._is_ignored


def test_ignore_exact_file():
    assert _is_ignored("notes.txt", ["notes.txt"])


def test_ignore_glob():
    assert _is_ignored("report.pdf", ["*.pdf"])
    assert not _is_ignored("report.txt", ["*.pdf"])


def test_ignore_directory_matches_direct_child():
    assert _is_ignored("data/file.csv", ["data"])


def test_ignore_directory_matches_nested_file():
    assert _is_ignored("data/subdir/file.csv", ["data"])


def test_ignore_directory_does_not_match_sibling():
    assert not _is_ignored("dataset/file.csv", ["data"])


def test_ignore_directory_with_trailing_path():
    assert _is_ignored("raw/2024/data.csv", ["raw"])


def test_negation_unignores_specific_file():
    patterns = ["data", "!data/keep.csv"]
    assert _is_ignored("data/drop.csv", patterns)
    assert not _is_ignored("data/keep.csv", patterns)


def test_negation_order_matters():
    # negation before the positive pattern — file ends up ignored
    patterns = ["!data/keep.csv", "data"]
    assert _is_ignored("data/keep.csv", patterns)


def test_no_patterns_ignores_nothing():
    assert not _is_ignored("anything.txt", [])


def test_take_inventory_respects_directory_ignore(tmp_path):
    """Integration: a directory pattern excludes all files beneath it."""
    dat_dir = tmp_path / ".dat"
    dat_dir.mkdir()
    (dat_dir / "ignore").write_text("raw\n")

    (tmp_path / "keep.txt").write_bytes(b"a")
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "drop.csv").write_bytes(b"b")
    (raw / "sub").mkdir()
    (raw / "sub" / "also_drop.csv").write_bytes(b"c")

    inv = take_inventory({}, root=tmp_path)

    assert "keep.txt" in inv
    assert "raw/drop.csv" not in inv
    assert "raw/sub/also_drop.csv" not in inv


def test_take_inventory_respects_negation(tmp_path):
    """Integration: negation pattern rescues a specific file from an ignored dir."""
    dat_dir = tmp_path / ".dat"
    dat_dir.mkdir()
    (dat_dir / "ignore").write_text("raw\n!raw/keep.csv\n")

    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "drop.csv").write_bytes(b"a")
    (raw / "keep.csv").write_bytes(b"b")

    inv = take_inventory({}, root=tmp_path)

    assert "raw/keep.csv" in inv
    assert "raw/drop.csv" not in inv
