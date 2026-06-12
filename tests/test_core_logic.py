"""Tests for the pure-logic functions (no S3 / no network)."""
import copy
from pathlib import Path

import pytest

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
