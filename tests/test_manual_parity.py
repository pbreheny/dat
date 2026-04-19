from pathlib import Path

from dat import needs_push, read_inventory, read_config, take_inventory


def test_needs_push_matches_existing_manual_test(monkeypatch):
    """Automated parity test for the legacy tests/dat script's needs_push check."""
    repo_root = Path(__file__).resolve().parents[1]
    fixture_dir = repo_root / "tests" / "obj" / "1"

    monkeypatch.chdir(fixture_dir)

    config = read_config()
    current = take_inventory(config)
    local = read_inventory("../local.txt")

    expected = ["a.txt", "c.txt", "d.txt", "g.txt", "h.txt", "j.txt", "k.txt"]
    assert sorted(needs_push(current, local)) == expected
