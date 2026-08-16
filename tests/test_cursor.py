"""Cursor store tests: atomic commit, load semantics, corrupt-file refusal."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from multiedge_relay import CursorCorruptError, FileCursorStore


def test_load_missing_returns_none(cursor_root: Path) -> None:
    store = FileCursorStore(root=cursor_root)
    assert store.load("strat-a") is None


def test_commit_then_load_round_trip(cursor_root: Path) -> None:
    store = FileCursorStore(root=cursor_root)
    store.commit("strat-a", 41)
    store.commit("strat-a", 42)
    assert store.load("strat-a") == 42
    assert store.load("strat-b") is None


def test_commit_leaves_no_temp_files(cursor_root: Path) -> None:
    store = FileCursorStore(root=cursor_root)
    store.commit("strat-a", 1)
    files = [p.name for p in cursor_root.iterdir()]
    assert files == ["strat-a.json"]


def test_failed_replace_keeps_previous_cursor(
    cursor_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = FileCursorStore(root=cursor_root)
    store.commit("strat-a", 10)

    real_replace = os.replace

    def broken_replace(src: str, dst: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", broken_replace)
    with pytest.raises(OSError):
        store.commit("strat-a", 11)
    monkeypatch.setattr(os, "replace", real_replace)

    assert store.load("strat-a") == 10  # old value intact — write was atomic


@pytest.mark.parametrize(
    "content",
    [
        "not json at all",
        "{}",
        '{"sequence": "not-an-int"}',
        '{"sequence": -5}',
        '{"other": 1}',
        "",
    ],
)
def test_corrupt_cursor_raises_never_resets(cursor_root: Path, content: str) -> None:
    store = FileCursorStore(root=cursor_root)
    store.commit("strat-a", 5)
    path = cursor_root / "strat-a.json"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(CursorCorruptError):
        store.load("strat-a")
    # the corrupt file is untouched — never silently reset
    assert path.read_text(encoding="utf-8") == content


def test_strategy_ids_are_sanitized_to_safe_filenames(cursor_root: Path) -> None:
    store = FileCursorStore(root=cursor_root)
    store.commit("strat/../../evil", 3)
    for p in cursor_root.rglob("*"):
        assert cursor_root in p.parents  # nothing escaped the root
    assert store.load("strat/../../evil") == 3
