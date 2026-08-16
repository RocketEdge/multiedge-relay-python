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


def test_commit_retries_replace_on_permission_error(
    cursor_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Windows: a concurrent reader briefly holding the file open makes os.replace
    # throw PermissionError; commit must retry with backoff instead of crashing.
    sleeps: list[float] = []
    store = FileCursorStore(root=cursor_root, sleep=sleeps.append)
    store.commit("strat-a", 1)

    real_replace = os.replace
    failures = {"remaining": 2}

    def flaky_replace(src: str, dst: str) -> None:
        if failures["remaining"] > 0:
            failures["remaining"] -= 1
            raise PermissionError("file is in use by another process")
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", flaky_replace)
    store.commit("strat-a", 2)

    assert store.load("strat-a") == 2
    assert sleeps == [0.02, 0.02]  # one backoff per PermissionError


def test_commit_raises_permission_error_after_exhaustion(
    cursor_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sleeps: list[float] = []
    store = FileCursorStore(root=cursor_root, sleep=sleeps.append)
    store.commit("strat-a", 10)

    def always_locked(src: str, dst: str) -> None:
        raise PermissionError("file is in use by another process")

    monkeypatch.setattr(os, "replace", always_locked)
    with pytest.raises(PermissionError):
        store.commit("strat-a", 11)

    assert sleeps == [0.02] * 4  # 5 attempts -> 4 backoffs
    monkeypatch.undo()
    assert store.load("strat-a") == 10  # previous cursor intact


def test_commit_does_not_retry_other_os_errors(
    cursor_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sleeps: list[float] = []
    store = FileCursorStore(root=cursor_root, sleep=sleeps.append)

    def disk_full(src: str, dst: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", disk_full)
    with pytest.raises(OSError, match="disk full"):
        store.commit("strat-a", 1)
    assert sleeps == []  # non-PermissionError is not retried


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
