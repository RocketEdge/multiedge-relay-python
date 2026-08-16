"""CLI tests: direct main([...]) invocation for dlq and cursor subcommands."""

from __future__ import annotations

from pathlib import Path

import pytest
from fake_relay import API_KEY, FakeRelay, SyncASGITransport

import multiedge_relay.cli as cli_module
from multiedge_relay import DiskDLQ, FileCursorStore, Signal
from multiedge_relay.cli import main


def seed_dlq(dlq_root: Path, n: int = 2, strategy: str = "strat-a") -> DiskDLQ:
    dlq = DiskDLQ(root=dlq_root)
    for i in range(n):
        dlq.append(
            Signal(strategy_id=strategy, payload={"n": i}, client_signal_id=f"c{i}"),
            error="relay unavailable",
            attempts=5,
        )
    return dlq


# ----------------------------------------------------------------------- dlq
def test_dlq_list_empty(dlq_root: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["dlq", "list", "--root", str(dlq_root)]) == 0
    assert "no pending" in capsys.readouterr().out.lower()


def test_dlq_list_shows_entries(dlq_root: Path, capsys: pytest.CaptureFixture[str]) -> None:
    seed_dlq(dlq_root)
    assert main(["dlq", "list", "--root", str(dlq_root)]) == 0
    out = capsys.readouterr().out
    assert "strat-a" in out
    assert out.count("c0") == 1
    assert out.count("c1") == 1


def test_dlq_purge(dlq_root: Path, capsys: pytest.CaptureFixture[str]) -> None:
    seed_dlq(dlq_root, n=3)
    assert main(["dlq", "purge", "--root", str(dlq_root)]) == 0
    assert "3" in capsys.readouterr().out
    assert list(DiskDLQ(root=dlq_root).pending()) == []


def test_dlq_resend_dry_run(
    dlq_root: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    seed_dlq(dlq_root, n=2)
    monkeypatch.setenv("MULTIEDGE_API_KEY", API_KEY)
    assert main(["dlq", "resend", "--dry-run", "--root", str(dlq_root)]) == 0
    out = capsys.readouterr().out.lower()
    assert "2" in out
    assert len(list(DiskDLQ(root=dlq_root).pending())) == 2  # untouched


def test_dlq_resend_against_fake_relay(
    dlq_root: Path,
    relay: FakeRelay,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_dlq(dlq_root, n=2)
    monkeypatch.setenv("MULTIEDGE_API_KEY", API_KEY)
    monkeypatch.setattr(cli_module, "_build_transport", lambda: SyncASGITransport(relay.app))
    assert main(["dlq", "resend", "--root", str(dlq_root)]) == 0
    out = capsys.readouterr().out.lower()
    assert "resent" in out
    assert list(DiskDLQ(root=dlq_root).pending()) == []
    assert len(relay.signals["strat-a"]) == 2


def test_dlq_resend_requires_api_key(
    dlq_root: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MULTIEDGE_API_KEY", raising=False)
    seed_dlq(dlq_root)
    assert main(["dlq", "resend", "--root", str(dlq_root)]) != 0
    assert "api key" in (capsys.readouterr().err + capsys.readouterr().out).lower()


# ----------------------------------------------------------------------- cursor
def test_cursor_show_empty(cursor_root: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["cursor", "show", "--root", str(cursor_root)]) == 0
    assert "no cursors" in capsys.readouterr().out.lower()


def test_cursor_show_lists_strategies(
    cursor_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = FileCursorStore(root=cursor_root)
    store.commit("strat-a", 41)
    store.commit("strat-b", 7)
    assert main(["cursor", "show", "--root", str(cursor_root)]) == 0
    out = capsys.readouterr().out
    assert "strat-a" in out and "41" in out
    assert "strat-b" in out and "7" in out


def test_cursor_reset_requires_to(cursor_root: Path) -> None:
    with pytest.raises(SystemExit):
        main(["cursor", "reset", "--strategy", "strat-a", "--root", str(cursor_root)])


def test_cursor_reset_prints_change(cursor_root: Path, capsys: pytest.CaptureFixture[str]) -> None:
    FileCursorStore(root=cursor_root).commit("strat-a", 10)
    assert (
        main(["cursor", "reset", "--strategy", "strat-a", "--to", "5", "--root", str(cursor_root)])
        == 0
    )
    out = capsys.readouterr().out
    assert "10" in out and "5" in out  # old -> new
    assert FileCursorStore(root=cursor_root).load("strat-a") == 5
