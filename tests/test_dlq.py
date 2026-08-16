"""Disk DLQ tests: append/pending round-trip, resend semantics, purge."""

from __future__ import annotations

import json
from pathlib import Path

from fake_relay import API_KEY, FakeRelay, SyncASGITransport

from multiedge_relay import DiskDLQ, Signal, SignalPublisher


def make_signal(n: int = 1, strategy: str = "strat-a") -> Signal:
    return Signal(strategy_id=strategy, payload={"n": n}, client_signal_id=f"csid-{n:04d}")


def test_append_writes_one_jsonl_per_strategy_per_day(dlq_root: Path) -> None:
    dlq = DiskDLQ(root=dlq_root)
    path1 = dlq.append(make_signal(1), error="boom", attempts=5)
    path2 = dlq.append(make_signal(2), error="boom again", attempts=5)
    path3 = dlq.append(make_signal(3, strategy="strat-b"), error="x", attempts=5)

    assert path1 == path2  # same strategy, same day -> same file
    assert path1 != path3
    assert path1.suffix == ".jsonl"
    lines = path1.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    entry = json.loads(lines[0])
    assert entry["signal"]["strategy_id"] == "strat-a"
    assert entry["error"] == "boom"
    assert entry["attempts"] == 5


def test_pending_round_trip(dlq_root: Path) -> None:
    dlq = DiskDLQ(root=dlq_root)
    dlq.append(make_signal(1), error="e1", attempts=5)
    dlq.append(make_signal(2, strategy="strat-b"), error="e2", attempts=3)

    all_entries = list(dlq.pending())
    assert len(all_entries) == 2
    only_b = list(dlq.pending("strat-b"))
    assert len(only_b) == 1
    assert only_b[0].signal.strategy_id == "strat-b"
    assert only_b[0].signal.payload == {"n": 2}
    assert only_b[0].attempts == 3


def test_pending_empty_when_no_root(dlq_root: Path) -> None:
    dlq = DiskDLQ(root=dlq_root / "never-created")
    assert list(dlq.pending()) == []


def test_resend_success_removes_entries(dlq_root: Path, relay: FakeRelay) -> None:
    dlq = DiskDLQ(root=dlq_root)
    dlq.append(make_signal(1), error="e", attempts=5)
    dlq.append(make_signal(2), error="e", attempts=5)
    publisher = SignalPublisher(
        api_key=API_KEY, dlq=dlq, transport=SyncASGITransport(relay.app), sleep=lambda _: None
    )

    report = dlq.resend(publisher)

    assert report.attempted == 2
    assert report.resent == 2
    assert report.failed == 0
    assert list(dlq.pending()) == []
    assert len(relay.signals["strat-a"]) == 2


def test_resend_dry_run_touches_nothing(dlq_root: Path, relay: FakeRelay) -> None:
    dlq = DiskDLQ(root=dlq_root)
    dlq.append(make_signal(1), error="e", attempts=5)
    publisher = SignalPublisher(
        api_key=API_KEY, dlq=dlq, transport=SyncASGITransport(relay.app), sleep=lambda _: None
    )

    report = dlq.resend(publisher, dry_run=True)

    assert report.dry_run is True
    assert report.attempted == 1
    assert report.resent == 0
    assert len(list(dlq.pending())) == 1
    assert "strat-a" not in relay.signals


def test_resend_failures_stay_in_dlq_without_duplicates(dlq_root: Path, relay: FakeRelay) -> None:
    dlq = DiskDLQ(root=dlq_root)
    dlq.append(make_signal(1), error="e", attempts=5)
    dlq.append(make_signal(2), error="e", attempts=5)
    publisher = SignalPublisher(
        api_key=API_KEY,
        dlq=dlq,
        transport=SyncASGITransport(relay.app),
        sleep=lambda _: None,
        max_attempts=2,
    )
    relay.fail_next(10, 503)  # both resends exhaust retries

    report = dlq.resend(publisher)

    assert report.attempted == 2
    assert report.resent == 0
    assert report.failed == 2
    remaining = list(dlq.pending())
    assert len(remaining) == 2  # kept exactly once each — no duplicate DLQ entries
    assert publisher.dlq is dlq  # resend restored the publisher's DLQ


def test_purge_removes_entries(dlq_root: Path) -> None:
    dlq = DiskDLQ(root=dlq_root)
    dlq.append(make_signal(1), error="e", attempts=5)
    dlq.append(make_signal(2, strategy="strat-b"), error="e", attempts=5)

    removed = dlq.purge()

    assert removed == 2
    assert list(dlq.pending()) == []
