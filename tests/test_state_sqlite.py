"""SqliteStateStore tests: cursor protocol, exactly-once dedup, tx-join, prune, corruption."""

from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import pytest
from fake_relay import FakeRelay
from test_subscriber import STRATEGY, Collector, make_subscriber, seed

from multiedge_relay import (
    CursorCorruptError,
    CursorStore,
    ReceivedSignal,
    SignalMeta,
    SqliteStateStore,
    StateStoreCorruptError,
)

PUBLISHED_AT = datetime(2026, 8, 17, 12, 0, 0, tzinfo=UTC)


def make_signal(
    sequence: int, *, signal_id: str | None = None, strategy_id: str = STRATEGY
) -> ReceivedSignal:
    return ReceivedSignal(
        sequence=sequence,
        signal_id=signal_id if signal_id is not None else f"sig_{sequence:04d}",
        strategy_id=strategy_id,
        published_at=PUBLISHED_AT,
        payload={"n": sequence},
    )


def make_meta(
    signal: ReceivedSignal, source: Literal["catchup", "live", "gapfill"] = "live"
) -> SignalMeta:
    return SignalMeta(
        sequence=signal.sequence,
        signal_id=signal.signal_id,
        published_at=signal.published_at,
        source=source,
    )


# ------------------------------------------------------------------- exceptions
def test_state_corrupt_is_a_cursor_corrupt_error() -> None:
    # Subclassing keeps a corrupt state DB inside the subscriber's fatal set.
    assert issubclass(StateStoreCorruptError, CursorCorruptError)


# ------------------------------------------------------------------- open + CursorStore
def test_open_creates_db_and_parent_dirs(tmp_path: Path) -> None:
    path = tmp_path / "deep" / "nested" / "state.db"
    with SqliteStateStore(path) as store:
        assert path.exists()
        assert store.load(STRATEGY) is None


def test_load_missing_returns_none(state_path: Path) -> None:
    with SqliteStateStore(state_path) as store:
        assert store.load("never-seen") is None


def test_commit_then_load_round_trip(state_path: Path) -> None:
    with SqliteStateStore(state_path) as store:
        store.commit("a", 3)
        store.commit("b", 7)
        store.commit("a", 5)
        assert store.load("a") == 5
        assert store.load("b") == 7


def test_commit_survives_reopen(state_path: Path) -> None:
    with SqliteStateStore(state_path) as store:
        store.commit(STRATEGY, 42)
    with SqliteStateStore(state_path) as reopened:
        assert reopened.load(STRATEGY) == 42


def test_satisfies_cursor_store_protocol(state_path: Path) -> None:
    with SqliteStateStore(state_path) as store:
        typed: CursorStore = store  # mypy enforces the structural match
        assert typed.load(STRATEGY) is None


def test_corrupt_file_raises_and_is_left_untouched(state_path: Path) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    garbage = b"this is not a sqlite database, definitely"
    state_path.write_bytes(garbage)
    with pytest.raises(StateStoreCorruptError):
        SqliteStateStore(state_path)
    assert state_path.read_bytes() == garbage


def test_unknown_schema_version_raises(state_path: Path) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(state_path)
    conn.execute("PRAGMA user_version = 99")
    conn.execute("CREATE TABLE unrelated (x)")
    conn.close()
    with pytest.raises(StateStoreCorruptError):
        SqliteStateStore(state_path)


# ------------------------------------------------------------------- subscriber drop-in
def test_drops_into_subscriber_as_cursor_store(
    relay: FakeRelay, cursor_root: Path, state_path: Path
) -> None:
    seed(relay, 5)
    collector = Collector()
    with SqliteStateStore(state_path) as store:
        count = make_subscriber(relay, cursor_root, collector, cursor_store=store).catch_up_only()
        assert count == 5
        assert collector.sequences == [1, 2, 3, 4, 5]
        assert store.load(STRATEGY) == 5


# ------------------------------------------------------------------- dedup core
def test_process_fresh_then_duplicate(state_path: Path) -> None:
    signal = make_signal(1)
    with SqliteStateStore(state_path) as store:
        assert store.seen(signal.signal_id) is False
        with store.process(signal) as fresh:
            assert fresh is True
        assert store.seen(signal.signal_id) is True
        with store.process(signal) as fresh:
            assert fresh is False


def test_process_body_exception_rolls_back_marker(state_path: Path) -> None:
    signal = make_signal(1)
    with SqliteStateStore(state_path) as store:
        with pytest.raises(RuntimeError, match="boom"), store.process(signal) as fresh:
            assert fresh is True
            raise RuntimeError("boom")
        assert store.seen(signal.signal_id) is False
        with store.process(signal) as fresh:  # retry is fresh and succeeds
            assert fresh is True
        assert store.seen(signal.signal_id) is True


def test_sequence_below_cursor_watermark_counts_as_processed(state_path: Path) -> None:
    # The cursor row is the compressed form of "everything <= N is processed":
    # a replayed old signal must stay a duplicate even after its marker is pruned.
    with SqliteStateStore(state_path) as store:
        store.commit(STRATEGY, 10)
        old = make_signal(7, signal_id="sig_never_marked")
        with store.process(old) as fresh:
            assert fresh is False


# ------------------------------------------------------------------- exactly_once wrapper
def test_exactly_once_invokes_handler_once_per_signal_id(state_path: Path) -> None:
    signal = make_signal(1)
    collector = Collector()
    with SqliteStateStore(state_path) as store:
        wrapped = store.exactly_once(collector)
        wrapped(signal, make_meta(signal, "catchup"))
        wrapped(signal, make_meta(signal, "live"))  # returns normally, no re-invocation
        assert collector.deliveries == [(1, "catchup")]


def test_exactly_once_handler_error_rolls_back_and_reraises(state_path: Path) -> None:
    signal = make_signal(2)
    collector = Collector(fail_on={2})
    with SqliteStateStore(state_path) as store:
        wrapped = store.exactly_once(collector)
        with pytest.raises(RuntimeError, match="callback crash at 2"):
            wrapped(signal, make_meta(signal))
        assert store.seen(signal.signal_id) is False
        wrapped(signal, make_meta(signal))  # redelivery retries the handler
        assert collector.sequences == [2]


def test_exactly_once_on_duplicate_hook(state_path: Path) -> None:
    signal = make_signal(1)
    duplicates: list[tuple[int, str]] = []
    with SqliteStateStore(state_path) as store:
        wrapped = store.exactly_once(
            Collector(),
            on_duplicate=lambda s, m: duplicates.append((s.sequence, m.source)),
        )
        wrapped(signal, make_meta(signal, "catchup"))
        wrapped(signal, make_meta(signal, "live"))
        assert duplicates == [(1, "live")]


def test_duplicates_in_any_order_process_each_signal_once(state_path: Path) -> None:
    signals = [make_signal(n) for n in (1, 2, 3)]
    delivery_order = [signals[0], signals[1], signals[1], signals[2], signals[0], signals[2]]
    collector = Collector()
    with SqliteStateStore(state_path) as store:
        wrapped = store.exactly_once(collector)
        for signal in delivery_order:
            wrapped(signal, make_meta(signal))
        assert sorted(collector.sequences) == [1, 2, 3]


# ------------------------------------------------------------------- the headline crash
class LossyCursorStore:
    """Delegates to an inner store but silently drops commits for chosen sequences.

    Simulates a crash in the gap between callback success and cursor commit —
    the exact window where FileCursorStore alone re-invokes the callback.
    """

    def __init__(self, inner: SqliteStateStore, fail_on: set[int]) -> None:
        self.inner = inner
        self.fail_on = fail_on

    def load(self, strategy_id: str) -> int | None:
        return self.inner.load(strategy_id)

    def commit(self, strategy_id: str, sequence: int) -> None:
        if sequence in self.fail_on:
            self.fail_on.discard(sequence)
            raise OSError(f"simulated crash at cursor commit {sequence}")
        self.inner.commit(strategy_id, sequence)


def test_lost_cursor_commit_does_not_reinvoke_handler(
    relay: FakeRelay, cursor_root: Path, state_path: Path
) -> None:
    # Contrast with test_crash_mid_page_redelivers_only_uncommitted: there the
    # redelivered signal re-enters the callback; here the marker absorbs it.
    seed(relay, 5)
    first = Collector()
    store = SqliteStateStore(state_path)
    try:
        subscriber = make_subscriber(
            relay, cursor_root, first, cursor_store=LossyCursorStore(store, fail_on={3})
        )
        subscriber.on_signal = store.exactly_once(first)
        with pytest.raises(OSError, match="simulated crash at cursor commit 3"):
            subscriber.catch_up_only()
        assert first.sequences == [1, 2, 3]  # handler DID run for 3; marker committed
        assert store.load(STRATEGY) == 2  # cursor commit for 3 was "lost"
    finally:
        store.close()

    second = Collector()
    with SqliteStateStore(state_path) as restarted:
        subscriber = make_subscriber(relay, cursor_root, second, cursor_store=restarted)
        subscriber.on_signal = restarted.exactly_once(second)
        count = subscriber.catch_up_only()
        assert count == 3  # 3, 4, 5 redelivered by the subscriber...
        assert second.sequences == [4, 5]  # ...but 3 never re-enters the handler
        assert restarted.load(STRATEGY) == 5

    handled = sorted(first.sequences + second.sequences)
    assert handled == [1, 2, 3, 4, 5]  # every sequence handled exactly once overall


# ------------------------------------------------------------------- tx-joined writes
def test_exactly_once_tx_handler_writes_commit_atomically_with_marker(
    state_path: Path,
) -> None:
    signal = make_signal(1)
    with SqliteStateStore(state_path) as store:
        store.connection.execute("CREATE TABLE user_fills (signal_id TEXT PRIMARY KEY)")
        runs: list[int] = []

        def handler(s: ReceivedSignal, m: SignalMeta, cur: sqlite3.Cursor) -> None:
            runs.append(s.sequence)
            cur.execute("INSERT INTO user_fills (signal_id) VALUES (?)", (s.signal_id,))

        wrapped = store.exactly_once_tx(handler)
        wrapped(signal, make_meta(signal))
        wrapped(signal, make_meta(signal))  # duplicate: neither marker nor row again
        assert runs == [1]
        rows = store.connection.execute("SELECT signal_id FROM user_fills").fetchall()
        assert rows == [(signal.signal_id,)]


def test_exactly_once_tx_rolls_back_handler_writes_on_error(state_path: Path) -> None:
    signal = make_signal(1)
    with SqliteStateStore(state_path) as store:
        store.connection.execute("CREATE TABLE user_fills (signal_id TEXT PRIMARY KEY)")

        def handler(s: ReceivedSignal, m: SignalMeta, cur: sqlite3.Cursor) -> None:
            cur.execute("INSERT INTO user_fills (signal_id) VALUES (?)", (s.signal_id,))
            raise RuntimeError("handler crash after write")

        wrapped = store.exactly_once_tx(handler)
        with pytest.raises(RuntimeError, match="handler crash after write"):
            wrapped(signal, make_meta(signal))
        # True exactly-once for tx-joined state: row AND marker vanished together.
        assert store.connection.execute("SELECT COUNT(*) FROM user_fills").fetchone() == (0,)
        assert store.seen(signal.signal_id) is False


# ------------------------------------------------------------------- pruning
def _processed_count(store: SqliteStateStore) -> int:
    row = store.connection.execute("SELECT COUNT(*) FROM processed").fetchone()
    assert row is not None
    return int(row[0])


def test_cursor_commit_prunes_processed_up_to_watermark(state_path: Path) -> None:
    signals = [make_signal(n) for n in (1, 2, 3)]
    with SqliteStateStore(state_path) as store:
        for signal in signals:
            with store.process(signal):
                pass
        assert _processed_count(store) == 3
        store.commit(STRATEGY, 3)
        assert _processed_count(store) == 0  # markers compressed into the cursor row
        with store.process(signals[1]) as fresh:  # replay of seq 2 is still a duplicate
            assert fresh is False


def test_prune_by_age(state_path: Path) -> None:
    now = [datetime(2026, 8, 17, tzinfo=UTC)]
    with SqliteStateStore(state_path, retention_days=90, clock=lambda: now[0]) as store:
        old = make_signal(1, signal_id="sig_old")
        with store.process(old):
            pass
        now[0] += timedelta(days=91)
        new = make_signal(2, signal_id="sig_new")
        with store.process(new):
            pass
        deleted = store.prune()
        assert deleted == 1
        assert store.seen("sig_old") is False
        assert store.seen("sig_new") is True


def test_prune_runs_on_open(state_path: Path) -> None:
    t0 = datetime(2026, 8, 17, tzinfo=UTC)
    with (
        SqliteStateStore(state_path, clock=lambda: t0) as store,
        store.process(make_signal(1, signal_id="sig_webhook_old")),
    ):
        pass
    late = t0 + timedelta(days=91)
    with SqliteStateStore(state_path, clock=lambda: late) as reopened:
        assert reopened.seen("sig_webhook_old") is False


def test_prune_vacuums_freed_pages(state_path: Path) -> None:
    now = [datetime(2026, 8, 17, tzinfo=UTC)]
    with SqliteStateStore(state_path, clock=lambda: now[0]) as store:
        for n in range(1, 201):
            with store.process(make_signal(n)):
                pass
        now[0] += timedelta(days=91)
        assert store.prune() == 200
        row = store.connection.execute("PRAGMA freelist_count").fetchone()
        assert row == (0,)  # freed pages returned to the OS, file does not balloon


def test_commit_vacuums_freed_pages(state_path: Path) -> None:
    # The subscriber-driven path: commit() deletes markers at or below the
    # watermark, so it must drain the freelist too. Regression guard for a
    # freelist larger than one page -- SQLite frees a single page per step, so
    # any fix that vacuums "once" passes a one-page fixture while a real store
    # still balloons.
    with SqliteStateStore(state_path) as store:
        for n in range(1, 201):
            with store.process(make_signal(n)):
                pass
        store.commit(STRATEGY, 200)
        row = store.connection.execute("PRAGMA freelist_count").fetchone()
        assert row == (0,)  # every freed page returned, not just the first


# ------------------------------------------------------------------- lifecycle
def test_close_is_idempotent_and_context_manager_closes(state_path: Path) -> None:
    store = SqliteStateStore(state_path)
    store.close()
    store.close()  # second close is a no-op
    with SqliteStateStore(state_path) as reopened:
        reopened.commit(STRATEGY, 1)
    with pytest.raises(sqlite3.ProgrammingError):
        reopened.connection.execute("SELECT 1")  # context exit closed the connection


def test_wrapper_is_thread_safe(state_path: Path) -> None:
    # The subscriber may deliver from a Web PubSub thread; webhook servers use
    # worker threads. One connection + an internal lock must serialize them.
    with SqliteStateStore(state_path) as store:
        processed: list[int] = []
        wrapped = store.exactly_once(lambda s, m: processed.append(s.sequence))

        def worker(start: int) -> None:
            for n in range(start, start + 50):
                signal = make_signal(n)
                wrapped(signal, make_meta(signal))

        threads = [
            threading.Thread(target=worker, args=(1,)),
            threading.Thread(target=worker, args=(51,)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10.0)
        assert sorted(processed) == list(range(1, 101))
