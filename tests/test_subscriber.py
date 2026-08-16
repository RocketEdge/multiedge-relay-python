"""Subscriber tests: cursor resume, paging, crash redelivery, gaps, dedupe, live poll."""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

import pytest
from fake_relay import API_KEY, FakeRelay, SyncASGITransport

from multiedge_relay import (
    CursorCorruptError,
    FileCursorStore,
    GapUnrecoverableError,
    ReceivedSignal,
    SignalMeta,
    SignalSubscriber,
)

STRATEGY = "s1"


class Collector:
    """Records (sequence, source) pairs and optionally fails on chosen sequences."""

    def __init__(self, fail_on: set[int] | None = None) -> None:
        self.deliveries: list[tuple[int, str]] = []
        self.fail_on = fail_on or set()

    def __call__(self, signal: ReceivedSignal, meta: SignalMeta) -> None:
        if signal.sequence in self.fail_on:
            self.fail_on.discard(signal.sequence)
            raise RuntimeError(f"callback crash at {signal.sequence}")
        self.deliveries.append((signal.sequence, meta.source))

    @property
    def sequences(self) -> list[int]:
        return [seq for seq, _ in self.deliveries]


def make_subscriber(
    relay: FakeRelay,
    cursor_root: Path,
    collector: Collector,
    **kwargs: Any,
) -> SignalSubscriber:
    kwargs.setdefault("cursor_store", FileCursorStore(root=cursor_root))
    kwargs.setdefault("page_size", 500)
    return SignalSubscriber(
        api_key=API_KEY,
        strategy_id=STRATEGY,
        on_signal=collector,
        transport=SyncASGITransport(relay.app),
        sleep=lambda _: None,
        random_fn=lambda: 1.0,
        **kwargs,
    )


def seed(relay: FakeRelay, n: int, start: int = 0) -> None:
    relay.seed(STRATEGY, [{"n": start + i} for i in range(n)])


# ------------------------------------------------------------------- catch-up basics
def test_catch_up_delivers_in_order_and_returns_count(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 5)
    collector = Collector()
    subscriber = make_subscriber(relay, cursor_root, collector)
    count = subscriber.catch_up_only()
    assert count == 5
    assert collector.deliveries == [(i, "catchup") for i in range(1, 6)]


def test_cursor_committed_after_each_callback(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 3)
    store = FileCursorStore(root=cursor_root)
    seen_cursors: list[int | None] = []

    def on_signal(signal: ReceivedSignal, meta: SignalMeta) -> None:
        seen_cursors.append(store.load(STRATEGY))

    subscriber = make_subscriber(relay, cursor_root, Collector(), cursor_store=store)
    subscriber.on_signal = on_signal
    subscriber.catch_up_only()
    # at callback time for seq n, only n-1 has been committed
    assert seen_cursors == [None, 1, 2]
    assert store.load(STRATEGY) == 3


def test_resume_from_cursor_delivers_each_sequence_once_per_run(
    relay: FakeRelay, cursor_root: Path
) -> None:
    seed(relay, 5)
    first = Collector()
    make_subscriber(relay, cursor_root, first).catch_up_only()
    assert first.sequences == [1, 2, 3, 4, 5]

    seed(relay, 3, start=5)
    second = Collector()
    count = make_subscriber(relay, cursor_root, second).catch_up_only()
    assert count == 3
    assert second.sequences == [6, 7, 8]  # nothing redelivered


def test_multi_page_catch_up(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 12)
    collector = Collector()
    count = make_subscriber(relay, cursor_root, collector, page_size=5).catch_up_only()
    assert count == 12
    assert collector.sequences == list(range(1, 13))
    list_calls = [r for r in relay.requests if r == "GET /v1/signals"]
    assert len(list_calls) >= 3  # 5 + 5 + 2


def test_crash_mid_page_redelivers_only_uncommitted(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 5)
    crashing = Collector(fail_on={3})
    subscriber = make_subscriber(relay, cursor_root, crashing)
    with pytest.raises(RuntimeError, match="callback crash at 3"):
        subscriber.catch_up_only()
    assert crashing.sequences == [1, 2]
    assert FileCursorStore(root=cursor_root).load(STRATEGY) == 2

    restarted = Collector()
    count = make_subscriber(relay, cursor_root, restarted).catch_up_only()
    assert count == 3
    assert restarted.sequences == [3, 4, 5]  # 1 and 2 were committed — not redelivered


def test_callback_error_invokes_on_error_then_raises(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 2)
    errors: list[Exception] = []
    subscriber = make_subscriber(relay, cursor_root, Collector(fail_on={1}), on_error=errors.append)
    with pytest.raises(RuntimeError):
        subscriber.catch_up_only()
    assert len(errors) == 1


# ------------------------------------------------------------------- start_from
def test_start_from_earliest_ignores_cursor(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 4)
    make_subscriber(relay, cursor_root, Collector()).catch_up_only()
    replay = Collector()
    make_subscriber(relay, cursor_root, replay, start_from="earliest").catch_up_only()
    assert replay.sequences == [1, 2, 3, 4]


def test_start_from_explicit_sequence(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 6)
    collector = Collector()
    make_subscriber(relay, cursor_root, collector, start_from=4).catch_up_only()
    assert collector.sequences == [5, 6]


def test_start_from_latest_skips_backlog(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 4)
    collector = Collector()
    subscriber = make_subscriber(relay, cursor_root, collector, start_from="latest")
    assert subscriber.catch_up_only() == 0
    assert collector.sequences == []
    seed(relay, 2, start=4)
    fresh = Collector()
    make_subscriber(relay, cursor_root, fresh).catch_up_only()
    assert fresh.sequences == [5, 6]  # cursor was advanced to 4


def test_corrupt_cursor_raises_before_any_delivery(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 3)
    cursor_root.mkdir(parents=True, exist_ok=True)
    (cursor_root / f"{STRATEGY}.json").write_text("garbage", encoding="utf-8")
    collector = Collector()
    with pytest.raises(CursorCorruptError):
        make_subscriber(relay, cursor_root, collector).catch_up_only()
    assert collector.sequences == []


def test_rest_holes_are_tolerated_in_catch_up(relay: FakeRelay, cursor_root: Path) -> None:
    # REST is the ordering truth: a hole in the relay's own log is delivered as-is.
    seed(relay, 5)
    relay.inject_gap(STRATEGY, [3])
    collector = Collector()
    count = make_subscriber(relay, cursor_root, collector).catch_up_only()
    assert count == 4
    assert collector.sequences == [1, 2, 4, 5]


# ------------------------------------------------------------------- live gap logic
def test_live_gap_parks_fills_then_drains(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 2)
    collector = Collector()
    subscriber = make_subscriber(relay, cursor_root, collector)
    subscriber.catch_up_only()
    seed(relay, 3, start=2)  # sequences 3, 4, 5 now in REST

    live_5 = relay.signals[STRATEGY][-1]
    subscriber._handle_live_message(ReceivedSignal.model_validate(live_5.as_received()))
    assert collector.deliveries == [
        (1, "catchup"),
        (2, "catchup"),
        (3, "gapfill"),
        (4, "gapfill"),
        (5, "live"),
    ]


def test_live_overlap_is_deduped_by_sequence(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 3)
    collector = Collector()
    subscriber = make_subscriber(relay, cursor_root, collector)
    subscriber.catch_up_only()

    stale = relay.signals[STRATEGY][0]  # sequence 1 <= cursor 3
    subscriber._handle_live_message(ReceivedSignal.model_validate(stale.as_received()))
    assert collector.sequences == [1, 2, 3]  # no redelivery


def test_live_in_order_message_is_delivered_immediately(
    relay: FakeRelay, cursor_root: Path
) -> None:
    seed(relay, 2)
    collector = Collector()
    subscriber = make_subscriber(relay, cursor_root, collector)
    subscriber.catch_up_only()
    seed(relay, 1, start=2)
    live_3 = relay.signals[STRATEGY][-1]
    subscriber._handle_live_message(ReceivedSignal.model_validate(live_3.as_received()))
    assert collector.deliveries[-1] == (3, "live")


def test_unfillable_gap_raises_gap_unrecoverable(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 2)
    collector = Collector()
    subscriber = make_subscriber(relay, cursor_root, collector)
    subscriber.catch_up_only()

    # a live message claims sequence 5, but REST has nothing past 2 and never will
    phantom = ReceivedSignal(
        sequence=5,
        signal_id="sig_phantom",
        strategy_id=STRATEGY,
        published_at=relay.signals[STRATEGY][0].published_at,
        payload={},
    )
    with pytest.raises(GapUnrecoverableError):
        subscriber._handle_live_message(phantom)
    assert collector.sequences == [1, 2]  # nothing skipped, nothing invented


# ------------------------------------------------------------------- live polling
def test_poll_loop_delivers_new_signals_exactly_once(relay: FakeRelay, cursor_root: Path) -> None:
    seed(relay, 3)
    collector = Collector()
    subscriber = make_subscriber(relay, cursor_root, collector, poll_interval=0.02)
    thread = threading.Thread(target=subscriber.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 5.0
        while len(collector.deliveries) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert collector.sequences == [1, 2, 3]

        time.sleep(0.1)  # several poll cycles: nothing redelivered
        assert collector.sequences == [1, 2, 3]

        seed(relay, 2, start=3)
        deadline = time.monotonic() + 5.0
        while len(collector.deliveries) < 5 and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        subscriber.stop()
        thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert collector.sequences == [1, 2, 3, 4, 5]
    catchup = [s for s, src in collector.deliveries if src == "catchup"]
    live = [s for s, src in collector.deliveries if src == "live"]
    assert catchup == [1, 2, 3]
    assert live == [4, 5]


def test_stop_unblocks_run(relay: FakeRelay, cursor_root: Path) -> None:
    subscriber = make_subscriber(relay, cursor_root, Collector(), poll_interval=10.0)
    thread = threading.Thread(target=subscriber.run, daemon=True)
    thread.start()
    time.sleep(0.05)
    subscriber.stop()
    thread.join(timeout=5.0)
    assert not thread.is_alive()


# ------------------------------------------------------------------- webpubsub extra
def test_webpubsub_without_extra_raises_helpful_import_error(
    relay: FakeRelay, cursor_root: Path
) -> None:
    subscriber = make_subscriber(relay, cursor_root, Collector(), live_transport="webpubsub")
    with pytest.raises(ImportError, match=r"multiedge-relay\[webpubsub\]"):
        subscriber.run()
