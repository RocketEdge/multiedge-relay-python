"""Round-trip test for the rebalance CSV examples against the fake relay.

Publishes the synthetic fixture CSV (one signal per SignalDate), subscribes it back,
reconstructs rows, and diffs the reconstruction against the original file.
"""

from __future__ import annotations

import csv
import importlib
import sys
from pathlib import Path
from typing import Any

import pytest
from conftest import FIXTURES
from fake_relay import API_KEY, FakeRelay, SyncASGITransport

from multiedge_relay import (
    DiskDLQ,
    FileCursorStore,
    ReceivedSignal,
    SignalMeta,
    SignalPublisher,
    SignalSubscriber,
)

EXAMPLES = Path(__file__).parent.parent / "examples"


@pytest.fixture(scope="module")
def example_modules() -> tuple[Any, Any]:
    sys.path.insert(0, str(EXAMPLES))
    try:
        pub = importlib.import_module("publish_rebalance_from_csv")
        sub = importlib.import_module("subscribe_rebalance_to_csv")
    finally:
        sys.path.remove(str(EXAMPLES))
    return pub, sub


def test_rebalance_round_trip(
    example_modules: tuple[Any, Any],
    relay: FakeRelay,
    tmp_path: Path,
) -> None:
    pub_ex, sub_ex = example_modules
    fixture = FIXTURES / "synthetic_rebalance.csv"

    signals = pub_ex.load_rebalance_signals(fixture, strategy_id="rebalance-demo")
    assert len(signals) == 5  # five distinct SignalDates in the fixture
    assert [s.payload["signal_date"] for s in signals] == sorted(
        {s.payload["signal_date"] for s in signals}
    )  # published in date order
    portfolio_days = [s for s in signals if s.payload["positions"] == []]
    assert len(portfolio_days) == 3  # PORTFOLIO/NONE days carry empty positions

    with SignalPublisher(
        api_key=API_KEY,
        dlq=DiskDLQ(root=tmp_path / "dlq"),
        transport=SyncASGITransport(relay.app),
        sleep=lambda _: None,
    ) as publisher:
        for signal in signals:
            publisher.publish(signal)

    received: list[ReceivedSignal] = []

    def on_signal(signal: ReceivedSignal, meta: SignalMeta) -> None:
        received.append(signal)

    subscriber = SignalSubscriber(
        api_key=API_KEY,
        strategy_id="rebalance-demo",
        on_signal=on_signal,
        cursor_store=FileCursorStore(root=tmp_path / "cursor"),
        transport=SyncASGITransport(relay.app),
    )
    assert subscriber.catch_up_only() == 5

    rows = sub_ex.signals_to_rows(received)

    with fixture.open(newline="", encoding="utf-8") as fh:
        expected = [
            {
                "SignalDate": row["SignalDate"],
                "PlannedExecutionDate": row["PlannedExecutionDate"],
                "Ticker": row["Ticker"],
                "Action": row["Action"],
                "SignalPortfolioWeight": row["SignalPortfolioWeight"],
            }
            for row in csv.DictReader(fh)
        ]

    assert rows == expected

    out_csv = tmp_path / "reconstructed.csv"
    sub_ex.write_rows(rows, out_csv)
    with out_csv.open(newline="", encoding="utf-8") as fh:
        assert list(csv.DictReader(fh)) == expected
