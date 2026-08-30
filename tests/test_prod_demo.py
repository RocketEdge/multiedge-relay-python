"""Tests for the two-terminal live-demo scripts in ``examples/prod_demo``.

Covers the synthetic rebalance CSV generator (deterministic, anonymized, correctly
shaped), the producer's CSV→signal mapping with deterministic ``client_signal_id``
(idempotent re-runs), the producer/consumer round trip against the fake relay, and
the control-plane bootstrap flow against a mocked API.
"""

from __future__ import annotations

import csv
import importlib
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from fake_relay import API_KEY, FakeRelay, SyncASGITransport

from multiedge_relay import (
    DiskDLQ,
    FileCursorStore,
    ReceivedSignal,
    SignalAck,
    SignalMeta,
    SignalPublisher,
    SignalSubscriber,
)

PROD_DEMO = Path(__file__).parent.parent / "examples" / "prod_demo"


@pytest.fixture(scope="module")
def demo_modules() -> tuple[Any, Any, Any, Any]:
    sys.path.insert(0, str(PROD_DEMO))
    try:
        gen = importlib.import_module("generate_demo_csv")
        setup = importlib.import_module("setup_demo")
        producer = importlib.import_module("producer_rebalance")
        consumer = importlib.import_module("consumer_rebalance")
    finally:
        sys.path.remove(str(PROD_DEMO))
    return gen, setup, producer, consumer


# --------------------------------------------------------------- generator


def test_generator_is_deterministic(demo_modules: tuple[Any, Any, Any, Any]) -> None:
    gen = demo_modules[0]
    rows_a = gen.generate_rows(seed=42, start=date(2024, 1, 2), months=6)
    rows_b = gen.generate_rows(seed=42, start=date(2024, 1, 2), months=6)
    assert rows_a == rows_b
    rows_c = gen.generate_rows(seed=7, start=date(2024, 1, 2), months=6)
    assert rows_a != rows_c


def test_generator_shape_and_anonymity(
    demo_modules: tuple[Any, Any, Any, Any], tmp_path: Path
) -> None:
    gen = demo_modules[0]
    rows = gen.generate_rows(seed=42, start=date(2024, 1, 2), months=18)
    out = tmp_path / "demo_rebalance_signals.csv"
    gen.write_csv(rows, out)

    text = out.read_text(encoding="utf-8")
    assert "ABA" not in text.upper().replace("REBALANCE", "")  # anonymized

    with out.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == [
            "SignalDate",
            "PlannedExecutionDate",
            "Ticker",
            "Action",
            "SignalPortfolioWeight",
            "TradeWeightDelta",
            "ImpliedPostTradeWeightAtSignalClose",
        ]
        parsed = list(reader)

    first_date = parsed[0]["SignalDate"]
    day_one = [r for r in parsed if r["SignalDate"] == first_date]
    assert {r["Action"] for r in day_one} == {"INITIALIZE"}
    assert all(r["SignalPortfolioWeight"] == "0.00000000" for r in day_one)
    # INITIALIZE executes same day; every later row plans for a later session.
    assert all(r["PlannedExecutionDate"] == first_date for r in day_one)

    none_rows = [r for r in parsed if r["Action"] == "NONE"]
    assert none_rows, "heartbeat days must exist"
    assert all(r["Ticker"] == "PORTFOLIO" for r in none_rows)
    assert all(
        r["SignalPortfolioWeight"] == ""
        and r["TradeWeightDelta"] == ""
        and r["ImpliedPostTradeWeightAtSignalClose"] == ""
        for r in none_rows
    )

    trade_rows = [r for r in parsed if r["Action"] in {"BUY", "SELL"}]
    assert trade_rows, "rebalance days must exist"
    for row in trade_rows:
        for column in (
            "SignalPortfolioWeight",
            "TradeWeightDelta",
            "ImpliedPostTradeWeightAtSignalClose",
        ):
            _whole, dot, frac = row[column].lstrip("-").partition(".")
            assert dot == "." and len(frac) == 8, f"{column} must use 8 decimals"
        assert (float(row["TradeWeightDelta"]) < 0) == (row["Action"] == "SELL")
        assert (
            abs(
                float(row["SignalPortfolioWeight"])
                + float(row["TradeWeightDelta"])
                - float(row["ImpliedPostTradeWeightAtSignalClose"])
            )
            < 1e-9
        )

    # Post-trade weights on every rebalance day form a fully invested portfolio.
    by_date: dict[str, float] = {}
    for row in trade_rows:
        by_date[row["SignalDate"]] = by_date.get(row["SignalDate"], 0.0) + float(
            row["ImpliedPostTradeWeightAtSignalClose"]
        )
    for day_total in by_date.values():
        assert abs(day_total - 1.0) < 1e-6

    # The late-joining asset must be absent at the start and present later on.
    dates_with_new_asset = sorted(
        {r["SignalDate"] for r in parsed if r["Ticker"] == gen.NEW_ASSET_TICKER}
    )
    assert dates_with_new_asset
    assert dates_with_new_asset[0] > first_date
    joined = [
        r
        for r in parsed
        if r["Ticker"] == gen.NEW_ASSET_TICKER and r["SignalDate"] == dates_with_new_asset[0]
    ]
    assert joined[0]["SignalPortfolioWeight"] == "0.00000000"
    assert joined[0]["Action"] == "BUY"


# ---------------------------------------------------------------- producer


def test_loader_groups_dates_and_assigns_deterministic_ids(
    demo_modules: tuple[Any, Any, Any, Any], tmp_path: Path
) -> None:
    gen, _, producer, _ = demo_modules
    out = tmp_path / "demo.csv"
    gen.write_csv(gen.generate_rows(seed=42, start=date(2024, 1, 2), months=3), out)

    signals = producer.load_rebalance_signals(out, strategy_id="01STRATEGYULID")
    dates = [str(s.payload["signal_date"]) for s in signals]
    assert dates == sorted(dates)
    assert len(dates) == len(set(dates))  # one signal per SignalDate
    assert all(s.client_signal_id == f"01STRATEGYULID:{s.payload['signal_date']}" for s in signals)
    heartbeat_days = [s for s in signals if s.payload["positions"] == []]
    trade_days = [s for s in signals if s.payload["positions"]]
    assert heartbeat_days and trade_days
    position = trade_days[0].payload["positions"][0]
    assert set(position) == {"ticker", "action", "signal_portfolio_weight"}
    assert isinstance(position["signal_portfolio_weight"], float)


def test_producer_consumer_round_trip_and_idempotent_rerun(
    demo_modules: tuple[Any, Any, Any, Any], relay: FakeRelay, tmp_path: Path
) -> None:
    gen, _, producer, consumer = demo_modules
    source = tmp_path / "demo.csv"
    gen.write_csv(gen.generate_rows(seed=42, start=date(2024, 1, 2), months=2), source)

    strategy_id = "01STRATEGYULID"
    signals = producer.load_rebalance_signals(source, strategy_id=strategy_id)

    def publish_all() -> list[SignalAck]:
        with SignalPublisher(
            api_key=API_KEY,
            dlq=DiskDLQ(root=tmp_path / "dlq"),
            transport=SyncASGITransport(relay.app),
            sleep=lambda _: None,
        ) as publisher:
            acks = producer.publish_signals(publisher, signals, pace=0.0, sleep=lambda _: None)
            assert isinstance(acks, list)
            return acks

    first_acks = publish_all()
    assert len(first_acks) == len(signals)
    assert all(not ack.deduplicated for ack in first_acks)

    rerun_acks = publish_all()  # deterministic ids make the rerun a no-op
    assert all(ack.deduplicated for ack in rerun_acks)
    assert [a.sequence for a in rerun_acks] == [a.sequence for a in first_acks]

    received: list[ReceivedSignal] = []

    def on_signal(signal: ReceivedSignal, meta: SignalMeta) -> None:
        received.append(signal)

    subscriber = SignalSubscriber(
        api_key=API_KEY,
        strategy_id=strategy_id,
        on_signal=on_signal,
        start_from="earliest",
        cursor_store=FileCursorStore(root=tmp_path / "cursor"),
        transport=SyncASGITransport(relay.app),
    )
    assert subscriber.catch_up_only() == len(signals)

    rows = consumer.signals_to_rows(received)
    out_csv = tmp_path / "reconstructed.csv"
    consumer.write_rows(rows, out_csv)

    with source.open(newline="", encoding="utf-8") as fh:
        expected = [{key: row[key] for key in consumer.CSV_COLUMNS} for row in csv.DictReader(fh)]
    with out_csv.open(newline="", encoding="utf-8") as fh:
        assert list(csv.DictReader(fh)) == expected


def test_consumer_formats_a_readable_line(
    demo_modules: tuple[Any, Any, Any, Any],
) -> None:
    consumer = demo_modules[3]
    signal = ReceivedSignal(
        sequence=7,
        signal_id="sig_x",
        strategy_id="01STRATEGYULID",
        published_at=datetime(2026, 8, 27, tzinfo=UTC),
        payload={
            "kind": "portfolio_rebalance",
            "signal_date": "2026-08-27",
            "planned_execution_date": "2026-08-28",
            "positions": [
                {"ticker": "EQTY", "action": "BUY", "signal_portfolio_weight": 0.25},
                {"ticker": "GOLD", "action": "SELL", "signal_portfolio_weight": 0.05},
            ],
        },
    )
    meta = SignalMeta(
        sequence=7,
        signal_id="sig_x",
        published_at=datetime(2026, 8, 27, tzinfo=UTC),
        source="live",
    )
    line = consumer.format_signal(signal, meta)
    assert "[live]" in line and "seq=7" in line and "2026-08-27" in line
    assert "1 BUY" in line and "1 SELL" in line

    heartbeat = signal.model_copy(update={"payload": {**signal.payload, "positions": []}})
    assert "no action" in consumer.format_signal(heartbeat, meta)


# ---------------------------------------------------------------- bootstrap


def _control_plane_mock(state: dict[str, Any]) -> httpx.MockTransport:
    """A minimal mock of the relay control plane for the bootstrap flow."""

    def handler(request: httpx.Request) -> httpx.Response:
        path, method = request.url.path, request.method
        if method == "POST" and path == "/v1/strategies":
            if state.get("strategy_exists"):
                return httpx.Response(409, json={"error": "slug_already_exists"})
            state["strategy_exists"] = True
            return httpx.Response(201, json={"strategy_id": "01STRAT", "slug": "demo-rebalance"})
        if method == "GET" and path == "/v1/strategies":
            return httpx.Response(
                200, json={"strategies": [{"strategy_id": "01STRAT", "slug": "demo-rebalance"}]}
            )
        if method == "GET" and path == "/v1/clients":
            existing = [{"client_id": "01CLIENT", "display_name": "Demo Subscriber Fund"}]
            clients = existing if state.get("client_exists") else []
            return httpx.Response(200, json={"clients": clients})
        if method == "POST" and path == "/v1/clients":
            state["client_exists"] = True
            return httpx.Response(201, json={"client_id": "01CLIENT"})
        if method == "POST" and path == "/v1/clients/01CLIENT/endpoints":
            return httpx.Response(201, json={"endpoint_id": "01ENDP", "secret_base64": "c2VjcmV0"})
        if method == "POST" and path == "/v1/entitlements":
            return httpx.Response(201, json={"entitlement_id": "01ENT"})
        if method == "POST" and path == "/v1/api-keys":
            import json

            scope = json.loads(request.content)["scope"]
            state.setdefault("scopes", []).append(scope)
            return httpx.Response(201, json={"api_key": f"mesk_{scope}", "scope": scope})
        return httpx.Response(404, json={"error": f"unexpected {method} {path}"})

    return httpx.MockTransport(handler)


def test_bootstrap_creates_the_full_chain(demo_modules: tuple[Any, Any, Any, Any]) -> None:
    setup = demo_modules[1]
    state: dict[str, Any] = {}
    with httpx.Client(
        base_url="https://relay-api.multiedge.ai", transport=_control_plane_mock(state)
    ) as client:
        result = setup.run_bootstrap(
            client,
            slug="demo-rebalance",
            display_name="Demo Rebalance",
            client_name="Demo Subscriber Fund",
        )
    assert result.strategy_id == "01STRAT"
    assert result.client_id == "01CLIENT"
    assert result.publisher_key == "mesk_publisher:01STRAT"
    assert result.subscriber_key == "mesk_subscriber:01CLIENT"
    assert state["scopes"] == ["publisher:01STRAT", "subscriber:01CLIENT"]


def test_bootstrap_reuses_existing_strategy_and_client(
    demo_modules: tuple[Any, Any, Any, Any],
) -> None:
    setup = demo_modules[1]
    state: dict[str, Any] = {"strategy_exists": True, "client_exists": True}
    with httpx.Client(
        base_url="https://relay-api.multiedge.ai", transport=_control_plane_mock(state)
    ) as client:
        result = setup.run_bootstrap(
            client,
            slug="demo-rebalance",
            display_name="Demo Rebalance",
            client_name="Demo Subscriber Fund",
        )
    assert result.strategy_id == "01STRAT"  # recovered via GET after the 409
    assert result.client_id == "01CLIENT"  # reused by display_name


# ------------------------------------------------- missing-dependency guard

# Running a demo script with a bare ``python`` resolves to the interpreter on PATH,
# where the SDK is absent — the README documents ``uv run python <script>`` precisely
# so no environment has to be built or activated by hand. The scripts must point at
# that same command instead of dumping an ImportError traceback at an operator who is
# mid-demo.
_BLOCK_AND_RUN = """\
import runpy
import sys
from importlib.abc import MetaPathFinder


class _Blocked(MetaPathFinder):
    def __init__(self, name: str) -> None:
        self.name = name

    def find_spec(self, fullname, path=None, target=None):
        if fullname == self.name or fullname.startswith(self.name + "."):
            raise ModuleNotFoundError(f"No module named {fullname!r}", name=fullname)
        return None


blocked, script = sys.argv[1], sys.argv[2]
sys.meta_path.insert(0, _Blocked(blocked))
sys.argv = [script, "--help"]
runpy.run_path(script, run_name="__main__")
"""


@pytest.mark.parametrize(
    ("script", "blocked"),
    [
        ("consumer_rebalance.py", "multiedge_relay"),
        ("producer_rebalance.py", "multiedge_relay"),
        ("setup_demo.py", "httpx"),
    ],
)
def test_demo_script_explains_a_missing_dependency(script: str, blocked: str) -> None:
    completed = subprocess.run(
        [sys.executable, "-c", _BLOCK_AND_RUN, blocked, str(PROD_DEMO / script)],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0
    message = completed.stderr
    assert "Traceback" not in message, "the guard must replace the traceback, not follow it"
    assert blocked in message
    assert sys.executable in message  # names the interpreter actually being used
    # The recommended fix is the documented one: uv builds the environment itself.
    assert f"uv run python {script}" in message
