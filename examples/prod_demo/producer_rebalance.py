"""Terminal 2 of the live demo: publish the rebalance CSV as paced signals.

Groups the instruction CSV by ``SignalDate`` into one ``portfolio_rebalance``
signal per date (``PORTFOLIO/NONE`` days become explicit empty-``positions``
heartbeats) and publishes them in date order — by default one signal every
``--pace`` seconds, simulating a daily feed you can watch arrive live in the
consumer terminal.

Every signal carries the deterministic ``client_signal_id``
``"<strategy_id>:<signal_date>"``, so re-running the producer is idempotent: the
relay answers with the original ack (``deduplicated=True``) instead of storing a
second copy.

Run:
    MULTIEDGE_API_KEY=mesk_...   # the publisher:<strategy_id> key
    uv run python producer_rebalance.py demo_rebalance_signals.csv --strategy-id <ULID> --pace 3
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path

try:
    from multiedge_relay import DiskDLQ, Signal, SignalAck, SignalPublisher
except ModuleNotFoundError as exc:  # the SDK is missing from THIS interpreter
    # A bare ``python demo.py`` resolves to the interpreter on PATH, not to the
    # project environment — naming that interpreter, and the uv command that builds
    # and uses the right one, is the whole fix.
    raise SystemExit(
        f"{exc}: this demo needs its dependencies installed in the interpreter "
        f"running it ({sys.executable}).\n"
        "\n"
        "Run it with uv from anywhere in the repo — uv creates and syncs the "
        "environment for you:\n"
        f"  uv run python {os.path.basename(sys.argv[0])} <same arguments>\n"
        "\n"
        "Not using uv? Install the SDK into this interpreter "
        "(pip install multiedge-relay), or activate the environment that has it."
    ) from exc

DEFAULT_BASE_URL = "https://relay-api.multiedge.ai"
DEMO_STATE_ROOT = Path(__file__).parent / ".demo"


def load_rebalance_signals(csv_path: Path, strategy_id: str) -> list[Signal]:
    """Group CSV rows by SignalDate into one portfolio_rebalance signal per date.

    Args:
        csv_path: Path to the instruction CSV (7 columns; see the module docstring
            of ``generate_demo_csv.py``). Only the schema-carried fields — ticker,
            action, and the pre-trade portfolio weight — are published.
        strategy_id: Strategy stream to publish on (the relay's strategy ULID).

    Returns:
        Signals in ascending SignalDate order, each with the deterministic
        ``client_signal_id`` ``"<strategy_id>:<signal_date>"`` so republishing the
        same file never duplicates data on the relay.
    """
    by_date: dict[str, dict[str, object]] = {}
    with csv_path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            signal_date = row["SignalDate"]
            day = by_date.setdefault(
                signal_date,
                {
                    "kind": "portfolio_rebalance",
                    "signal_date": signal_date,
                    "planned_execution_date": row["PlannedExecutionDate"],
                    "positions": [],
                },
            )
            if row["Ticker"] != "PORTFOLIO":
                positions = day["positions"]
                assert isinstance(positions, list)
                positions.append(
                    {
                        "ticker": row["Ticker"],
                        "action": row["Action"],
                        "signal_portfolio_weight": float(row["SignalPortfolioWeight"]),
                    }
                )
    return [
        Signal(
            strategy_id=strategy_id,
            payload=by_date[signal_date],
            client_signal_id=f"{strategy_id}:{signal_date}",
        )
        for signal_date in sorted(by_date)
    ]


def publish_signals(
    publisher: SignalPublisher,
    signals: list[Signal],
    *,
    pace: float,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = print,
) -> list[SignalAck]:
    """Publish signals in order, pausing ``pace`` seconds between them.

    Args:
        publisher: An open ``SignalPublisher``.
        signals: Signals in the order to publish (ascending SignalDate).
        pace: Seconds to wait between consecutive publishes; ``0`` publishes the
            whole list as one batch (backlog mode).
        sleep: Injectable pause function (tests pass a no-op).
        log: Injectable line sink for per-signal progress.

    Returns:
        One ack per signal, in publish order. A signal the relay had already
        accepted (same ``client_signal_id``) yields its original ack with
        ``deduplicated=True``.

    Raises:
        PublishFailed: After retries are exhausted; the signal is preserved in
            the publisher's disk DLQ (never silent loss).
    """
    acks: list[SignalAck] = []
    for index, signal in enumerate(signals):
        if pace > 0 and index > 0:
            sleep(pace)
        ack = publisher.publish(signal)
        acks.append(ack)
        positions = signal.payload["positions"]
        assert isinstance(positions, list)
        log(
            f"{signal.payload['signal_date']}: seq={ack.sequence} "
            f"positions={len(positions)}" + (" (deduplicated)" if ack.deduplicated else "")
        )
    return acks


def main() -> None:
    """Publish the CSV named on the command line to the relay, paced."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--strategy-id", required=True, help="strategy ULID from setup_demo.py")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--pace", type=float, default=3.0, help="seconds between signals; 0=bulk")
    parser.add_argument("--limit", type=int, default=None, help="publish only the first N dates")
    args = parser.parse_args()

    api_key = os.environ.get("MULTIEDGE_API_KEY")
    if not api_key:
        print("set MULTIEDGE_API_KEY to the publisher key from setup_demo.py", file=sys.stderr)
        raise SystemExit(2)

    signals = load_rebalance_signals(args.csv_path, strategy_id=args.strategy_id)
    if args.limit is not None:
        signals = signals[: args.limit]

    with SignalPublisher(
        api_key=api_key,
        base_url=args.base_url,
        dlq=DiskDLQ(root=DEMO_STATE_ROOT / "dlq"),
    ) as publisher:
        acks = publish_signals(publisher, signals, pace=args.pace)

    deduplicated = sum(1 for ack in acks if ack.deduplicated)
    print(f"published {len(acks)} signal(s) ({deduplicated} deduplicated by the relay)")


if __name__ == "__main__":
    main()
