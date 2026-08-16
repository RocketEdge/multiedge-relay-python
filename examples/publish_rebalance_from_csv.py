"""Publish a portfolio-rebalance instruction CSV as one signal per signal date.

Input CSV columns (one row per instrument per date):
    SignalDate, PlannedExecutionDate, Ticker, Action, SignalPortfolioWeight,
    TradeWeightDelta, ImpliedPostTradeWeightAtSignalClose

Days with no trades appear as a single ``PORTFOLIO/NONE`` row and are published with
an empty ``positions`` list, so subscribers still receive an explicit "no action
today" signal (an absent day is indistinguishable from an outage; an empty one
is not).

Run:
    MULTIEDGE_API_KEY=mek_your_api_key python publish_rebalance_from_csv.py signals.csv
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

from multiedge_relay import Signal, SignalPublisher


def load_rebalance_signals(csv_path: Path, strategy_id: str) -> list[Signal]:
    """Group CSV rows by SignalDate into one portfolio_rebalance signal per date.

    Args:
        csv_path: Path to the instruction CSV (columns documented above).
        strategy_id: Strategy stream to publish the signals on.

    Returns:
        Signals in ascending SignalDate order. ``PORTFOLIO/NONE`` days produce
        ``positions: []``; trade days produce one position per non-PORTFOLIO row
        with the ticker, action, and pre-trade portfolio weight.
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
        Signal(strategy_id=strategy_id, payload=by_date[signal_date])
        for signal_date in sorted(by_date)
    ]


def main() -> None:
    """Read the CSV named on the command line and publish it in date order."""
    if len(sys.argv) != 2:
        print("usage: python publish_rebalance_from_csv.py <signals.csv>", file=sys.stderr)
        raise SystemExit(2)
    api_key = os.environ.get("MULTIEDGE_API_KEY", "mek_your_api_key")
    signals = load_rebalance_signals(Path(sys.argv[1]), strategy_id="rebalance-demo")
    with SignalPublisher(api_key=api_key) as publisher:
        for signal in signals:
            ack = publisher.publish(signal)
            print(
                f"{signal.payload['signal_date']}: sequence={ack.sequence}"
                + (" (deduplicated)" if ack.deduplicated else "")
            )
    print(f"published {len(signals)} rebalance signal(s)")


if __name__ == "__main__":
    main()
