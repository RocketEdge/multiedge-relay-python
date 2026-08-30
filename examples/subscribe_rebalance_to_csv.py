"""Reconstruct a rebalance instruction CSV from received portfolio_rebalance signals.

The inverse of ``publish_rebalance_from_csv.py``: drains the strategy's backlog with
``catch_up_only()`` and writes one CSV row per position (or a single
``PORTFOLIO/NONE`` row for empty days).

Run:
    MULTIEDGE_API_KEY=mesk_your_api_key python subscribe_rebalance_to_csv.py out.csv
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

from multiedge_relay import ReceivedSignal, SignalMeta, SignalSubscriber

CSV_COLUMNS = ["SignalDate", "PlannedExecutionDate", "Ticker", "Action", "SignalPortfolioWeight"]


def signals_to_rows(signals: list[ReceivedSignal]) -> list[dict[str, str]]:
    """Flatten portfolio_rebalance signals back into instruction CSV rows.

    Args:
        signals: Received signals whose payloads follow the portfolio_rebalance
            schema (``signal_date``, ``planned_execution_date``, ``positions``).

    Returns:
        One dict per position with the original 8-decimal weight formatting;
        empty-position days yield a single ``PORTFOLIO/NONE`` row with blank weight.
    """
    rows: list[dict[str, str]] = []
    for signal in sorted(signals, key=lambda s: str(s.payload["signal_date"])):
        payload = signal.payload
        base = {
            "SignalDate": str(payload["signal_date"]),
            "PlannedExecutionDate": str(payload["planned_execution_date"]),
        }
        positions = payload["positions"]
        if not positions:
            rows.append(
                {**base, "Ticker": "PORTFOLIO", "Action": "NONE", "SignalPortfolioWeight": ""}
            )
            continue
        for position in positions:
            rows.append(
                {
                    **base,
                    "Ticker": str(position["ticker"]),
                    "Action": str(position["action"]),
                    "SignalPortfolioWeight": f"{float(position['signal_portfolio_weight']):.8f}",
                }
            )
    return rows


def write_rows(rows: list[dict[str, str]], out_path: Path) -> None:
    """Write reconstructed rows to ``out_path`` with the canonical column order."""
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    """Drain the backlog and write the reconstructed CSV named on the command line."""
    if len(sys.argv) != 2:
        print("usage: python subscribe_rebalance_to_csv.py <out.csv>", file=sys.stderr)
        raise SystemExit(2)
    received: list[ReceivedSignal] = []

    def on_signal(signal: ReceivedSignal, meta: SignalMeta) -> None:
        received.append(signal)

    subscriber = SignalSubscriber(
        api_key=os.environ.get("MULTIEDGE_API_KEY", "mesk_your_api_key"),
        strategy_id="rebalance-demo",
        on_signal=on_signal,
        start_from="earliest",
    )
    count = subscriber.catch_up_only()
    rows = signals_to_rows(received)
    write_rows(rows, Path(sys.argv[1]))
    print(f"received {count} signal(s); wrote {len(rows)} row(s) to {sys.argv[1]}")


if __name__ == "__main__":
    main()
