"""Terminal 1 of the live demo: receive rebalance signals and watch them arrive.

Subscribes to the demo strategy over REST polling (no extra dependencies): first a
catch-up pass over anything already in the relay's log, then live polling. Each
signal prints as one line tagged ``[catchup]`` or ``[live]``.

The cursor is persisted under ``examples/prod_demo/.demo/cursor/`` and committed
only after each signal is handled — kill this process mid-stream (Ctrl+C or close
the terminal), restart it, and it resumes exactly where it stopped with no loss
and no duplicates. That resume is the demo's headline proof.

Run:
    MULTIEDGE_API_KEY=mesk_...   # the subscriber:<client_id> key
    uv run python consumer_rebalance.py --strategy-id <ULID>
    uv run python consumer_rebalance.py --strategy-id <ULID> --catchup-only --out received.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

try:
    from multiedge_relay import (
        FileCursorStore,
        ReceivedSignal,
        SignalMeta,
        SignalSubscriber,
    )
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

CSV_COLUMNS = ["SignalDate", "PlannedExecutionDate", "Ticker", "Action", "SignalPortfolioWeight"]


def format_signal(signal: ReceivedSignal, meta: SignalMeta) -> str:
    """Render one received signal as a single human-readable console line.

    Args:
        signal: The received portfolio_rebalance signal.
        meta: Delivery metadata; ``meta.source`` tags the line ``[catchup]``,
            ``[live]``, or ``[gapfill]``.

    Returns:
        A line like ``[live] seq=7 2026-08-27 -> 2026-08-28: 9 position(s) — 4 BUY,
        3 SELL, 2 HOLD`` (or ``no action (heartbeat)`` for empty-positions days).
    """
    payload = signal.payload
    positions = payload["positions"]
    assert isinstance(positions, list)
    head = (
        f"[{meta.source}] seq={signal.sequence} "
        f"{payload['signal_date']} -> {payload['planned_execution_date']}:"
    )
    if not positions:
        return f"{head} no action (heartbeat)"
    buys = sum(1 for p in positions if p["action"] in {"BUY", "INITIALIZE"})
    sells = sum(1 for p in positions if p["action"] == "SELL")
    holds = sum(1 for p in positions if p["action"] == "HOLD")
    return f"{head} {len(positions)} position(s) — {buys} BUY, {sells} SELL, {holds} HOLD"


def signals_to_rows(signals: list[ReceivedSignal]) -> list[dict[str, str]]:
    """Flatten portfolio_rebalance signals back into instruction CSV rows.

    Args:
        signals: Received signals following the portfolio_rebalance schema.

    Returns:
        One dict per position with the feed's 8-decimal weight formatting;
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
        assert isinstance(positions, list)
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
    """Run the demo consumer: catch up, then follow the strategy live."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strategy-id", required=True, help="strategy ULID from setup_demo.py")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument(
        "--catchup-only", action="store_true", help="drain the backlog and exit (no live phase)"
    )
    parser.add_argument("--out", type=Path, default=None, help="write received rows as CSV on exit")
    args = parser.parse_args()

    api_key = os.environ.get("MULTIEDGE_API_KEY")
    if not api_key:
        print("set MULTIEDGE_API_KEY to the subscriber key from setup_demo.py", file=sys.stderr)
        raise SystemExit(2)

    received: list[ReceivedSignal] = []

    def on_signal(signal: ReceivedSignal, meta: SignalMeta) -> None:
        received.append(signal)
        print(format_signal(signal, meta))

    subscriber = SignalSubscriber(
        api_key=api_key,
        strategy_id=args.strategy_id,
        on_signal=on_signal,
        base_url=args.base_url,
        live_transport="poll",
        poll_interval=args.poll_interval,
        cursor_store=FileCursorStore(root=DEMO_STATE_ROOT / "cursor"),
    )
    try:
        if args.catchup_only:
            count = subscriber.catch_up_only()
            print(f"caught up: {count} signal(s)")
        else:
            print("catching up, then polling live — Ctrl+C to stop (cursor is preserved)")
            subscriber.run()
    except KeyboardInterrupt:
        subscriber.stop()
        print("\nstopped; the cursor marks the last processed signal — restart to resume")
    finally:
        subscriber.close()

    if args.out is not None:
        rows = signals_to_rows(received)
        write_rows(rows, args.out)
        print(f"wrote {len(rows)} row(s) from this session to {args.out}")


if __name__ == "__main__":
    main()
