"""Generate a synthetic, anonymized monthly-rebalance instruction CSV.

Produces the same shape a real portfolio rebalance feed has — an INITIALIZE day,
month-end rebalances with small drift-correcting trades, one ad-hoc mid-month
rebalance, daily ``PORTFOLIO/NONE`` heartbeat rows in between, and one asset
(``CRYP``) joining the allocation mid-history — but every ticker and every weight
is fabricated. The generator is fully deterministic for a given seed, so the demo
data can be regenerated bit-identically anywhere.

Columns:
    SignalDate, PlannedExecutionDate, Ticker, Action, SignalPortfolioWeight,
    TradeWeightDelta, ImpliedPostTradeWeightAtSignalClose

Run:
    python generate_demo_csv.py --seed 42 --out demo_rebalance_signals.csv
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

CSV_COLUMNS = [
    "SignalDate",
    "PlannedExecutionDate",
    "Ticker",
    "Action",
    "SignalPortfolioWeight",
    "TradeWeightDelta",
    "ImpliedPostTradeWeightAtSignalClose",
]

# Fictional multi-asset allocation. Weights sum to exactly 1.0.
BASE_TARGETS: dict[str, float] = {
    "EQTY": 0.25,  # global equity
    "LOWV": 0.10,  # low-volatility equity
    "TREND": 0.15,  # managed futures
    "BOND": 0.15,  # intermediate treasuries
    "TIPS": 0.15,  # inflation-linked bonds
    "CMDTY": 0.10,  # broad commodities
    "GOLD": 0.05,  # gold
    "CASH": 0.05,  # T-bills
}

# One asset joins the allocation later, funded from the equity sleeve — the
# structural analogue of a new instrument entering a live model portfolio.
NEW_ASSET_TICKER = "CRYP"
NEW_ASSET_WEIGHT = 0.03
NEW_ASSET_FUNDED_FROM = "EQTY"
NEW_ASSET_JOIN_REBALANCE_INDEX = 12  # joins on the 13th month-end rebalance

MONTHLY_DRIFT_SIGMA = 0.02  # relative drift of held weights between rebalances
ADHOC_DRIFT_SIGMA = 0.06  # the ad-hoc rebalance follows a turbulent stretch


def _add_months(day: date, months: int) -> date:
    """Return the first day of the month ``months`` after ``day``'s month."""
    month_index = day.year * 12 + (day.month - 1) + months
    return date(month_index // 12, month_index % 12 + 1, 1)


def _business_days(start: date, end: date) -> list[date]:
    """All weekdays in ``[start, end)`` (synthetic data uses no holiday calendar)."""
    days: list[date] = []
    current = start
    while current < end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _next_business_day(day: date) -> date:
    """The first weekday strictly after ``day``."""
    current = day + timedelta(days=1)
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current


def _fmt(value: float) -> str:
    """Format a weight with the feed's canonical 8-decimal precision."""
    return f"{value:.8f}"


def _rebalance_rows(
    rng: random.Random,
    signal_date: date,
    held_targets: dict[str, float],
    new_targets: dict[str, float],
    drift_sigma: float,
) -> list[dict[str, str]]:
    """Build one rebalance day: drifted current weights traded back to targets.

    Args:
        rng: Seeded generator driving the drift (determinism contract).
        signal_date: The day the rebalance signal is computed (at the close).
        held_targets: Targets the portfolio drifted AROUND since the last trade.
        new_targets: Targets the trades move the portfolio TO. An asset present
            here but absent from ``held_targets`` enters at a current weight of 0.
        drift_sigma: Relative standard deviation of the simulated drift.

    Returns:
        One CSV row per ticker in ``new_targets``, sorted by ticker; the printed
        post-trade weights equal the targets, so each rebalance day sums to 1.
    """
    drifted = {
        ticker: weight * (1.0 + rng.gauss(0.0, drift_sigma))
        for ticker, weight in held_targets.items()
    }
    total = sum(drifted.values())
    current = {ticker: weight / total for ticker, weight in drifted.items()}

    execution_date = _next_business_day(signal_date)
    rows: list[dict[str, str]] = []
    for ticker in sorted(new_targets):
        signal_weight = _fmt(current.get(ticker, 0.0))
        post_weight = _fmt(new_targets[ticker])
        delta = float(post_weight) - float(signal_weight)
        action = "SELL" if delta < 0 else "BUY"
        rows.append(
            {
                "SignalDate": signal_date.isoformat(),
                "PlannedExecutionDate": execution_date.isoformat(),
                "Ticker": ticker,
                "Action": action,
                "SignalPortfolioWeight": signal_weight,
                "TradeWeightDelta": _fmt(delta),
                "ImpliedPostTradeWeightAtSignalClose": post_weight,
            }
        )
    return rows


def generate_rows(*, seed: int, start: date, months: int) -> list[dict[str, str]]:
    """Generate the full synthetic instruction history.

    Args:
        seed: Seed for the drift generator; identical seeds yield identical rows.
        start: First candidate calendar day (the first weekday on or after it
            becomes the INITIALIZE day).
        months: Calendar months of history to generate.

    Returns:
        CSV rows in feed order: INITIALIZE rows on day one, then per business day
        either a single ``PORTFOLIO/NONE`` heartbeat row or one row per ticker on
        rebalance days (month-ends plus one seeded ad-hoc rebalance).
    """
    rng = random.Random(seed)
    days = _business_days(start, _add_months(start, months))
    if not days:
        return []

    init_day = days[0]
    month_ends = [
        day
        for index, day in enumerate(days)
        if index + 1 == len(days) or days[index + 1].month != day.month
    ]
    rebalance_days = [day for day in month_ends if day != init_day]

    # One seeded ad-hoc rebalance on a mid-month day (never day one, never a
    # month-end): the feed's analogue of an off-cycle risk event.
    adhoc_candidates = [
        day for day in days if day not in rebalance_days and day != init_day and 10 <= day.day <= 20
    ]
    adhoc_day = rng.choice(adhoc_candidates) if adhoc_candidates else None

    rows: list[dict[str, str]] = [
        {
            "SignalDate": init_day.isoformat(),
            "PlannedExecutionDate": init_day.isoformat(),
            "Ticker": ticker,
            "Action": "INITIALIZE",
            "SignalPortfolioWeight": _fmt(0.0),
            "TradeWeightDelta": _fmt(BASE_TARGETS[ticker]),
            "ImpliedPostTradeWeightAtSignalClose": _fmt(BASE_TARGETS[ticker]),
        }
        for ticker in sorted(BASE_TARGETS)
    ]

    with_new_asset = dict(BASE_TARGETS)
    with_new_asset[NEW_ASSET_FUNDED_FROM] = round(
        BASE_TARGETS[NEW_ASSET_FUNDED_FROM] - NEW_ASSET_WEIGHT, 8
    )
    with_new_asset[NEW_ASSET_TICKER] = NEW_ASSET_WEIGHT

    completed_rebalances = 0
    held_targets = dict(BASE_TARGETS)
    for day in days[1:]:
        if day in rebalance_days or day == adhoc_day:
            new_targets = (
                with_new_asset
                if completed_rebalances >= NEW_ASSET_JOIN_REBALANCE_INDEX
                else BASE_TARGETS
            )
            sigma = ADHOC_DRIFT_SIGMA if day == adhoc_day else MONTHLY_DRIFT_SIGMA
            rows.extend(_rebalance_rows(rng, day, held_targets, dict(new_targets), sigma))
            held_targets = dict(new_targets)
            completed_rebalances += 1
        else:
            rows.append(
                {
                    "SignalDate": day.isoformat(),
                    "PlannedExecutionDate": _next_business_day(day).isoformat(),
                    "Ticker": "PORTFOLIO",
                    "Action": "NONE",
                    "SignalPortfolioWeight": "",
                    "TradeWeightDelta": "",
                    "ImpliedPostTradeWeightAtSignalClose": "",
                }
            )
    return rows


def write_csv(rows: list[dict[str, str]], out_path: Path) -> None:
    """Write generated rows to ``out_path`` with the canonical column order."""
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    """Generate the demo CSV named by ``--out`` (deterministic per ``--seed``)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2024, 1, 2))
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--out", type=Path, default=Path("demo_rebalance_signals.csv"))
    args = parser.parse_args()

    rows = generate_rows(seed=args.seed, start=args.start, months=args.months)
    write_csv(rows, args.out)
    signal_dates = {row["SignalDate"] for row in rows}
    print(f"wrote {len(rows)} row(s) across {len(signal_dates)} signal date(s) to {args.out}")


if __name__ == "__main__":
    main()
