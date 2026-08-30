"""Batch publish with explicit failure accounting and DLQ recovery.

Every signal ends as either an ack or a PublishFailed carrying its DLQ spill path —
nothing is silently lost. Failed signals can later be recovered with:

    multiedge dlq list
    multiedge dlq resend

Run:
    MULTIEDGE_API_KEY=mesk_your_api_key python publish_batch_with_dlq.py
"""

from __future__ import annotations

import os

from multiedge_relay import PublishFailed, Signal, SignalAck, SignalPublisher


def main() -> None:
    """Publish a batch; print per-signal outcomes instead of raising on the first failure."""
    api_key = os.environ.get("MULTIEDGE_API_KEY", "mesk_your_api_key")
    # Three SEPARATE days, each a complete portfolio — genuinely independent
    # signals, which is what publish_many is for. Slicing ONE portfolio into one
    # signal per ticker would be the anti-pattern: N sequence numbers, no
    # atomicity, and a subscriber with no way to know the book was complete.
    signals = [
        Signal(
            strategy_id="example-strategy",
            client_signal_id=f"example-strategy:{signal_date}",
            payload={
                "kind": "portfolio_rebalance",
                "signal_date": signal_date,
                "planned_execution_date": execution_date,
                "positions": [
                    {"ticker": "SPY", "action": "BUY", "signal_portfolio_weight": weight},
                    {"ticker": "TLT", "action": "SELL", "signal_portfolio_weight": 1 - weight},
                ],
            },
        )
        for signal_date, execution_date, weight in (
            ("2026-08-27", "2026-08-28", 0.5),
            ("2026-08-28", "2026-08-31", 0.6),
            ("2026-08-31", "2026-09-01", 0.7),
        )
    ]
    with SignalPublisher(api_key=api_key) as publisher:
        results = publisher.publish_many(signals, raise_on_partial=False)

    for signal, result in zip(signals, results, strict=True):
        day = signal.payload["signal_date"]
        if isinstance(result, SignalAck):
            print(f"{day}: accepted at sequence {result.sequence}")
        elif isinstance(result, PublishFailed):
            print(
                f"{day}: FAILED after {result.attempts} attempts — "
                f"dead-lettered at {result.dlq_path} (recover with `multiedge dlq resend`)"
            )


if __name__ == "__main__":
    main()
