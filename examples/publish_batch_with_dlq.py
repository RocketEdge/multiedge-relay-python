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
    signals = [
        Signal(strategy_id="example-strategy", payload={"action": "BUY", "ticker": t})
        for t in ("SPY", "TLT", "GLD")
    ]
    with SignalPublisher(api_key=api_key) as publisher:
        results = publisher.publish_many(signals, raise_on_partial=False)

    for signal, result in zip(signals, results, strict=True):
        ticker = signal.payload["ticker"]
        if isinstance(result, SignalAck):
            print(f"{ticker}: accepted at sequence {result.sequence}")
        elif isinstance(result, PublishFailed):
            print(
                f"{ticker}: FAILED after {result.attempts} attempts — "
                f"dead-lettered at {result.dlq_path} (recover with `multiedge dlq resend`)"
            )


if __name__ == "__main__":
    main()
