"""Subscriber example: catch up from the cursor, then follow live.

Offline for a weekend — you miss nothing: the subscriber resumes from its persisted
cursor, replays the backlog in order, then polls for new signals.

Run:
    MULTIEDGE_API_KEY=mek_your_api_key python subscribe_catchup.py
"""

from __future__ import annotations

import os

from multiedge_relay import ReceivedSignal, SignalMeta, SignalSubscriber


def on_signal(signal: ReceivedSignal, meta: SignalMeta) -> None:
    """Handle one delivery. MUST be idempotent: delivery is at-least-once.

    Key any side effect (order placement, DB write) on ``signal.signal_id`` or
    ``signal.sequence`` so a redelivery after a crash is a no-op.
    """
    print(f"[{meta.source}] seq={signal.sequence} {signal.strategy_id}: {signal.payload}")


def main() -> None:
    """Run the subscriber until Ctrl-C; the cursor survives restarts."""
    subscriber = SignalSubscriber(
        api_key=os.environ.get("MULTIEDGE_API_KEY", "mek_your_api_key"),
        strategy_id="example-strategy",
        on_signal=on_signal,
        live_transport="poll",  # or "webpubsub" with the [webpubsub] extra
        poll_interval=5.0,
    )
    try:
        subscriber.run()
    except KeyboardInterrupt:
        subscriber.stop()
        print("stopped; cursor is persisted — restart resumes exactly where you left off")


if __name__ == "__main__":
    main()
