"""Subscriber example: exactly-once processing with the SQLite state store.

The relay delivers at-least-once — after a crash between your callback and the
cursor commit, the same signal is redelivered on restart. With a plain cursor
store that redelivery re-enters your handler; with ``SqliteStateStore`` the
processed-marker (committed atomically when the handler returns) absorbs it, so
the handler completes at most once per ``signal_id`` across restarts.

One file (default ``~/.multiedge/state.db``) is both the cursor store and the
dedup ledger; the same file also serves webhook receivers (see the webhook
examples).

Run:
    MULTIEDGE_API_KEY=mesk_your_api_key python subscribe_exactly_once.py
"""

from __future__ import annotations

import os

from multiedge_relay import ReceivedSignal, SignalMeta, SignalSubscriber, SqliteStateStore


def handle(signal: ReceivedSignal, meta: SignalMeta) -> None:
    """Handle one signal — runs at most once per signal_id, even across restarts.

    Honest limit: external side effects (order placement, e-mail) retain a tiny
    at-least-once window — a crash after this returns but before the marker
    commits re-runs it once. State written to the store's own database via
    ``store.exactly_once_tx`` commits atomically with the marker and is truly
    exactly-once.
    """
    print(f"[{meta.source}] seq={signal.sequence} {signal.strategy_id}: {signal.payload}")


def main() -> None:
    """Run the subscriber with durable exactly-once processing."""
    store = SqliteStateStore()  # ~/.multiedge/state.db
    subscriber = SignalSubscriber(
        api_key=os.environ.get("MULTIEDGE_API_KEY", "mesk_your_api_key"),
        strategy_id="example-strategy",
        on_signal=store.exactly_once(handle),
        cursor_store=store,
        live_transport="poll",
        poll_interval=5.0,
    )
    try:
        subscriber.run()
    except KeyboardInterrupt:
        subscriber.stop()
        print("stopped; restart resumes from the cursor without re-running your handler")
    finally:
        store.close()


if __name__ == "__main__":
    main()
