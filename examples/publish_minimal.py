"""Minimal publish example: one signal, one ack.

Run:
    MULTIEDGE_API_KEY=mek_your_api_key python publish_minimal.py
"""

from __future__ import annotations

import os

from multiedge_relay import Signal, SignalPublisher


def main() -> None:
    """Publish a single BUY signal and print the relay's acknowledgment."""
    api_key = os.environ.get("MULTIEDGE_API_KEY", "mek_your_api_key")
    with SignalPublisher(api_key=api_key) as publisher:
        ack = publisher.publish(
            Signal(
                strategy_id="example-strategy",
                payload={"action": "BUY", "ticker": "SPY", "target_weight": 0.25},
            )
        )
    print(f"accepted: signal_id={ack.signal_id} sequence={ack.sequence}")
    if ack.deduplicated:
        print("(the relay had already seen this client_signal_id — nothing double-published)")


if __name__ == "__main__":
    main()
