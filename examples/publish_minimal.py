"""Minimal publish example: one signal carrying a whole portfolio, one ack.

The payload follows the relay's shipped standard schema ``portfolio_rebalance/1.0``:
one signal states the COMPLETE portfolio for one signal date. That is the shape the
relay validates by default, and the reason there is no batch publish endpoint — the
batching lives inside ``positions``, not across requests.

Run:
    MULTIEDGE_API_KEY=mesk_your_api_key python publish_minimal.py
"""

from __future__ import annotations

import os
from typing import Any

from multiedge_relay import Signal, SignalPublisher


def rebalance_payload() -> dict[str, Any]:
    """Build a portfolio_rebalance/1.0 payload: the whole book in one signal.

    Contract:
        Matches the relay's standard schema exactly — ``kind``, ``signal_date``,
        ``planned_execution_date`` and ``positions`` are all required, each position
        carries ``ticker`` / ``action`` / ``signal_portfolio_weight``, and the schema
        is ``additionalProperties: false``, so any extra key is refused with 422.

    Returns:
        A payload dict. An empty ``positions`` list would be a valid explicit
        no-action heartbeat; this example states two target weights instead.
    """
    return {
        "kind": "portfolio_rebalance",
        "signal_date": "2026-08-31",
        "planned_execution_date": "2026-09-01",
        "positions": [
            {"ticker": "SPY", "action": "BUY", "signal_portfolio_weight": 0.6},
            {"ticker": "TLT", "action": "SELL", "signal_portfolio_weight": 0.4},
        ],
    }


def main() -> None:
    """Publish one portfolio signal and print the relay's acknowledgment."""
    api_key = os.environ.get("MULTIEDGE_API_KEY", "mesk_your_api_key")
    with SignalPublisher(api_key=api_key) as publisher:
        ack = publisher.publish(Signal(strategy_id="example-strategy", payload=rebalance_payload()))
    print(f"accepted: signal_id={ack.signal_id} sequence={ack.sequence}")
    if ack.deduplicated:
        print("(the relay had already seen this client_signal_id — nothing double-published)")


if __name__ == "__main__":
    main()
