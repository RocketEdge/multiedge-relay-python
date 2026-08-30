"""Sealed-mode publish: end-to-end encrypted — the relay cannot read the payload.

One-time setup (see README "Sealed Mode"):
    multiedge sealed keygen --kind sender --out sender.key.json
    multiedge sealed register --key sender.key.json --strategy STRATEGY_ID --api-key mesk_...
    # ...and each subscriber registers a recipient key.

Run:
    MULTIEDGE_API_KEY=mesk_your_api_key MULTIEDGE_STRATEGY_ID=your_strategy \
        python seal_publish.py

Requires: pip install "multiedge-relay[sealed]"
"""

from __future__ import annotations

import os

from multiedge_relay import Signal, SignalPublisher
from multiedge_relay.sealed import Sealer, SenderKeypair


def main() -> None:
    """Seal a rebalance payload to every entitled subscriber and publish it."""
    api_key = os.environ.get("MULTIEDGE_API_KEY", "mesk_your_api_key")
    strategy_id = os.environ.get("MULTIEDGE_STRATEGY_ID", "example-strategy")

    sender = SenderKeypair.load(os.environ.get("MULTIEDGE_SENDER_KEY", "sender.key.json"))
    # Fetches the entitled subscribers' public bundles and re-verifies every
    # fingerprint locally. For the strongest configuration pass
    # pinned_recipients={...fingerprints confirmed out-of-band...} — the relay
    # then cannot substitute keys without being caught.
    sealer = Sealer.from_relay(api_key=api_key, strategy_id=strategy_id, sender=sender)

    with SignalPublisher(api_key=api_key, sealer=sealer) as publisher:
        ack = publisher.publish(
            Signal(
                strategy_id=strategy_id,
                payload={"action": "BUY", "ticker": "SPY", "target_weight": 0.25},
            )
        )
    print(f"sealed + accepted: signal_id={ack.signal_id} sequence={ack.sequence}")
    print("the relay stored only ciphertext — payload plaintext never left this process")


if __name__ == "__main__":
    main()
