"""Sealed-mode subscribe: payloads decrypt only inside this process.

One-time setup (see README "Sealed Mode"):
    multiedge sealed keygen --kind recipient --out recipient.key.json
    multiedge sealed register --key recipient.key.json --client CLIENT_ID --api-key mesk_...
    # Read the printed fingerprint to the publisher over a separate channel.

Run:
    MULTIEDGE_API_KEY=mesk_your_api_key MULTIEDGE_STRATEGY_ID=your_strategy \
        python unseal_subscribe.py

Requires: pip install "multiedge-relay[sealed]"
"""

from __future__ import annotations

import os

from multiedge_relay import ReceivedSignal, SignalMeta, SignalSubscriber
from multiedge_relay.sealed import RecipientKeypair, Unsealer


def on_signal(signal: ReceivedSignal, meta: SignalMeta) -> None:
    """Handle one verified, decrypted signal (idempotent — delivery is at-least-once)."""
    print(f"[{meta.source}] seq={signal.sequence} plaintext={signal.payload}")


def main() -> None:
    """Follow a sealed strategy stream, decrypting before every callback."""
    api_key = os.environ.get("MULTIEDGE_API_KEY", "mesk_your_api_key")
    strategy_id = os.environ.get("MULTIEDGE_STRATEGY_ID", "example-strategy")

    recipient = RecipientKeypair.load(
        os.environ.get("MULTIEDGE_RECIPIENT_KEY", "recipient.key.json")
    )
    # Fetches the publisher's signing bundle and re-verifies its fingerprint.
    # Pass pinned_sender="<fingerprint confirmed with the publisher out-of-band>"
    # to remove the relay from the trust path entirely.
    unsealer = Unsealer.from_relay(api_key=api_key, strategy_id=strategy_id, recipient=recipient)

    subscriber = SignalSubscriber(
        api_key=api_key,
        strategy_id=strategy_id,
        on_signal=on_signal,
        unsealer=unsealer,
    )
    # An unseal failure (tampering, wrong key, signature downgrade) surfaces
    # loudly and the cursor is NOT committed — never silent loss, never
    # unverified ciphertext in your callback.
    subscriber.run()


if __name__ == "__main__":
    main()
