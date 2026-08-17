"""Tests for the model-level Sealer/Unsealer wrappers.

Purpose:
    ``Sealer``/``Unsealer`` connect the pure crypto core to the SDK's frozen
    pydantic models: seal a ``Signal`` before publish, unseal a
    ``ReceivedSignal`` before delivery, preserving every metadata field.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

pytest.importorskip("cryptography", reason="sealed tests need the [sealed] extra")

from multiedge_relay.exceptions import SealedError, UnsealError
from multiedge_relay.models import ReceivedSignal, Signal
from multiedge_relay.sealed.keys import RecipientKeypair, SenderKeypair
from multiedge_relay.sealed.registry import Sealer, Unsealer

PAYLOAD = {"kind": "portfolio_rebalance", "positions": []}


@pytest.fixture(scope="module")
def sender() -> SenderKeypair:
    """Module-scoped dual sender."""
    return SenderKeypair.generate()


@pytest.fixture(scope="module")
def recipient() -> RecipientKeypair:
    """Module-scoped recipient."""
    return RecipientKeypair.generate()


def test_sealer_seals_signal_model_and_preserves_metadata(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """seal_signal replaces only the payload; all other fields survive."""
    sealer = Sealer(sender=sender, recipients=[recipient.public_bundle()])
    signal = Signal(
        strategy_id="strat_A",
        payload=PAYLOAD,
        client_signal_id="01J8ZC2V7QXYZABCDEF0123456",
        correlation_id="corr-1",
    )

    sealed = sealer.seal_signal(signal)

    assert sealed.payload["sealed"] == "v1"
    assert sealed.strategy_id == "strat_A"
    assert sealed.client_signal_id == "01J8ZC2V7QXYZABCDEF0123456"
    assert sealed.correlation_id == "corr-1"
    assert signal.payload == PAYLOAD  # the original frozen model is untouched


def test_sealer_requires_client_signal_id(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """Sealing without the idempotency key is a caller bug (AAD needs it)."""
    sealer = Sealer(sender=sender, recipients=[recipient.public_bundle()])

    with pytest.raises(SealedError, match="client_signal_id"):
        sealer.seal_signal(Signal(strategy_id="strat_A", payload=PAYLOAD))


def test_unsealer_returns_received_signal_with_plaintext_payload(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """unseal_signal round-trips the sealed payload back to plaintext."""
    sealer = Sealer(sender=sender, recipients=[recipient.public_bundle()])
    sealed = sealer.seal_signal(
        Signal(
            strategy_id="strat_A",
            payload=PAYLOAD,
            client_signal_id="01J8ZC2V7QXYZABCDEF0123456",
        )
    )
    received = ReceivedSignal(
        sequence=7,
        signal_id="01J8ZD000000000000000000",
        strategy_id="strat_A",
        client_signal_id="01J8ZC2V7QXYZABCDEF0123456",
        published_at=datetime.now(UTC),
        payload=sealed.payload,
    )

    unsealer = Unsealer(recipient=recipient, sender_bundle=sender.public_bundle())
    plain = unsealer.unseal_signal(received)

    assert plain.payload == PAYLOAD
    assert plain.sequence == 7
    assert plain.signal_id == received.signal_id
    assert received.payload["sealed"] == "v1"  # original untouched


def test_unsealer_requires_client_signal_id_on_received(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """A received signal without client_signal_id cannot be unsealed (AAD input)."""
    received = ReceivedSignal(
        sequence=1,
        signal_id="01J8ZD000000000000000000",
        strategy_id="strat_A",
        published_at=datetime.now(UTC),
        payload={"sealed": "v1"},
    )
    unsealer = Unsealer(recipient=recipient, sender_bundle=sender.public_bundle())

    with pytest.raises(UnsealError, match="client_signal_id"):
        unsealer.unseal_signal(received)


def test_received_signal_accepts_client_signal_id_field() -> None:
    """ReceivedSignal carries the relay envelope's client_signal_id (additive field)."""
    received = ReceivedSignal(
        sequence=1,
        signal_id="01J8ZD000000000000000000",
        strategy_id="strat_A",
        client_signal_id="01J8ZC2V7QXYZABCDEF0123456",
        published_at=datetime.now(UTC),
        payload={},
    )
    assert received.client_signal_id == "01J8ZC2V7QXYZABCDEF0123456"
