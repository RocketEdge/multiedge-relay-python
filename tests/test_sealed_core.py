"""Tests for sealed envelope v1 seal/unseal.

Purpose:
    ``multiedge_relay.sealed.core`` is the entire cryptographic core of sealed
    mode. These tests pin the wire format, the AAD identity binding, the hybrid
    KEM wrap, multi-recipient behavior, and every rejection path (tampering,
    substitution, signature stripping, unknown algorithms).
"""

from __future__ import annotations

import base64
import copy
import json
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("cryptography", reason="sealed tests need the [sealed] extra")

from hypothesis import given, settings
from hypothesis import strategies as st

from multiedge_relay.exceptions import NotARecipientError, SealedError, UnsealError
from multiedge_relay.sealed.core import seal, unseal
from multiedge_relay.sealed.keys import RecipientKeypair, SenderKeypair

STRATEGY_ID = "strat_01J8ZC2V7Q"
CLIENT_SIGNAL_ID = "01J8ZC2V7QXYZABCDEF0123456"
PAYLOAD = {
    "kind": "portfolio_rebalance",
    "signal_date": "2026-08-17",
    "positions": [{"ticker": "SPY", "action": "BUY", "signal_portfolio_weight": 0.6}],
}


@pytest.fixture(scope="module")
def sender() -> SenderKeypair:
    """One dual sender for the module (keygen is the slow part)."""
    return SenderKeypair.generate()


@pytest.fixture(scope="module")
def recipient() -> RecipientKeypair:
    """One recipient for the module."""
    return RecipientKeypair.generate()


def _seal(
    sender: SenderKeypair,
    *recipients: RecipientKeypair,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Seal PAYLOAD to the given recipients with the module identities."""
    return seal(
        payload if payload is not None else PAYLOAD,
        strategy_id=STRATEGY_ID,
        client_signal_id=CLIENT_SIGNAL_ID,
        recipients=[r.public_bundle() for r in recipients],
        sender=sender,
    )


def _unseal(
    envelope: dict[str, Any],
    recipient: RecipientKeypair,
    sender: SenderKeypair,
    *,
    strategy_id: str = STRATEGY_ID,
    client_signal_id: str = CLIENT_SIGNAL_ID,
) -> dict[str, Any]:
    """Unseal with the module identities."""
    return unseal(
        envelope,
        strategy_id=strategy_id,
        client_signal_id=client_signal_id,
        recipient=recipient,
        sender_bundle=sender.public_bundle(),
    )


def test_seal_unseal_roundtrip_single_recipient(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """The fundamental contract: unseal(seal(p)) == p."""
    envelope = _seal(sender, recipient)

    assert envelope["sealed"] == "v1"
    assert envelope["alg"] == {
        "aead": "chacha20poly1305",
        "kem": "x25519-mlkem768",
        "kdf": "hkdf-sha256",
        "sig": "ed25519+mldsa65",
    }
    assert _unseal(envelope, recipient, sender) == PAYLOAD


def test_envelope_is_json_object_of_documented_shape(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """The envelope round-trips through JSON and has exactly the v1 fields."""
    envelope = json.loads(json.dumps(_seal(sender, recipient)))

    assert set(envelope) == {
        "sealed",
        "alg",
        "sender_kid",
        "nonce",
        "ct",
        "recipients",
        "sig",
        "sig_pq",
    }
    assert len(base64.b64decode(envelope["nonce"])) == 12
    (entry,) = envelope["recipients"]
    assert set(entry) == {"kid", "epk", "kem_ct", "wrap"}
    assert entry["kid"] == recipient.fingerprint
    assert len(base64.b64decode(entry["epk"])) == 32
    assert len(base64.b64decode(entry["kem_ct"])) == 1088
    assert len(base64.b64decode(entry["wrap"])) == 48
    assert len(base64.b64decode(envelope["sig"])) == 64
    assert len(base64.b64decode(envelope["sig_pq"])) == 3309
    assert envelope["sender_kid"] == sender.fingerprint


def test_roundtrip_multi_recipient_each_can_decrypt(sender: SenderKeypair) -> None:
    """Every listed recipient independently recovers the same plaintext."""
    recipients = [RecipientKeypair.generate() for _ in range(3)]
    envelope = _seal(sender, *recipients)

    assert len(envelope["recipients"]) == 3
    for keypair in recipients:
        assert _unseal(envelope, keypair, sender) == PAYLOAD


def test_wrong_recipient_key_raises_not_a_recipient(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """A keypair whose kid is not listed gets the documented error."""
    envelope = _seal(sender, recipient)
    outsider = RecipientKeypair.generate()

    with pytest.raises(NotARecipientError, match="sealed before"):
        _unseal(envelope, outsider, sender)


def test_tampered_ct_fails(sender: SenderKeypair, recipient: RecipientKeypair) -> None:
    """Flipping a ciphertext byte must fail (signature or AEAD — never plaintext)."""
    envelope = _seal(sender, recipient)
    raw = bytearray(base64.b64decode(envelope["ct"]))
    raw[0] ^= 0xFF
    envelope["ct"] = base64.b64encode(bytes(raw)).decode()

    with pytest.raises(UnsealError):
        _unseal(envelope, recipient, sender)


def test_tampered_recipient_entry_fails(sender: SenderKeypair) -> None:
    """Swapping recipient entries between envelopes fails (transcript is in the KDF)."""
    keypair_a = RecipientKeypair.generate()
    keypair_b = RecipientKeypair.generate()
    envelope_one = _seal(sender, keypair_a, keypair_b)
    envelope_two = _seal(sender, keypair_a, keypair_b, payload={"other": True})

    grafted = copy.deepcopy(envelope_one)
    grafted["recipients"][0] = envelope_two["recipients"][0]

    with pytest.raises(UnsealError):
        _unseal(grafted, keypair_a, sender)


def test_strategy_substitution_fails_aad(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """An envelope replayed under a different strategy fails the AAD binding."""
    envelope = _seal(sender, recipient)

    with pytest.raises(UnsealError):
        _unseal(envelope, recipient, sender, strategy_id="strat_OTHER")


def test_client_signal_id_substitution_fails_aad(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """An envelope re-attached to a different client_signal_id fails the AAD binding."""
    envelope = _seal(sender, recipient)

    with pytest.raises(UnsealError):
        _unseal(envelope, recipient, sender, client_signal_id="01JDIFFERENT0000000000000")


def test_signature_tamper_fails(sender: SenderKeypair, recipient: RecipientKeypair) -> None:
    """A modified envelope field must fail signature verification."""
    envelope = _seal(sender, recipient)
    envelope["sender_kid"] = "0" * 64

    with pytest.raises(UnsealError):
        _unseal(envelope, recipient, sender)


def test_sig_pq_strip_downgrade_rejected_when_sender_bundle_is_dual(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """Removing sig_pq from a dual sender's envelope is an attack, not a fallback."""
    envelope = _seal(sender, recipient)
    del envelope["sig_pq"]
    envelope["alg"]["sig"] = "ed25519"

    with pytest.raises(UnsealError, match=r"downgrade|sig_pq"):
        _unseal(envelope, recipient, sender)


def test_classical_only_sender_roundtrip(recipient: RecipientKeypair) -> None:
    """A dual=False sender produces a valid ed25519-only envelope."""
    classical = SenderKeypair.generate(dual=False)
    envelope = seal(
        PAYLOAD,
        strategy_id=STRATEGY_ID,
        client_signal_id=CLIENT_SIGNAL_ID,
        recipients=[recipient.public_bundle()],
        sender=classical,
    )

    assert envelope["alg"]["sig"] == "ed25519"
    assert "sig_pq" not in envelope
    result = unseal(
        envelope,
        strategy_id=STRATEGY_ID,
        client_signal_id=CLIENT_SIGNAL_ID,
        recipient=recipient,
        sender_bundle=classical.public_bundle(),
    )
    assert result == PAYLOAD


def test_sender_bundle_mismatch_rejected(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """Verifying against a different sender's bundle fails before any decryption."""
    envelope = _seal(sender, recipient)
    imposter = SenderKeypair.generate()

    with pytest.raises(UnsealError, match="sender"):
        _unseal(envelope, recipient, imposter)


def test_unknown_alg_ids_rejected(sender: SenderKeypair, recipient: RecipientKeypair) -> None:
    """Envelopes advertising unknown algorithms are rejected up front."""
    envelope = _seal(sender, recipient)
    envelope["alg"]["aead"] = "aes-128-cbc"

    with pytest.raises(UnsealError, match="alg"):
        _unseal(envelope, recipient, sender)


def test_seal_requires_recipients(sender: SenderKeypair) -> None:
    """Sealing to nobody is a caller bug, not an empty envelope."""
    with pytest.raises(SealedError, match="recipient"):
        seal(
            PAYLOAD,
            strategy_id=STRATEGY_ID,
            client_signal_id=CLIENT_SIGNAL_ID,
            recipients=[],
            sender=sender,
        )


def test_vector_stability(sender: SenderKeypair, recipient: RecipientKeypair) -> None:
    """A committed envelope fixture still unseals — guards accidental format drift.

    The fixture was generated once by this implementation and committed; if the
    KDF info string, AAD layout, transcript, or signature input ever changes,
    this test fails even though fresh roundtrips would still pass.
    """
    fixture_path = Path(__file__).parent / "fixtures" / "sealed_v1_vector.json"
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))

    loaded_recipient = RecipientKeypair.load(fixture_path.parent / "sealed_v1_recipient.key.json")
    plaintext = unseal(
        fixture["envelope"],
        strategy_id=fixture["strategy_id"],
        client_signal_id=fixture["client_signal_id"],
        recipient=loaded_recipient,
        sender_bundle=fixture["sender_bundle"],
    )

    assert plaintext == fixture["payload"]


@settings(max_examples=25, deadline=None)
@given(
    payload=st.dictionaries(
        st.text(min_size=1, max_size=8),
        st.recursive(
            st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False) | st.text(),
            lambda children: st.lists(children, max_size=3),
            max_leaves=6,
        ),
        max_size=5,
    )
)
def test_hypothesis_roundtrip(payload: dict[str, Any]) -> None:
    """Arbitrary JSON payloads survive seal/unseal unchanged."""
    sender = _HYPO_SENDER
    recipient = _HYPO_RECIPIENT
    envelope = seal(
        payload,
        strategy_id=STRATEGY_ID,
        client_signal_id=CLIENT_SIGNAL_ID,
        recipients=[recipient.public_bundle()],
        sender=sender,
    )
    result = unseal(
        envelope,
        strategy_id=STRATEGY_ID,
        client_signal_id=CLIENT_SIGNAL_ID,
        recipient=recipient,
        sender_bundle=sender.public_bundle(),
    )
    assert result == payload


# Module-level identities for hypothesis (fixtures are not usable inside @given).
_HYPO_SENDER = SenderKeypair.generate()
_HYPO_RECIPIENT = RecipientKeypair.generate()
