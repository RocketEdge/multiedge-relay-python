"""Sealed envelope v1: seal and unseal.

Purpose:
    The cryptographic core of sealed mode. ``seal()`` turns a plaintext payload
    dict into the sealed envelope JSON object that travels in the relay's
    ``payload`` field; ``unseal()`` reverses it after verifying authenticity.
    The relay never runs any of this code — it stores and forwards the envelope
    as opaque JSON.

Contract (sealed envelope v1, normative — mirrored in relay ADR 0004):
    * Plaintext: ``canonical_json(payload)`` — deterministic bytes, so relay
      JSON re-serialization cannot break anything.
    * Confidentiality: fresh 32-byte DEK per signal; ChaCha20-Poly1305 with a
      random 12-byte nonce (carried) and an AAD that binds
      ``strategy_id``/``client_signal_id``/``sender_kid`` — an envelope moved
      to another strategy or idempotency identity fails authentication.
    * Per-recipient DEK wrap: hybrid X25519 (one fresh ephemeral per signal) +
      ML-KEM-768 (fresh encapsulation per recipient); both shared secrets and
      the full transcript hash feed HKDF-SHA256 → per-recipient KEK; the DEK
      is wrapped under the KEK with a fixed zero nonce (the KEK is provably
      single-use — HPKE's own base-nonce pattern).
    * Authenticity: encrypt-then-sign. ``sig`` (Ed25519) is always present;
      ``sig_pq`` (ML-DSA-65) is present for dual senders. Verification derives
      the requirement from the PINNED sender bundle, so stripping ``sig_pq``
      (downgrade) is rejected.
    * Side-effects: none. Randomness: injectable ``rng`` for DEK/nonce; the
      ephemeral X25519 and ML-KEM encapsulation draw from the library CSPRNG.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from collections.abc import Callable, Sequence
from typing import Any

from cryptography.exceptions import InvalidSignature, InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ed25519, mldsa, mlkem, x25519
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from ..exceptions import NotARecipientError, SealedError, SealedKeyError, UnsealError
from .keys import RecipientKeypair, SenderKeypair, bundle_fingerprint, canonical_json

_VERSION = "v1"
_DOMAIN = b"multiedge-sealed-v1"
_KEK_INFO = b"multiedge-sealed-v1-kek"
_ZERO_NONCE = b"\x00" * 12
_ALG_DUAL = {
    "aead": "chacha20poly1305",
    "kem": "x25519-mlkem768",
    "kdf": "hkdf-sha256",
    "sig": "ed25519+mldsa65",
}
_ALG_CLASSICAL = {**_ALG_DUAL, "sig": "ed25519"}


def _b64(data: bytes) -> str:
    """Encode bytes as standard base64 text."""
    return base64.b64encode(data).decode("ascii")


def _b64d(text: Any) -> bytes:
    """Decode standard base64; raise UnsealError on malformed input."""
    if not isinstance(text, str):
        raise UnsealError("sealed envelope: expected a base64 string")
    try:
        return base64.b64decode(text, validate=True)
    except (ValueError, TypeError) as exc:
        raise UnsealError(f"sealed envelope: malformed base64: {exc}") from exc


def _aad(strategy_id: str, client_signal_id: str, sender_kid: str) -> bytes:
    """Build the AEAD associated data binding envelope to its identity.

    NUL-separated (all components are ULIDs/hex/slugs — NUL-free), prefixed
    with the domain tag. Binding ``client_signal_id`` means replay within the
    strategy is already blocked by the relay's idempotency index, and moving
    the envelope to a different publish identity breaks decryption.
    """
    return b"\x00".join(
        [_DOMAIN, strategy_id.encode(), client_signal_id.encode(), sender_kid.encode()]
    )


def _kek(
    ss_x25519: bytes, ss_mlkem: bytes, epk: bytes, kem_ct: bytes, kid: str, aad: bytes
) -> bytes:
    """Derive the per-recipient key-encryption key (hybrid concatenation KDF).

    Both shared secrets are concatenated as IKM; the transcript hash (ephemeral
    public key, KEM ciphertext, recipient kid, AAD) is bound via HKDF ``info``
    so a recipient entry grafted from another envelope derives a different KEK
    (IND-CCA holds if either component KEM holds).
    """
    transcript = hashlib.sha256(epk + kem_ct + kid.encode() + aad).digest()
    return HKDF(
        algorithm=hashes.SHA256(), length=32, salt=None, info=_KEK_INFO + transcript
    ).derive(ss_x25519 + ss_mlkem)


def _signature_input(envelope: dict[str, Any]) -> bytes:
    """Hash the canonical envelope-minus-signatures — the message both schemes sign."""
    unsigned = {key: value for key, value in envelope.items() if key not in ("sig", "sig_pq")}
    return hashlib.sha256(canonical_json(unsigned)).digest()


def seal(
    payload: dict[str, Any],
    *,
    strategy_id: str,
    client_signal_id: str,
    recipients: Sequence[dict[str, Any]],
    sender: SenderKeypair,
    rng: Callable[[int], bytes] = os.urandom,
) -> dict[str, Any]:
    """Seal a payload to a set of recipient bundles.

    Args:
        payload: The plaintext business payload (any JSON-serializable dict).
        strategy_id: The strategy this signal will be published under (bound
            into the AAD — the envelope is unusable under any other strategy).
        client_signal_id: The signal's idempotency key; MUST already be
            assigned (the publisher seals after ULID assignment).
        recipients: Public recipient bundles (``purpose: "recipient"``) to
            seal to; every entitled subscriber must be listed or it cannot
            decrypt — late-entitled subscribers cannot read history.
        sender: The publisher's signing keypair (dual → Ed25519 + ML-DSA-65).
        rng: Randomness source for the DEK and nonce (injectable for tests).

    Returns:
        The sealed envelope v1 dict — publish it as the signal ``payload``.

    Raises:
        SealedError: On empty recipients or a malformed recipient bundle.
    """
    if not recipients:
        raise SealedError("seal() needs at least one recipient bundle")

    aad = _aad(strategy_id, client_signal_id, sender.fingerprint)
    dek = rng(32)
    nonce = rng(12)
    ciphertext = ChaCha20Poly1305(dek).encrypt(nonce, canonical_json(payload), aad)

    ephemeral = x25519.X25519PrivateKey.generate()
    epk = ephemeral.public_key().public_bytes_raw()

    entries: list[dict[str, str]] = []
    for bundle in recipients:
        try:
            kid = bundle_fingerprint(bundle)
            peer_x25519 = x25519.X25519PublicKey.from_public_bytes(
                base64.b64decode(bundle["x25519_pub"], validate=True)
            )
            peer_mlkem = mlkem.MLKEM768PublicKey.from_public_bytes(
                base64.b64decode(bundle["mlkem768_ek"], validate=True)
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise SealedKeyError(f"malformed recipient bundle: {exc}") from exc
        ss_x25519 = ephemeral.exchange(peer_x25519)
        ss_mlkem, kem_ct = peer_mlkem.encapsulate()
        kek = _kek(ss_x25519, ss_mlkem, epk, kem_ct, kid, aad)
        wrap = ChaCha20Poly1305(kek).encrypt(_ZERO_NONCE, dek, b"")
        entries.append({"kid": kid, "epk": _b64(epk), "kem_ct": _b64(kem_ct), "wrap": _b64(wrap)})

    envelope: dict[str, Any] = {
        "sealed": _VERSION,
        "alg": dict(_ALG_DUAL if sender.dual else _ALG_CLASSICAL),
        "sender_kid": sender.fingerprint,
        "nonce": _b64(nonce),
        "ct": _b64(ciphertext),
        "recipients": entries,
    }
    digest = _signature_input(envelope)
    envelope["sig"] = _b64(sender.sign(_DOMAIN + digest))
    if sender.dual:
        envelope["sig_pq"] = _b64(sender.sign_pq(digest, _DOMAIN))
    return envelope


def unseal(
    sealed_envelope: dict[str, Any],
    *,
    strategy_id: str,
    client_signal_id: str,
    recipient: RecipientKeypair,
    sender_bundle: dict[str, Any],
) -> dict[str, Any]:
    """Verify and decrypt a sealed envelope.

    Verification order (fail-fast, nothing decrypted before authenticity):
    algorithm allow-list → sender bundle fingerprint vs ``sender_kid`` →
    downgrade rule → both signatures → locate own recipient entry →
    hybrid decapsulation → DEK unwrap → AEAD open with reconstructed AAD.

    Args:
        sealed_envelope: The received ``payload`` dict.
        strategy_id: From the received signal (outer envelope) — AAD input.
        client_signal_id: From the received signal — AAD input.
        recipient: This subscriber's keypair.
        sender_bundle: The publisher's PINNED public bundle (fetched once and
            fingerprint-verified; never taken from the envelope itself).

    Returns:
        The plaintext payload dict.

    Raises:
        UnsealError: On any verification or decryption failure.
        NotARecipientError: When this keypair is not in the recipient list.
    """
    envelope = sealed_envelope
    if not isinstance(envelope, dict) or envelope.get("sealed") != _VERSION:
        raise UnsealError("not a sealed v1 envelope")
    alg = envelope.get("alg")
    if alg not in (_ALG_DUAL, _ALG_CLASSICAL):
        raise UnsealError(f"unknown or unsupported alg suite: {alg!r}")

    sender_kid = envelope.get("sender_kid")
    if bundle_fingerprint(sender_bundle) != sender_kid:
        raise UnsealError(
            "sender bundle fingerprint does not match the envelope's sender_kid — "
            "verify the publisher's fingerprint out-of-band"
        )

    sender_is_dual = "mldsa65_pub" in sender_bundle
    if sender_is_dual and (alg["sig"] != "ed25519+mldsa65" or "sig_pq" not in envelope):
        raise UnsealError(
            "signature downgrade rejected: the pinned sender key is dual "
            "(Ed25519 + ML-DSA-65) but the envelope lacks sig_pq"
        )

    digest = _signature_input(envelope)
    try:
        ed25519.Ed25519PublicKey.from_public_bytes(_b64d(sender_bundle["ed25519_pub"])).verify(
            _b64d(envelope.get("sig")), _DOMAIN + digest
        )
        if sender_is_dual:
            mldsa.MLDSA65PublicKey.from_public_bytes(_b64d(sender_bundle["mldsa65_pub"])).verify(
                _b64d(envelope.get("sig_pq")), digest, context=_DOMAIN
            )
    except InvalidSignature as exc:
        raise UnsealError("sealed envelope signature verification failed") from exc
    except (KeyError, ValueError) as exc:
        raise UnsealError(f"malformed sender bundle or signature: {exc}") from exc

    entries = envelope.get("recipients")
    if not isinstance(entries, list):
        raise UnsealError("sealed envelope: recipients must be a list")
    own = next(
        (e for e in entries if isinstance(e, dict) and e.get("kid") == recipient.fingerprint), None
    )
    if own is None:
        raise NotARecipientError(
            f"no recipient entry for key {recipient.fingerprint[:16]}… — this signal was "
            "sealed before this key was registered and entitled (sealed history is never "
            "re-encrypted), or the key is not registered for this strategy"
        )

    aad = _aad(strategy_id, client_signal_id, str(sender_kid))
    epk = _b64d(own.get("epk"))
    kem_ct = _b64d(own.get("kem_ct"))
    try:
        ss_x25519 = recipient.x25519_exchange(epk)
        ss_mlkem = recipient.mlkem_decapsulate(kem_ct)
        kek = _kek(ss_x25519, ss_mlkem, epk, kem_ct, str(own["kid"]), aad)
        dek = ChaCha20Poly1305(kek).decrypt(_ZERO_NONCE, _b64d(own.get("wrap")), b"")
        plaintext = ChaCha20Poly1305(dek).decrypt(
            _b64d(envelope.get("nonce")), _b64d(envelope.get("ct")), aad
        )
    except InvalidTag as exc:
        raise UnsealError(
            "sealed envelope failed authenticated decryption — tampering, or the "
            "envelope was moved to a different strategy/client_signal_id"
        ) from exc
    except ValueError as exc:
        raise UnsealError(f"sealed envelope: invalid cryptographic field: {exc}") from exc

    result: dict[str, Any] = json.loads(plaintext)
    return result
