"""Hybrid key identities for sealed mode.

Purpose:
    Defines the two key identities of sealed envelope v1 and their on-disk form:

    * ``RecipientKeypair`` — held by subscribers; X25519 (classical) +
      ML-KEM-768 (post-quantum, FIPS 203) for hybrid DEK wrapping.
    * ``SenderKeypair`` — held by publishers; Ed25519 (classical) +
      ML-DSA-65 (post-quantum, FIPS 204) for dual envelope signatures.

Contract:
    * The public **bundle** is a plain JSON dict (``bundle: "mek-v1"``); its
      **fingerprint** is ``SHA256(canonical_json(bundle))`` as 64 lowercase hex
      and doubles as the key id (``kid``) on the wire. Any party can recompute
      it — the relay never does (it is untrusted for key authenticity).
    * Key files are single JSON documents holding the seeds plus the public
      bundle, created with ``O_CREAT | O_EXCL`` and mode ``0600`` (best-effort
      on Windows); ``save()`` never overwrites.
    * All binary fields are standard base64 (with padding).
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric import ed25519, mldsa, mlkem, x25519

from ..exceptions import SealedKeyError

_FILE_KIND = "multiedge-sealed-key-v1"


def canonical_json(obj: Any) -> bytes:
    """Serialize ``obj`` to canonical JSON bytes (sorted keys, compact, ASCII).

    This is the deterministic serialization used for bundle fingerprints, the
    AEAD plaintext, and the signature input — every party must produce the
    same bytes from the same parsed value, regardless of JSON re-serialization
    in transit (the relay round-trips payloads through its own serializer).

    Args:
        obj: Any JSON-serializable value.

    Returns:
        UTF-8 bytes of ``json.dumps(obj, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True)``.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def bundle_fingerprint(bundle: dict[str, Any]) -> str:
    """Compute the fingerprint (= key id) of a public key bundle.

    Args:
        bundle: The public bundle dict (order-insensitive).

    Returns:
        64 lowercase hex chars: ``SHA256(canonical_json(bundle))``.
    """
    return hashlib.sha256(canonical_json(bundle)).hexdigest()


def _b64(data: bytes) -> str:
    """Encode bytes as standard base64 text."""
    return base64.b64encode(data).decode("ascii")


def _b64d(text: str) -> bytes:
    """Decode standard base64 text to bytes."""
    return base64.b64decode(text, validate=True)


def _display(fingerprint: str) -> str:
    """Render a fingerprint in groups of 4 for out-of-band comparison."""
    return " ".join(fingerprint[i : i + 4] for i in range(0, len(fingerprint), 4))


def _write_private_file(path: Path, document: dict[str, Any]) -> None:
    """Write a key file exclusively (never overwrite) with mode 0600.

    Args:
        path: Target file path.
        document: JSON-serializable key document.

    Raises:
        FileExistsError: When ``path`` already exists.
    """
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(document, handle, indent=2, sort_keys=True)


def _read_private_file(path: Path, expected_purpose: str) -> dict[str, Any]:
    """Read and structurally validate a key file.

    Args:
        path: Key file path.
        expected_purpose: ``"recipient"`` or ``"sender"``.

    Returns:
        The parsed key document.

    Raises:
        SealedKeyError: When the file is not a sealed key file of the expected
            purpose, or its stored fingerprint does not match its bundle.
    """
    try:
        document: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SealedKeyError(f"cannot read sealed key file {path}: {exc}") from exc
    if not isinstance(document, dict) or document.get("kind") != _FILE_KIND:
        raise SealedKeyError(f"{path} is not a {_FILE_KIND} file")
    bundle = document.get("bundle")
    if not isinstance(bundle, dict) or bundle.get("purpose") != expected_purpose:
        raise SealedKeyError(f"{path} does not hold a {expected_purpose} key")
    if document.get("kid") != bundle_fingerprint(bundle):
        raise SealedKeyError(f"{path}: stored kid does not match the bundle fingerprint")
    return document


class RecipientKeypair:
    """A subscriber's hybrid decryption identity (X25519 + ML-KEM-768).

    Purpose:
        Holds the private halves needed to unwrap a sealed envelope's DEK and
        exposes the public bundle publishers seal to.

    Contract:
        Instances are immutable in practice (attributes are never reassigned).
        Construct via ``generate()`` or ``load()``; never pass raw key objects.
    """

    def __init__(
        self,
        x25519_priv: x25519.X25519PrivateKey,
        mlkem_priv: mlkem.MLKEM768PrivateKey,
        created_at: str,
    ) -> None:
        """Bind the private keys; internal — use ``generate()`` or ``load()``.

        Args:
            x25519_priv: Classical ECDH private key.
            mlkem_priv: ML-KEM-768 decapsulation key.
            created_at: ISO-8601 UTC creation timestamp (frozen into the bundle,
                so it is part of the fingerprint).
        """
        self._x25519_priv = x25519_priv
        self._mlkem_priv = mlkem_priv
        self._created_at = created_at
        self._bundle: dict[str, Any] = {
            "bundle": "mek-v1",
            "purpose": "recipient",
            "x25519_pub": _b64(x25519_priv.public_key().public_bytes_raw()),
            "mlkem768_ek": _b64(mlkem_priv.public_key().public_bytes_raw()),
            "created_at": created_at,
        }

    @classmethod
    def generate(cls) -> RecipientKeypair:
        """Generate a fresh recipient keypair from the library CSPRNG."""
        return cls(
            x25519.X25519PrivateKey.generate(),
            mlkem.MLKEM768PrivateKey.generate(),
            datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        )

    @classmethod
    def load(cls, path: Path | str) -> RecipientKeypair:
        """Load a recipient key file written by ``save()``.

        Args:
            path: The key file path.

        Raises:
            SealedKeyError: When the file is not a valid recipient key file.
        """
        document = _read_private_file(Path(path), "recipient")
        try:
            return cls(
                x25519.X25519PrivateKey.from_private_bytes(_b64d(document["x25519_priv"])),
                mlkem.MLKEM768PrivateKey.from_seed_bytes(_b64d(document["mlkem768_seed"])),
                document["bundle"]["created_at"],
            )
        except (KeyError, ValueError) as exc:
            raise SealedKeyError(f"{path}: malformed recipient key material: {exc}") from exc

    def save(self, path: Path | str) -> None:
        """Write the key file (seeds + bundle + kid), refusing to overwrite.

        Args:
            path: Target path; created with mode 0600 (best-effort on Windows).

        Raises:
            FileExistsError: When the file already exists.
        """
        _write_private_file(
            Path(path),
            {
                "kind": _FILE_KIND,
                "kid": self.fingerprint,
                "bundle": self._bundle,
                "x25519_priv": _b64(self._x25519_priv.private_bytes_raw()),
                "mlkem768_seed": _b64(self._mlkem_priv.private_bytes_raw()),
            },
        )

    def public_bundle(self) -> dict[str, Any]:
        """Return the public bundle to register with the relay (a copy)."""
        return dict(self._bundle)

    @property
    def fingerprint(self) -> str:
        """The bundle fingerprint (= wire ``kid``), 64 lowercase hex."""
        return bundle_fingerprint(self._bundle)

    def display_fingerprint(self) -> str:
        """The fingerprint in groups of 4 for out-of-band verification."""
        return _display(self.fingerprint)

    def x25519_exchange(self, peer_public: bytes) -> bytes:
        """ECDH with the envelope's ephemeral X25519 public key (32 bytes in/out)."""
        return self._x25519_priv.exchange(x25519.X25519PublicKey.from_public_bytes(peer_public))

    def mlkem_decapsulate(self, kem_ct: bytes) -> bytes:
        """Decapsulate an ML-KEM-768 ciphertext to its 32-byte shared secret."""
        return self._mlkem_priv.decapsulate(kem_ct)


class SenderKeypair:
    """A publisher's signing identity (Ed25519, plus ML-DSA-65 when dual).

    Purpose:
        Signs sealed envelopes so subscribers can prove origin — the relay
        cannot forge signals it cannot sign.

    Contract:
        ``dual=True`` (the default and the recommended mode) carries both an
        Ed25519 and an ML-DSA-65 key; verification then REQUIRES both
        signatures (strip-downgrade is rejected by ``core.unseal``).
    """

    def __init__(
        self,
        ed25519_priv: ed25519.Ed25519PrivateKey,
        mldsa_priv: mldsa.MLDSA65PrivateKey | None,
        created_at: str,
    ) -> None:
        """Bind the private keys; internal — use ``generate()`` or ``load()``.

        Args:
            ed25519_priv: Classical signature key (always present).
            mldsa_priv: Post-quantum signature key, or ``None`` for
                classical-only senders.
            created_at: ISO-8601 UTC creation timestamp (part of the bundle,
                hence of the fingerprint).
        """
        self._ed25519_priv = ed25519_priv
        self._mldsa_priv = mldsa_priv
        self._created_at = created_at
        bundle: dict[str, Any] = {
            "bundle": "mek-v1",
            "purpose": "sender",
            "ed25519_pub": _b64(ed25519_priv.public_key().public_bytes_raw()),
            "created_at": created_at,
        }
        if mldsa_priv is not None:
            bundle["mldsa65_pub"] = _b64(mldsa_priv.public_key().public_bytes_raw())
        self._bundle = bundle

    @classmethod
    def generate(cls, dual: bool = True) -> SenderKeypair:
        """Generate a fresh sender keypair.

        Args:
            dual: When ``True`` (default), include an ML-DSA-65 key for fully
                post-quantum authenticity.
        """
        return cls(
            ed25519.Ed25519PrivateKey.generate(),
            mldsa.MLDSA65PrivateKey.generate() if dual else None,
            datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        )

    @classmethod
    def load(cls, path: Path | str) -> SenderKeypair:
        """Load a sender key file written by ``save()``.

        Args:
            path: The key file path.

        Raises:
            SealedKeyError: When the file is not a valid sender key file.
        """
        document = _read_private_file(Path(path), "sender")
        try:
            mldsa_priv = (
                mldsa.MLDSA65PrivateKey.from_seed_bytes(_b64d(document["mldsa65_seed"]))
                if "mldsa65_seed" in document
                else None
            )
            return cls(
                ed25519.Ed25519PrivateKey.from_private_bytes(_b64d(document["ed25519_priv"])),
                mldsa_priv,
                document["bundle"]["created_at"],
            )
        except (KeyError, ValueError) as exc:
            raise SealedKeyError(f"{path}: malformed sender key material: {exc}") from exc

    def save(self, path: Path | str) -> None:
        """Write the key file (seeds + bundle + kid), refusing to overwrite.

        Args:
            path: Target path; created with mode 0600 (best-effort on Windows).

        Raises:
            FileExistsError: When the file already exists.
        """
        document: dict[str, Any] = {
            "kind": _FILE_KIND,
            "kid": self.fingerprint,
            "bundle": self._bundle,
            "ed25519_priv": _b64(self._ed25519_priv.private_bytes_raw()),
        }
        if self._mldsa_priv is not None:
            document["mldsa65_seed"] = _b64(self._mldsa_priv.private_bytes_raw())
        _write_private_file(Path(path), document)

    def public_bundle(self) -> dict[str, Any]:
        """Return the public bundle to register with the relay (a copy)."""
        return dict(self._bundle)

    @property
    def fingerprint(self) -> str:
        """The bundle fingerprint (= wire ``sender_kid``), 64 lowercase hex."""
        return bundle_fingerprint(self._bundle)

    def display_fingerprint(self) -> str:
        """The fingerprint in groups of 4 for out-of-band verification."""
        return _display(self.fingerprint)

    @property
    def dual(self) -> bool:
        """Whether this sender carries the ML-DSA-65 half (dual signatures)."""
        return self._mldsa_priv is not None

    def sign(self, message: bytes) -> bytes:
        """Ed25519-sign ``message`` (64-byte signature)."""
        return self._ed25519_priv.sign(message)

    def sign_pq(self, message: bytes, context: bytes) -> bytes:
        """ML-DSA-65-sign ``message`` with a domain-separation context.

        Raises:
            SealedKeyError: When this sender is classical-only.
        """
        if self._mldsa_priv is None:
            raise SealedKeyError("this sender keypair has no ML-DSA-65 key (dual=False)")
        return self._mldsa_priv.sign(message, context=context)
