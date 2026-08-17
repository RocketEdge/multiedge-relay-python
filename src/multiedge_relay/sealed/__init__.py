"""Sealed mode — end-to-end payload encryption for MultiEdge Signal Relay.

Purpose:
    Publishers seal signal payloads client-side and subscribers unseal them
    client-side, so the relay stores and forwards only ciphertext and can never
    read (or forge) signal contents. The relay sees envelope metadata only:
    IDs, sequence, timestamps, size, and recipient count.

Contract:
    * Requires the optional extra: ``pip install "multiedge-relay[sealed]"``
      (``cryptography>=47``). Importing this subpackage without it raises an
      actionable ``ImportError``; the core SDK never imports this subpackage.
    * Scheme (sealed envelope v1): per-signal 32-byte DEK, ChaCha20-Poly1305
      AEAD over canonical JSON; DEK wrapped per recipient with a hybrid
      X25519 + ML-KEM-768 KEM combined via HKDF-SHA256 (post-quantum
      confidentiality); envelope signed with Ed25519 + ML-DSA-65
      (post-quantum authenticity, strip-downgrade rejected).
    * Trust model: the relay is untrusted even for key distribution — bundle
      fingerprints are recomputed locally and verified out-of-band or pinned.
"""

from __future__ import annotations

try:  # pragma: no cover - exercised via an import-hook test
    import cryptography  # noqa: F401
except ImportError as exc:  # pragma: no cover - exercised via an import-hook test
    raise ImportError(
        "sealed mode requires the optional dependency: install with "
        'pip install "multiedge-relay[sealed]"  (cryptography>=47 — ships '
        "ML-KEM-768 and ML-DSA-65; plain publishing/subscribing needs no extras)"
    ) from exc

from .core import seal, unseal
from .keys import RecipientKeypair, SenderKeypair, bundle_fingerprint, canonical_json
from .registry import Sealer, Unsealer

__all__ = [
    "RecipientKeypair",
    "Sealer",
    "SenderKeypair",
    "Unsealer",
    "bundle_fingerprint",
    "canonical_json",
    "seal",
    "unseal",
]
