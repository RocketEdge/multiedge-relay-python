"""Webhook signature verification: HMAC-SHA256 with freshness and constant-time compare.

Purpose:
    Subscribers that receive signals by webhook must authenticate every request
    before trusting it. ``verify_signature`` checks the relay's signature scheme and
    parses the body into a ``ReceivedSignal`` only after verification passes.

Contract:
    * Signature: ``HMAC-SHA256(secret, f"{timestamp}." + raw_body)``, hex-encoded,
      sent as ``X-MultiEdge-Signature: sha256=<hex>``.
    * Freshness: ``X-MultiEdge-Timestamp`` (unix seconds) must be within ``max_age``
      of ``now`` in either direction (replay and clock-skew guard).
    * Comparison uses ``hmac.compare_digest`` — constant time, no early exit.
    * Verify the raw received bytes exactly as they arrived; re-serializing the body
      changes the bytes and breaks the signature by design.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta

from .exceptions import SignatureVerificationError
from .models import ReceivedSignal

SIGNATURE_HEADER = "X-MultiEdge-Signature"
TIMESTAMP_HEADER = "X-MultiEdge-Timestamp"

_RAW_SECRET_LENGTH = 32  # relay endpoint secrets are base64 of 32 random bytes


def _resolve_hmac_key(secret: str) -> bytes:
    """Resolve the HMAC key from an endpoint secret string, matching the server.

    Precedence (mirrors the relay's signing side exactly):
        1. If ``secret`` is valid standard base64 decoding to exactly 32 bytes —
           the format the relay issues endpoint secrets in — the HMAC key is the
           DECODED raw bytes.
        2. Otherwise the key is the UTF-8 bytes of the string (back-compat for
           ad-hoc/self-managed secrets).

    Keying on the base64 text itself was a live bug: the server signs with the
    decoded bytes, so genuine deliveries failed verification.
    """
    try:
        decoded = base64.b64decode(secret, validate=True)
    except (binascii.Error, ValueError):
        return secret.encode()
    if len(decoded) == _RAW_SECRET_LENGTH:
        return decoded
    return secret.encode()


def verify_signature(
    raw_body: bytes,
    headers: Mapping[str, str],
    secret: str,
    *,
    max_age: timedelta = timedelta(minutes=5),
    now: Callable[[], datetime] | None = None,
) -> ReceivedSignal:
    """Verify a webhook request and parse its body into a ``ReceivedSignal``.

    Args:
        raw_body: The request body EXACTLY as received (raw bytes — never
            re-serialized JSON; frameworks must hand over the original bytes).
        headers: Request headers; lookup is case-insensitive.
        secret: The endpoint's shared signing secret. Key precedence: if the
            string is valid base64 of exactly 32 bytes (the format the relay
            issues endpoint secrets in), the HMAC key is the base64-DECODED raw
            bytes — matching the server's signing side; otherwise the UTF-8
            bytes of the string are used (ad-hoc/self-managed secrets).
        max_age: Maximum allowed |now - timestamp| (default 5 minutes). Applies in
            both directions to also reject future-dated timestamps.
        now: Injectable clock returning an aware ``datetime`` (test seam);
            defaults to ``datetime.now(UTC)``.

    Returns:
        The verified, parsed signal.

    Raises:
        SignatureVerificationError: Missing/malformed headers, stale or future
            timestamp, or digest mismatch. The message states the reason; treat the
            request as untrusted and do not process the body.
    """
    lowered = {k.lower(): v for k, v in headers.items()}
    signature_header = lowered.get(SIGNATURE_HEADER.lower())
    if signature_header is None:
        raise SignatureVerificationError(f"missing {SIGNATURE_HEADER} header")
    timestamp_header = lowered.get(TIMESTAMP_HEADER.lower())
    if timestamp_header is None:
        raise SignatureVerificationError(f"missing {TIMESTAMP_HEADER} header")

    if not signature_header.startswith("sha256="):
        raise SignatureVerificationError(
            f"malformed {SIGNATURE_HEADER} header: expected 'sha256=<hex>' format"
        )
    claimed_hex = signature_header.removeprefix("sha256=")

    try:
        timestamp = int(timestamp_header)
    except ValueError as exc:
        raise SignatureVerificationError(
            f"malformed {TIMESTAMP_HEADER} header: not an integer unix timestamp"
        ) from exc

    current = now() if now is not None else datetime.now(UTC)
    age = abs(current - datetime.fromtimestamp(timestamp, tz=UTC))
    if age > max_age:
        raise SignatureVerificationError(
            f"timestamp outside tolerance: |now - timestamp| = {age} > {max_age}"
        )

    expected = hmac.new(
        _resolve_hmac_key(secret), f"{timestamp}.".encode() + raw_body, hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, claimed_hex):
        raise SignatureVerificationError("signature mismatch")

    return ReceivedSignal.model_validate_json(raw_body)
