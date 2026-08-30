"""Delivery signature verification: HMAC-SHA256 with freshness, constant-time compare.

Purpose:
    Subscribers must authenticate every relay delivery before trusting it.
    ``verify_signature`` checks a WEBHOOK request; ``verify_ws_frame`` checks a
    Web PubSub push frame. Both parse into a ``ReceivedSignal`` only after
    verification passes, and both share the same MAC construction — only the
    envelope differs (HTTP headers vs a JSON frame).

Contract (mirrors the relay's ``WebhookSignature`` exactly):
    * Signature: ``HMAC-SHA256(secret, f"{timestamp}." + raw_bytes)``, lowercase
      hex. Webhooks send it as ``X-MultiEdge-Signature: sha256=<hex>``; ws frames
      carry it in the ``signature`` field WITHOUT the ``sha256=`` prefix.
    * Freshness: the unix-seconds timestamp must be within ``max_age`` of ``now``
      in either direction (replay and clock-skew guard).
    * Comparison uses ``hmac.compare_digest`` — constant time, no early exit.
    * Verify the raw received bytes exactly as they arrived; re-serializing
      changes the bytes and breaks the signature by design. For ws frames that
      means the UTF-8 bytes of the ``envelope`` STRING, verified before parsing.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from .exceptions import SignatureVerificationError
from .models import ReceivedSignal

if TYPE_CHECKING:  # pragma: no cover - the crypto extra is never imported at runtime here
    from .sealed.registry import Unsealer

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


def _check_freshness(
    timestamp: int, max_age: timedelta, now: Callable[[], datetime] | None
) -> None:
    """Reject a timestamp outside ``max_age`` of now (both directions)."""
    current = now() if now is not None else datetime.now(UTC)
    age = abs(current - datetime.fromtimestamp(timestamp, tz=UTC))
    if age > max_age:
        raise SignatureVerificationError(
            f"timestamp outside tolerance: |now - timestamp| = {age} > {max_age}"
        )


def _check_digest(secret: str, timestamp: int, raw: bytes, claimed_hex: str) -> None:
    """Constant-time comparison of the relay MAC over ``"{timestamp}." + raw``."""
    expected = hmac.new(
        _resolve_hmac_key(secret), f"{timestamp}.".encode() + raw, hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, claimed_hex):
        raise SignatureVerificationError("signature mismatch")


def verify_signature(
    raw_body: bytes,
    headers: Mapping[str, str],
    secret: str,
    *,
    max_age: timedelta = timedelta(minutes=5),
    now: Callable[[], datetime] | None = None,
    unsealer: Unsealer | None = None,
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
        unsealer: Sealed-mode unsealer (``multiedge-relay[sealed]``); applied
            AFTER HMAC verification (transport authenticity first), so the
            returned signal carries the decrypted plaintext payload.

    Returns:
        The verified, parsed signal (plaintext when ``unsealer`` is given).

    Raises:
        SignatureVerificationError: Missing/malformed headers, stale or future
            timestamp, or digest mismatch. The message states the reason; treat the
            request as untrusted and do not process the body.
        UnsealError: The sealed payload failed verification or decryption.
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

    _check_freshness(timestamp, max_age, now)
    _check_digest(secret, timestamp, raw_body, claimed_hex)

    received = ReceivedSignal.model_validate_json(raw_body)
    if unsealer is not None:
        received = unsealer.unseal_signal(received)
    return received


def verify_ws_frame(
    frame: str | bytes | bytearray | Mapping[str, Any],
    secret: str,
    *,
    max_age: timedelta = timedelta(minutes=5),
    now: Callable[[], datetime] | None = None,
    unsealer: Unsealer | None = None,
) -> ReceivedSignal:
    """Verify a Web PubSub push frame and parse its envelope into a ``ReceivedSignal``.

    The relay pushes each live signal to the endpoint group as::

        {"envelope": "<raw envelope JSON string>", "signature": "<hex>", "timestamp": <unix_s>}

    The HMAC is computed over the UTF-8 bytes of the ``envelope`` STRING (with
    the ``"{timestamp}."`` prefix, per-endpoint secret) and MUST be checked
    before the envelope is parsed — this function does exactly that. Note the
    frame's ``signature`` field carries bare lowercase hex, WITHOUT the
    ``sha256=`` prefix webhooks use.

    Args:
        frame: The frame as received — raw text/bytes, or the already-parsed
            mapping some Web PubSub clients hand to callbacks.
        secret: The endpoint's signing secret (same key precedence as
            :func:`verify_signature`: base64-of-32-bytes decodes to the raw key,
            anything else keys on the UTF-8 bytes).
        max_age: Maximum allowed |now - timestamp| (default 5 minutes).
        now: Injectable clock returning an aware ``datetime`` (test seam).
        unsealer: Sealed-mode unsealer, applied AFTER HMAC verification.

    Returns:
        The verified, parsed signal.

    Raises:
        SignatureVerificationError: Malformed frame, stale/future timestamp, or
            digest mismatch. Treat the frame as untrusted and do not process it.
        UnsealError: The sealed payload failed verification or decryption.
    """
    if isinstance(frame, (str, bytes, bytearray)):
        try:
            parsed = json.loads(frame)
        except ValueError as exc:
            raise SignatureVerificationError("malformed ws frame: not valid JSON") from exc
    else:
        parsed = dict(frame)
    if not isinstance(parsed, dict):
        raise SignatureVerificationError("malformed ws frame: expected a JSON object")

    envelope = parsed.get("envelope")
    signature = parsed.get("signature")
    timestamp = parsed.get("timestamp")
    if (
        not isinstance(envelope, str)
        or not isinstance(signature, str)
        or isinstance(timestamp, bool)
        or not isinstance(timestamp, int)
    ):
        raise SignatureVerificationError(
            "malformed ws frame: expected string 'envelope', hex string 'signature' "
            "and integer unix 'timestamp'"
        )

    _check_freshness(timestamp, max_age, now)
    _check_digest(secret, timestamp, envelope.encode(), signature)

    received = ReceivedSignal.model_validate_json(envelope)
    if unsealer is not None:
        received = unsealer.unseal_signal(received)
    return received
