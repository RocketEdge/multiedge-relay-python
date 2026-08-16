"""Webhook signature verification tests (HMAC-SHA256, freshness, constant time).

The relay delivers endpoint secrets as base64 of 32 random bytes and signs with the
base64-DECODED raw bytes. ``sign`` below mirrors the REAL server's keying so these
tests fail if the SDK keys on the wrong bytes.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

import pytest

from multiedge_relay import SignatureVerificationError, verify_signature

# A relay-issued secret: base64 of 32 raw bytes (fixed for determinism).
SECRET_RAW_BYTES = bytes(range(32))
SECRET = base64.b64encode(SECRET_RAW_BYTES).decode()
# An ad-hoc secret that is NOT valid base64-of-32-bytes (legacy/self-managed).
ADHOC_SECRET = "whsec_test_secret"
NOW = datetime(2026, 8, 16, 12, 0, 0, tzinfo=UTC)


def _server_key(secret: str) -> bytes:
    """Key resolution exactly as the relay server does it: decoded 32 raw bytes when
    the secret is base64-of-32, else the UTF-8 bytes of the string."""
    try:
        decoded = base64.b64decode(secret, validate=True)
    except (ValueError, TypeError):
        return secret.encode()
    return decoded if len(decoded) == 32 else secret.encode()


def make_body(sequence: int = 1) -> bytes:
    return json.dumps(
        {
            "sequence": sequence,
            "signal_id": f"sig_{sequence}",
            "strategy_id": "strat-a",
            "published_at": "2026-08-16T11:59:00+00:00",
            "payload": {"action": "BUY", "ticker": "TEST"},
        }
    ).encode()


def sign(
    body: bytes,
    *,
    secret: str = SECRET,
    timestamp: int | None = None,
    key_override: bytes | None = None,
) -> dict[str, str]:
    ts = int(NOW.timestamp()) if timestamp is None else timestamp
    key = key_override if key_override is not None else _server_key(secret)
    digest = hmac.new(key, f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return {
        "X-MultiEdge-Signature": f"sha256={digest}",
        "X-MultiEdge-Timestamp": str(ts),
    }


def now() -> datetime:
    return NOW


def test_valid_signature_returns_received_signal() -> None:
    body = make_body(sequence=9)
    signal = verify_signature(body, sign(body), SECRET, now=now)
    assert signal.sequence == 9
    assert signal.strategy_id == "strat-a"
    assert signal.payload["ticker"] == "TEST"


def test_headers_are_case_insensitive() -> None:
    body = make_body()
    headers = {k.lower(): v for k, v in sign(body).items()}
    assert verify_signature(body, headers, SECRET, now=now).sequence == 1


def test_tampered_body_rejected() -> None:
    body = make_body()
    headers = sign(body)
    tampered = body.replace(b"BUY", b"SEL")
    with pytest.raises(SignatureVerificationError):
        verify_signature(tampered, headers, SECRET, now=now)


def test_wrong_secret_rejected() -> None:
    body = make_body()
    headers = sign(body, secret="whsec_other")
    with pytest.raises(SignatureVerificationError):
        verify_signature(body, headers, SECRET, now=now)


# --------------------------------------------------------- HMAC keying (regression)
def test_base64_secret_verifies_against_decoded_key_signature() -> None:
    # The real server signs with the base64-DECODED 32 raw bytes.
    body = make_body(sequence=4)
    headers = sign(body, key_override=SECRET_RAW_BYTES)
    assert verify_signature(body, headers, SECRET, now=now).sequence == 4


def test_base64_secret_rejects_signature_keyed_on_raw_base64_text() -> None:
    # Regression: keying HMAC on the base64 STRING's UTF-8 bytes is the bug that
    # broke live verification — such a signature must NOT verify.
    body = make_body()
    headers = sign(body, key_override=SECRET.encode())
    with pytest.raises(SignatureVerificationError, match="mismatch"):
        verify_signature(body, headers, SECRET, now=now)


def test_adhoc_secret_falls_back_to_utf8_key() -> None:
    # Back-compat: a secret that is not base64-of-32-bytes keys on its UTF-8 bytes.
    body = make_body(sequence=7)
    headers = sign(body, secret=ADHOC_SECRET, key_override=ADHOC_SECRET.encode())
    signal = verify_signature(body, headers, ADHOC_SECRET, now=now)
    assert signal.sequence == 7


def test_base64_of_wrong_length_falls_back_to_utf8_key() -> None:
    # Valid base64 but not 32 bytes decoded -> UTF-8 fallback, matching the server.
    short_secret = base64.b64encode(b"only-16-bytes!!!").decode()
    body = make_body()
    headers = sign(body, secret=short_secret, key_override=short_secret.encode())
    assert verify_signature(body, headers, short_secret, now=now).sequence == 1


def test_stale_timestamp_rejected() -> None:
    body = make_body()
    stale = int((NOW - timedelta(minutes=6)).timestamp())
    with pytest.raises(SignatureVerificationError, match="timestamp"):
        verify_signature(body, sign(body, timestamp=stale), SECRET, now=now)


def test_future_timestamp_rejected() -> None:
    body = make_body()
    future = int((NOW + timedelta(minutes=6)).timestamp())
    with pytest.raises(SignatureVerificationError, match="timestamp"):
        verify_signature(body, sign(body, timestamp=future), SECRET, now=now)


def test_custom_max_age() -> None:
    body = make_body()
    ts = int((NOW - timedelta(minutes=6)).timestamp())
    headers = sign(body, timestamp=ts)
    signal = verify_signature(body, headers, SECRET, max_age=timedelta(minutes=10), now=now)
    assert signal.sequence == 1


@pytest.mark.parametrize("missing", ["X-MultiEdge-Signature", "X-MultiEdge-Timestamp"])
def test_missing_headers_rejected(missing: str) -> None:
    body = make_body()
    headers = sign(body)
    del headers[missing]
    with pytest.raises(SignatureVerificationError, match=r"[Mm]issing"):
        verify_signature(body, headers, SECRET, now=now)


def test_malformed_signature_prefix_rejected() -> None:
    body = make_body()
    headers = sign(body)
    headers["X-MultiEdge-Signature"] = headers["X-MultiEdge-Signature"].removeprefix("sha256=")
    with pytest.raises(SignatureVerificationError):
        verify_signature(body, headers, SECRET, now=now)


def test_non_numeric_timestamp_rejected() -> None:
    body = make_body()
    headers = sign(body)
    headers["X-MultiEdge-Timestamp"] = "yesterday"
    with pytest.raises(SignatureVerificationError):
        verify_signature(body, headers, SECRET, now=now)


def test_comparison_uses_compare_digest() -> None:
    body = make_body()
    headers = sign(body)
    calls: list[Any] = []
    real = hmac.compare_digest

    def spy(a: Any, b: Any) -> bool:
        calls.append((a, b))
        return real(a, b)

    with mock.patch("multiedge_relay.webhook.hmac.compare_digest", side_effect=spy):
        verify_signature(body, headers, SECRET, now=now)
    assert calls, "verify_signature must compare via hmac.compare_digest"
