"""Webhook signature verification tests (HMAC-SHA256, freshness, constant time)."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

import pytest

from multiedge_relay import SignatureVerificationError, verify_signature

SECRET = "whsec_test_secret"
NOW = datetime(2026, 8, 16, 12, 0, 0, tzinfo=UTC)


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


def sign(body: bytes, *, secret: str = SECRET, timestamp: int | None = None) -> dict[str, str]:
    ts = int(NOW.timestamp()) if timestamp is None else timestamp
    digest = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
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
