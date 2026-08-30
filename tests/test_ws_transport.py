"""Web PubSub transport contract tests: negotiate body, frame HMAC, buffering.

The deployed relay's ws contract (relay repo, docs/specs/ws-resume-protocol.md):

* Negotiate is ``POST /v1/ws/negotiate {"endpoint_id": ...}`` — NOT strategy_id.
* Each live frame is ``{"envelope": "<raw envelope JSON string>",
  "signature": "<hex>", "timestamp": <unix_s>}`` and the HMAC-SHA256 over
  ``"{timestamp}." + envelope-utf8-bytes`` MUST be verified (per-endpoint
  secret, +-5 min freshness) BEFORE the envelope is parsed.
* Frames arriving between socket-open and the end of catch-up are buffered and
  drained afterwards, deduped by sequence.

``build_frame`` below signs exactly as the relay's ``WebhookSignature.Sign``
does (decoded 32-byte key, lowercase hex, NO ``sha256=`` prefix on the frame
field), so these tests fail if the SDK keys or parses the wrong bytes.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fake_relay import API_KEY, FakeRelay, SyncASGITransport

from multiedge_relay import (
    FileCursorStore,
    ReceivedSignal,
    SignalMeta,
    SignalSubscriber,
    SignatureVerificationError,
    ValidationRejected,
    verify_ws_frame,
)

STRATEGY = "s1"
ENDPOINT_ID = "01ENDPOINTAAAAAAAAAAAAAA01"
SECRET_RAW = bytes(range(32))
SECRET = base64.b64encode(SECRET_RAW).decode()
ADHOC_SECRET = "wssec_test_secret"  # not base64-of-32: keyed on UTF-8 bytes
NOW = datetime(2026, 8, 16, 12, 0, 0, tzinfo=UTC)


def envelope_json(sequence: int = 1) -> str:
    return json.dumps(
        {
            "sequence": sequence,
            "signal_id": f"sig_{sequence}",
            "strategy_id": STRATEGY,
            "published_at": "2026-08-16T11:59:00+00:00",
            "payload": {"n": sequence},
        }
    )


def build_frame(
    sequence: int = 1,
    *,
    timestamp: int | None = None,
    secret_key: bytes = SECRET_RAW,
    tamper: bool = False,
) -> str:
    """One relay PushFrame, signed exactly like the server signs it."""
    text = envelope_json(sequence)
    ts = int(NOW.timestamp()) if timestamp is None else timestamp
    digest = hmac.new(secret_key, f"{ts}.".encode() + text.encode(), hashlib.sha256).hexdigest()
    if tamper:
        text = text.replace(f'"n": {sequence}', f'"n": {sequence + 100}')
    return json.dumps({"envelope": text, "signature": digest, "timestamp": ts})


def now() -> datetime:
    return NOW


# ------------------------------------------------------------------ verify_ws_frame
def test_valid_frame_verifies_and_parses() -> None:
    received = verify_ws_frame(build_frame(7), SECRET, now=now)
    assert isinstance(received, ReceivedSignal)
    assert received.sequence == 7
    assert received.payload == {"n": 7}


def test_frame_accepts_bytes_and_dict_inputs() -> None:
    raw = build_frame(3)
    assert verify_ws_frame(raw.encode(), SECRET, now=now).sequence == 3
    assert verify_ws_frame(json.loads(raw), SECRET, now=now).sequence == 3


def test_tampered_envelope_rejected() -> None:
    with pytest.raises(SignatureVerificationError, match="mismatch"):
        verify_ws_frame(build_frame(1, tamper=True), SECRET, now=now)


def test_wrong_secret_rejected() -> None:
    with pytest.raises(SignatureVerificationError, match="mismatch"):
        verify_ws_frame(build_frame(1), base64.b64encode(bytes(32)).decode(), now=now)


def test_stale_timestamp_rejected() -> None:
    old = int(NOW.timestamp()) - 600  # 10 min > 5 min tolerance
    with pytest.raises(SignatureVerificationError, match="tolerance"):
        verify_ws_frame(build_frame(1, timestamp=old), SECRET, now=now)


def test_malformed_frame_rejected() -> None:
    with pytest.raises(SignatureVerificationError, match="frame"):
        verify_ws_frame(json.dumps({"signature": "aa", "timestamp": 1}), SECRET, now=now)
    with pytest.raises(SignatureVerificationError, match="frame"):
        verify_ws_frame("not json at all", SECRET, now=now)


def test_adhoc_secret_keys_on_utf8_bytes() -> None:
    frame = build_frame(2, secret_key=ADHOC_SECRET.encode())
    assert verify_ws_frame(frame, ADHOC_SECRET, now=now).sequence == 2


# ------------------------------------------------------------------ subscriber wiring
class Collector:
    def __init__(self) -> None:
        self.deliveries: list[tuple[int, str]] = []
        self.errors: list[Exception] = []

    def __call__(self, signal: ReceivedSignal, meta: SignalMeta) -> None:
        self.deliveries.append((signal.sequence, meta.source))

    def on_error(self, exc: Exception) -> None:
        self.errors.append(exc)


class FakeWsClient:
    """Stand-in for azure WebPubSubClient: records the subscribe callback."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.callbacks: dict[str, Any] = {}
        self.on_subscribe: Any = None  # optional hook fired inside subscribe()

    def __enter__(self) -> FakeWsClient:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def subscribe(self, event: str, callback: Any) -> None:
        self.callbacks[event] = callback
        if self.on_subscribe is not None:
            self.on_subscribe(callback)

    def push(self, frame: str) -> None:
        self.callbacks["group-message"](SimpleNamespace(data=frame))


def make_ws_subscriber(
    relay: FakeRelay, cursor_root: Path, collector: Collector, **kwargs: Any
) -> tuple[SignalSubscriber, list[FakeWsClient]]:
    clients: list[FakeWsClient] = []
    on_subscribe = kwargs.pop("on_subscribe", None)

    def factory(url: str) -> FakeWsClient:
        client = FakeWsClient(url)
        client.on_subscribe = on_subscribe
        clients.append(client)
        return client

    subscriber = SignalSubscriber(
        api_key=API_KEY,
        strategy_id=STRATEGY,
        on_signal=collector,
        transport=SyncASGITransport(relay.app),
        cursor_store=FileCursorStore(root=cursor_root),
        live_transport="webpubsub",
        endpoint_id=kwargs.pop("endpoint_id", ENDPOINT_ID),
        endpoint_secret=kwargs.pop("endpoint_secret", SECRET),
        on_error=collector.on_error,
        sleep=lambda _: None,
        random_fn=lambda: 1.0,
        ws_client_factory=kwargs.pop("ws_client_factory", factory),
        **kwargs,
    )
    return subscriber, clients


def fresh_frame(sequence: int, *, tamper: bool = False, secret_key: bytes = SECRET_RAW) -> str:
    """A frame stamped with the real current time (run() verifies with a real clock)."""
    return build_frame(sequence, timestamp=int(time.time()), secret_key=secret_key, tamper=tamper)


def run_until(
    subscriber: SignalSubscriber, predicate: Any, timeout: float = 5.0
) -> threading.Thread:
    thread = threading.Thread(target=subscriber.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not predicate():
        time.sleep(0.01)
    assert predicate(), "condition not reached before timeout"
    return thread


def test_webpubsub_requires_endpoint_id_and_secret(relay: FakeRelay, cursor_root: Path) -> None:
    with pytest.raises(ValueError, match="endpoint_id"):
        SignalSubscriber(
            api_key=API_KEY,
            strategy_id=STRATEGY,
            on_signal=Collector(),
            live_transport="webpubsub",
            endpoint_secret=SECRET,
        )
    with pytest.raises(ValueError, match="endpoint_secret"):
        SignalSubscriber(
            api_key=API_KEY,
            strategy_id=STRATEGY,
            on_signal=Collector(),
            live_transport="webpubsub",
            endpoint_id=ENDPOINT_ID,
        )


def test_negotiate_sends_endpoint_id_then_live_frame_delivered(
    relay: FakeRelay, cursor_root: Path
) -> None:
    relay.seed(STRATEGY, [{"n": 1}, {"n": 2}])
    collector = Collector()
    subscriber, clients = make_ws_subscriber(relay, cursor_root, collector)
    thread = run_until(subscriber, lambda: clients and "group-message" in clients[0].callbacks)
    assert relay.ws_negotiate_bodies[-1] == {"endpoint_id": ENDPOINT_ID}

    clients[0].push(fresh_frame(3))
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and (3, "live") not in collector.deliveries:
        time.sleep(0.01)
    subscriber.stop()
    thread.join(timeout=5.0)
    assert collector.deliveries == [(1, "catchup"), (2, "catchup"), (3, "live")]


def test_bad_signature_frame_dropped_reported_never_delivered(
    relay: FakeRelay, cursor_root: Path
) -> None:
    relay.seed(STRATEGY, [{"n": 1}])
    collector = Collector()
    subscriber, clients = make_ws_subscriber(relay, cursor_root, collector)
    thread = run_until(subscriber, lambda: clients and "group-message" in clients[0].callbacks)

    clients[0].push(fresh_frame(2, tamper=True))
    clients[0].push(fresh_frame(2))  # the genuine copy still goes through
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and (2, "live") not in collector.deliveries:
        time.sleep(0.01)
    subscriber.stop()
    thread.join(timeout=5.0)
    assert (2, "live") in collector.deliveries
    assert any(isinstance(e, SignatureVerificationError) for e in collector.errors)
    assert [d for d in collector.deliveries if d[0] == 2] == [(2, "live")]  # exactly once


def test_negotiate_rejection_is_fatal_not_retried(relay: FakeRelay, cursor_root: Path) -> None:
    relay.ws_negotiate_status = 404  # unknown_endpoint — a config error, not transient
    collector = Collector()
    subscriber, _clients = make_ws_subscriber(relay, cursor_root, collector)
    with pytest.raises(ValidationRejected, match="negotiate"):
        subscriber.run()
    assert len(relay.ws_negotiate_bodies) == 1  # no retry storm on a config error


def test_frames_during_catchup_buffer_then_drain_in_order(
    relay: FakeRelay, cursor_root: Path
) -> None:
    relay.seed(STRATEGY, [{"n": 1}, {"n": 2}, {"n": 3}])
    collector = Collector()

    # Fired synchronously inside subscribe(): the frame arrives "from the
    # instant the socket opens", BEFORE catch-up has run (spec step 2).
    def on_subscribe(callback: Any) -> None:
        callback(SimpleNamespace(data=fresh_frame(4)))
        callback(SimpleNamespace(data=fresh_frame(3)))  # dup of catch-up data

    subscriber, _clients = make_ws_subscriber(
        relay, cursor_root, collector, on_subscribe=on_subscribe
    )
    thread = run_until(subscriber, lambda: (4, "live") in collector.deliveries)
    subscriber.stop()
    thread.join(timeout=5.0)
    # Catch-up delivered 1-3 first; the buffered live 4 drained AFTER; the
    # buffered dup of 3 was discarded by sequence.
    assert collector.deliveries == [
        (1, "catchup"),
        (2, "catchup"),
        (3, "catchup"),
        (4, "live"),
    ]
