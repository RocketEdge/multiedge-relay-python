"""Publisher tests: retry timing/classification (respx) + end-to-end via the fake relay."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx
from fake_relay import API_KEY, FakeRelay, SyncASGITransport

from multiedge_relay import (
    AuthError,
    DiskDLQ,
    PublishFailed,
    Signal,
    SignalAck,
    SignalPublisher,
    ValidationRejected,
)

BASE = "https://relay-api.multiedge.ai"


def make_publisher(
    dlq_root: Path,
    *,
    transport: httpx.BaseTransport | None = None,
    max_attempts: int = 5,
    sleeps: list[float] | None = None,
) -> SignalPublisher:
    return SignalPublisher(
        api_key=API_KEY,
        dlq=DiskDLQ(root=dlq_root),
        transport=transport,
        max_attempts=max_attempts,
        sleep=(sleeps.append if sleeps is not None else lambda _: None),
        random_fn=lambda: 1.0,
    )


# --------------------------------------------------------------------- fake relay e2e
def test_publish_returns_ack_and_auto_ulid(relay: FakeRelay, dlq_root: Path) -> None:
    with make_publisher(dlq_root, transport=SyncASGITransport(relay.app)) as publisher:
        ack = publisher.publish(Signal(strategy_id="s1", payload={"a": 1}))
    assert isinstance(ack, SignalAck)
    assert ack.sequence == 1
    assert ack.deduplicated is False
    assert len(ack.client_signal_id) == 26  # auto-generated ULID
    assert relay.signals["s1"][0].payload == {"a": 1}


def test_publish_accepts_plain_dict(relay: FakeRelay, dlq_root: Path) -> None:
    with make_publisher(dlq_root, transport=SyncASGITransport(relay.app)) as publisher:
        ack = publisher.publish({"strategy_id": "s1", "payload": {"b": 2}})
    assert ack.sequence == 1


def test_duplicate_client_signal_id_returns_deduplicated_ack(
    relay: FakeRelay, dlq_root: Path
) -> None:
    sig = Signal(strategy_id="s1", payload={"a": 1}, client_signal_id="fixed-id")
    with make_publisher(dlq_root, transport=SyncASGITransport(relay.app)) as publisher:
        first = publisher.publish(sig)
        second = publisher.publish(sig)
    assert first.deduplicated is False
    assert second.deduplicated is True
    assert second.sequence == first.sequence
    assert len(relay.signals["s1"]) == 1


def test_transient_failure_retries_then_succeeds(relay: FakeRelay, dlq_root: Path) -> None:
    relay.fail_next(2, 503)
    with make_publisher(dlq_root, transport=SyncASGITransport(relay.app)) as publisher:
        ack = publisher.publish(Signal(strategy_id="s1", payload={}))
    assert ack.sequence == 1


# --------------------------------------------------------------------- classification
@respx.mock
def test_auth_error_no_retry_no_dlq(dlq_root: Path) -> None:
    route = respx.post(f"{BASE}/v1/signals").mock(return_value=httpx.Response(401))
    publisher = make_publisher(dlq_root)
    with pytest.raises(AuthError):
        publisher.publish(Signal(strategy_id="s1", payload={}))
    assert route.call_count == 1
    assert list(publisher.dlq.pending()) == []  # type: ignore[union-attr]


@respx.mock
@pytest.mark.parametrize("status", [422, 413])
def test_validation_rejected_no_retry(dlq_root: Path, status: int) -> None:
    route = respx.post(f"{BASE}/v1/signals").mock(return_value=httpx.Response(status))
    publisher = make_publisher(dlq_root)
    with pytest.raises(ValidationRejected):
        publisher.publish(Signal(strategy_id="s1", payload={}))
    assert route.call_count == 1


@respx.mock
def test_retry_exhaustion_spills_to_dlq_and_raises(dlq_root: Path) -> None:
    route = respx.post(f"{BASE}/v1/signals").mock(return_value=httpx.Response(500))
    sleeps: list[float] = []
    publisher = make_publisher(dlq_root, sleeps=sleeps)
    with pytest.raises(PublishFailed) as excinfo:
        publisher.publish(Signal(strategy_id="s1", payload={"k": "v"}))
    assert route.call_count == 5
    assert excinfo.value.attempts == 5
    assert excinfo.value.dlq_path is not None
    assert excinfo.value.dlq_path.exists()
    # full-jitter backoff with random=1.0: 0.5 * 2^n between the 5 attempts
    assert sleeps == [0.5, 1.0, 2.0, 4.0]
    entries = list(publisher.dlq.pending())  # type: ignore[union-attr]
    assert len(entries) == 1
    assert entries[0].signal.payload == {"k": "v"}


@respx.mock
def test_transport_error_retries_then_fails(dlq_root: Path) -> None:
    route = respx.post(f"{BASE}/v1/signals").mock(
        side_effect=httpx.ConnectError("connection refused")
    )
    publisher = make_publisher(dlq_root, max_attempts=3)
    with pytest.raises(PublishFailed) as excinfo:
        publisher.publish(Signal(strategy_id="s1", payload={}))
    assert route.call_count == 3
    assert excinfo.value.attempts == 3


@respx.mock
def test_unexpected_4xx_fails_fast_to_dlq(dlq_root: Path) -> None:
    route = respx.post(f"{BASE}/v1/signals").mock(return_value=httpx.Response(400))
    publisher = make_publisher(dlq_root)
    with pytest.raises(PublishFailed) as excinfo:
        publisher.publish(Signal(strategy_id="s1", payload={}))
    assert route.call_count == 1  # not retryable
    assert excinfo.value.dlq_path is not None


@respx.mock
def test_no_dlq_configured_gives_none_path(dlq_root: Path) -> None:
    respx.post(f"{BASE}/v1/signals").mock(return_value=httpx.Response(500))
    publisher = SignalPublisher(
        api_key=API_KEY, dlq=None, sleep=lambda _: None, random_fn=lambda: 1.0
    )
    with pytest.raises(PublishFailed) as excinfo:
        publisher.publish(Signal(strategy_id="s1", payload={}))
    assert excinfo.value.dlq_path is None


@respx.mock
def test_sends_bearer_auth_and_user_agent(dlq_root: Path) -> None:
    route = respx.post(f"{BASE}/v1/signals").mock(
        return_value=httpx.Response(
            201,
            json={
                "signal_id": "sig_1",
                "client_signal_id": "c",
                "sequence": 1,
                "accepted_at": "2026-08-16T00:00:00+00:00",
            },
        )
    )
    make_publisher(dlq_root).publish(Signal(strategy_id="s1", payload={}))
    request = route.calls.last.request
    assert request.headers["Authorization"] == f"Bearer {API_KEY}"
    assert request.headers["User-Agent"].startswith("multiedge-relay-python/")


# --------------------------------------------------------------------- publish_many
def test_publish_many_all_success(relay: FakeRelay, dlq_root: Path) -> None:
    signals = [Signal(strategy_id="s1", payload={"n": n}) for n in range(3)]
    with make_publisher(dlq_root, transport=SyncASGITransport(relay.app)) as publisher:
        acks = publisher.publish_many(signals)
    assert [a.sequence for a in acks if isinstance(a, SignalAck)] == [1, 2, 3]


def test_publish_many_raise_on_partial_default(relay: FakeRelay, dlq_root: Path) -> None:
    relay.fail_next(10, 500)
    signals = [Signal(strategy_id="s1", payload={"n": n}) for n in range(2)]
    with (
        make_publisher(
            dlq_root, transport=SyncASGITransport(relay.app), max_attempts=2
        ) as publisher,
        pytest.raises(PublishFailed),
    ):
        publisher.publish_many(signals)


def test_publish_many_collects_failures_when_not_raising(relay: FakeRelay, dlq_root: Path) -> None:
    relay.fail_next(2, 500)  # first signal exhausts its 2 attempts; second succeeds
    signals = [Signal(strategy_id="s1", payload={"n": n}) for n in range(2)]
    with make_publisher(
        dlq_root, transport=SyncASGITransport(relay.app), max_attempts=2
    ) as publisher:
        results = publisher.publish_many(signals, raise_on_partial=False)
    assert isinstance(results[0], PublishFailed)
    assert results[0].dlq_path is not None
    assert isinstance(results[1], SignalAck)
