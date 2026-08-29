"""Async publisher tests: identical surface to the sync publisher, via ASGITransport."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx
from fake_relay import API_KEY, FakeRelay

from multiedge_relay import (
    AsyncSignalPublisher,
    AuthError,
    DiskDLQ,
    PublishFailed,
    Signal,
    SignalAck,
)

BASE = "https://relay-api.multiedge.ai"


def make_publisher(
    relay: FakeRelay, dlq_root: Path, *, max_attempts: int = 5
) -> AsyncSignalPublisher:
    return AsyncSignalPublisher(
        api_key=API_KEY,
        dlq=DiskDLQ(root=dlq_root),
        transport=httpx.ASGITransport(app=relay.app),
        max_attempts=max_attempts,
        sleep=_no_sleep,
        random_fn=lambda: 1.0,
    )


async def _no_sleep(_: float) -> None:
    return None


async def test_async_publish_ack(relay: FakeRelay, dlq_root: Path) -> None:
    async with make_publisher(relay, dlq_root) as publisher:
        ack = await publisher.publish(Signal(strategy_id="s1", payload={"a": 1}))
    assert isinstance(ack, SignalAck)
    assert ack.sequence == 1
    assert len(ack.client_signal_id) == 26


async def test_async_dedupe(relay: FakeRelay, dlq_root: Path) -> None:
    sig = Signal(strategy_id="s1", payload={}, client_signal_id="dup")
    async with make_publisher(relay, dlq_root) as publisher:
        first = await publisher.publish(sig)
        second = await publisher.publish(sig)
    assert first.deduplicated is False
    assert second.deduplicated is True


async def test_async_retry_then_success(relay: FakeRelay, dlq_root: Path) -> None:
    relay.fail_next(2, 503)
    async with make_publisher(relay, dlq_root) as publisher:
        ack = await publisher.publish(Signal(strategy_id="s1", payload={}))
    assert ack.sequence == 1


async def test_async_exhaustion_spills_to_dlq(relay: FakeRelay, dlq_root: Path) -> None:
    relay.fail_next(10, 500)
    async with make_publisher(relay, dlq_root, max_attempts=2) as publisher:
        with pytest.raises(PublishFailed) as excinfo:
            await publisher.publish(Signal(strategy_id="s1", payload={}))
    assert excinfo.value.attempts == 2
    assert excinfo.value.dlq_path is not None


@respx.mock
async def test_async_auth_error_no_retry(dlq_root: Path) -> None:
    route = respx.post(f"{BASE}/v1/signals").mock(return_value=httpx.Response(403))
    publisher = AsyncSignalPublisher(
        api_key=API_KEY, dlq=DiskDLQ(root=dlq_root), sleep=_no_sleep, random_fn=lambda: 1.0
    )
    with pytest.raises(AuthError):
        await publisher.publish(Signal(strategy_id="s1", payload={}))
    await publisher.aclose()
    assert route.call_count == 1


async def test_async_publish_many_partial(relay: FakeRelay, dlq_root: Path) -> None:
    relay.fail_next(2, 500)
    signals = [Signal(strategy_id="s1", payload={"n": n}) for n in range(2)]
    async with make_publisher(relay, dlq_root, max_attempts=2) as publisher:
        results = await publisher.publish_many(signals, raise_on_partial=False)
    assert isinstance(results[0], PublishFailed)
    assert isinstance(results[1], SignalAck)


# --------------------------------------------------------- deployment outage ride-out
class AsyncFakeClock:
    """Monotonic clock advanced only by the retry loop's own awaited sleeps.

    The async mirror of ``test_publisher.FakeClock``: it lets a test spend the whole
    90 s retry budget instantly and deterministically.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay

    def __call__(self) -> float:
        return self.now


@respx.mock
async def test_async_publish_rides_out_an_outage_longer_than_the_old_budget(
    dlq_root: Path,
) -> None:
    clock = AsyncFakeClock()
    route = respx.post(f"{BASE}/v1/signals").mock(
        side_effect=[httpx.Response(503)] * 12
        + [
            httpx.Response(
                201,
                json={
                    "signal_id": "sig_1",
                    "client_signal_id": "c",
                    "sequence": 1,
                    "accepted_at": "2026-08-16T00:00:00+00:00",
                },
            )
        ],
    )
    publisher = AsyncSignalPublisher(
        api_key=API_KEY,
        dlq=DiskDLQ(root=dlq_root),
        sleep=clock.sleep,
        monotonic=clock,
        random_fn=lambda: 1.0,
    )
    ack = await publisher.publish(Signal(strategy_id="s1", payload={"k": "v"}))
    assert ack.sequence == 1
    assert route.call_count == 13
    assert clock.sleeps == [0.5, 1.0, 2.0, 4.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0]


@respx.mock
async def test_async_publish_honours_retry_after_and_spills_when_budget_is_spent(
    dlq_root: Path,
) -> None:
    clock = AsyncFakeClock()
    respx.post(f"{BASE}/v1/signals").mock(
        return_value=httpx.Response(429, headers={"Retry-After": "5"})
    )
    publisher = AsyncSignalPublisher(
        api_key=API_KEY,
        dlq=DiskDLQ(root=dlq_root),
        retry_budget_seconds=20.0,
        sleep=clock.sleep,
        monotonic=clock,
        random_fn=lambda: 1.0,
    )
    with pytest.raises(PublishFailed) as excinfo:
        await publisher.publish(Signal(strategy_id="s1", payload={}))
    assert clock.sleeps == [5.0, 5.0, 5.0, 5.0]  # the server's hint, four times over
    assert excinfo.value.dlq_path is not None and excinfo.value.dlq_path.exists()
