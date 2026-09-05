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
    IdempotencyConflict,
    PublishFailed,
    Signal,
    SignalAck,
    SignalPublisher,
    ValidationRejected,
)
from multiedge_relay._retry import DEFAULT_RETRY_BUDGET_SECONDS

BASE = "https://relay-api.multiedge.ai"

_ACCEPTED = httpx.Response(
    201,
    json={
        "signal_id": "sig_1",
        "client_signal_id": "c",
        "sequence": 1,
        "accepted_at": "2026-08-16T00:00:00+00:00",
    },
)


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
    assert ack.duplicate is False
    assert len(ack.client_signal_id) == 26  # auto-generated ULID
    assert relay.signals["s1"][0].payload == {"a": 1}


def test_publish_accepts_plain_dict(relay: FakeRelay, dlq_root: Path) -> None:
    with make_publisher(dlq_root, transport=SyncASGITransport(relay.app)) as publisher:
        ack = publisher.publish({"strategy_id": "s1", "payload": {"b": 2}})
    assert ack.sequence == 1


def test_duplicate_client_signal_id_returns_the_original_ack(
    relay: FakeRelay, dlq_root: Path
) -> None:
    sig = Signal(strategy_id="s1", payload={"a": 1}, client_signal_id="fixed-id")
    with make_publisher(dlq_root, transport=SyncASGITransport(relay.app)) as publisher:
        first = publisher.publish(sig)
        second = publisher.publish(sig)
    assert first.duplicate is False
    assert second.duplicate is True
    assert second.sequence == first.sequence
    assert len(relay.signals["s1"]) == 1


def test_changed_payload_under_reused_id_raises_idempotency_conflict(
    relay: FakeRelay, dlq_root: Path
) -> None:
    """ADR 0015: a corrected payload resent under its original id is a loud 409.

    The exception carries the ORIGINAL signal's identity, is never retried, and
    never spills to the DLQ — resending the same bytes can never succeed, so a
    DLQ entry would be a forever-409 trap for ``dlq resend``.
    """
    with make_publisher(dlq_root, transport=SyncASGITransport(relay.app)) as publisher:
        first = publisher.publish(
            Signal(strategy_id="s1", payload={"weight": 0.25}, client_signal_id="s1:2026-09-14")
        )
        with pytest.raises(IdempotencyConflict) as excinfo:
            publisher.publish(
                Signal(strategy_id="s1", payload={"weight": 0.40}, client_signal_id="s1:2026-09-14")
            )
    assert excinfo.value.signal_id == first.signal_id
    assert excinfo.value.sequence == first.sequence
    assert ":r2" in str(excinfo.value)
    assert len(relay.signals["s1"]) == 1, "nothing was published"
    assert relay.requests.count("POST /v1/signals") == 2, "a conflict is never retried"
    assert not list(DiskDLQ(root=dlq_root).pending()), "a conflict never spills to the DLQ"


def test_other_409_bodies_keep_the_publish_failed_dlq_path(
    relay: FakeRelay, dlq_root: Path
) -> None:
    """A 409 that is NOT client_signal_id_conflict (e.g. strategy_archived) stays
    an unexpected terminal status: fail fast, spill to the DLQ."""
    relay.fail_next(1, 409)
    with (
        make_publisher(dlq_root, transport=SyncASGITransport(relay.app)) as publisher,
        pytest.raises(PublishFailed) as excinfo,
    ):
        publisher.publish(Signal(strategy_id="s1", payload={"a": 1}))
    assert excinfo.value.dlq_path is not None
    assert len(list(DiskDLQ(root=dlq_root).pending())) == 1


def test_schema_version_reaches_the_wire(relay: FakeRelay, dlq_root: Path) -> None:
    """``Signal.schema_version`` must land in the publish body, not be dropped."""
    with make_publisher(dlq_root, transport=SyncASGITransport(relay.app)) as publisher:
        publisher.publish(
            Signal(
                strategy_id="s1",
                payload={"a": 1},
                schema_version="portfolio_rebalance/1.1",
            )
        )
    assert relay.publish_bodies[-1]["schema_version"] == "portfolio_rebalance/1.1"


@respx.mock
def test_duplicate_flag_is_read_from_the_body_not_only_the_status(dlq_root: Path) -> None:
    """A 201 whose body says ``duplicate`` must be believed.

    Regression guard for the wire-name drift: the SDK read a field the relay never
    sends, so the flag survived only because a 200 status forced it. Serving the
    truth on a 201 isolates the body as the source.
    """
    respx.post(f"{BASE}/v1/signals").mock(
        return_value=httpx.Response(
            201,
            json={
                "signal_id": "sig_1",
                "client_signal_id": "c1",
                "sequence": 3,
                "accepted_at": "2026-08-30T00:00:00+00:00",
                "duplicate": True,
            },
        )
    )
    with make_publisher(dlq_root) as publisher:
        ack = publisher.publish(Signal(strategy_id="s1", payload={"a": 1}))
    assert ack.duplicate is True


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


# --------------------------------------------------------- deployment outage ride-out
class FakeClock:
    """Monotonic clock advanced only by the retry loop's own sleeps.

    Lets a test spend a 90 s retry budget instantly and deterministically: the
    publisher measures elapsed time through ``__call__`` and burns it through
    ``sleep``, so budget accounting is exercised for real without any waiting.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay

    def __call__(self) -> float:
        return self.now

    @property
    def elapsed(self) -> float:
        return self.now


@respx.mock
def test_publish_rides_out_an_outage_longer_than_the_old_five_attempt_budget(
    dlq_root: Path,
) -> None:
    # 12 x 503 is ~30 s of Container Apps revision swap; the old 5-attempt/7.5 s
    # budget dead-lettered the signal here.
    clock = FakeClock()
    route = respx.post(f"{BASE}/v1/signals").mock(
        side_effect=[httpx.Response(503)] * 12 + [_ACCEPTED],
    )
    publisher = SignalPublisher(
        api_key=API_KEY,
        dlq=DiskDLQ(root=dlq_root),
        sleep=clock.sleep,
        monotonic=clock,
        random_fn=lambda: 1.0,
    )
    ack = publisher.publish(Signal(strategy_id="s1", payload={"k": "v"}))
    assert ack.sequence == 1
    assert route.call_count == 13
    assert clock.elapsed <= DEFAULT_RETRY_BUDGET_SECONDS
    assert list(publisher.dlq.pending()) == []  # type: ignore[union-attr]


@respx.mock
def test_publish_retry_delay_is_capped_so_one_sleep_never_runs_away(dlq_root: Path) -> None:
    clock = FakeClock()
    respx.post(f"{BASE}/v1/signals").mock(
        side_effect=[httpx.Response(503)] * 8 + [_ACCEPTED],
    )
    SignalPublisher(
        api_key=API_KEY,
        dlq=DiskDLQ(root=dlq_root),
        sleep=clock.sleep,
        monotonic=clock,
        random_fn=lambda: 1.0,
    ).publish(Signal(strategy_id="s1", payload={}))
    assert clock.sleeps == [0.5, 1.0, 2.0, 4.0, 8.0, 8.0, 8.0, 8.0]


@respx.mock
def test_publish_honours_the_servers_retry_after_hint(dlq_root: Path) -> None:
    clock = FakeClock()
    respx.post(f"{BASE}/v1/signals").mock(
        side_effect=[httpx.Response(429, headers={"Retry-After": "3"}), _ACCEPTED],
    )
    ack = SignalPublisher(
        api_key=API_KEY,
        dlq=DiskDLQ(root=dlq_root),
        sleep=clock.sleep,
        monotonic=clock,
        random_fn=lambda: 1.0,
    ).publish(Signal(strategy_id="s1", payload={}))
    assert ack.sequence == 1
    assert clock.sleeps == [3.0]  # the server's hint, not our 0.5 s backoff


@respx.mock
def test_publish_spills_to_dlq_once_the_retry_budget_is_spent(dlq_root: Path) -> None:
    clock = FakeClock()
    route = respx.post(f"{BASE}/v1/signals").mock(return_value=httpx.Response(503))
    publisher = SignalPublisher(
        api_key=API_KEY,
        dlq=DiskDLQ(root=dlq_root),
        retry_budget_seconds=20.0,
        sleep=clock.sleep,
        monotonic=clock,
        random_fn=lambda: 1.0,
    )
    with pytest.raises(PublishFailed) as excinfo:
        publisher.publish(Signal(strategy_id="s1", payload={"k": "v"}))
    # Never overshoots the budget, and never silently loses the signal.
    assert clock.elapsed == pytest.approx(20.0)
    assert route.call_count == excinfo.value.attempts
    assert excinfo.value.dlq_path is not None and excinfo.value.dlq_path.exists()


@respx.mock
def test_publish_auth_error_still_fails_on_the_first_attempt(dlq_root: Path) -> None:
    # The long budget must not make a terminal failure slow: 401 is never retried.
    clock = FakeClock()
    route = respx.post(f"{BASE}/v1/signals").mock(return_value=httpx.Response(401))
    publisher = SignalPublisher(
        api_key=API_KEY, dlq=DiskDLQ(root=dlq_root), sleep=clock.sleep, monotonic=clock
    )
    with pytest.raises(AuthError):
        publisher.publish(Signal(strategy_id="s1", payload={}))
    assert route.call_count == 1
    assert clock.sleeps == []
