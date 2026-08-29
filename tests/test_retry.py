"""Retry policy tests: backoff schedule, jitter, and status classification.

Timing/classification through the publisher is covered with respx + injected sleep in
test_publisher.py; this module unit-tests the policy helpers themselves.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from multiedge_relay._retry import (
    DEFAULT_RETRY_BUDGET_SECONDS,
    MAX_DELAY_SECONDS,
    MAX_RETRY_AFTER_SECONDS,
    RetryPolicy,
    backoff_delay,
    is_retryable_status,
    parse_retry_after,
)


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504, 599])
def test_retryable_statuses(status: int) -> None:
    assert is_retryable_status(status)


@pytest.mark.parametrize("status", [200, 201, 301, 400, 401, 403, 404, 413, 422])
def test_non_retryable_statuses(status: int) -> None:
    assert not is_retryable_status(status)


def test_backoff_schedule_full_jitter_upper_bound() -> None:
    # random_fn returning 1.0 yields the maximum delay: 0.5 * 2**n seconds.
    delays = [backoff_delay(n, random_fn=lambda: 1.0) for n in range(4)]
    assert delays == [0.5, 1.0, 2.0, 4.0]


def test_backoff_full_jitter_scales_by_random() -> None:
    assert backoff_delay(2, random_fn=lambda: 0.0) == 0.0
    assert backoff_delay(2, random_fn=lambda: 0.5) == pytest.approx(1.0)


# ------------------------------------------------------------------ delay ceiling
def test_backoff_delay_is_capped_so_a_long_budget_cannot_produce_a_huge_sleep() -> None:
    # Without a cap, attempt 10 would be 0.5 * 2**10 = 512 s in a single sleep.
    assert backoff_delay(10, random_fn=lambda: 1.0) == MAX_DELAY_SECONDS
    assert backoff_delay(4, random_fn=lambda: 1.0) == MAX_DELAY_SECONDS
    assert backoff_delay(3, random_fn=lambda: 1.0) == 4.0  # below the cap: unchanged


def test_backoff_delay_cap_is_overridable() -> None:
    assert backoff_delay(10, random_fn=lambda: 1.0, max_delay=2.0) == 2.0


# ------------------------------------------------------------------ Retry-After
@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("5", 5.0),
        ("0", 0.0),
        ("  12  ", 12.0),
        ("-3", 0.0),  # a past hint means "retry now", never a negative sleep
        (None, None),
        ("", None),
        ("soon", None),  # unparseable: fall back to exponential backoff
    ],
)
def test_parse_retry_after_delta_seconds(header: str | None, expected: float | None) -> None:
    assert parse_retry_after(header) == expected


def test_parse_retry_after_http_date_form() -> None:
    now = datetime(2026, 8, 29, 12, 0, 0, tzinfo=UTC)
    assert parse_retry_after("Sat, 29 Aug 2026 12:00:30 GMT", now=lambda: now) == 30.0
    # A date already in the past clamps to zero rather than going negative.
    assert parse_retry_after("Sat, 29 Aug 2026 11:59:00 GMT", now=lambda: now) == 0.0


def test_parse_retry_after_clamps_an_absurd_hint() -> None:
    assert parse_retry_after("999999") == MAX_RETRY_AFTER_SECONDS


# ------------------------------------------------------------------ RetryPolicy
def test_policy_rides_out_an_outage_far_longer_than_the_old_five_attempt_budget() -> None:
    # The regression this whole change exists for: an Azure deployment window is
    # 30-120 s, and the old 5-attempt budget gave up after ~7.5 s worst case.
    policy = RetryPolicy()
    elapsed = 0.0
    attempts = 0
    while True:
        attempts += 1
        delay = policy.next_delay(
            attempts=attempts, elapsed=elapsed, retry_after=None, random_fn=lambda: 1.0
        )
        if delay is None:
            break
        elapsed += delay
    assert elapsed == pytest.approx(DEFAULT_RETRY_BUDGET_SECONDS)
    assert attempts > 5


def test_policy_stops_when_the_wall_clock_budget_is_spent() -> None:
    policy = RetryPolicy(budget_seconds=10.0)
    assert (
        policy.next_delay(attempts=1, elapsed=10.0, retry_after=None, random_fn=lambda: 1.0) is None
    )


def test_policy_trims_the_last_delay_to_the_remaining_budget() -> None:
    policy = RetryPolicy(budget_seconds=10.0)
    # Attempt 5's uncapped delay would be 8 s, but only 1.5 s of budget remains.
    assert policy.next_delay(
        attempts=5, elapsed=8.5, retry_after=None, random_fn=lambda: 1.0
    ) == pytest.approx(1.5)


def test_policy_stops_at_the_attempt_cap_even_with_budget_left() -> None:
    policy = RetryPolicy(max_attempts=3, budget_seconds=1000.0)
    assert (
        policy.next_delay(attempts=2, elapsed=0.0, retry_after=None, random_fn=lambda: 1.0)
        is not None
    )
    assert (
        policy.next_delay(attempts=3, elapsed=0.0, retry_after=None, random_fn=lambda: 1.0) is None
    )


def test_policy_retries_indefinitely_when_both_bounds_are_none() -> None:
    policy = RetryPolicy(max_attempts=None, budget_seconds=None)
    assert policy.next_delay(
        attempts=10_000, elapsed=1e9, retry_after=None, random_fn=lambda: 1.0
    ) == pytest.approx(MAX_DELAY_SECONDS)


def test_policy_prefers_the_servers_retry_after_over_its_own_backoff() -> None:
    policy = RetryPolicy()
    # The server's hint wins even when it exceeds our own exponential ceiling:
    # the cap bounds OUR growth, it does not override an explicit instruction.
    assert policy.next_delay(
        attempts=1, elapsed=0.0, retry_after="30", random_fn=lambda: 1.0
    ) == pytest.approx(30.0)


def test_policy_ignores_an_unparseable_retry_after() -> None:
    policy = RetryPolicy()
    assert policy.next_delay(
        attempts=1, elapsed=0.0, retry_after="whenever", random_fn=lambda: 1.0
    ) == pytest.approx(0.5)
