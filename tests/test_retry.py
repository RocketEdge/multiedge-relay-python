"""Retry policy tests: backoff schedule, jitter, and status classification.

Timing/classification through the publisher is covered with respx + injected sleep in
test_publisher.py; this module unit-tests the policy helpers themselves.
"""

from __future__ import annotations

import pytest

from multiedge_relay._retry import backoff_delay, is_retryable_status


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
