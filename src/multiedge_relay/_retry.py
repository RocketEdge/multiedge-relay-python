"""Retry policy: exponential backoff with full jitter, retryable-status classification.

Purpose:
    Shared policy helpers for the sync and async publishers and the subscriber's
    REST paging. Kept side-effect free so the policy is trivially unit-testable —
    the callers own the loop and inject ``sleep``/``random_fn``.

Contract:
    * Retry ONLY on HTTP 408, 429, and 5xx, and on transport-level errors
      (``httpx.TransportError``). Everything else is terminal.
    * Delay before retry ``n`` (0-based) is ``random() * 0.5 * 2**n`` seconds —
      "full jitter": uniformly distributed in ``[0, 0.5 * 2**n)``.
    * Default budget is 5 attempts total (4 retries: max ~7.5 s worst case).
"""

from __future__ import annotations

from collections.abc import Callable

DEFAULT_MAX_ATTEMPTS = 5
BASE_DELAY_SECONDS = 0.5


def is_retryable_status(status: int) -> bool:
    """Return ``True`` when an HTTP status code warrants a retry.

    Args:
        status: HTTP status code from the relay.

    Returns:
        ``True`` for 408 (request timeout), 429 (rate limited), and any 5xx;
        ``False`` for everything else (success, auth, validation, client errors).
    """
    return status in (408, 429) or 500 <= status <= 599


def backoff_delay(attempt: int, random_fn: Callable[[], float]) -> float:
    """Compute the full-jitter backoff delay before retry ``attempt``.

    Args:
        attempt: 0-based retry index (0 = delay before the second HTTP attempt).
        random_fn: Uniform ``[0, 1)`` source; injectable so tests are deterministic
            (``lambda: 1.0`` yields the schedule 0.5, 1, 2, 4, ...).

    Returns:
        Delay in seconds: ``random_fn() * 0.5 * 2**attempt``.
    """
    return random_fn() * BASE_DELAY_SECONDS * (2.0**attempt)
