"""Retry policy: outage-tolerant backoff with full jitter and a wall-clock budget.

Purpose:
    Shared policy for the sync and async publishers and the subscriber's REST paging.
    Kept side-effect free so the policy is trivially unit-testable — the callers own
    the loop and inject ``sleep``/``random_fn``/``monotonic``.

Contract:
    * Retry ONLY on HTTP 408, 429, and 5xx, and on transport-level errors
      (``httpx.TransportError``). Everything else is terminal.
    * Backoff before retry ``n`` (0-based) is ``random() * 0.5 * 2**n`` seconds —
      "full jitter" — capped at ``MAX_DELAY_SECONDS`` so a long budget can never
      produce one enormous sleep.
    * A server ``Retry-After`` header WINS over the computed backoff (the cap bounds
      the SDK's own growth; it does not override an explicit server instruction),
      clamped to ``MAX_RETRY_AFTER_SECONDS`` and to the remaining budget.
    * ``RetryPolicy`` bounds a retry loop by WALL CLOCK (``budget_seconds``) with an
      attempt cap as a secondary safety net. Wall clock is the primary bound because
      the failure being ridden out — an API restarting for a deployment — has a
      duration, not an attempt count. Either bound may be ``None`` (unbounded), which
      is how the long-running subscriber retries until ``stop()``.

Why the default budget is 90 s:
    The relay runs on Azure Container Apps; a revision swap or an EF migration bundle
    takes tens of seconds, during which the ingress answers 503 or refuses the
    connection. The previous 5-attempt budget expired after ~7.5 s worst case, so a
    routine deployment dead-lettered every in-flight publish.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

DEFAULT_MAX_ATTEMPTS = 25
"""Secondary safety net on attempt count.

Sized so the wall-clock budget is what actually binds under the defaults: with the
delay capped at 8 s (mean 4 s under full jitter), 90 s of budget is ~22 retries.
Callers who want an attempt-bounded loop pass ``max_attempts`` explicitly.
"""

DEFAULT_RETRY_BUDGET_SECONDS = 90.0
"""Default wall-clock retry budget — long enough to ride out a deployment."""

BASE_DELAY_SECONDS = 0.5

MAX_DELAY_SECONDS = 8.0
"""Ceiling on a single backoff sleep, so a long budget stays responsive."""

MAX_RETRY_AFTER_SECONDS = 300.0
"""Ceiling on an honoured ``Retry-After`` hint (a hostile or absurd value is clamped)."""

_MAX_BACKOFF_EXPONENT = 32
"""Exponent clamp: ``2.0**attempt`` overflows a float once an unbounded loop runs long
enough. At 32 the term is already ~2.1e9 s, far above any ``max_delay``, so clamping is
exact — it only stops the arithmetic from raising ``OverflowError``."""


def is_retryable_status(status: int) -> bool:
    """Return ``True`` when an HTTP status code warrants a retry.

    Args:
        status: HTTP status code from the relay.

    Returns:
        ``True`` for 408 (request timeout), 429 (rate limited), and any 5xx — the
        family a restarting or draining API answers with; ``False`` for everything
        else (success, auth, validation, client errors).
    """
    return status in (408, 429) or 500 <= status <= 599


def backoff_delay(
    attempt: int,
    random_fn: Callable[[], float],
    max_delay: float = MAX_DELAY_SECONDS,
) -> float:
    """Compute the capped full-jitter backoff delay before retry ``attempt``.

    Args:
        attempt: 0-based retry index (0 = delay before the second HTTP attempt).
        random_fn: Uniform ``[0, 1)`` source; injectable so tests are deterministic
            (``lambda: 1.0`` yields the schedule 0.5, 1, 2, 4, 8, 8, ...).
        max_delay: Ceiling on the returned delay in seconds. The cap matters once the
            budget allows many retries: uncapped, retry 10 would sleep up to 512 s in
            one go and stay deaf to a ``stop()`` for that whole time.

    Returns:
        Delay in seconds: ``random_fn() * 0.5 * 2**attempt``, capped at ``max_delay``.
    """
    exponent = min(max(attempt, 0), _MAX_BACKOFF_EXPONENT)
    return min(random_fn() * BASE_DELAY_SECONDS * (2.0**exponent), max_delay)


def parse_retry_after(
    value: str | None,
    now: Callable[[], datetime] | None = None,
) -> float | None:
    """Parse an HTTP ``Retry-After`` header into a delay in seconds.

    Both RFC 9110 forms are accepted: delta-seconds (``"30"``) and an HTTP-date
    (``"Sat, 29 Aug 2026 12:00:30 GMT"``). The relay emits the delta form on 429;
    the date form is parsed anyway because a fronting gateway may substitute it.

    Args:
        value: Raw header value, or ``None`` when the response carried no header.
        now: Clock used to resolve the HTTP-date form (test seam); defaults to
            ``datetime.now(UTC)``.

    Returns:
        A non-negative delay in seconds clamped to ``MAX_RETRY_AFTER_SECONDS``, or
        ``None`` when there is no header or it cannot be parsed — in which case the
        caller falls back to its own exponential backoff rather than failing.
    """
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        seconds = float(int(raw))
    except ValueError:
        try:
            when = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:  # pragma: no cover - defensive; RFC dates carry a zone
            when = when.replace(tzinfo=UTC)
        current = now() if now is not None else datetime.now(UTC)
        seconds = (when - current).total_seconds()
    return min(max(seconds, 0.0), MAX_RETRY_AFTER_SECONDS)


@dataclass(frozen=True)
class RetryPolicy:
    """How long a retry loop keeps trying, and how long it waits between attempts.

    The policy computes delays; it never sleeps and never reads a clock. Callers own
    the loop, measure their own elapsed time, and inject ``random_fn`` — which keeps
    the whole policy deterministic under test.

    Attributes:
        max_attempts: Hard cap on total HTTP attempts, or ``None`` for no cap. A
            secondary bound: under the defaults ``budget_seconds`` is what binds.
        budget_seconds: Wall-clock seconds, measured from the first attempt, after
            which the loop gives up. ``None`` retries indefinitely — used by the
            subscriber, which is bounded by ``stop()`` instead.
        max_delay_seconds: Ceiling on any single backoff sleep.
    """

    max_attempts: int | None = DEFAULT_MAX_ATTEMPTS
    budget_seconds: float | None = DEFAULT_RETRY_BUDGET_SECONDS
    max_delay_seconds: float = MAX_DELAY_SECONDS

    def next_delay(
        self,
        *,
        attempts: int,
        elapsed: float,
        retry_after: str | None,
        random_fn: Callable[[], float],
    ) -> float | None:
        """Decide whether to retry again, and how long to wait first.

        Args:
            attempts: HTTP attempts already made (1 after the first request).
            elapsed: Wall-clock seconds since the first attempt started.
            retry_after: Raw ``Retry-After`` header from the failed response, or
                ``None`` (transport error, or no such header).
            random_fn: Uniform ``[0, 1)`` source for the jitter.

        Returns:
            Seconds to sleep before the next attempt, or ``None`` when the loop must
            stop because a bound is exhausted. A returned delay never exceeds the
            remaining budget, so the loop cannot overshoot ``budget_seconds``.
        """
        if self.max_attempts is not None and attempts >= self.max_attempts:
            return None
        hint = parse_retry_after(retry_after)
        delay = (
            hint
            if hint is not None
            else backoff_delay(attempts - 1, random_fn, self.max_delay_seconds)
        )
        if self.budget_seconds is not None:
            remaining = self.budget_seconds - elapsed
            if remaining <= 0.0:
                return None
            delay = min(delay, remaining)
        return delay
