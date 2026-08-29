# tasks/todo.md — outage-tolerant retry (deployment ride-out)

Design approved 2026-08-29 (operator): publish retry budget **90 s on by default**;
subscriber REST catch-up **retries indefinitely with capped backoff** until `stop()`.

## Problem

Retry *classification* was already correct (408/429/5xx + `httpx.TransportError`), but the
*budget* was ~3.75 s expected / 7.5 s worst case over 5 attempts. An Azure Container Apps
revision swap or an EF migration bundle is 30–120 s of 503s, so the SDK gave up mid-deploy:
publishers dead-lettered the batch, and `SignalSubscriber.run()` raised `MultiEdgeError`
and died. `Retry-After` (which the relay emits on 429) was ignored entirely.

## Plan

- [x] Design + operator approval (budget defaults)
- [x] `_retry.py`: failing tests for delay cap, `parse_retry_after`, `RetryPolicy.next_delay`
- [x] `_retry.py`: implement `MAX_DELAY_SECONDS`, `DEFAULT_RETRY_BUDGET_SECONDS`,
      `parse_retry_after`, `RetryPolicy`; raise `DEFAULT_MAX_ATTEMPTS` 5 -> 25 so the
      wall-clock budget is the binding bound under the defaults
- [x] `publisher.py`: failing tests (rides out an outage longer than the old budget;
      honours `Retry-After`; budget exhaustion still spills to the DLQ)
- [x] `publisher.py` + `publisher_async.py`: one shared loop shape driven by `RetryPolicy`;
      new `retry_budget_seconds` + `monotonic` (test seam) constructor params
- [x] `subscriber.py`: failing test (catch-up survives a burst longer than 5 attempts)
- [x] `subscriber.py`: unbounded default retry in `_fetch_page`, interruptible by `stop()`,
      every retry reported through `on_error`
- [x] Docs: CHANGELOG, README, CLAUDE.md + .github/copilot-instructions.md (same commit),
      version 0.4.0 -> 0.5.0
- [x] Gates fresh: pytest -m "not integration", ruff, black --check, mypy --strict src

## Review

All four gates green (fresh run, 2026-08-29): pytest 228 passed / 1 skipped, ruff clean,
black clean, mypy --strict clean; `uv build` produces 0.5.0 sdist + wheel.

Red-green proof (not just "it passes once"):

* Publisher/policy: restoring `DEFAULT_MAX_ATTEMPTS=5` + `DEFAULT_RETRY_BUDGET_SECONDS=7.5`
  fails `test_policy_rides_out_...`, `test_publish_rides_out_...`, and the async twin.
* Subscriber: setting its `max_attempts` default back to 5 fails
  `test_catch_up_survives_an_outage_...`. Restored -> passes.

What did NOT work / what was learned:

* An attempt-counted budget cannot express "ride out a deployment". 5 attempts of
  full-jitter backoff is ~3.75 s expected / 7.5 s worst case; the outage being ridden out
  is 30-120 s. Raising the attempt count alone was rejected: with full jitter the actual
  ride-out duration is unpredictable (expected ~half the cap), and it still ignores
  `Retry-After`. The bound has to be wall clock.
* The first `RetryPolicy` unit test caught a real `OverflowError`: `2.0**attempt` blows up
  once an unbounded subscriber loop runs long enough. Fixed with `_MAX_BACKOFF_EXPONENT`.
* An unbounded retry loop must (a) slice its sleeps so `stop()` lands within a second and
  (b) report every attempt through `on_error` — otherwise a permanent outage looks
  identical to a healthy idle subscriber.