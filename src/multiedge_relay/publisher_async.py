"""Asynchronous signal publisher — identical surface and semantics to the sync twin.

Purpose:
    ``AsyncSignalPublisher`` mirrors ``SignalPublisher`` exactly (same retry policy,
    same DLQ spill, same exceptions) over ``httpx.AsyncClient``. Kept as a separate
    module (not a shared async/sync abstraction) per the flat-and-explicit rule: each
    twin is independently readable and regenerable.

Contract:
    See ``publisher.py`` — the classification and never-silent-loss rules are shared
    via ``prepare_signal`` / ``ack_from_response`` / ``raise_for_terminal_status``.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable, Iterable
from types import TracebackType
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:  # pragma: no cover - the crypto extra is never imported at runtime here
    from .sealed.registry import Sealer

from ._http import DEFAULT_BASE_URL, DEFAULT_TIMEOUT_SECONDS, build_async_client
from ._retry import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_RETRY_BUDGET_SECONDS,
    RetryPolicy,
    is_retryable_status,
)
from .dlq import DiskDLQ
from .exceptions import PublishFailed
from .models import Signal, SignalAck
from .publisher import ack_from_response, prepare_signal, raise_for_terminal_status

_UNSET: Any = object()


class AsyncSignalPublisher:
    """Asynchronous publisher for MultiEdge Signal Relay.

    Usage::

        async with AsyncSignalPublisher(api_key="mek_...") as publisher:
            ack = await publisher.publish(Signal(strategy_id="s", payload={...}))

    Attributes:
        dlq: The disk DLQ used on retry exhaustion, or ``None`` to disable spilling.
            DLQ appends are synchronous local-file writes (microseconds; only on the
            already-slow failure path).
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_attempts: int | None = DEFAULT_MAX_ATTEMPTS,
        retry_budget_seconds: float | None = DEFAULT_RETRY_BUDGET_SECONDS,
        dlq: DiskDLQ | None = _UNSET,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_fn: Callable[[], float] = random.random,
        monotonic: Callable[[], float] = time.monotonic,
        sealer: Sealer | None = None,
    ) -> None:
        """Create an async publisher; parameters mirror ``SignalPublisher`` exactly,
        except ``sleep`` which is an awaitable-returning callable (test seam).

        In particular ``retry_budget_seconds`` (default 90 s) is the wall-clock
        window a single ``publish`` keeps retrying a transient failure — long
        enough to ride out a relay deployment rather than dead-lettering through it.
        """
        self.dlq: DiskDLQ | None = DiskDLQ() if dlq is _UNSET else dlq
        self._sealer = sealer
        self._policy = RetryPolicy(max_attempts=max_attempts, budget_seconds=retry_budget_seconds)
        self._sleep = sleep
        self._random_fn = random_fn
        self._monotonic = monotonic
        self._client = build_async_client(
            api_key, base_url=base_url, timeout=timeout, transport=transport
        )

    async def publish(self, signal: Signal | dict[str, Any]) -> SignalAck:
        """Publish one signal; see ``SignalPublisher.publish`` for the full contract.

        Raises:
            AuthError: Invalid API key (401/403); not retried.
            ValidationRejected: Invalid or oversized signal (422/413); not retried.
            PublishFailed: Retries exhausted; DLQ spill path attached when configured.
        """
        prepared = prepare_signal(signal, self._sealer)
        body = prepared.model_dump(mode="json")
        attempts = 0
        started = self._monotonic()
        last_error = "unknown error"
        while True:
            attempts += 1
            retry_after: str | None = None
            try:
                response = await self._client.post("/v1/signals", json=body)
            except httpx.TransportError as exc:
                # A relay mid-deployment refuses connections before it 503s.
                last_error = f"transport error: {exc!r}"
            else:
                if response.status_code in (200, 201):
                    return ack_from_response(response)
                raise_for_terminal_status(response)
                last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                if not is_retryable_status(response.status_code):
                    break
                retry_after = response.headers.get("Retry-After")
            delay = self._policy.next_delay(
                attempts=attempts,
                elapsed=self._monotonic() - started,
                retry_after=retry_after,
                random_fn=self._random_fn,
            )
            if delay is None:
                break
            await self._sleep(delay)
        dlq_path = (
            self.dlq.append(prepared, error=last_error, attempts=attempts)
            if self.dlq is not None
            else None
        )
        raise PublishFailed(prepared, attempts, dlq_path)

    async def publish_many(
        self,
        signals: Iterable[Signal | dict[str, Any]],
        *,
        raise_on_partial: bool = True,
    ) -> list[SignalAck | PublishFailed]:
        """Publish signals in order; see ``SignalPublisher.publish_many``."""
        results: list[SignalAck | PublishFailed] = []
        for signal in signals:
            try:
                results.append(await self.publish(signal))
            except PublishFailed as failure:
                if raise_on_partial:
                    raise
                results.append(failure)
        return results

    async def aclose(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> AsyncSignalPublisher:
        """Enter a context that closes the HTTP client on exit."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the HTTP client."""
        await self.aclose()
