"""Synchronous signal publisher: idempotent publish with retries and a disk DLQ.

Purpose:
    ``SignalPublisher`` is the never-silent-loss publish path. Every signal gets a
    ULID idempotency key, transient failures are retried with full-jitter backoff,
    terminal failures raise typed exceptions, and exhausted retries spill the signal
    to the disk DLQ before raising ``PublishFailed`` with the spill path.

Contract:
    * 401/403 -> ``AuthError`` (no retry); 422/413 -> ``ValidationRejected`` (no retry).
    * 408/429/5xx and transport errors -> retried up to ``max_attempts``.
    * Any other status -> fail fast (one attempt) to the DLQ.
    * HTTP 200 (vs 201) means the relay deduplicated by ``client_signal_id`` and
      returned the original ack: ``SignalAck.deduplicated`` is ``True``.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterable
from types import TracebackType
from typing import Any

import httpx

from ._http import DEFAULT_BASE_URL, DEFAULT_TIMEOUT_SECONDS, build_client
from ._retry import DEFAULT_MAX_ATTEMPTS, backoff_delay, is_retryable_status
from .dlq import DiskDLQ
from .exceptions import AuthError, PublishFailed, ValidationRejected
from .models import Signal, SignalAck
from .ulid import new_ulid

_UNSET: Any = object()


def prepare_signal(signal: Signal | dict[str, Any]) -> Signal:
    """Coerce dict input to ``Signal`` and ensure a ULID ``client_signal_id``.

    Args:
        signal: A ``Signal`` or a plain dict matching its schema.

    Returns:
        A ``Signal`` whose ``client_signal_id`` is set (auto-ULID when absent), so
        every retry and DLQ resend of this signal is deduplicated by the relay.
    """
    if isinstance(signal, dict):
        signal = Signal.model_validate(signal)
    if signal.client_signal_id is None:
        signal = signal.model_copy(update={"client_signal_id": new_ulid()})
    return signal


def ack_from_response(response: httpx.Response) -> SignalAck:
    """Parse an accept response (200/201) into a ``SignalAck``.

    A 200 status means the relay had already accepted this ``client_signal_id`` and
    returned the original ack — ``deduplicated`` is forced ``True`` in that case.
    """
    ack = SignalAck.model_validate(response.json())
    if response.status_code == 200 and not ack.deduplicated:
        ack = ack.model_copy(update={"deduplicated": True})
    return ack


def raise_for_terminal_status(response: httpx.Response) -> None:
    """Raise the typed, never-retried exception for terminal statuses.

    Raises:
        AuthError: On 401/403 — fix the API key; retrying cannot help.
        ValidationRejected: On 422/413 — fix the signal; retrying cannot help.
    """
    if response.status_code in (401, 403):
        raise AuthError(f"relay rejected the API key (HTTP {response.status_code})")
    if response.status_code in (413, 422):
        raise ValidationRejected(
            f"relay rejected the signal (HTTP {response.status_code}): " f"{response.text[:500]}"
        )


class SignalPublisher:
    """Synchronous publisher for MultiEdge Signal Relay.

    Usage::

        with SignalPublisher(api_key="mek_...") as publisher:
            ack = publisher.publish(Signal(strategy_id="s", payload={...}))

    Attributes:
        dlq: The disk DLQ used on retry exhaustion, or ``None`` to disable spilling
            (then ``PublishFailed.dlq_path`` is ``None`` and the caller owns the
            signal's fate). Public so ``DiskDLQ.resend`` can suspend it.
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        dlq: DiskDLQ | None = _UNSET,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        random_fn: Callable[[], float] = random.random,
    ) -> None:
        """Create a publisher.

        Args:
            api_key: Relay API key (Bearer).
            base_url: Relay origin; override for staging deployments.
            timeout: Per-request timeout in seconds.
            max_attempts: Total HTTP attempts per signal (retries = attempts - 1).
            dlq: Dead-letter queue; defaults to ``DiskDLQ()`` under
                ``~/.multiedge/dlq``. Pass ``None`` to disable spilling.
            transport: httpx transport override (test seam).
            sleep: Injectable sleep for backoff (test seam).
            random_fn: Injectable uniform [0,1) source for jitter (test seam).
        """
        self.dlq: DiskDLQ | None = DiskDLQ() if dlq is _UNSET else dlq
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._random_fn = random_fn
        self._client = build_client(
            api_key, base_url=base_url, timeout=timeout, transport=transport
        )

    def publish(self, signal: Signal | dict[str, Any]) -> SignalAck:
        """Publish one signal; never silently lose it.

        Args:
            signal: A ``Signal`` or dict; a missing ``client_signal_id`` is assigned
                a ULID so retries are idempotent.

        Returns:
            The relay's ``SignalAck`` (``deduplicated=True`` when the relay had
            already seen this ``client_signal_id``).

        Raises:
            AuthError: Invalid API key (401/403); not retried, not dead-lettered.
            ValidationRejected: Invalid or oversized signal (422/413); not retried.
            PublishFailed: Retries exhausted or unexpected terminal status; the
                signal was appended to the DLQ first (``dlq_path`` set) when a DLQ
                is configured.
        """
        prepared = prepare_signal(signal)
        body = prepared.model_dump(mode="json")
        attempts = 0
        last_error = "unknown error"
        while attempts < self._max_attempts:
            attempts += 1
            try:
                response = self._client.post("/v1/signals", json=body)
            except httpx.TransportError as exc:
                last_error = f"transport error: {exc!r}"
                if attempts < self._max_attempts:
                    self._sleep(backoff_delay(attempts - 1, self._random_fn))
                continue
            if response.status_code in (200, 201):
                return ack_from_response(response)
            raise_for_terminal_status(response)
            last_error = f"HTTP {response.status_code}: {response.text[:200]}"
            if not is_retryable_status(response.status_code):
                break  # unexpected terminal status — fail fast to the DLQ
            if attempts < self._max_attempts:
                self._sleep(backoff_delay(attempts - 1, self._random_fn))
        dlq_path = (
            self.dlq.append(prepared, error=last_error, attempts=attempts)
            if self.dlq is not None
            else None
        )
        raise PublishFailed(prepared, attempts, dlq_path)

    def publish_many(
        self,
        signals: Iterable[Signal | dict[str, Any]],
        *,
        raise_on_partial: bool = True,
    ) -> list[SignalAck | PublishFailed]:
        """Publish signals in order, accounting for every one.

        Args:
            signals: Signals to publish sequentially (order preserved).
            raise_on_partial: When ``True`` (default) the first ``PublishFailed``
                propagates; when ``False`` failures are returned in-place so the
                caller can reconcile the batch (``AuthError`` always propagates —
                the whole batch would fail identically).

        Returns:
            One ``SignalAck`` or ``PublishFailed`` per input signal, in order.
        """
        results: list[SignalAck | PublishFailed] = []
        for signal in signals:
            try:
                results.append(self.publish(signal))
            except PublishFailed as failure:
                if raise_on_partial:
                    raise
                results.append(failure)
        return results

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def __enter__(self) -> SignalPublisher:
        """Enter a context that closes the HTTP client on exit."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the HTTP client."""
        self.close()
