"""Synchronous signal publisher: idempotent publish with retries and a disk DLQ.

Purpose:
    ``SignalPublisher`` is the never-silent-loss publish path. Every signal gets a
    ULID idempotency key, transient failures are retried with full-jitter backoff,
    terminal failures raise typed exceptions, and exhausted retries spill the signal
    to the disk DLQ before raising ``PublishFailed`` with the spill path.

Contract:
    * 401/403 -> ``AuthError`` (no retry); 422/413 -> ``ValidationRejected`` (no retry).
    * 408/429/5xx and transport errors -> retried until the wall-clock retry budget
      (default 90 s — long enough to ride out a relay deployment) or ``max_attempts``
      is exhausted. A server ``Retry-After`` hint overrides the computed backoff.
    * Any other status -> fail fast (one attempt) to the DLQ.
    * HTTP 200 (vs 201) means the relay deduplicated by ``client_signal_id`` and
      returned the original ack: ``SignalAck.duplicate`` is ``True``.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterable
from types import TracebackType
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:  # pragma: no cover - the crypto extra is never imported at runtime here
    from .sealed.registry import Sealer

from ._http import DEFAULT_BASE_URL, DEFAULT_TIMEOUT_SECONDS, build_client
from ._retry import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_RETRY_BUDGET_SECONDS,
    RetryPolicy,
    is_retryable_status,
)
from .dlq import DiskDLQ
from .exceptions import AuthError, IdempotencyConflict, PublishFailed, ValidationRejected
from .models import Signal, SignalAck
from .ulid import new_ulid

_UNSET: Any = object()


def prepare_signal(signal: Signal | dict[str, Any], sealer: Sealer | None = None) -> Signal:
    """Coerce dict input to ``Signal``, ensure a ULID ``client_signal_id``, seal.

    Sealing happens HERE — after the idempotency ULID is assigned (the sealed
    envelope's identity binding needs it) and before any DLQ spill (so the DLQ
    stores ciphertext and resends are byte-identical, keeping dedup intact).

    Args:
        signal: A ``Signal`` or a plain dict matching its schema.
        sealer: Optional sealed-mode sealer; when given, the returned signal's
            payload is the sealed envelope.

    Returns:
        A ``Signal`` whose ``client_signal_id`` is set (auto-ULID when absent), so
        every retry and DLQ resend of this signal is deduplicated by the relay.
    """
    if isinstance(signal, dict):
        signal = Signal.model_validate(signal)
    if signal.client_signal_id is None:
        signal = signal.model_copy(update={"client_signal_id": new_ulid()})
    if sealer is not None:
        signal = sealer.seal_signal(signal)
    return signal


def ack_from_response(response: httpx.Response) -> SignalAck:
    """Parse an accept response (200/201) into a ``SignalAck``.

    The relay states the outcome twice — the body's ``duplicate`` field and the status
    code (200 for a replayed ack, 201 for a fresh publish) — and the two cannot
    disagree. The body is authoritative; the status is kept as a belt-and-braces
    upgrade so a relay that ever omitted the field still yields a correct flag.
    """
    ack = SignalAck.model_validate(response.json())
    if response.status_code == 200 and not ack.duplicate:
        ack = ack.model_copy(update={"duplicate": True})
    return ack


def raise_for_terminal_status(response: httpx.Response) -> None:
    """Raise the typed, never-retried exception for terminal statuses.

    Raises:
        AuthError: On 401/403 — fix the API key; retrying cannot help.
        ValidationRejected: On 422/413 — fix the signal; retrying cannot help.
        IdempotencyConflict: On 409 ``client_signal_id_conflict`` — the id already
            names a signal with a different payload (relay ADR 0015); publish the
            correction under a new id. Never retried, never dead-lettered: the
            same bytes under the same id can never succeed, so a DLQ entry would
            be a forever-409 trap for ``dlq resend``. Other 409 bodies (e.g.
            ``strategy_archived``) fall through to the fail-fast DLQ path.
    """
    if response.status_code in (401, 403):
        raise AuthError(f"relay rejected the API key (HTTP {response.status_code})")
    if response.status_code in (413, 422):
        raise ValidationRejected(
            f"relay rejected the signal (HTTP {response.status_code}): " f"{response.text[:500]}"
        )
    if response.status_code == 409:
        try:
            body = response.json()
        except ValueError:
            return
        if isinstance(body, dict) and body.get("error") == "client_signal_id_conflict":
            raise IdempotencyConflict(
                str(body.get("signal_id", "")),
                int(body.get("sequence", 0)),
                hint=body.get("hint"),
            )


class SignalPublisher:
    """Synchronous publisher for MultiEdge Signal Relay.

    Usage::

        with SignalPublisher(api_key="mesk_...") as publisher:
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
        max_attempts: int | None = DEFAULT_MAX_ATTEMPTS,
        retry_budget_seconds: float | None = DEFAULT_RETRY_BUDGET_SECONDS,
        dlq: DiskDLQ | None = _UNSET,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        random_fn: Callable[[], float] = random.random,
        monotonic: Callable[[], float] = time.monotonic,
        sealer: Sealer | None = None,
    ) -> None:
        """Create a publisher.

        Args:
            api_key: Relay API key (Bearer).
            base_url: Relay origin; override for staging deployments.
            timeout: Per-request timeout in seconds.
            max_attempts: Cap on total HTTP attempts per signal, or ``None`` for no
                cap. A safety net only: under the defaults ``retry_budget_seconds``
                is the bound that binds.
            retry_budget_seconds: Wall-clock seconds a single ``publish`` keeps
                retrying a transient failure before giving up and dead-lettering.
                The default rides out a relay deployment (a Container Apps revision
                swap or migration bundle answers 503 for tens of seconds). Pass a
                smaller value for a latency-sensitive path, or ``None`` to retry
                until ``max_attempts`` alone stops the loop.
            dlq: Dead-letter queue; defaults to ``DiskDLQ()`` under
                ``~/.multiedge/dlq``. Pass ``None`` to disable spilling.
            transport: httpx transport override (test seam).
            sleep: Injectable sleep for backoff (test seam).
            random_fn: Injectable uniform [0,1) source for jitter (test seam).
            monotonic: Injectable monotonic clock used to measure the retry budget
                (test seam); a fake clock advanced by the injected ``sleep`` lets a
                test spend the whole budget instantly.
            sealer: Sealed-mode sealer (``multiedge-relay[sealed]``); when given,
                every payload is end-to-end encrypted before it leaves this
                process — the relay (and its DLQ files) see only ciphertext.
        """
        self.dlq: DiskDLQ | None = DiskDLQ() if dlq is _UNSET else dlq
        self._sealer = sealer
        self._policy = RetryPolicy(max_attempts=max_attempts, budget_seconds=retry_budget_seconds)
        self._sleep = sleep
        self._random_fn = random_fn
        self._monotonic = monotonic
        self._client = build_client(
            api_key, base_url=base_url, timeout=timeout, transport=transport
        )

    def publish(self, signal: Signal | dict[str, Any]) -> SignalAck:
        """Publish one signal; never silently lose it.

        One signal carries one COMPLETE portfolio state: put the whole book in
        ``signal.payload`` (the shipped ``portfolio_rebalance/1.0`` schema is exactly
        that — an unbounded ``positions`` list for one signal date). 64 KB fits
        roughly 900 positions, and the result is ONE sequence number a subscriber
        applies atomically. Reach for ``publish_many`` only for signals that are
        genuinely independent of each other.

        Args:
            signal: A ``Signal`` or dict; a missing ``client_signal_id`` is assigned
                a ULID so retries are idempotent.

        Returns:
            The relay's ``SignalAck`` (``duplicate=True`` when the relay had
            already seen this ``client_signal_id``).

        Raises:
            AuthError: Invalid API key (401/403); not retried, not dead-lettered.
            ValidationRejected: Invalid or oversized signal (422/413); not retried.
            PublishFailed: Retries exhausted or unexpected terminal status; the
                signal was appended to the DLQ first (``dlq_path`` set) when a DLQ
                is configured.
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
                response = self._client.post("/v1/signals", json=body)
            except httpx.TransportError as exc:
                # A relay mid-deployment refuses connections before it 503s.
                last_error = f"transport error: {exc!r}"
            else:
                if response.status_code in (200, 201):
                    return ack_from_response(response)
                raise_for_terminal_status(response)
                last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                if not is_retryable_status(response.status_code):
                    break  # unexpected terminal status — fail fast to the DLQ
                retry_after = response.headers.get("Retry-After")
            delay = self._policy.next_delay(
                attempts=attempts,
                elapsed=self._monotonic() - started,
                retry_after=retry_after,
                random_fn=self._random_fn,
            )
            if delay is None:
                break
            self._sleep(delay)
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

        This is **N HTTP requests, N sequence numbers, and it is NOT atomic** — a
        failure part-way leaves the earlier signals published. There is no batch
        publish endpoint to make it otherwise: the relay accepts one signal per
        request by design, because one signal is one atomic, sequenced event.

        So do not use this to split one portfolio across many signals — a subscriber
        would receive N callbacks with no completeness signal and could durably apply
        half a portfolio. Put the whole portfolio in one ``publish`` instead, and keep
        ``publish_many`` for signals that are genuinely independent.

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
