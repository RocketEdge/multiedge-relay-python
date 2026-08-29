"""At-least-once subscriber: REST catch-up from a durable cursor, then live delivery.

Purpose:
    ``SignalSubscriber`` implements the product's core promise — offline for a
    weekend, you miss nothing. On start it replays the backlog from the persisted
    cursor over REST (phase 1, ``source="catchup"``), then follows live (phase 2)
    via polling or Azure Web PubSub. The relay ``sequence`` is the only ordering
    truth; the cursor is committed only AFTER each callback returns.

Contract:
    * At-least-once, in-order delivery: within one run each sequence is delivered
      exactly once; across restarts uncommitted signals are redelivered. The
      ``on_signal`` callback MUST therefore be idempotent.
    * A live-transport gap (sequence jump > 1) parks messages in a bounded buffer,
      back-fills the hole from REST (``source="gapfill"``), then drains in order.
      An unfillable gap raises ``GapUnrecoverableError`` — never silently skipped.
    * A corrupt cursor raises ``CursorCorruptError`` before any delivery.
    * Holes in the REST log itself are delivered as-is: REST is authoritative.
    * A transient REST failure (relay deploying, 5xx, refused connection) is retried
      indefinitely with capped backoff and reported through ``on_error`` — a daemon
      with no DLQ must ride out a deployment, not exit. ``stop()`` is the bound.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

import httpx

if TYPE_CHECKING:  # pragma: no cover - the crypto extra is never imported at runtime here
    from .sealed.registry import Unsealer

from ._http import DEFAULT_BASE_URL, DEFAULT_TIMEOUT_SECONDS, build_client
from ._retry import RetryPolicy, backoff_delay, is_retryable_status
from .cursor import CursorStore, FileCursorStore
from .exceptions import (
    AuthError,
    BufferFullError,
    CursorCorruptError,
    GapUnrecoverableError,
    MultiEdgeError,
)
from .models import ReceivedSignal, SignalMeta

StartFrom = int | Literal["cursor", "earliest", "latest"]
LiveTransport = Literal["webpubsub", "poll"]

_FATAL_TYPES = (AuthError, GapUnrecoverableError, BufferFullError, CursorCorruptError)

_STOP_CHECK_INTERVAL_SECONDS = 1.0
"""Longest a backoff sleep may run before the retry loop re-checks ``stop()``."""


class SignalSubscriber:
    """Cursor-based subscriber for one strategy stream.

    Usage::

        subscriber = SignalSubscriber(
            api_key="mek_...", strategy_id="my-strategy", on_signal=callback
        )
        subscriber.run()          # blocks: catch-up, then live
        # ... from another thread or a signal handler:
        subscriber.stop()
    """

    def __init__(
        self,
        api_key: str,
        strategy_id: str,
        on_signal: Callable[[ReceivedSignal, SignalMeta], None],
        *,
        base_url: str = DEFAULT_BASE_URL,
        cursor_store: CursorStore | None = None,
        start_from: StartFrom = "cursor",
        live_transport: LiveTransport = "poll",
        poll_interval: float = 5.0,
        page_size: int = 500,
        on_error: Callable[[Exception], None] | None = None,
        transport: httpx.BaseTransport | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_attempts: int | None = None,
        retry_budget_seconds: float | None = None,
        sleep: Callable[[float], None] = time.sleep,
        random_fn: Callable[[], float] = random.random,
        monotonic: Callable[[], float] = time.monotonic,
        max_buffer: int = 10_000,
        gap_fill_rounds: int = 3,
        unsealer: Unsealer | None = None,
    ) -> None:
        """Create a subscriber.

        Args:
            api_key: Relay API key (Bearer).
            strategy_id: The strategy stream to follow.
            on_signal: Delivery callback ``(signal, meta)``. MUST be idempotent —
                delivery is at-least-once. An exception here halts the subscriber
                WITHOUT committing the cursor, so the signal is redelivered on
                restart (never silently lost).
            base_url: Relay origin.
            cursor_store: Cursor persistence; defaults to ``FileCursorStore()``
                under ``~/.multiedge/cursor``.
            start_from: ``"cursor"`` (default) resumes from the persisted cursor
                (or the beginning when none); ``"earliest"`` replays everything;
                ``"latest"`` skips the backlog; an ``int`` N starts after sequence N.
            live_transport: ``"poll"`` (default, zero extra deps) repeats the
                catch-up query every ``poll_interval`` seconds; ``"webpubsub"``
                uses Azure Web PubSub push (requires the ``[webpubsub]`` extra).
            poll_interval: Seconds between live polls (poll transport only).
            page_size: REST page size for catch-up and gap fill.
            on_error: Observability hook. Called once per failed HTTP attempt on a
                catch-up page (which is then retried) and with callback errors
                (which are then re-raised). Wire it up: with the default unbounded
                retry it is the only way an ongoing relay outage is visible.
            transport: httpx transport override (test seam).
            timeout: Per-request HTTP timeout in seconds.
            max_attempts: Cap on HTTP attempts per catch-up page request, or ``None``
                (default) for no cap — a subscriber is a daemon that should ride out
                a relay deployment rather than exit, and it has no DLQ to fall back
                on. Retries are bounded by ``stop()``.
            retry_budget_seconds: Wall-clock cap on retrying one page request, or
                ``None`` (default) for no cap. Set both bounds to make catch-up
                fail fast instead.
            sleep: Injectable sleep for backoff (test seam).
            random_fn: Injectable uniform [0,1) source for jitter (test seam).
            monotonic: Injectable monotonic clock for the retry budget (test seam).
            max_buffer: Bound on the live reorder buffer; exceeding it raises
                ``BufferFullError`` rather than dropping parked messages.
            gap_fill_rounds: Consecutive empty REST rounds tolerated during a gap
                fill before raising ``GapUnrecoverableError``.
            unsealer: Sealed-mode unsealer (``multiedge-relay[sealed]``); when
                given, every payload is verified and decrypted BEFORE the
                callback, which then sees plaintext. An ``UnsealError`` is
                treated like a callback failure: ``on_error`` is notified, the
                cursor is NOT committed, and the error propagates — never
                silent loss, never delivering unverified ciphertext.
        """
        self.strategy_id = strategy_id
        self.on_signal = on_signal
        self._cursor_store: CursorStore = (
            cursor_store if cursor_store is not None else FileCursorStore()
        )
        self._start_from: StartFrom = start_from
        self._live_transport: LiveTransport = live_transport
        self._poll_interval = poll_interval
        self._page_size = page_size
        self._on_error = on_error
        self._policy = RetryPolicy(max_attempts=max_attempts, budget_seconds=retry_budget_seconds)
        self._sleep = sleep
        self._random_fn = random_fn
        self._monotonic = monotonic
        self._max_buffer = max_buffer
        self._gap_fill_rounds = gap_fill_rounds
        self._unsealer = unsealer
        self._client = build_client(
            api_key, base_url=base_url, timeout=timeout, transport=transport
        )
        self._stop_event = threading.Event()
        self._delivery_lock = threading.Lock()
        self._buffer: dict[int, ReceivedSignal] = {}
        self._position = 0  # last processed sequence; set for real in _start_position
        self._started = False
        # Error stashed by the Web PubSub callback thread for the main loop to raise.
        self._pending_error: Exception | None = None

    # ------------------------------------------------------------------ lifecycle
    def run(self) -> None:
        """Block: catch up from the cursor, then follow live until ``stop()``.

        Raises:
            CursorCorruptError: The persisted cursor is unreadable (never reset).
            AuthError: The relay rejected the API key.
            GapUnrecoverableError: A live gap could not be filled from REST.
            ImportError: ``live_transport="webpubsub"`` without the extra installed.
            Exception: Whatever the ``on_signal`` callback raised (after ``on_error``
                was notified); the failing signal's cursor was NOT committed.
        """
        if self._live_transport == "webpubsub":
            self._require_webpubsub()
        self._start_position()
        self._catch_up("catchup")
        if self._stop_event.is_set():
            return
        if self._live_transport == "poll":
            self._run_poll()
        else:
            self._run_webpubsub()

    def catch_up_only(self) -> int:
        """Run phase 1 only: drain the REST backlog from the cursor, then return.

        Returns:
            Number of signals delivered in this call.

        Raises:
            Same as :meth:`run` (minus live-transport errors).
        """
        self._start_position()
        return self._catch_up("catchup")

    def stop(self) -> None:
        """Request a graceful stop; ``run()`` returns after the in-flight delivery."""
        self._stop_event.set()

    # ------------------------------------------------------------------ phase 1
    def _start_position(self) -> None:
        """Resolve the starting position (last-processed sequence) once per run."""
        start = self._start_from
        if isinstance(start, bool):  # bool is an int subclass; reject explicitly
            raise ValueError("start_from must be an int or 'cursor'/'earliest'/'latest'")
        if isinstance(start, int):
            self._position = start
        elif start == "cursor":
            self._position = self._cursor_store.load(self.strategy_id) or 0
        elif start == "earliest":
            self._position = 0
        elif start == "latest":
            self._position = self._scan_latest()
            if self._position > 0:
                self._cursor_store.commit(self.strategy_id, self._position)
        else:  # pragma: no cover - typing forbids it
            raise ValueError(f"invalid start_from: {start!r}")
        self._started = True

    def _scan_latest(self) -> int:
        """Find the strategy's current last sequence by paging without delivering."""
        position = 0
        while True:
            page = self._fetch_page(position)
            if page:
                position = page[-1].sequence
            if len(page) < self._page_size:
                return position

    def _catch_up(self, source: Literal["catchup", "live", "gapfill"]) -> int:
        """Deliver everything past the current position from REST, page by page."""
        delivered = 0
        while not self._stop_event.is_set():
            page = self._fetch_page(self._position)
            for received in page:
                if self._stop_event.is_set():
                    return delivered
                if received.sequence <= self._position:
                    continue  # overlap safety — dedupe by sequence
                self._deliver(received, source)
                delivered += 1
            if len(page) < self._page_size:
                break
        return delivered

    def _fetch_page(self, since_sequence: int) -> list[ReceivedSignal]:
        """GET one page of signals after ``since_sequence``, with retry policy.

        By default this retries a transient failure INDEFINITELY (capped backoff),
        because a subscriber is a long-running daemon with no DLQ to fall back on:
        a relay deployment used to raise straight out of ``run()`` and require an
        operator restart. Every retry is reported through ``on_error``, so an
        unbounded loop is never a silent one. Bound it with ``max_attempts`` or
        ``retry_budget_seconds`` to get the fail-fast behaviour instead.

        Returns:
            The page's signals, or an empty list when ``stop()`` was requested while
            retrying (the caller's loops all re-check the stop event).

        Raises:
            AuthError: 401/403 — never retried.
            MultiEdgeError: A non-retryable status, or a bound was exhausted.
        """
        attempts = 0
        started = self._monotonic()
        last_error = "unknown error"
        while not self._stop_event.is_set():
            attempts += 1
            retry_after: str | None = None
            try:
                response = self._client.get(
                    "/v1/signals",
                    params={
                        "strategy_id": self.strategy_id,
                        "since_sequence": since_sequence,
                        "limit": self._page_size,
                    },
                )
            except httpx.TransportError as exc:
                # A relay mid-deployment refuses connections before it 503s.
                last_error = f"transport error: {exc!r}"
            else:
                if response.status_code in (401, 403):
                    raise AuthError(f"relay rejected the API key (HTTP {response.status_code})")
                if response.status_code == 200:
                    rows = response.json().get("signals", [])
                    return [ReceivedSignal.model_validate(row) for row in rows]
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
            if self._on_error is not None:
                self._on_error(
                    MultiEdgeError(
                        f"catch-up query attempt {attempts} failed: {last_error}"
                        + ("" if delay is None else f"; retrying in {delay:.1f}s")
                    )
                )
            if delay is None:
                break
            self._sleep_interruptible(delay)
        if self._stop_event.is_set():
            return []
        raise MultiEdgeError(f"catch-up query failed after {attempts} attempt(s): {last_error}")

    def _sleep_interruptible(self, delay: float) -> None:
        """Sleep ``delay`` seconds in slices, returning early once ``stop()`` is set.

        The retry loop may be unbounded, so it must not stay deaf to ``stop()`` for a
        whole backoff. Slicing (rather than ``Event.wait``) keeps the injected
        ``sleep`` seam — and therefore the tests — in charge of the actual waiting.
        """
        remaining = delay
        while remaining > 0.0 and not self._stop_event.is_set():
            slice_seconds = min(remaining, _STOP_CHECK_INTERVAL_SECONDS)
            self._sleep(slice_seconds)
            remaining -= slice_seconds

    def _deliver(
        self, received: ReceivedSignal, source: Literal["catchup", "live", "gapfill"]
    ) -> None:
        """Invoke the callback, then commit the cursor (at-least-once ordering)."""
        meta = SignalMeta(
            sequence=received.sequence,
            signal_id=received.signal_id,
            published_at=received.published_at,
            source=source,
        )
        try:
            if self._unsealer is not None:
                received = self._unsealer.unseal_signal(received)
            self.on_signal(received, meta)
        except Exception as exc:
            # Never silent loss: the cursor is NOT committed, so this signal is
            # redelivered on restart. Surface via on_error, then propagate.
            # Unseal failures take the same path — ciphertext that cannot be
            # verified is never delivered and never silently skipped.
            if self._on_error is not None:
                self._on_error(exc)
            raise
        self._position = received.sequence
        self._cursor_store.commit(self.strategy_id, received.sequence)

    # ------------------------------------------------------------------ phase 2: poll
    def _run_poll(self) -> None:
        """Live phase for the poll transport: repeat catch-up every poll_interval."""
        while not self._stop_event.wait(self._poll_interval):
            try:
                self._catch_up("live")
            except _FATAL_TYPES:
                raise
            except (httpx.HTTPError, MultiEdgeError) as exc:
                # Transient relay/network trouble: notify and keep polling — the
                # next cycle re-queries from the committed cursor, losing nothing.
                if self._on_error is not None:
                    self._on_error(exc)

    # ------------------------------------------------------------------ phase 2: live push
    def _handle_live_message(self, received: ReceivedSignal) -> None:
        """Process one live-transport message: dedupe, park on gap, fill, drain.

        Contract:
            * ``sequence <= position`` -> duplicate of already-delivered data: dropped.
            * ``sequence == position + 1`` -> delivered immediately (``source="live"``).
            * Larger jump -> parked; the hole is filled from REST
              (``source="gapfill"``), then parked messages drain in order.

        Raises:
            BufferFullError: The reorder buffer exceeded ``max_buffer``.
            GapUnrecoverableError: REST could not supply the missing range.
        """
        with self._delivery_lock:
            if not self._started:
                raise MultiEdgeError("subscriber not started — run() or catch_up_only() first")
            if received.sequence <= self._position:
                return
            self._buffer[received.sequence] = received
            if len(self._buffer) > self._max_buffer:
                raise BufferFullError(f"live reorder buffer exceeded {self._max_buffer} messages")
            self._drain_with_fill()

    def _drain_with_fill(self) -> None:
        """Drain the reorder buffer in order, REST-filling any leading gap."""
        while self._buffer and not self._stop_event.is_set():
            next_sequence = self._position + 1
            if next_sequence in self._buffer:
                self._deliver(self._buffer.pop(next_sequence), "live")
                continue
            self._fill_from_rest(target=min(self._buffer) - 1)

    def _fill_from_rest(self, target: int) -> None:
        """Deliver sequences ``position+1 .. target`` from REST (``source="gapfill"``).

        REST is the ordering truth: whatever it returns inside the range is
        delivered in order; sequences REST itself does not have are treated as
        relay-side holes and passed over. If REST yields nothing new for
        ``gap_fill_rounds`` consecutive rounds while the gap remains, the gap is
        unrecoverable and delivery refuses to jump over it.

        Raises:
            GapUnrecoverableError: The relay could not supply the missing range.
        """
        empty_rounds = 0
        while self._position < target and not self._stop_event.is_set():
            page = self._fetch_page(self._position)
            fresh = [r for r in page if self._position < r.sequence <= target]
            if not fresh:
                empty_rounds += 1
                if empty_rounds >= self._gap_fill_rounds:
                    raise GapUnrecoverableError(
                        f"cannot fill gap {self._position + 1}..{target} for strategy "
                        f"{self.strategy_id!r}: relay returned no data after "
                        f"{empty_rounds} attempts"
                    )
                self._sleep(backoff_delay(empty_rounds - 1, self._random_fn))
                continue
            empty_rounds = 0
            for received in fresh:
                self._buffer.pop(received.sequence, None)  # REST copy wins — dedupe
                self._deliver(received, "gapfill")
            if len(page) < self._page_size and self._position < target:
                # REST exhausted below the buffered message: relay-side hole
                # between position and target — nothing more will come on REST.
                raise GapUnrecoverableError(
                    f"relay log ends at sequence {self._position} but live transport "
                    f"delivered sequence {target + 1} for strategy {self.strategy_id!r}"
                )

    def _require_webpubsub(self) -> None:
        """Fail fast with a clear message when the ``[webpubsub]`` extra is missing."""
        try:
            import azure.messaging.webpubsubclient  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                'live_transport="webpubsub" requires the optional dependency: '
                'install with  pip install "multiedge-relay[webpubsub]"  '
                '(or use live_transport="poll", which needs no extras)'
            ) from exc

    def _run_webpubsub(self) -> None:
        """Live phase over Azure Web PubSub with jittered reconnect.

        Flow per connection: negotiate a client URL over REST, connect, subscribe,
        re-run REST catch-up (closing the connect-window gap), then park until
        ``stop()``. Any connection failure notifies ``on_error`` and reconnects
        with jittered backoff; fatal SDK errors and callback errors propagate.
        """
        from azure.messaging.webpubsubclient import WebPubSubClient

        reconnect_round = 0
        while not self._stop_event.is_set():
            try:
                response = self._client.post(
                    "/v1/ws/negotiate", json={"strategy_id": self.strategy_id}
                )
                if response.status_code in (401, 403):
                    raise AuthError(f"relay rejected the API key (HTTP {response.status_code})")
                response.raise_for_status()
                url = response.json()["url"]
                client = WebPubSubClient(url)
                with client:
                    client.subscribe("group-message", self._on_ws_event)
                    reconnect_round = 0
                    # Close the gap between catch-up and the connect instant.
                    self._catch_up("catchup")
                    while not self._stop_event.wait(1.0):
                        if self._pending_error is not None:
                            error = self._pending_error
                            self._pending_error = None
                            raise error
            except _FATAL_TYPES:
                raise
            except Exception as exc:  # third-party client errors have no stable type
                if self._on_error is not None:
                    self._on_error(exc)
                reconnect_round += 1
                self._sleep(backoff_delay(min(reconnect_round, 6), self._random_fn))

    def _on_ws_event(self, event: Any) -> None:
        """Web PubSub message handler: parse and hand to the gap-aware pipeline.

        Runs on the Azure client's thread; errors are stashed for the main loop to
        re-raise (the third-party callback runner would otherwise swallow them).
        """
        try:
            data = event.data
            if isinstance(data, (str, bytes)):
                received = ReceivedSignal.model_validate_json(data)
            else:
                received = ReceivedSignal.model_validate(data)
            self._handle_live_message(received)
        except Exception as exc:
            self._pending_error = exc

    # ------------------------------------------------------------------ cleanup
    def close(self) -> None:
        """Close the underlying HTTP client (after ``run`` has returned)."""
        self._client.close()
