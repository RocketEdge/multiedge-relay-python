"""Exception hierarchy for the MultiEdge Signal Relay SDK.

Purpose:
    Every failure mode is explicit — the SDK never swallows an error or silently
    drops a signal. Callers can catch ``MultiEdgeError`` for everything SDK-raised,
    or the specific subclass for targeted handling.

Contract:
    * ``AuthError`` and ``ValidationRejected`` are terminal: the SDK never retries them.
    * ``PublishFailed`` is raised only after the retry budget is exhausted (or a
      non-retryable, non-auth status is seen) and carries the DLQ spill path when a
      DLQ is configured.
    * ``CursorCorruptError`` is raised instead of ever silently resetting a cursor.
    * ``GapUnrecoverableError`` is raised when a live-transport gap cannot be filled
      from REST — data is never silently skipped.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import cycle guard, typing only
    from .models import Signal


class MultiEdgeError(Exception):
    """Base class for all errors raised by the multiedge-relay SDK."""


class AuthError(MultiEdgeError):
    """The relay rejected the API key (HTTP 401/403). Never retried.

    Fix the credential; retrying an invalid key cannot succeed.
    """


class ValidationRejected(MultiEdgeError):
    """The relay rejected the signal as invalid (HTTP 422) or too large (HTTP 413).

    Never retried: the same bytes would be rejected again. Fix the signal.
    """


class PublishFailed(MultiEdgeError):
    """A publish did not succeed after all retry attempts.

    Attributes:
        signal: The signal that failed to publish (with its ``client_signal_id`` set,
            so a later resend is deduplicated by the relay).
        attempts: Number of HTTP attempts made before giving up.
        dlq_path: Path of the DLQ file the signal was appended to, or ``None`` when
            no DLQ is configured (the caller then owns the signal's fate).
    """

    def __init__(self, signal: Signal, attempts: int, dlq_path: Path | None) -> None:
        self.signal = signal
        self.attempts = attempts
        self.dlq_path = dlq_path
        location = f"spilled to DLQ at {dlq_path}" if dlq_path else "no DLQ configured"
        super().__init__(
            f"publish failed for strategy {signal.strategy_id!r} after "
            f"{attempts} attempt(s); {location}"
        )


class BufferFullError(MultiEdgeError):
    """The live-transport reorder buffer exceeded its bound.

    Raised instead of dropping parked messages; indicates a pathological gap or a
    stalled REST back-fill.
    """


class GapUnrecoverableError(MultiEdgeError):
    """A live-delivery gap could not be filled from the REST log.

    The subscriber refuses to deliver past a hole it cannot fill — delivering would
    silently reorder or lose signals. Restart the subscriber (it will re-run REST
    catch-up) or contact support if the gap persists.
    """


class SignatureVerificationError(MultiEdgeError):
    """A webhook request failed HMAC verification.

    The message states the reason (missing header, malformed signature, stale
    timestamp, or digest mismatch). Treat the request as untrusted.
    """


class CursorCorruptError(MultiEdgeError):
    """A cursor file exists but cannot be parsed as a valid cursor.

    Never auto-reset: a silent reset would replay the whole history into the
    callback. Inspect the file, then fix it explicitly with
    ``multiedge cursor reset --strategy X --to N``.
    """
