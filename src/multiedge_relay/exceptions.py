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


class SealedError(MultiEdgeError):
    """Base class for sealed-mode (end-to-end encryption) failures.

    Sealed mode is provided by the optional ``multiedge-relay[sealed]`` extra;
    the exception taxonomy lives in core (stdlib-only) so callers can catch it
    without importing the crypto subpackage.
    """


class UnsealError(SealedError):
    """A sealed envelope failed verification or decryption.

    Raised for signature failures, AEAD authentication failures (tampering or
    strategy/identity substitution), malformed envelopes, and algorithm
    downgrade attempts. Treat the envelope as untrusted; never deliver its
    contents.
    """


class NotARecipientError(UnsealError):
    """None of the envelope's recipient entries match this keypair.

    Most common cause: the signal was sealed before this subscriber's key was
    registered and entitled — sealed signals published earlier can never be
    decrypted by later-entitled subscribers (there is no re-encryption of
    history). Verify the key registration otherwise.
    """


class SealedKeyError(SealedError):
    """A key bundle is malformed, or its fingerprint does not match its ``key_id``.

    The relay is untrusted for key authenticity: the SDK recomputes every
    bundle fingerprint locally and refuses any mismatch.
    """


class KeyPinningError(SealedError):
    """The fetched key set does not match the pinned fingerprints.

    The message lists the unexpected and missing fingerprints. Do not publish:
    a mismatch may mean an entitlement changed — or that the relay substituted
    keys. Confirm fingerprints out-of-band, then update the pin set.
    """


class StateStoreCorruptError(CursorCorruptError):
    """The SQLite state file exists but is not a valid exactly-once state store.

    Raised when the file is not a SQLite database or carries an unknown schema
    version (e.g. written by a newer SDK). Never auto-reset: silently recreating
    the database would forget which signals were processed and replay history
    into the handler. Inspect or move the file, then restart.

    Subclasses ``CursorCorruptError`` so the subscriber treats it as fatal
    (it is in the subscriber's non-retryable exception set) without any change
    to the subscriber itself.
    """
