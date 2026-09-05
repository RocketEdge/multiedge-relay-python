"""Official Python SDK for MultiEdge Signal Relay.

Auditable signal-distribution infrastructure, not execution. See README.md for the
full contract: never-silent-loss publishing, at-least-once cursor-based subscription,
and HMAC-verified webhooks.
"""

__version__ = "0.8.0"

from .cursor import CursorStore, FileCursorStore
from .dlq import DiskDLQ, DLQEntry, DLQResendReport
from .exceptions import (
    AuthError,
    BufferFullError,
    CursorCorruptError,
    GapUnrecoverableError,
    IdempotencyConflict,
    MultiEdgeError,
    PublishFailed,
    SignatureVerificationError,
    StateStoreCorruptError,
    ValidationRejected,
)
from .models import ReceivedSignal, Signal, SignalAck, SignalMeta
from .publisher import SignalPublisher
from .publisher_async import AsyncSignalPublisher
from .state_sqlite import SqliteStateStore
from .subscriber import SignalSubscriber
from .ulid import new_ulid
from .webhook import verify_signature, verify_ws_frame

__all__ = [
    "AsyncSignalPublisher",
    "AuthError",
    "BufferFullError",
    "CursorCorruptError",
    "CursorStore",
    "DLQEntry",
    "DLQResendReport",
    "DiskDLQ",
    "FileCursorStore",
    "GapUnrecoverableError",
    "IdempotencyConflict",
    "MultiEdgeError",
    "PublishFailed",
    "ReceivedSignal",
    "Signal",
    "SignalAck",
    "SignalMeta",
    "SignalPublisher",
    "SignalSubscriber",
    "SignatureVerificationError",
    "SqliteStateStore",
    "StateStoreCorruptError",
    "ValidationRejected",
    "__version__",
    "new_ulid",
    "verify_signature",
    "verify_ws_frame",
]
