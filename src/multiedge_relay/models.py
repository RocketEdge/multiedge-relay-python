"""Frozen pydantic v2 models for the MultiEdge Signal Relay wire contract.

Purpose:
    These models are the SDK's public data surface. They are immutable (``frozen``)
    so a signal cannot be mutated between publish attempts, and they round-trip
    losslessly through JSON (``model_dump_json`` / ``model_validate_json``).

Contract:
    * ``Signal`` is what a publisher sends; ``client_signal_id`` is the idempotency
      key (auto-assigned a ULID by the publisher when absent).
    * ``SignalAck`` is the relay's acceptance receipt; ``sequence`` is the strategy's
      monotonically increasing log position — the ordering truth for subscribers.
    * ``ReceivedSignal`` is what subscribers and webhook receivers consume.
    * ``SignalMeta`` tells the callback which delivery path produced the signal.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class Signal(BaseModel):
    """A signal to publish to the relay.

    Attributes:
        strategy_id: The strategy stream this signal belongs to.
        payload: Arbitrary JSON-serializable signal content. The relay carries it
            opaquely; it never interprets or executes it.
        client_signal_id: Publisher-side idempotency key. When ``None``, the
            publisher assigns a ULID before sending, so retries and DLQ resends are
            deduplicated by the relay.
        published_at: Optional publisher-side timestamp (UTC). The relay stamps its
            own authoritative ``accepted_at`` regardless.
        expires_at: Optional advisory expiry; subscribers may drop stale signals.
        correlation_id: Optional caller-supplied tracing identifier.
    """

    model_config = ConfigDict(frozen=True)

    strategy_id: str
    payload: dict[str, Any]
    client_signal_id: str | None = None
    published_at: datetime | None = None
    expires_at: datetime | None = None
    correlation_id: str | None = None


class SignalAck(BaseModel):
    """The relay's acceptance receipt for a published signal.

    Attributes:
        signal_id: Relay-assigned globally unique signal identifier.
        client_signal_id: Echo of the idempotency key the publish carried.
        sequence: Position in the strategy's sequenced log (ordering truth).
        accepted_at: Relay-side acceptance timestamp (UTC).
        deduplicated: ``True`` when the relay had already accepted a signal with the
            same ``client_signal_id`` and returned the original ack (HTTP 200
            instead of 201) — the retry was safe and nothing was double-published.
    """

    model_config = ConfigDict(frozen=True)

    signal_id: str
    client_signal_id: str
    sequence: int
    accepted_at: datetime
    deduplicated: bool = False


class ReceivedSignal(BaseModel):
    """A signal as delivered to a subscriber or webhook receiver.

    Attributes:
        sequence: Position in the strategy's sequenced log; strictly increasing per
            strategy. Use it (or ``signal_id``) to key idempotent side effects.
        signal_id: Relay-assigned globally unique signal identifier.
        strategy_id: The strategy stream the signal belongs to.
        published_at: Relay-side acceptance timestamp (UTC).
        payload: The publisher's opaque signal content.
    """

    model_config = ConfigDict(frozen=True)

    sequence: int
    signal_id: str
    strategy_id: str
    published_at: datetime
    payload: dict[str, Any]


class SignalMeta(BaseModel):
    """Delivery metadata passed to the subscriber callback alongside each signal.

    Attributes:
        sequence: Same as ``ReceivedSignal.sequence`` (convenience).
        signal_id: Same as ``ReceivedSignal.signal_id`` (convenience).
        published_at: Same as ``ReceivedSignal.published_at`` (convenience).
        source: Which delivery path produced this delivery — ``"catchup"`` (REST
            backlog replay), ``"live"`` (live transport), or ``"gapfill"`` (REST
            back-fill of a live-transport gap).
    """

    model_config = ConfigDict(frozen=True)

    sequence: int
    signal_id: str
    published_at: datetime
    source: Literal["catchup", "live", "gapfill"]
