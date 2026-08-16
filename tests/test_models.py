"""Model contract tests: frozen pydantic v2 models + hypothesis round-trip."""

from __future__ import annotations

from datetime import UTC, datetime

import pydantic
import pytest
from hypothesis import given
from hypothesis import strategies as st

from multiedge_relay import ReceivedSignal, Signal, SignalAck, SignalMeta


def test_signal_minimal_defaults() -> None:
    sig = Signal(strategy_id="s1", payload={"a": 1})
    assert sig.strategy_id == "s1"
    assert sig.payload == {"a": 1}
    assert sig.client_signal_id is None
    assert sig.published_at is None
    assert sig.expires_at is None
    assert sig.correlation_id is None


def test_signal_is_frozen() -> None:
    sig = Signal(strategy_id="s1", payload={})
    with pytest.raises(pydantic.ValidationError):
        sig.strategy_id = "other"  # type: ignore[misc]


def test_signal_ack_defaults() -> None:
    ack = SignalAck(
        signal_id="sig_1",
        client_signal_id="c1",
        sequence=7,
        accepted_at=datetime.now(UTC),
    )
    assert ack.deduplicated is False


def test_received_signal_fields() -> None:
    rs = ReceivedSignal(
        sequence=3,
        signal_id="sig_3",
        strategy_id="s1",
        published_at=datetime.now(UTC),
        payload={"x": "y"},
    )
    assert rs.sequence == 3
    assert rs.payload == {"x": "y"}


def test_signal_meta_source_literal() -> None:
    meta = SignalMeta(sequence=1, signal_id="sig", published_at=datetime.now(UTC), source="catchup")
    assert meta.source == "catchup"
    with pytest.raises(pydantic.ValidationError):
        SignalMeta(sequence=1, signal_id="sig", published_at=datetime.now(UTC), source="bogus")


json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**53), max_value=2**53),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=30),
)


@given(
    strategy_id=st.text(min_size=1, max_size=40),
    payload=st.dictionaries(st.text(max_size=20), json_scalars, max_size=8),
    client_signal_id=st.none() | st.text(min_size=1, max_size=26),
)
def test_signal_json_round_trip(
    strategy_id: str, payload: dict[str, object], client_signal_id: str | None
) -> None:
    sig = Signal(
        strategy_id=strategy_id,
        payload=payload,
        client_signal_id=client_signal_id,
        published_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    )
    restored = Signal.model_validate_json(sig.model_dump_json())
    assert restored == sig
