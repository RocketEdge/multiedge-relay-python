"""ULID generator tests: format, timestamp encoding, monotonicity."""

from __future__ import annotations

import time

from multiedge_relay.ulid import CROCKFORD_ALPHABET, new_ulid


def test_ulid_shape() -> None:
    value = new_ulid()
    assert len(value) == 26
    assert all(c in CROCKFORD_ALPHABET for c in value)


def test_ulid_timestamp_component_close_to_now() -> None:
    before_ms = int(time.time() * 1000)
    value = new_ulid()
    after_ms = int(time.time() * 1000)
    encoded_ms = 0
    for char in value[:10]:
        encoded_ms = encoded_ms * 32 + CROCKFORD_ALPHABET.index(char)
    assert before_ms <= encoded_ms <= after_ms


def test_ulid_monotonic_within_burst() -> None:
    values = [new_ulid() for _ in range(2000)]
    assert values == sorted(values)
    assert len(set(values)) == len(values)
