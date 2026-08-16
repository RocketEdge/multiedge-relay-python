"""Dependency-free ULID generator (Crockford base32, monotonic within a millisecond).

Purpose:
    Generates the ``client_signal_id`` idempotency keys the publisher attaches to
    every signal. ULIDs are lexicographically sortable by creation time, which keeps
    DLQ files and relay-side dedupe indexes naturally ordered.

Contract:
    * 26 characters, Crockford base32 alphabet (no I, L, O, U).
    * First 10 chars encode unix milliseconds (48 bits); last 16 encode 80 random bits.
    * Within the same millisecond the random component is incremented, so successive
      calls are strictly increasing (monotonic) even under bursts.
    * Thread-safe via a module lock.
"""

from __future__ import annotations

import secrets
import threading
import time

CROCKFORD_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

_lock = threading.Lock()
_last_ms = -1
_last_random = 0

_MAX_RANDOM = (1 << 80) - 1


def _encode(value: int, length: int) -> str:
    """Encode ``value`` as fixed-width Crockford base32 (most significant first)."""
    chars = []
    for _ in range(length):
        chars.append(CROCKFORD_ALPHABET[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def new_ulid() -> str:
    """Return a new 26-character ULID, monotonic within the same millisecond.

    Returns:
        A 26-character Crockford base32 string; later calls sort after earlier ones.

    Raises:
        OverflowError: In the astronomically unlikely event the 80-bit random
            component overflows within a single millisecond.
    """
    global _last_ms, _last_random
    with _lock:
        now_ms = time.time_ns() // 1_000_000
        if now_ms == _last_ms:
            if _last_random >= _MAX_RANDOM:
                raise OverflowError("ULID random component overflow within one millisecond")
            _last_random += 1
        else:
            _last_ms = now_ms
            # keep headroom so same-ms increments cannot overflow in practice
            _last_random = secrets.randbits(80) & ~(0xFFFF)
        return _encode(now_ms, 10) + _encode(_last_random, 16)
