"""Cursor persistence: the subscriber's durable "last processed sequence" per strategy.

Purpose:
    The cursor is what makes "offline for a weekend — you miss nothing" true: on
    restart the subscriber resumes REST catch-up from the persisted sequence. It is
    committed only AFTER the user callback returns, giving at-least-once delivery.

Contract:
    * ``commit`` is atomic: write to a temp file in the same directory, then
      ``os.replace`` — a crash mid-commit leaves the previous cursor intact.
    * ``load`` of a missing file returns ``None`` (fresh subscriber).
    * A corrupt file raises ``CursorCorruptError`` and is left untouched — the store
      NEVER silently resets (that would replay all history into the callback).
"""

from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from .exceptions import CursorCorruptError
from .ulid import new_ulid

_SAFE_COMPONENT = re.compile(r"[^A-Za-z0-9._-]")


class CursorStore(Protocol):
    """Persistence contract for subscriber cursors.

    Implementations must make ``commit`` durable and ``load`` reflect the last
    successful commit; they must raise rather than guess on corrupt state.
    """

    def load(self, strategy_id: str) -> int | None:
        """Return the last committed sequence for ``strategy_id``, or ``None``."""
        ...

    def commit(self, strategy_id: str, sequence: int) -> None:
        """Durably record ``sequence`` as processed for ``strategy_id``."""
        ...


REPLACE_ATTEMPTS = 5
REPLACE_BACKOFF_SECONDS = 0.02


class FileCursorStore:
    """JSON-file-per-strategy cursor store under a root directory."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Create a store rooted at ``root`` (default ``~/.multiedge/cursor``).

        Args:
            root: Directory holding one ``<strategy>.json`` per strategy.
            sleep: Injectable sleep used by the Windows sharing-violation retry in
                :meth:`commit` (test seam).
        """
        self.root = root if root is not None else Path.home() / ".multiedge" / "cursor"
        self._sleep = sleep

    def path_for(self, strategy_id: str) -> Path:
        """Return the cursor file path for ``strategy_id`` (sanitized filename)."""
        safe = _SAFE_COMPONENT.sub("_", strategy_id) or "_"
        return self.root / f"{safe}.json"

    def load(self, strategy_id: str) -> int | None:
        """Load the last committed sequence.

        Returns:
            The committed sequence, or ``None`` when no cursor file exists yet.

        Raises:
            CursorCorruptError: The file exists but is not a valid cursor. The file
                is left exactly as found — never silently reset.
        """
        path = self.path_for(strategy_id)
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CursorCorruptError(f"cursor file {path} is not valid JSON: {exc}") from exc
        sequence = data.get("sequence") if isinstance(data, dict) else None
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
            raise CursorCorruptError(
                f"cursor file {path} has no valid non-negative integer 'sequence' field"
            )
        return sequence

    def commit(self, strategy_id: str, sequence: int) -> None:
        """Atomically persist ``sequence`` (temp file + ``os.replace``).

        On Windows, ``os.replace`` raises ``PermissionError`` while a concurrent
        reader briefly holds the destination open (sharing violation). That is
        transient, so it is retried up to ``REPLACE_ATTEMPTS`` times with
        ``REPLACE_BACKOFF_SECONDS`` between attempts. Any other ``OSError`` — and
        ``PermissionError`` persisting past the budget — propagates.

        Raises:
            OSError: The underlying write or replace failed (after retries for
                ``PermissionError``); the previous cursor file (if any) is
                untouched.
        """
        path = self.path_for(strategy_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {
                "strategy_id": strategy_id,
                "sequence": sequence,
                "updated_at": datetime.now(UTC).isoformat(),
            }
        )
        tmp = path.parent / f".{path.name}.{new_ulid()}.tmp"
        try:
            tmp.write_text(payload, encoding="utf-8")
            for attempt in range(1, REPLACE_ATTEMPTS + 1):
                try:
                    os.replace(tmp, path)
                    break
                except PermissionError:
                    # Transient Windows sharing violation from a concurrent reader.
                    if attempt == REPLACE_ATTEMPTS:
                        raise
                    self._sleep(REPLACE_BACKOFF_SECONDS)
        finally:
            tmp.unlink(missing_ok=True)
