"""Disk dead-letter queue: signals that exhausted publish retries are never lost.

Purpose:
    When a publish exhausts its retry budget, the publisher appends the signal (with
    its idempotency key, the error, and the attempt count) to a JSONL file under the
    DLQ root — one file per strategy per day. ``pending`` enumerates entries;
    ``resend`` replays them through a publisher and removes the successes.

Contract:
    * Append-only files; ``resend`` rewrites a file only to drop successfully
      resent entries (failures are kept exactly once — no duplicate entries).
    * Because every DLQ'd signal carries its original ``client_signal_id``, a resend
      that races an earlier half-delivered publish is deduplicated by the relay.
    * File layout: ``<root>/<sanitized_strategy_id>/<YYYY-MM-DD>.jsonl``.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from .exceptions import PublishFailed
from .models import Signal
from .ulid import new_ulid

if TYPE_CHECKING:  # pragma: no cover - typing-only import cycle guard
    from .publisher import SignalPublisher

_SAFE_COMPONENT = re.compile(r"[^A-Za-z0-9._-]")


def _sanitize(strategy_id: str) -> str:
    """Map a strategy id to a filesystem-safe single path component."""
    safe = _SAFE_COMPONENT.sub("_", strategy_id)
    return safe or "_"


@dataclass(frozen=True)
class DLQEntry:
    """One dead-lettered signal.

    Attributes:
        entry_id: Unique ULID for this DLQ entry.
        signal: The failed signal, exactly as attempted (idempotency key included).
        error: Human-readable description of the final failure.
        attempts: HTTP attempts made before the signal was dead-lettered.
        failed_at: When the entry was written (UTC).
        path: The JSONL file holding this entry.
    """

    entry_id: str
    signal: Signal
    error: str
    attempts: int
    failed_at: datetime
    path: Path


@dataclass(frozen=True)
class DLQResendReport:
    """Outcome of a ``resend`` pass.

    Attributes:
        attempted: Entries considered (all pending, or all pending for a strategy).
        resent: Entries successfully re-published and removed from the DLQ.
        failed: Entries that failed again and remain in the DLQ.
        dry_run: ``True`` when nothing was actually sent or modified.
    """

    attempted: int
    resent: int
    failed: int
    dry_run: bool = False


class DiskDLQ:
    """JSONL-on-disk dead-letter queue, one file per strategy per day."""

    def __init__(self, root: Path | None = None) -> None:
        """Create a DLQ rooted at ``root`` (default ``~/.multiedge/dlq``).

        The root directory is created lazily on first append.
        """
        self.root = root if root is not None else Path.home() / ".multiedge" / "dlq"

    def append(self, signal: Signal, error: str, attempts: int) -> Path:
        """Append a failed signal to the DLQ and return the file it landed in.

        Args:
            signal: The signal that failed to publish.
            error: Final error description (kept for the operator, not re-parsed).
            attempts: Number of HTTP attempts that were made.

        Returns:
            Path of the JSONL file the entry was appended to.
        """
        day = datetime.now(UTC).strftime("%Y-%m-%d")
        path = self.root / _sanitize(signal.strategy_id) / f"{day}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "entry_id": new_ulid(),
            "signal": signal.model_dump(mode="json"),
            "error": error,
            "attempts": attempts,
            "failed_at": datetime.now(UTC).isoformat(),
        }
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")
        return path

    def pending(self, strategy_id: str | None = None) -> Iterator[DLQEntry]:
        """Yield pending entries, oldest file first.

        Args:
            strategy_id: Restrict to one strategy; ``None`` yields all strategies.

        Yields:
            ``DLQEntry`` for every line in every matching JSONL file.
        """
        if not self.root.is_dir():
            return
        if strategy_id is not None:
            dirs = [self.root / _sanitize(strategy_id)]
        else:
            dirs = sorted(p for p in self.root.iterdir() if p.is_dir())
        for strategy_dir in dirs:
            if not strategy_dir.is_dir():
                continue
            for path in sorted(strategy_dir.glob("*.jsonl")):
                yield from self._read_file(path)

    def _read_file(self, path: Path) -> Iterator[DLQEntry]:
        """Parse one JSONL file into entries (skipping nothing — bad lines raise)."""
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            yield DLQEntry(
                entry_id=record["entry_id"],
                signal=Signal.model_validate(record["signal"]),
                error=record["error"],
                attempts=int(record["attempts"]),
                failed_at=datetime.fromisoformat(record["failed_at"]),
                path=path,
            )

    def resend(self, publisher: SignalPublisher, *, dry_run: bool = False) -> DLQResendReport:
        """Re-publish pending entries; drop the successes, keep the failures.

        The publisher's own DLQ is suspended for the duration so a failing resend
        does not write a duplicate entry — the original entry simply remains.

        Args:
            publisher: A configured ``SignalPublisher`` to send through.
            dry_run: When ``True``, only count what would be resent.

        Returns:
            A ``DLQResendReport`` with attempted/resent/failed counts.
        """
        entries_by_path: dict[Path, list[DLQEntry]] = {}
        for entry in self.pending():
            entries_by_path.setdefault(entry.path, []).append(entry)
        attempted = sum(len(v) for v in entries_by_path.values())
        if dry_run:
            return DLQResendReport(attempted=attempted, resent=0, failed=0, dry_run=True)

        resent = 0
        failed = 0
        original_dlq = publisher.dlq
        publisher.dlq = None  # a failing resend must not duplicate the entry
        try:
            for path, entries in entries_by_path.items():
                survivors: list[DLQEntry] = []
                for entry in entries:
                    try:
                        publisher.publish(entry.signal)
                    except PublishFailed:
                        failed += 1
                        survivors.append(entry)
                    else:
                        resent += 1
                self._rewrite(path, survivors)
        finally:
            publisher.dlq = original_dlq
        return DLQResendReport(attempted=attempted, resent=resent, failed=failed)

    def _rewrite(self, path: Path, survivors: list[DLQEntry]) -> None:
        """Atomically replace ``path`` with only the surviving entries (or remove it)."""
        if not survivors:
            path.unlink(missing_ok=True)
            return
        tmp = path.with_suffix(".jsonl.tmp")
        lines = [
            json.dumps(
                {
                    "entry_id": e.entry_id,
                    "signal": e.signal.model_dump(mode="json"),
                    "error": e.error,
                    "attempts": e.attempts,
                    "failed_at": e.failed_at.isoformat(),
                },
                separators=(",", ":"),
            )
            for e in survivors
        ]
        tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
        tmp.replace(path)

    def purge(self, strategy_id: str | None = None) -> int:
        """Delete pending entries (explicit, operator-initiated data loss).

        Args:
            strategy_id: Restrict to one strategy; ``None`` purges everything.

        Returns:
            Number of entries removed.
        """
        removed = 0
        paths: set[Path] = set()
        for entry in self.pending(strategy_id):
            removed += 1
            paths.add(entry.path)
        for path in paths:
            path.unlink(missing_ok=True)
        return removed
