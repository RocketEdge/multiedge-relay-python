"""Exactly-once processing state: cursor + processed-signal ledger in one SQLite file.

Purpose:
    The relay delivers at-least-once (reconnect overlap, webhook retry ladders,
    operator replay). :class:`SqliteStateStore` upgrades *processing* to
    effectively-once: one local SQLite file records which signals the handler has
    completed, so redeliveries never re-invoke it. It is simultaneously a
    :class:`~multiedge_relay.cursor.CursorStore` (drop it into
    ``SignalSubscriber(cursor_store=...)``) and a dedup ledger usable from webhook
    receivers — one file covers every delivery channel, keyed on the globally
    unique relay ``signal_id``.

Contract (the exactly-once invariant):
    * A signal S counts as **processed** iff ``S.sequence <= cursor[S.strategy_id]``
      OR ``S.signal_id`` is in the ``processed`` table. The cursor row is the
      compressed form of "everything at or below N is processed", which makes
      watermark-pruning of markers sound even when subscriber and webhook usage
      share one file.
    * The handler only ever runs inside a transaction that atomically records the
      marker on success — it can never *complete* twice for one signal, no matter
      how often the relay delivers it. Handler failure rolls the marker (and any
      handler writes made through the provided transaction cursor) back, so the
      signal is retried on redelivery.
    * The subscriber's cursor commit is a separate, later write. Losing it (crash
      in the gap after the callback) only causes a redelivery, which the marker
      absorbs without re-invoking the handler — the case a file-based cursor alone
      cannot handle.
    * Honest limit: side effects written through the transaction cursor
      (:meth:`exactly_once_tx`) are truly exactly-once; *external* side effects
      (orders, emails) retain a tiny at-least-once window — a crash after the
      handler returns but before COMMIT re-runs the handler on redelivery.
    * Corrupt or unrecognized state files raise
      :class:`~multiedge_relay.exceptions.StateStoreCorruptError` and are left
      untouched — never silently reset (that would replay history).

Concurrency & durability:
    Single-process use only (a second process hits SQLITE_BUSY after the busy
    timeout). Within the process it is thread-safe: one connection guarded by an
    internal re-entrant lock (the subscriber may deliver from a Web PubSub event
    thread; webhook servers use worker threads). WAL journal with
    ``synchronous=FULL`` — a lost marker would mean a double-invocation, so the
    marker fsync is the product; signal rates make the cost irrelevant.
    ``auto_vacuum=INCREMENTAL`` plus a post-delete incremental-vacuum DRAIN
    returns freed pages to the OS, so the file never balloons past its live
    content (see :meth:`SqliteStateStore._vacuum_freed_pages`).
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType

from .exceptions import StateStoreCorruptError
from .models import ReceivedSignal, SignalMeta

SCHEMA_VERSION = 1
DEFAULT_RETENTION_DAYS = 90
"""Default marker retention. Matches the relay's replay/retention window: a
delivery older than this cannot recur through any path (retry, replay, resume),
so pruning its marker can never cause a duplicate invocation."""

BUSY_TIMEOUT_MS = 5000

Handler = Callable[[ReceivedSignal, SignalMeta], None]
TxHandler = Callable[[ReceivedSignal, SignalMeta, sqlite3.Cursor], None]

# Executed one statement at a time inside an explicit transaction —
# executescript() would implicitly COMMIT the open transaction first.
_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS cursor (
        strategy_id TEXT PRIMARY KEY,
        sequence    INTEGER NOT NULL CHECK (sequence >= 0),
        updated_at  TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS processed (
        signal_id    TEXT PRIMARY KEY,
        strategy_id  TEXT NOT NULL,
        sequence     INTEGER NOT NULL,
        processed_at TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS processed_strategy_sequence
        ON processed(strategy_id, sequence)
    """,
)


def _utc_now() -> datetime:
    """Return the current UTC time (default ``clock`` implementation)."""
    return datetime.now(UTC)


class SqliteStateStore:
    """One SQLite file giving exactly-once *processing* on top of at-least-once delivery.

    Implements the ``CursorStore`` protocol (:meth:`load` / :meth:`commit`) so it
    drops into ``SignalSubscriber(cursor_store=...)``, and keeps a ``processed``
    ledger keyed on the relay ``signal_id`` consulted by :meth:`exactly_once`,
    :meth:`exactly_once_tx`, :meth:`process`, and :meth:`seen`. See the module
    docstring for the invariant and its honest limits.
    """

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        retention_days: int = DEFAULT_RETENTION_DAYS,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        """Open (creating if needed) the state database at ``path``.

        Args:
            path: Database file location; parent directories are created. Defaults
                to ``~/.multiedge/state.db``. WAL side files (``-wal``/``-shm``)
                appear next to it while the store is open.
            retention_days: Age-based marker retention used by :meth:`prune` (and
                the opportunistic prune on open). Defaults to the relay's 90-day
                replay window — older deliveries cannot recur, so pruning them is
                always safe.
            clock: Injectable UTC time source (test seam). Must return
                timezone-aware UTC datetimes.

        Raises:
            StateStoreCorruptError: The file exists but is not a SQLite database
                or has an unknown schema version. The file is left untouched.
        """
        self.path = Path(path) if path is not None else Path.home() / ".multiedge" / "state.db"
        self._retention = timedelta(days=retention_days)
        self._clock = clock
        # Re-entrant so a handler running under exactly_once/process may call
        # seen()/prune() on the same store without deadlocking.
        self._lock = threading.RLock()
        self._closed = False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False: delivery may come from a Web PubSub event
        # thread; the lock serializes all access. isolation_level=None gives
        # autocommit with explicit BEGIN/COMMIT (3.11-compatible manual txns).
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        try:
            self._conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
            # First header read: a non-SQLite file fails here, before any write.
            self._conn.execute("PRAGMA schema_version").fetchone()
            # Must be set before the first table is created to take effect.
            self._conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
            self._conn.execute("PRAGMA journal_mode = WAL")
            # FULL, not NORMAL: losing the newest marker commit on power failure
            # would re-invoke the handler — marker durability is the product.
            self._conn.execute("PRAGMA synchronous = FULL")
            row = self._conn.execute("PRAGMA user_version").fetchone()
            version = int(row[0]) if row is not None else 0
            if version == 0:
                self._conn.execute("BEGIN IMMEDIATE")
                for statement in _SCHEMA_STATEMENTS:
                    self._conn.execute(statement)
                self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                self._conn.execute("COMMIT")
            elif version != SCHEMA_VERSION:
                raise StateStoreCorruptError(
                    f"state db {self.path} has unknown schema version {version} "
                    f"(this SDK supports {SCHEMA_VERSION}); refusing to touch it"
                )
        except sqlite3.DatabaseError as exc:
            self._conn.close()
            if isinstance(exc, StateStoreCorruptError):
                raise
            raise StateStoreCorruptError(f"state db {self.path} is not a database: {exc}") from exc
        except StateStoreCorruptError:
            self._conn.close()
            raise
        self.prune()

    # ------------------------------------------------------------- CursorStore protocol
    def load(self, strategy_id: str) -> int | None:
        """Return the last committed sequence for ``strategy_id``, or ``None``."""
        with self._lock:
            row = self._conn.execute(
                "SELECT sequence FROM cursor WHERE strategy_id = ?", (strategy_id,)
            ).fetchone()
        return int(row[0]) if row is not None else None

    def commit(self, strategy_id: str, sequence: int) -> None:
        """Durably record ``sequence`` as processed and prune covered markers.

        Upserts the cursor row and, in the same transaction, deletes ``processed``
        markers for the strategy at or below the watermark — the cursor row now
        represents them, so subscriber-driven stores stay near-empty. Freed pages
        are vacuumed back to the OS.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT INTO cursor (strategy_id, sequence, updated_at) VALUES (?, ?, ?) "
                    "ON CONFLICT(strategy_id) DO UPDATE "
                    "SET sequence = excluded.sequence, updated_at = excluded.updated_at",
                    (strategy_id, sequence, self._clock().isoformat()),
                )
                deleted = self._conn.execute(
                    "DELETE FROM processed WHERE strategy_id = ? AND sequence <= ?",
                    (strategy_id, sequence),
                ).rowcount
                self._conn.execute("COMMIT")
            except BaseException:
                self._rollback()
                raise
            if deleted:
                self._vacuum_freed_pages()

    # ------------------------------------------------------------- exactly-once wrappers
    def exactly_once(self, handler: Handler, *, on_duplicate: Handler | None = None) -> Handler:
        """Wrap ``handler`` so it completes at most once per ``signal_id``.

        Returns a callback for ``SignalSubscriber(on_signal=...)``. A duplicate
        delivery returns normally (so the subscriber still advances its cursor
        past the redelivered signal) and invokes ``on_duplicate`` if given. A
        handler exception rolls the marker back and re-raises — the signal will
        be retried on the next delivery.
        """
        return self.exactly_once_tx(
            lambda signal, meta, _cur: handler(signal, meta), on_duplicate=on_duplicate
        )

    def exactly_once_tx(
        self, handler: TxHandler, *, on_duplicate: Handler | None = None
    ) -> Handler:
        """Like :meth:`exactly_once`, but the handler joins the marker transaction.

        The handler receives a ``sqlite3.Cursor`` bound to the open transaction;
        rows it writes through that cursor commit atomically with the processed
        marker (and roll back with it on failure) — true exactly-once for state
        kept in this database. Create your tables up front via :attr:`connection`.
        """

        def callback(signal: ReceivedSignal, meta: SignalMeta) -> None:
            duplicate = False
            with self._lock:
                cur = self._conn.cursor()
                try:
                    cur.execute("BEGIN IMMEDIATE")
                    try:
                        if self._is_processed(signal):
                            cur.execute("ROLLBACK")
                            duplicate = True
                        else:
                            handler(signal, meta, cur)
                            self._insert_marker(signal)
                            cur.execute("COMMIT")
                    except BaseException:
                        self._rollback()
                        raise
                finally:
                    cur.close()
                if duplicate and on_duplicate is not None:
                    on_duplicate(signal, meta)

        return callback

    # ------------------------------------------------------------- webhook-side helpers
    @contextmanager
    def process(self, signal: ReceivedSignal) -> Iterator[bool]:
        """Context manager for webhook receivers: ``yields`` whether ``signal`` is fresh.

        Fresh: the body runs inside the marker transaction (lock held for its
        duration — webhook worker threads are serialized); normal exit commits
        the marker, an exception rolls it back and re-raises. Duplicate: yields
        ``False`` with no transaction open — still answer the webhook 2xx so the
        relay's retry ladder stops.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            fresh = not self._is_processed(signal)
            if not fresh:
                self._conn.execute("ROLLBACK")
                yield False
                return
            try:
                yield True
            except BaseException:
                self._rollback()
                raise
            self._insert_marker(signal)
            self._conn.execute("COMMIT")

    def seen(self, signal_id: str) -> bool:
        """Return whether a marker exists for ``signal_id`` (watermark not consulted)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM processed WHERE signal_id = ?", (signal_id,)
            ).fetchone()
        return row is not None

    # ------------------------------------------------------------- housekeeping
    def prune(self, *, before: datetime | None = None) -> int:
        """Delete markers processed before ``before`` (default: ``retention_days`` ago).

        Safe because the relay cannot redeliver past its retention window. Runs
        opportunistically on open; webhook-only users (who never commit a cursor)
        rely on it to bound the ledger. Freed pages are vacuumed back to the OS.

        Returns:
            Number of markers deleted.
        """
        cutoff = before if before is not None else self._clock() - self._retention
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                deleted = self._conn.execute(
                    "DELETE FROM processed WHERE processed_at < ?", (cutoff.isoformat(),)
                ).rowcount
                self._conn.execute("COMMIT")
            except BaseException:
                self._rollback()
                raise
            if deleted:
                self._vacuum_freed_pages()
        return int(deleted)

    @property
    def connection(self) -> sqlite3.Connection:
        """The underlying connection, for user tables and ad-hoc queries.

        Not lock-guarded: take your own care off the delivery path, or do
        transactional work inside an :meth:`exactly_once_tx` handler instead.
        """
        return self._conn

    def close(self) -> None:
        """Close the store (idempotent). Reopen by constructing a new instance."""
        with self._lock:
            if not self._closed:
                self._conn.close()
                self._closed = True

    def __enter__(self) -> SqliteStateStore:
        """Return ``self`` (context-manager support)."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the store on context exit."""
        self.close()

    # ------------------------------------------------------------- internals
    def _is_processed(self, signal: ReceivedSignal) -> bool:
        """Apply the invariant: marker present OR sequence at/below the cursor watermark."""
        marker = self._conn.execute(
            "SELECT 1 FROM processed WHERE signal_id = ?", (signal.signal_id,)
        ).fetchone()
        if marker is not None:
            return True
        watermark = self._conn.execute(
            "SELECT 1 FROM cursor WHERE strategy_id = ? AND sequence >= ?",
            (signal.strategy_id, signal.sequence),
        ).fetchone()
        return watermark is not None

    def _insert_marker(self, signal: ReceivedSignal) -> None:
        """Record the processed marker for ``signal`` inside the open transaction."""
        self._conn.execute(
            "INSERT INTO processed (signal_id, strategy_id, sequence, processed_at) "
            "VALUES (?, ?, ?, ?)",
            (signal.signal_id, signal.strategy_id, signal.sequence, self._clock().isoformat()),
        )

    def _vacuum_freed_pages(self) -> None:
        """Return every page freed by a delete to the OS.

        ``PRAGMA incremental_vacuum`` reclaims ONE page per ``sqlite3_step()``.
        Older SQLite (<= 3.50) surfaced a row per reclaimed page, so a single
        ``execute(...).fetchall()`` happened to step the pragma to exhaustion;
        SQLite 3.51+ returns no rows, ``fetchall()`` stops immediately, and that
        idiom reclaims exactly one page per call — the ledger then grows without
        bound on a long-running subscriber. Passing an explicit page count does
        NOT help: ``incremental_vacuum(N)`` still yields after one page there.
        So drive it explicitly and stop when the freelist is drained.

        Caller must hold ``self._lock`` and be outside a transaction (the pragma
        is a no-op inside one). Bounded by the freelist shrinking: the loop exits
        on the first iteration that frees nothing, so a SQLite that refuses to
        vacuum costs one extra pragma rather than spinning forever.
        """
        remaining = self._freelist_count()
        while remaining:
            self._conn.execute("PRAGMA incremental_vacuum").fetchall()
            after = self._freelist_count()
            if after >= remaining:
                break  # no progress: stop rather than spin
            remaining = after

    def _freelist_count(self) -> int:
        """Pages on the database freelist (unused but still allocated in the file)."""
        row = self._conn.execute("PRAGMA freelist_count").fetchone()
        return int(row[0]) if row is not None else 0

    def _rollback(self) -> None:
        """Roll back the open transaction, tolerating an already-closed one."""
        if self._conn.in_transaction:
            self._conn.execute("ROLLBACK")
