"""The checkpoint store: which buckets have been rolled up, and which have not.

SQLite, one small file. Not a service, not a table in one of the backends -- the
checkpoint must survive the backends being wiped, and it is the one piece of state whose
loss would cause either silent double-counting or a silent gap.

## The ordering that makes this safe

A bucket moves `claimed` -> `committed`, and **the commit happens strictly after the
sinks have durably accepted the data**. That ordering is the whole design:

  * Crash between write and commit -> the bucket stays `claimed`, gets reclaimed on the
    next run, and is written again. Safe, because rollup writes are keyed on
    (signal, granularity, bucket_start, dimension_hash) and overwrite rather than append.
  * Commit before write -> a crash loses the bucket forever, with the checkpoint
    cheerfully asserting it was done.

So the failure mode is deliberately biased toward doing work twice rather than skipping
it. Duplicated idempotent work costs seconds; a skipped bucket is permanent and silent.

This is the same distinction that made the trace grace period wrong at first (F-006):
"the writer thinks it is done" and "the reader can see it" are two different instants.
One layer up, "the job finished" and "it is safe to record as committed" are also two
different instants, and `commit()` must only be called at the second one. It is the
caller's contract -- see `pipeline.py`, which is the only place that calls it.

## Why the frontier advances contiguously

`frontier` is the start of the oldest bucket not yet committed, and it advances only
across an unbroken run of committed buckets. If buckets 1, 2 and 4 succeed but 3 fails,
the frontier stays at 3. Taking `max(committed)` instead would step over the hole and
lose bucket 3 permanently -- the exact silent gap this pipeline exists to prevent.

A permanently failing bucket therefore stalls the frontier, which is intentional: the
job keeps processing newer buckets (they are simply re-examined and found committed each
run), while `edge-rollup status` shows the stall. Lateness is recoverable; a gap is not.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from edgerollup.model import TimeRange
from edgerollup.windows import Granularity

log = logging.getLogger(__name__)

CLAIMED = "claimed"
COMMITTED = "committed"
FAILED = "failed"

# How long a claim is honoured before another run may take it over. Generous on purpose:
# reclaiming a bucket that a live process is still working on is harmless (the write is
# idempotent) but wasteful, whereas a lease too long delays recovery from a real crash.
# An hourly job that cannot finish one bucket in 30 minutes has a different problem.
DEFAULT_LEASE = timedelta(minutes=30)

SCHEMA = """
CREATE TABLE IF NOT EXISTS bucket_state (
    signal        TEXT    NOT NULL,
    granularity   TEXT    NOT NULL,
    -- Epoch seconds, not an ISO string. SQLite has no date type, and comparing ISO
    -- strings works only while they are all the same length and offset -- a property
    -- nothing enforces.
    bucket_start  INTEGER NOT NULL,
    status        TEXT    NOT NULL,
    claimed_at    INTEGER,
    lease_until   INTEGER,
    committed_at  INTEGER,
    record_count  INTEGER,
    -- Which writer produced this bucket's output. A commit is only honoured by a
    -- writer that identifies itself the same way, so a checkpoint left by a different
    -- (or absent) rollup implementation cannot cause its work to be skipped.
    writer_version TEXT,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    PRIMARY KEY (signal, granularity, bucket_start)
);

-- Keyed by writer as well as by signal and granularity. The frontier is a claim about
-- how far a PARTICULAR writer has got; sharing one across writers would let a new writer
-- start past work it never did, which is the same hole the writer_version check on
-- bucket_state closes -- and it has to be closed in both places or the new writer never
-- reaches the buckets to check them.
CREATE TABLE IF NOT EXISTS frontier (
    signal         TEXT    NOT NULL,
    granularity    TEXT    NOT NULL,
    writer_version TEXT    NOT NULL,
    next_start     INTEGER NOT NULL,
    PRIMARY KEY (signal, granularity, writer_version)
);

CREATE INDEX IF NOT EXISTS bucket_state_status
    ON bucket_state (signal, granularity, status);
"""


def _epoch(moment: datetime) -> int:
    if moment.tzinfo is None:
        raise ValueError("state store requires timezone-aware instants")
    return int(moment.timestamp())


def _instant(seconds: int | None) -> datetime | None:
    return None if seconds is None else datetime.fromtimestamp(seconds, tz=UTC)


@dataclass(frozen=True, slots=True)
class BucketRecord:
    signal: str
    granularity: str
    bucket_start: datetime
    status: str
    attempts: int
    record_count: int | None
    writer_version: str | None
    committed_at: datetime | None
    last_error: str | None


class StateStore:
    """Bucket checkpoints, backed by a SQLite file."""

    def __init__(self, path: Path, lease: timedelta = DEFAULT_LEASE) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lease = lease
        self._conn = sqlite3.connect(self.path, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        # WAL so a reader (`status`) never blocks the running job.
        self._conn.execute("PRAGMA journal_mode=WAL")
        # FULL rather than NORMAL. This file is tiny and written a handful of times per
        # run, so the cost is irrelevant, and it removes the one case where NORMAL can
        # lose the most recent transaction on power loss. Losing a commit is survivable
        # (the bucket is redone) but there is no reason to accept even that here.
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        """One atomic unit.

        IMMEDIATE takes the write lock up front rather than on first write, so two
        concurrent runs collide here -- at claim time -- instead of halfway through a
        read-modify-write of the frontier.
        """
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    # --- reading ---------------------------------------------------------------
    def frontier(
        self, signal: str, granularity: Granularity, writer_version: str
    ) -> datetime | None:
        row = self._conn.execute(
            "SELECT next_start FROM frontier "
            "WHERE signal = ? AND granularity = ? AND writer_version = ?",
            (signal, granularity.name, writer_version),
        ).fetchone()
        return _instant(row["next_start"]) if row else None

    def frontiers(self) -> list[tuple[str, str, str, datetime]]:
        """Every frontier, for `status`. One row per (signal, granularity, writer)."""
        return [
            (r["signal"], r["granularity"], r["writer_version"], _instant(r["next_start"]))
            for r in self._conn.execute(
                "SELECT * FROM frontier ORDER BY signal, granularity, writer_version"
            )
        ]

    def status_of(self, signal: str, granularity: Granularity, bucket: TimeRange) -> str | None:
        row = self._conn.execute(
            "SELECT status FROM bucket_state "
            "WHERE signal = ? AND granularity = ? AND bucket_start = ?",
            (signal, granularity.name, _epoch(bucket.start)),
        ).fetchone()
        return row["status"] if row else None

    def is_committed(
        self,
        signal: str,
        granularity: Granularity,
        bucket: TimeRange,
        writer_version: str | None = None,
    ) -> bool:
        """Whether this bucket is done AND was done by the writer now asking.

        The `writer_version` check is not bookkeeping. A checkpoint records that a
        bucket was processed, but "processed" is only meaningful relative to what did
        the processing. The boundary gate runs a processor that writes nothing, so
        without this check it would leave behind a checkpoint asserting that dozens of
        buckets were complete -- and the real rollup writer would then skip every one of
        them, producing a permanent hole that looks exactly like a successful run.
        (Observed for real: a no-op run committed 23 log buckets.)

        The same mechanism covers schema drift. When the aggregation output changes
        shape incompatibly, bumping the writer version makes every previously committed
        bucket eligible again, rather than leaving a cold tier that silently mixes two
        formats.
        """
        row = self._conn.execute(
            "SELECT status, writer_version FROM bucket_state "
            "WHERE signal = ? AND granularity = ? AND bucket_start = ?",
            (signal, granularity.name, _epoch(bucket.start)),
        ).fetchone()
        if row is None or row["status"] != COMMITTED:
            return False
        if writer_version is None:
            return True
        return row["writer_version"] == writer_version

    def buckets(self, signal: str | None = None, status: str | None = None) -> list[BucketRecord]:
        query = "SELECT * FROM bucket_state"
        clauses, params = [], []
        if signal:
            clauses.append("signal = ?")
            params.append(signal)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY signal, granularity, bucket_start"

        return [
            BucketRecord(
                signal=row["signal"],
                granularity=row["granularity"],
                bucket_start=_instant(row["bucket_start"]),
                status=row["status"],
                attempts=row["attempts"],
                record_count=row["record_count"],
                writer_version=row["writer_version"],
                committed_at=_instant(row["committed_at"]),
                last_error=row["last_error"],
            )
            for row in self._conn.execute(query, params)
        ]

    # --- writing ---------------------------------------------------------------
    def claim(
        self,
        signal: str,
        granularity: Granularity,
        bucket: TimeRange,
        now: datetime,
        writer_version: str,
    ) -> bool:
        """Take ownership of a bucket. False means someone else holds it, or it is done.

        Returns False for a bucket already committed BY THIS WRITER, which is what makes
        a second run over the same window a no-op rather than a repeat.

        A bucket committed by a *different* writer is claimable. It has to be: the whole
        point of tracking the writer is that another writer's commit says nothing about
        whether this writer's output exists. Refusing here would leave the bucket
        permanently unwritten while every report called it "already done".
        """
        start = _epoch(bucket.start)
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT status, lease_until, attempts, writer_version FROM bucket_state "
                "WHERE signal = ? AND granularity = ? AND bucket_start = ?",
                (signal, granularity.name, start),
            ).fetchone()

            if row is not None:
                if row["status"] == COMMITTED and row["writer_version"] == writer_version:
                    return False
                if row["status"] == CLAIMED:
                    lease_until = row["lease_until"] or 0
                    if lease_until > _epoch(now):
                        # A live run holds it. Not an error -- the other run will
                        # finish it -- so this one simply moves on.
                        return False
                    log.warning(
                        "%s/%s bucket %s: reclaiming an expired lease (previous run "
                        "likely crashed mid-bucket)",
                        signal,
                        granularity.name,
                        bucket.start.isoformat(),
                    )

            attempts = (row["attempts"] if row else 0) + 1
            conn.execute(
                """
                INSERT INTO bucket_state
                    (signal, granularity, bucket_start, status, claimed_at, lease_until,
                     attempts)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (signal, granularity, bucket_start) DO UPDATE SET
                    status = excluded.status,
                    claimed_at = excluded.claimed_at,
                    lease_until = excluded.lease_until,
                    attempts = excluded.attempts
                """,
                (
                    signal,
                    granularity.name,
                    start,
                    CLAIMED,
                    _epoch(now),
                    _epoch(now + self.lease),
                    attempts,
                ),
            )
            return True

    def commit(
        self,
        signal: str,
        granularity: Granularity,
        bucket: TimeRange,
        now: datetime,
        record_count: int,
        writer_version: str,
    ) -> None:
        """Record a bucket as done, and advance the frontier over it.

        MUST only be called once every sink has durably accepted this bucket's output.
        Calling it when the writes are merely *issued* reintroduces exactly the gap this
        module exists to close: the checkpoint would assert completion for data that a
        crash could still lose.
        """
        start = _epoch(bucket.start)
        with self._transaction() as conn:
            conn.execute(
                """
                UPDATE bucket_state
                   SET status = ?, committed_at = ?, record_count = ?,
                       writer_version = ?, lease_until = NULL, last_error = NULL
                 WHERE signal = ? AND granularity = ? AND bucket_start = ?
                """,
                (
                    COMMITTED,
                    _epoch(now),
                    record_count,
                    writer_version,
                    signal,
                    granularity.name,
                    start,
                ),
            )
            self._advance_frontier(conn, signal, granularity, writer_version)

    def fail(
        self,
        signal: str,
        granularity: Granularity,
        bucket: TimeRange,
        error: str,
    ) -> None:
        """Record that a bucket could not be processed.

        The frontier is deliberately NOT advanced. A failed bucket blocks it until it
        succeeds, so the gap stays visible instead of being stepped over.
        """
        self._conn.execute(
            """
            UPDATE bucket_state
               SET status = ?, lease_until = NULL, last_error = ?
             WHERE signal = ? AND granularity = ? AND bucket_start = ?
            """,
            (FAILED, error[:500], signal, granularity.name, _epoch(bucket.start)),
        )

    def _advance_frontier(
        self,
        conn: sqlite3.Connection,
        signal: str,
        granularity: Granularity,
        writer_version: str,
    ) -> None:
        """Walk the frontier forward across an unbroken run of committed buckets.

        Stops at the first bucket that is missing or not committed. That stop is the
        point of the whole routine -- see the module docstring on why max(committed)
        would be wrong.
        """
        row = conn.execute(
            "SELECT next_start FROM frontier "
            "WHERE signal = ? AND granularity = ? AND writer_version = ?",
            (signal, granularity.name, writer_version),
        ).fetchone()
        if row is None:
            # No frontier yet: start from the oldest bucket THIS writer has committed.
            oldest = conn.execute(
                "SELECT MIN(bucket_start) AS s FROM bucket_state "
                "WHERE signal = ? AND granularity = ? AND writer_version = ?",
                (signal, granularity.name, writer_version),
            ).fetchone()
            if oldest is None or oldest["s"] is None:
                return
            cursor = int(oldest["s"])
        else:
            cursor = int(row["next_start"])

        while True:
            found = conn.execute(
                "SELECT status, writer_version FROM bucket_state "
                "WHERE signal = ? AND granularity = ? AND bucket_start = ?",
                (signal, granularity.name, cursor),
            ).fetchone()
            if found is None or found["status"] != COMMITTED:
                break
            if found["writer_version"] != writer_version:
                # Committed, but by a different writer. It counts as not done for this
                # one, so the frontier stops here and the bucket is redone.
                break
            cursor += granularity.seconds

        conn.execute(
            """
            INSERT INTO frontier (signal, granularity, writer_version, next_start)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (signal, granularity, writer_version)
                DO UPDATE SET next_start = excluded.next_start
            """,
            (signal, granularity.name, writer_version, cursor),
        )

    def reset(self, signal: str | None = None) -> None:
        """Forget checkpoints. Used by `backfill --reprocess` and by tests.

        Not exposed as a bare CLI verb: clearing a checkpoint is how you turn an
        idempotent pipeline into a double-counting one, so it belongs behind an explicit
        operation that also rewrites the data.
        """
        with self._transaction() as conn:
            if signal:
                conn.execute("DELETE FROM bucket_state WHERE signal = ?", (signal,))
                conn.execute("DELETE FROM frontier WHERE signal = ?", (signal,))
            else:
                conn.execute("DELETE FROM bucket_state")
                conn.execute("DELETE FROM frontier")
