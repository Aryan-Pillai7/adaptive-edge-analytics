"""Orchestration: which buckets to process, in what order, and when to commit.

This module contains no aggregation and no backend knowledge. It walks sealed buckets in
order, hands each one's records to a processor, and records the outcome. The processor is
injected, so the ordering guarantees below can be -- and are -- tested with a processor
that does nothing at all, before any real aggregation exists to mask a failure.

The sequence per bucket is fixed, and the order is the correctness argument:

    claim -> read -> process (durably) -> commit

`commit` last is what biases every failure toward doing work twice rather than skipping
it. A crash anywhere before it leaves the bucket claimed but uncommitted, so the next run
redoes it; because rollup writes are keyed and overwrite, redoing is free. The reverse
order would let a crash lose a bucket while the checkpoint asserted it was finished.

A processor MUST NOT return until its writes are durable. "The function returned" and
"the data is safe" are two different instants, and only the second one justifies a
commit -- the same distinction that made the trace grace period wrong when only the
writer-side buffers were counted (decisions.md F-006).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta

from edgerollup.clock import Clock
from edgerollup.model import RawRecord, TimeRange
from edgerollup.sources.base import Source, SourceError
from edgerollup.state import StateStore
from edgerollup.windows import Granularity, cold_start_floor, sealed_buckets

log = logging.getLogger(__name__)

#: Given a bucket and its raw records, write the rollup and return how many records were
#: written. Must not return until those writes are durable.
Processor = Callable[[str, Granularity, TimeRange, list[RawRecord]], int]

#: Identifies what produced a bucket's output, and is stored alongside the checkpoint.
#: A bucket only counts as done for the writer that matches. This is what stops the
#: no-op processor below from leaving behind checkpoints that the real rollup writer
#: would then honour and skip -- and, later, what makes a change to the aggregation
#: output shape re-open every bucket instead of quietly mixing two formats.
NOOP_WRITER = "noop-v1"

# A signal that fails this many buckets in a row is almost certainly failing for one
# systemic reason (backend down, credentials wrong), and grinding through the remaining
# buckets would just produce the same error many times over.
MAX_CONSECUTIVE_FAILURES = 3


def count_only(
    signal: str, granularity: Granularity, bucket: TimeRange, records: list[RawRecord]
) -> int:
    """A processor that writes nothing.

    This is what the boundary gate runs against. Proving claim/commit/resume with a
    no-op processor means a failure there can only be a failure of the ordering itself --
    there is no aggregation to blame, and no partially-written output to confuse the
    picture. Replaced by the real rollup writer in the aggregation milestone.
    """
    return len(records)


@dataclass
class RunReport:
    signal: str
    granularity: str
    processed: list[TimeRange] = field(default_factory=list)
    skipped_committed: list[TimeRange] = field(default_factory=list)
    skipped_locked: list[TimeRange] = field(default_factory=list)
    failed: list[tuple[TimeRange, str]] = field(default_factory=list)
    records_written: int = 0

    @property
    def did_work(self) -> bool:
        return bool(self.processed)

    def summary(self) -> str:
        return (
            f"{self.signal}/{self.granularity}: "
            f"{len(self.processed)} processed, "
            f"{len(self.skipped_committed)} already done, "
            f"{len(self.skipped_locked)} held elsewhere, "
            f"{len(self.failed)} failed, "
            f"{self.records_written} records"
        )


def run_signal(
    *,
    signal: str,
    granularity: Granularity,
    source: Source,
    store: StateStore,
    clock: Clock,
    grace: timedelta,
    max_backfill: timedelta,
    processor: Processor = count_only,
    writer_version: str = NOOP_WRITER,
    max_buckets: int | None = None,
    dry_run: bool = False,
) -> RunReport:
    """Process every sealed, uncommitted bucket for one signal at one granularity."""
    report = RunReport(signal=signal, granularity=granularity.name)
    now = clock.now()

    # Resume from the frontier if there is one. On a cold start, reach back a bounded
    # distance rather than to the beginning of retention -- see cold_start_floor.
    # The frontier is only meaningful for the writer that built it; a different writer
    # starts from scratch rather than inheriting someone else's idea of "done".
    start = store.frontier(signal, granularity, writer_version)
    if start is None:
        start = cold_start_floor(now, granularity, max_backfill)
        log.info(
            "%s/%s: no checkpoint, cold-starting from %s",
            signal,
            granularity.name,
            start.isoformat(),
        )

    consecutive_failures = 0

    for bucket in sealed_buckets(granularity, start, now, grace, limit=max_buckets):
        # Cheap pre-check before taking the write lock. The claim below re-checks under
        # the transaction, so this is an optimisation and not the actual guard.
        if store.is_committed(signal, granularity, bucket, writer_version):
            report.skipped_committed.append(bucket)
            continue

        if dry_run:
            # Read and count, but claim nothing and commit nothing. A dry run must leave
            # the checkpoint exactly as it found it, or "let me see what it would do"
            # silently becomes "it did it".
            try:
                records = source.read(bucket)
            except SourceError as exc:
                report.failed.append((bucket, str(exc)))
                continue
            report.processed.append(bucket)
            report.records_written += len(records)
            continue

        if not store.claim(signal, granularity, bucket, clock.now(), writer_version):
            report.skipped_locked.append(bucket)
            continue

        try:
            records = source.read(bucket)
            written = processor(signal, granularity, bucket, records)
        # Broad on purpose: one bad bucket must not take down a run that could still
        # complete every other bucket. The failure is recorded and blocks the frontier.
        except Exception as exc:
            log.error("%s/%s bucket %s failed: %s", signal, granularity.name, bucket, exc)
            store.fail(signal, granularity, bucket, str(exc))
            report.failed.append((bucket, str(exc)))
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                log.error(
                    "%s/%s: %d consecutive failures, stopping this signal",
                    signal,
                    granularity.name,
                    consecutive_failures,
                )
                break
            continue

        # Only now. Everything above has to have succeeded, and `processor` has to have
        # made its writes durable before returning.
        store.commit(signal, granularity, bucket, clock.now(), written, writer_version)
        report.processed.append(bucket)
        report.records_written += written
        consecutive_failures = 0

    return report
