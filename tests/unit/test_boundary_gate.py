"""The boundary gate: idempotency and gap-free resume, on empty payloads.

Everything here runs with a processor that writes nothing and a source that returns
nothing. That is deliberate. If these properties are proven only once aggregation exists,
a failure has two possible homes and the aggregation output is there to make a wrong
answer look like a plausible one. With an empty payload, "the second run processed a
bucket again" is unambiguous.

The three properties the gate has to establish:

  1. Running twice over the same window is a no-op the second time.
  2. A run killed mid-bucket resumes with no gap and no overlap.
  3. A bucket still inside its grace period is never claimed.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest

from edgerollup.clock import FrozenClock
from edgerollup.model import RawRecord, TimeRange
from edgerollup.pipeline import MAX_CONSECUTIVE_FAILURES, NOOP_WRITER, run_signal
from edgerollup.state import COMMITTED, FAILED, StateStore
from edgerollup.windows import Granularity

HOUR = Granularity("1h", 3600)
GRACE = timedelta(minutes=10)
BACKFILL = timedelta(hours=24)


def at(hour: int, minute: int = 0, day: int = 27) -> datetime:
    return datetime(2026, 8, day, hour, minute, tzinfo=UTC)


class SilentSource:
    """A backend holding nothing. Records what it was asked for."""

    signal = "metrics"

    def __init__(self, records_per_bucket: int = 0):
        self.reads: list[TimeRange] = []
        self.records_per_bucket = records_per_bucket

    def read(self, window: TimeRange) -> list[RawRecord]:
        self.reads.append(window)
        return [
            RawRecord(
                signal="metrics",
                timestamp=window.start,
                dimensions=(),
                value=1.0,
                identity=f"{window.start.isoformat()}#{i}",
            )
            for i in range(self.records_per_bucket)
        ]


class ExplodingSource(SilentSource):
    """Fails on the nominated bucket starts, succeeds on the rest."""

    def __init__(self, fail_on: set[datetime]):
        super().__init__()
        self.fail_on = fail_on

    def read(self, window: TimeRange) -> list[RawRecord]:
        self.reads.append(window)
        if window.start in self.fail_on:
            raise RuntimeError(f"backend refused {window.start.isoformat()}")
        return []


@pytest.fixture
def store(tmp_path) -> StateStore:
    with StateStore(tmp_path / "checkpoints.db") as s:
        yield s


def run(store, clock, source, **kwargs):
    return run_signal(
        signal="metrics",
        granularity=HOUR,
        source=source,
        store=store,
        clock=clock,
        grace=GRACE,
        max_backfill=BACKFILL,
        **kwargs,
    )


class TestIdempotency:
    def test_a_second_run_over_the_same_window_does_nothing(self, store):
        """Property 1. The single most important behaviour in the pipeline."""
        clock = FrozenClock(at(12, 30))
        source = SilentSource()

        first = run(store, clock, source)
        assert first.processed, "the first run should have had work to do"
        reads_after_first = len(source.reads)

        second = run(store, clock, source)

        assert second.processed == [], "the second run re-processed committed buckets"
        assert second.records_written == 0
        # Not merely "skipped after checking" — the frontier means the second run does
        # not even look at the committed buckets, so it issues no backend queries at
        # all. That is what makes an hourly cron cheap rather than re-scanning a day's
        # worth of windows every time.
        assert len(source.reads) == reads_after_first, "the second run re-queried the backend"

    def test_a_third_and_fourth_run_are_also_no_ops(self, store):
        # Guards against a frontier that advances one bucket per run regardless of what
        # was actually committed -- which would pass a two-run test and still be wrong.
        clock = FrozenClock(at(12, 30))
        source = SilentSource()
        run(store, clock, source)
        for _ in range(3):
            assert run(store, clock, source).processed == []

    def test_each_bucket_is_read_exactly_once_across_repeated_runs(self, store):
        clock = FrozenClock(at(12, 30))
        source = SilentSource()
        run(store, clock, source)
        run(store, clock, source)
        run(store, clock, source)

        starts = [w.start for w in source.reads]
        assert len(starts) == len(set(starts)), "a bucket was read more than once"

    def test_time_moving_forward_adds_only_the_newly_sealed_buckets(self, store):
        clock = FrozenClock(at(12, 30))
        source = SilentSource()
        first = run(store, clock, source)

        clock.advance(timedelta(hours=2))
        second = run(store, clock, source)

        assert second.processed, "two hours later there should be new buckets"
        overlap = {b.start for b in first.processed} & {b.start for b in second.processed}
        assert overlap == set(), "a bucket was processed in both runs"

        # And together they still tile without a hole.
        every = sorted(b.start for b in first.processed + second.processed)
        for earlier, later in pairwise(every):
            assert later - earlier == HOUR.delta


class TestGapFreeResume:
    def test_a_crash_before_commit_leaves_the_bucket_claimed_not_committed(self, store):
        """Property 2, first half. The ordering that makes crashes safe.

        A processor that raises stands in for the process dying between the write and
        the commit -- the window where the two orderings differ.
        """
        clock = FrozenClock(at(12, 30))
        source = SilentSource()

        def explode(*_args):
            raise RuntimeError("killed mid-bucket")

        report = run(store, clock, source, processor=explode, max_buckets=1)

        assert report.processed == []
        assert len(report.failed) == 1
        bucket = report.failed[0][0]
        assert store.status_of("metrics", HOUR, bucket) == FAILED
        assert not store.is_committed("metrics", HOUR, bucket)

    def test_the_bucket_is_retried_on_the_next_run(self, store):
        clock = FrozenClock(at(12, 30))
        source = SilentSource()
        calls = {"n": 0}

        def flaky(signal, granularity, bucket, records):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient")
            return len(records)

        first = run(store, clock, source, processor=flaky, max_buckets=1)
        failed_bucket = first.failed[0][0]

        second = run(store, clock, source, processor=flaky, max_buckets=1)

        assert [b.start for b in second.processed] == [failed_bucket.start]
        assert store.is_committed("metrics", HOUR, failed_bucket)

    def test_a_failed_bucket_holds_the_frontier_back(self, store):
        """The anti-gap property, stated directly.

        If the frontier took max(committed) it would step straight over the hole and the
        failed bucket would never be revisited -- a permanent, silent gap in the cold
        tier while every dashboard showed the job succeeding.
        """
        clock = FrozenClock(at(12, 30))
        hole = at(9)
        source = ExplodingSource(fail_on={hole})

        run(store, clock, source)

        frontier = store.frontier("metrics", HOUR, NOOP_WRITER)
        assert frontier == hole, f"frontier stepped over the failed bucket: {frontier}"

        # Later buckets did still get processed -- lateness on one bucket must not stall
        # the whole signal.
        committed = {r.bucket_start for r in store.buckets(status=COMMITTED)}
        assert any(start > hole for start in committed)

    def test_the_frontier_advances_once_the_hole_is_filled(self, store):
        clock = FrozenClock(at(12, 30))
        hole = at(9)
        run(store, clock, ExplodingSource(fail_on={hole}))
        assert store.frontier("metrics", HOUR, NOOP_WRITER) == hole

        run(store, clock, ExplodingSource(fail_on=set()))

        frontier = store.frontier("metrics", HOUR, NOOP_WRITER)
        assert frontier > hole, "frontier did not recover after the hole was filled"

    def test_resume_covers_every_bucket_exactly_once_across_a_crash(self, store):
        """The end-to-end version: crash partway, resume, and check the tiling."""
        clock = FrozenClock(at(12, 30))
        source = SilentSource()
        processed_starts: list[datetime] = []

        def record_then_die(signal, granularity, bucket, records):
            if len(processed_starts) == 3:
                raise RuntimeError("crash")
            processed_starts.append(bucket.start)
            return 0

        run(store, clock, source, processor=record_then_die)
        run(store, clock, source, processor=lambda *a: processed_starts.append(a[2].start) or 0)

        assert len(processed_starts) == len(set(processed_starts)), "a bucket was done twice"
        ordered = sorted(processed_starts)
        for earlier, later in pairwise(ordered):
            assert later - earlier == HOUR.delta, f"gap between {earlier} and {later}"


class TestGraceIsRespected:
    def test_a_bucket_inside_its_grace_period_is_never_claimed(self, store):
        """Property 3. Claiming an unsealed bucket reads a partial window and records
        the partial answer as final."""
        clock = FrozenClock(at(12, 5))
        source = SilentSource()

        run(store, clock, source)

        # [11:00,12:00) needs now >= 12:10 with a 10-minute grace.
        current = TimeRange(at(11), at(12))
        assert store.status_of("metrics", HOUR, current) is None
        assert all(w.end <= at(11) for w in source.reads)

    def test_the_bucket_becomes_available_the_moment_grace_elapses(self, store):
        clock = FrozenClock(at(12, 9))
        source = SilentSource()
        run(store, clock, source)
        assert not store.is_committed("metrics", HOUR, TimeRange(at(11), at(12)))

        clock.advance(timedelta(minutes=1))
        run(store, clock, source)
        assert store.is_committed("metrics", HOUR, TimeRange(at(11), at(12)))

    def test_no_bucket_is_ever_read_past_the_watermark(self, store):
        clock = FrozenClock(at(12, 34))
        source = SilentSource()
        run(store, clock, source)
        for window in source.reads:
            assert window.end + GRACE <= clock.now(), f"{window} was not sealed"


class TestClaimSemantics:
    def test_a_live_claim_blocks_another_runner(self, store):
        """Concurrent runs must not both process a bucket. Harmless for correctness,
        since writes are idempotent, but it is wasted work and it muddies the report."""
        clock = FrozenClock(at(12, 30))
        bucket = TimeRange(at(10), at(11))

        assert store.claim("metrics", HOUR, bucket, clock.now(), NOOP_WRITER) is True
        assert store.claim("metrics", HOUR, bucket, clock.now(), NOOP_WRITER) is False

    def test_an_expired_lease_is_reclaimable(self, store):
        """A process that dies holding a claim must not block the bucket forever."""
        clock = FrozenClock(at(12, 30))
        bucket = TimeRange(at(10), at(11))
        assert store.claim("metrics", HOUR, bucket, clock.now(), NOOP_WRITER) is True

        clock.advance(timedelta(minutes=31))
        assert store.claim("metrics", HOUR, bucket, clock.now(), NOOP_WRITER) is True

    def test_a_committed_bucket_is_never_reclaimable(self, store):
        clock = FrozenClock(at(12, 30))
        bucket = TimeRange(at(10), at(11))
        store.claim("metrics", HOUR, bucket, clock.now(), NOOP_WRITER)
        store.commit("metrics", HOUR, bucket, clock.now(), 0, NOOP_WRITER)

        clock.advance(timedelta(days=7))
        assert store.claim("metrics", HOUR, bucket, clock.now(), NOOP_WRITER) is False


class TestDryRun:
    def test_a_dry_run_leaves_the_checkpoint_untouched(self, store):
        """ "Show me what it would do" must not become "it did it"."""
        clock = FrozenClock(at(12, 30))
        source = SilentSource()

        report = run(store, clock, source, dry_run=True)

        assert report.processed, "a dry run should still report what it would process"
        assert store.frontier("metrics", HOUR, NOOP_WRITER) is None
        assert store.buckets() == []

    def test_a_real_run_after_a_dry_run_still_does_the_work(self, store):
        clock = FrozenClock(at(12, 30))
        source = SilentSource()
        dry = run(store, clock, source, dry_run=True)
        real = run(store, clock, source)
        assert [b.start for b in real.processed] == [b.start for b in dry.processed]


class TestSystemicFailureHandling:
    def test_stops_after_repeated_consecutive_failures(self, store):
        """A signal failing every bucket is failing for one reason. Grinding through a
        day of buckets to produce the same error 24 times helps nobody."""
        clock = FrozenClock(at(12, 30))
        source = SilentSource()

        def always_fail(*_args):
            raise RuntimeError("backend down")

        report = run(store, clock, source, processor=always_fail)
        assert len(report.failed) == MAX_CONSECUTIVE_FAILURES


class TestColdStart:
    def test_a_cold_start_is_bounded_by_max_backfill(self, store):
        """Without a bound, a first run would try to scan the whole retention window."""
        clock = FrozenClock(at(12, 30))
        source = SilentSource()
        run(store, clock, source, max_buckets=None)
        earliest = min(w.start for w in source.reads)
        assert earliest >= clock.now() - BACKFILL - HOUR.delta

    def test_record_counts_are_recorded_per_bucket(self, store):
        clock = FrozenClock(at(12, 30))
        source = SilentSource(records_per_bucket=4)
        run(store, clock, source, max_buckets=2)
        counts = [r.record_count for r in store.buckets(status=COMMITTED)]
        assert counts == [4, 4]


class TestWriterVersionGuard:
    """A commit is only honoured by the writer that made it.

    Found by running the real thing: a boundary-gate run with the no-op processor
    committed 23 log buckets. Nothing was written -- the processor writes nothing by
    design -- but the checkpoint said "done", and the first real rollup writer would
    have skipped all 23. A permanent hole, produced by a green run.
    """

    def test_another_writer_does_not_inherit_these_commits(self, store):
        clock = FrozenClock(at(12, 30))
        source = SilentSource()
        first = run(store, clock, source, writer_version="noop-v1")
        assert first.processed

        second = run(store, clock, source, writer_version="rollup-v1")

        assert [b.start for b in second.processed] == [b.start for b in first.processed], (
            "a different writer inherited checkpoints for output it never produced"
        )

    def test_the_same_writer_still_sees_its_own_commits(self, store):
        clock = FrozenClock(at(12, 30))
        source = SilentSource()
        run(store, clock, source, writer_version="rollup-v1")
        assert run(store, clock, source, writer_version="rollup-v1").processed == []

    def test_bumping_the_version_reopens_every_bucket(self, store):
        """The schema-drift lever.

        When the aggregation output changes shape incompatibly, bumping the version is
        what re-derives the cold tier instead of leaving it silently mixing two formats.
        """
        clock = FrozenClock(at(12, 30))
        source = SilentSource()
        original = run(store, clock, source, writer_version="rollup-v1")

        reopened = run(store, clock, source, writer_version="rollup-v2")

        assert len(reopened.processed) == len(original.processed)

    def test_the_frontier_does_not_advance_on_another_writers_commits(self, store):
        clock = FrozenClock(at(12, 30))
        source = SilentSource()
        run(store, clock, source, writer_version="noop-v1")

        # A fresh writer must start from its own cold-start floor, not from the
        # frontier the previous writer left behind.
        report = run(store, clock, source, writer_version="rollup-v1", max_buckets=1)
        assert report.processed, "the new writer inherited a frontier past all the work"
