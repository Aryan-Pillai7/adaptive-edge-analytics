"""The boundary gate against the real backends.

The unit suite proves the ordering with a fake source and a frozen clock, which is where
the edge cases belong. This proves the same properties survive contact with real
backends, real network latency, and real clock time -- the parts a fake cannot model.

Still no aggregation: the processor writes nothing. A failure here can only be the
checkpoint logic or the read path, both of which are already covered elsewhere, so it
localises cleanly.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest

from edgerollup.clock import SystemClock
from edgerollup.pipeline import NOOP_WRITER, run_signal
from edgerollup.state import COMMITTED, StateStore
from edgerollup.windows import Granularity

pytestmark = pytest.mark.integration

HOUR = Granularity("1h", 3600)
# A short backfill horizon rather than a bucket cap. The distinction matters: with a cap,
# a run stops early and the NEXT run legitimately continues to fresh buckets, so "the
# second run does nothing" is simply false and asserting it tests the cap rather than
# idempotency. Bounding the horizon instead means the first run drains everything
# available, which is the state the property is actually about. A few hours also keeps
# the metrics read (tens of thousands of samples per hour) quick enough for a test.
BACKFILL = timedelta(hours=3)


@pytest.fixture
def store(tmp_path) -> StateStore:
    """A throwaway checkpoint DB per test.

    Never the real one: a test that advanced the operator's frontier would cause the
    next real run to skip buckets it had not actually written.
    """
    with StateStore(tmp_path / "checkpoints.db") as s:
        yield s


@pytest.fixture(params=["metrics", "logs", "traces"])
def signal(request) -> str:
    return request.param


def go(store, sources, settings, signal, **kwargs):
    return run_signal(
        signal=signal,
        granularity=HOUR,
        source=sources[signal],
        store=store,
        clock=SystemClock(),
        grace=settings.grace(signal),
        max_backfill=BACKFILL,
        **kwargs,
    )


class TestIdempotencyAgainstRealBackends:
    def test_running_twice_processes_nothing_the_second_time(
        self, store, sources, settings, signal
    ):
        first = go(store, sources, settings, signal)
        assert first.processed, f"{signal}: nothing sealed to process"

        second = go(store, sources, settings, signal)

        assert second.processed == [], f"{signal}: the second run redid committed work"
        assert second.records_written == 0
        # And nothing was silently left behind by the first run either.
        assert second.failed == []

    def test_the_record_count_is_stable_across_a_reprocess(self, store, sources, settings, signal):
        """The counts a rollup would sum must not drift between runs.

        Idempotency at the bucket level is not enough on its own: if the same sealed
        window returns a different number of records on a second read, no amount of
        checkpointing makes the output stable. This is the read gate's repeatability
        property, restated as the thing the checkpoint actually depends on.
        """
        first = go(store, sources, settings, signal)
        if not first.processed:
            pytest.skip(f"{signal}: nothing sealed to process")
        first_counts = {r.bucket_start: r.record_count for r in store.buckets(signal, COMMITTED)}

        store.reset(signal)
        second = go(store, sources, settings, signal)
        second_counts = {r.bucket_start: r.record_count for r in store.buckets(signal, COMMITTED)}

        assert second.processed, f"{signal}: reset did not make the work available again"
        assert first_counts == second_counts, (
            f"{signal}: re-reading the same sealed buckets gave different counts"
        )


class TestSealingAgainstRealTime:
    def test_no_unsealed_bucket_is_ever_processed(self, store, sources, settings, signal):
        """The grace period, enforced against wall-clock time rather than a frozen one.

        This is the property that F-006 showed matters most for traces: a bucket
        processed too early reads as empty and the emptiness is recorded as fact.
        """
        report = go(store, sources, settings, signal)
        now = datetime.now(UTC)
        grace = settings.grace(signal)
        for bucket in report.processed:
            assert bucket.end + grace <= now, (
                f"{signal}: processed {bucket} which is still inside its grace period"
            )

    def test_processed_buckets_are_contiguous_and_aligned(self, store, sources, settings, signal):
        report = go(store, sources, settings, signal)
        if len(report.processed) < 2:
            pytest.skip(f"{signal}: need at least two buckets to check the tiling")

        for earlier, later in zip(report.processed, report.processed[1:], strict=False):
            assert earlier.end == later.start, "a gap or overlap between buckets"
        for bucket in report.processed:
            assert int(bucket.start.timestamp()) % HOUR.seconds == 0, "bucket not aligned"


class TestResumeAgainstRealBackends:
    def test_a_crash_partway_resumes_without_gap_or_overlap(self, store, sources, settings, signal):
        seen: list[datetime] = []

        def die_on_the_second(sig, gran, bucket, records):
            if len(seen) == 1:
                raise RuntimeError("simulated crash")
            seen.append(bucket.start)
            return len(records)

        go(store, sources, settings, signal, processor=die_on_the_second)
        go(store, sources, settings, signal, processor=lambda s, g, b, r: seen.append(b.start) or 0)

        if not seen:
            pytest.skip(f"{signal}: nothing sealed to process")
        assert len(seen) == len(set(seen)), f"{signal}: a bucket was processed twice"
        ordered = sorted(seen)
        for earlier, later in pairwise(ordered):
            assert later - earlier == HOUR.delta, f"{signal}: gap between {earlier} and {later}"


class TestDryRunAgainstRealBackends:
    def test_a_dry_run_writes_no_checkpoint(self, store, sources, settings, signal):
        report = go(store, sources, settings, signal, dry_run=True)
        assert report.processed, f"{signal}: nothing sealed to process"
        assert store.frontier(signal, HOUR, NOOP_WRITER) is None
        assert store.buckets(signal) == []
