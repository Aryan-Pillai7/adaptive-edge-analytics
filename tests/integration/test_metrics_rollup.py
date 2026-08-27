"""The metrics rollup end to end, against the real stack.

The properties that matter once the processor actually writes something: the same window
rolled twice must not double-count, the cold tier must not feed itself, and what lands in
each sink must be what the aggregation produced.
"""

from __future__ import annotations

from datetime import timedelta

import httpx
import pyarrow.parquet as pq
import pytest

from edgerollup.clock import SystemClock
from edgerollup.pipeline import run_signal
from edgerollup.registry import dimensions_for
from edgerollup.rollups import MetricsRollup
from edgerollup.sinks import ParquetSink, VictoriaMetricsSink
from edgerollup.sinks.base import Sink, SinkError
from edgerollup.state import COMMITTED, StateStore
from edgerollup.windows import Granularity
from edgerollup.writer import RollupWriter, writer_version

pytestmark = pytest.mark.integration

HOUR = Granularity("1h", 3600)
BACKFILL = timedelta(hours=3)
SIGNAL = "metrics"


@pytest.fixture
def store(tmp_path) -> StateStore:
    with StateStore(tmp_path / "checkpoints.db") as s:
        yield s


@pytest.fixture
def parquet(tmp_path) -> ParquetSink:
    """A throwaway Parquet root. Never the real one -- a test must not pollute the
    operator's cold tier, and comparing files needs a directory only it writes to."""
    return ParquetSink(tmp_path / "cold")


def rollup_writer(sinks: list[Sink]) -> RollupWriter:
    return RollupWriter(MetricsRollup(dimensions_for(SIGNAL)), sinks)


def go(store, sources, settings, sinks, **kwargs):
    return run_signal(
        signal=SIGNAL,
        granularity=HOUR,
        source=sources[SIGNAL],
        store=store,
        clock=SystemClock(),
        grace=settings.grace(SIGNAL),
        max_backfill=BACKFILL,
        processor=rollup_writer(sinks),
        writer_version=writer_version(SIGNAL),
        **kwargs,
    )


class TestRollupIsIdempotent:
    def test_running_twice_writes_the_same_parquet_bytes(self, store, sources, settings, parquet):
        """Byte-identical, not just semantically equal.

        With one file per bucket and an atomic replace, a re-run must be a true no-op on
        disk. Anything else means a retry after a partial dual-write leaves the
        authoritative copy in a different state than the first attempt did.
        """
        first = go(store, sources, settings, [parquet])
        assert first.processed, "nothing sealed to roll up"

        before = {path: path.read_bytes() for path in sorted(parquet.root.rglob("*.parquet"))}
        assert before, "no parquet files were written"

        store.reset(SIGNAL)
        go(store, sources, settings, [parquet])

        after = {path: path.read_bytes() for path in sorted(parquet.root.rglob("*.parquet"))}
        assert after.keys() == before.keys(), "a re-run changed which files exist"
        for path, content in before.items():
            assert after[path] == content, f"{path.name} changed on re-run"

    def test_a_second_run_without_reset_does_nothing_at_all(
        self, store, sources, settings, parquet
    ):
        go(store, sources, settings, [parquet])
        second = go(store, sources, settings, [parquet])
        assert second.processed == []
        assert second.records_written == 0

    def test_rewriting_a_bucket_does_not_accumulate_rows(self, store, sources, settings, parquet):
        """The double-counting check, at the file level."""
        go(store, sources, settings, [parquet])
        paths = sorted(parquet.root.rglob("*.parquet"))
        counts_before = {p: pq.read_table(p).num_rows for p in paths}

        for _ in range(2):
            store.reset(SIGNAL)
            go(store, sources, settings, [parquet])

        for path, expected in counts_before.items():
            assert pq.read_table(path).num_rows == expected, f"{path.name} grew on re-run"


class TestNoFeedbackLoop:
    def test_the_source_never_reads_the_cold_tier_back_in(self, store, sources, settings, parquet):
        """Caught by running it: the first version's selector matched everything, so a
        second pass found `aea_rollup_edgeapp_requests_total_delta` in its own input and
        started rolling up its own rollups."""
        go(store, sources, settings, [parquet])

        seen_metrics = set()
        for path in parquet.root.rglob("*.parquet"):
            table = pq.read_table(path)
            seen_metrics.update(table.column("metric").to_pylist())

        offenders = {m for m in seen_metrics if m.startswith("aea_rollup_")}
        assert offenders == set(), f"rollups of rollups: {sorted(offenders)[:5]}"

    def test_cold_tier_rows_are_never_in_the_raw_read(self, sources, settings):
        from edgerollup.model import TimeRange

        now = SystemClock().now()
        window = TimeRange(now - timedelta(hours=2), now - settings.grace(SIGNAL) * 2)
        for record in sources[SIGNAL].read(window):
            assert not record.signal_kind.startswith("aea_rollup_")
            assert record.dims().get("tier") != "cold"


class TestTheRollupActuallyReduces:
    def test_output_rows_are_far_fewer_than_input_records(self, store, sources, settings, parquet):
        """The point of the whole exercise, asserted rather than assumed."""
        from edgerollup.model import TimeRange

        report = go(store, sources, settings, [parquet])
        if not report.processed:
            pytest.skip("nothing sealed to roll up")

        raw = sum(len(sources[SIGNAL].read(TimeRange(b.start, b.end))) for b in report.processed)
        rolled = report.records_written

        assert rolled > 0
        assert rolled < raw, f"no reduction: {rolled} rows from {raw} records"

    def test_every_raw_record_is_accounted_for_in_a_row(self, store, sources, settings, parquet):
        """Reduction must be aggregation, not loss.

        The summed `count` across rows has to equal the number of raw records that went
        in -- otherwise records were dropped somewhere between read and write.
        """
        from edgerollup.model import TimeRange

        report = go(store, sources, settings, [parquet])
        if not report.processed:
            pytest.skip("nothing sealed to roll up")

        for bucket in report.processed:
            raw = len(sources[SIGNAL].read(TimeRange(bucket.start, bucket.end)))
            path = parquet.path_for(SIGNAL, HOUR, bucket)
            table = pq.read_table(path)
            counted = sum(table.column("count").to_pylist())
            assert counted == raw, f"{bucket}: {counted} counted vs {raw} read"


class TestDualWriteAgainstRealSinks:
    def test_a_failing_second_sink_leaves_the_bucket_uncommitted(
        self, store, sources, settings, parquet
    ):
        """The dual-write rule, with a real Parquet write in front of the failure.

        Parquet lands, the second sink explodes, and the bucket must NOT be committed --
        so the next run redoes it in full rather than leaving the projection missing
        forever.
        """

        class Exploding(Sink):
            name = "exploding"

            def write(self, *args):
                raise RuntimeError("simulated sink outage")

        report = go(store, sources, settings, [parquet, Exploding()], max_buckets=1)

        assert report.processed == []
        assert report.failed, "the failure was not recorded"
        assert store.buckets(SIGNAL, COMMITTED) == []
        # Parquet did land -- and the retry will replace it atomically.
        assert list(parquet.root.rglob("*.parquet")), "the first sink should have written"

    def test_the_error_names_the_sink_that_had_already_succeeded(
        self, store, sources, settings, parquet
    ):
        class Exploding(Sink):
            name = "exploding"

            def write(self, *args):
                raise RuntimeError("boom")

        report = go(store, sources, settings, [parquet, Exploding()], max_buckets=1)
        _, message = report.failed[0]
        assert "parquet" in message
        assert "NOT committed" in message

    def test_the_retry_after_a_partial_write_succeeds_cleanly(
        self, store, sources, settings, parquet
    ):
        class Flaky(Sink):
            name = "flaky"

            def __init__(self):
                self.calls = 0

            def write(self, *args):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("transient")
                return 0

        flaky = Flaky()
        go(store, sources, settings, [parquet, flaky], max_buckets=1)
        second = go(store, sources, settings, [parquet, flaky], max_buckets=1)

        assert second.processed, "the bucket was not retried"
        assert store.buckets(SIGNAL, COMMITTED), "the retry did not commit"


class TestVictoriaMetricsProjection:
    def test_rollups_are_written_under_their_own_name_and_tier(
        self, store, sources, settings, parquet, tmp_path
    ):
        """Cold data must be impossible to confuse with raw (D-003)."""
        with httpx.Client(timeout=settings.http_timeout_seconds) as client:
            vm = VictoriaMetricsSink(settings.victoriametrics_url, client)
            report = go(store, sources, settings, [parquet, vm], max_buckets=2)
            if not report.processed:
                pytest.skip("nothing sealed to roll up")

            # Read back through the query API rather than trusting the write's 204.
            response = client.get(
                f"{settings.victoriametrics_url}/api/v1/export",
                params={
                    "match[]": '{__name__=~"aea_rollup_.*", tier="cold"}',
                    "start": report.processed[0].start.timestamp(),
                    "end": report.processed[-1].end.timestamp(),
                },
            )
            response.raise_for_status()
            assert response.text.strip(), "no cold-tier series readable after write"

    def test_an_unreachable_victoriametrics_is_a_sink_error(self, settings):
        """A connection refusal must surface as SinkError, not as a raw httpx error.

        Needs a real row: an empty bucket short-circuits before any request, so passing
        [] would test nothing. (It did, at first -- the assertion never fired.)
        """
        from edgerollup.model import TimeRange, canonical_dimensions
        from edgerollup.schema import RollupRow

        now = SystemClock().now().replace(minute=0, second=0, microsecond=0)
        bucket = TimeRange(now - HOUR.delta, now)
        row = RollupRow(
            signal=SIGNAL,
            granularity=HOUR.name,
            bucket_start=bucket.start,
            metric="probe_total",
            dimensions=canonical_dimensions({"service_name": "svc"}),
            count=1,
            sum=1.0,
            min=1.0,
            max=1.0,
            first=1.0,
            last=1.0,
            delta=1.0,
            extras=(("avg", 1.0), ("is_counter", 1.0)),
        )
        with httpx.Client(timeout=2.0) as client:
            vm = VictoriaMetricsSink("http://127.0.0.1:1", client)
            with pytest.raises(SinkError, match="import failed"):
                vm.write(SIGNAL, HOUR, bucket, [row])
