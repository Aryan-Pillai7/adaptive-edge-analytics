"""The trace rollup end to end, against the real stack."""

from __future__ import annotations

from datetime import timedelta

import httpx
import pyarrow.parquet as pq
import pytest

from edgerollup.clock import SystemClock
from edgerollup.model import TimeRange
from edgerollup.pipeline import run_signal
from edgerollup.registry import dimensions_for
from edgerollup.rollups import TracesRollup
from edgerollup.sinks import ParquetSink, VictoriaMetricsSink
from edgerollup.sinks.base import Sink
from edgerollup.state import StateStore
from edgerollup.windows import Granularity
from edgerollup.writer import RollupWriter, writer_version

pytestmark = pytest.mark.integration

HOUR = Granularity("1h", 3600)
SIGNAL = "traces"


@pytest.fixture
def store(tmp_path) -> StateStore:
    with StateStore(tmp_path / "checkpoints.db") as s:
        yield s


@pytest.fixture
def parquet(tmp_path) -> ParquetSink:
    return ParquetSink(tmp_path / "cold")


def go(store, sources, settings, backfill_for, sinks: list[Sink], **kwargs):
    return run_signal(
        signal=SIGNAL,
        granularity=HOUR,
        source=sources[SIGNAL],
        store=store,
        clock=SystemClock(),
        grace=settings.grace(SIGNAL),
        max_backfill=backfill_for[SIGNAL],
        processor=RollupWriter(
            TracesRollup.from_config(dimensions_for(SIGNAL), {"percentiles": [50, 90, 95, 99]}),
            sinks,
        ),
        writer_version=writer_version(SIGNAL),
        **kwargs,
    )


def rows_of(parquet: ParquetSink):
    for path in parquet.root.rglob("*.parquet"):
        table = pq.read_table(path)
        for dims, count, low, high, extras in zip(
            table.column("dimensions").to_pylist(),
            table.column("count").to_pylist(),
            table.column("min").to_pylist(),
            table.column("max").to_pylist(),
            table.column("extras").to_pylist(),
            strict=True,
        ):
            yield dict(dims), count, low, high, dict(extras)


class TestErrorPartitionOnRealData:
    def test_every_trace_is_counted_exactly_once(
        self, store, sources, settings, parquet, backfill_for
    ):
        """The property the F-008 asymmetry could break.

        Errors are queryable directly, non-errors are a set difference. If grouping and
        percentile computation resolved that split independently, a trace on the boundary
        would land in both groups or neither. Resolving it once in the source layer is
        what makes this hold.
        """
        report = go(store, sources, settings, backfill_for, [parquet])
        if not report.processed:
            pytest.skip("no traces sealed to roll up")

        for bucket in report.processed:
            raw = len(sources[SIGNAL].read(TimeRange(bucket.start, bucket.end)))
            table = pq.read_table(parquet.path_for(SIGNAL, HOUR, bucket))
            counted = sum(table.column("count").to_pylist())
            assert counted == raw, f"{bucket}: {counted} counted vs {raw} traces read"

    def test_status_only_ever_takes_the_two_partition_values(
        self, store, sources, settings, parquet, backfill_for
    ):
        report = go(store, sources, settings, backfill_for, [parquet])
        if not report.processed:
            pytest.skip("no traces sealed to roll up")

        statuses = {dims.get("status") for dims, *_ in rows_of(parquet)}
        assert statuses, "no rows produced"
        assert statuses <= {"ok", "error"}, f"a third status appeared: {statuses}"
        assert None not in statuses

    def test_error_rate_is_computable_and_sane(
        self, store, sources, settings, parquet, backfill_for
    ):
        """Not stored -- derived from the counts, which is only valid because the status
        groups partition the population."""
        report = go(store, sources, settings, backfill_for, [parquet])
        if not report.processed:
            pytest.skip("no traces sealed to roll up")

        rows = list(rows_of(parquet))
        total = sum(count for _, count, *_ in rows)
        errors = sum(count for dims, count, *_ in rows if dims.get("status") == "error")
        if total == 0:
            pytest.skip("no traces in the window")
        assert 0.0 <= errors / total <= 1.0


class TestPercentilesOnRealData:
    def test_percentiles_lie_between_min_and_max(
        self, store, sources, settings, parquet, backfill_for
    ):
        report = go(store, sources, settings, backfill_for, [parquet])
        if not report.processed:
            pytest.skip("no traces sealed to roll up")

        rows = list(rows_of(parquet))
        assert rows, "no rows produced"
        for dims, _, low, high, extras in rows:
            for p in (50, 90, 95, 99):
                value = extras[f"p{p}"]
                assert low <= value <= high, f"p{p}={value} outside [{low}, {high}] for {dims}"

    def test_percentiles_are_monotonic_within_a_row(
        self, store, sources, settings, parquet, backfill_for
    ):
        report = go(store, sources, settings, backfill_for, [parquet])
        if not report.processed:
            pytest.skip("no traces sealed to roll up")

        for dims, _, _, _, extras in rows_of(parquet):
            ordered = [extras[f"p{p}"] for p in (50, 90, 95, 99)]
            assert ordered == sorted(ordered), f"percentiles out of order for {dims}"


class TestIdempotency:
    def test_running_twice_writes_identical_parquet_bytes(
        self, store, sources, settings, parquet, backfill_for
    ):
        first = go(store, sources, settings, backfill_for, [parquet])
        if not first.processed:
            pytest.skip("no traces sealed to roll up")
        before = {p: p.read_bytes() for p in sorted(parquet.root.rglob("*.parquet"))}

        store.reset(SIGNAL)
        go(store, sources, settings, backfill_for, [parquet])

        after = {p: p.read_bytes() for p in sorted(parquet.root.rglob("*.parquet"))}
        assert after.keys() == before.keys()
        for path, content in before.items():
            assert after[path] == content, f"{path.name} changed on re-run"

    def test_a_second_run_does_nothing(self, store, sources, settings, parquet, backfill_for):
        go(store, sources, settings, backfill_for, [parquet])
        assert go(store, sources, settings, backfill_for, [parquet]).processed == []


class TestVictoriaMetricsProjection:
    def test_the_rollup_is_readable_back_under_its_own_name(
        self, store, sources, settings, parquet, backfill_for
    ):
        """Unlike Loki (F-017), VictoriaMetrics does serve an old-timestamped write back
        immediately via export, so this one CAN be verified by reading it."""
        with httpx.Client(timeout=settings.http_timeout_seconds) as client:
            vm = VictoriaMetricsSink(settings.victoriametrics_url, client)
            report = go(store, sources, settings, backfill_for, [parquet, vm])
            if not report.records_written:
                pytest.skip("no trace rollup rows were written in this window")
            assert report.failed == []

            response = client.get(
                f"{settings.victoriametrics_url}/api/v1/export",
                params={
                    "match[]": '{__name__=~"aea_rollup_trace_duration_ms_.*", tier="cold"}',
                    "start": report.processed[0].start.timestamp(),
                    "end": report.processed[-1].end.timestamp(),
                },
            )
            response.raise_for_status()
            if not response.text.strip():
                pytest.skip("no trace rollups landed in this window")
            assert "p95" in response.text, "percentiles did not reach the queryable tier"

    def test_traces_are_never_written_back_to_tempo(self, settings):
        """Tempo has no host write path and rolled-up traces are not traces."""
        from edgerollup.config import load_rollup_config

        assert "tempo" not in load_rollup_config()["signals"]["traces"]["sinks"]


class TestNoFeedbackLoop:
    def test_tempo_cannot_contain_rollups(self, sources, settings):
        """Nothing is ever written to Tempo, so unlike metrics (F-013) there is no loop
        to exclude -- asserted rather than assumed, since it is the reason the traces
        selector has no cold-tier exclusion."""
        now = SystemClock().now()
        window = TimeRange(now - timedelta(hours=3), now - settings.grace(SIGNAL) * 2)
        records = sources[SIGNAL].read(window)
        if not records:
            pytest.skip("no raw data in the window — nothing to check for feedback")
        for record in records:
            assert not record.signal_kind.startswith("aea_rollup_")
            assert record.dims().get("tier") != "cold"
