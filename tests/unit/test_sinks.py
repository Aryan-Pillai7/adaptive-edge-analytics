"""Sinks, and the dual-write rule that governs writing to both."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pyarrow.parquet as pq
import pytest

from edgerollup.model import TimeRange, canonical_dimensions
from edgerollup.rollups.metrics import MetricsRollup
from edgerollup.schema import RollupRow
from edgerollup.sinks import ParquetSink, VictoriaMetricsSink
from edgerollup.sinks.base import Sink, SinkError
from edgerollup.windows import Granularity
from edgerollup.writer import RollupWriter, writer_version

from .conftest import stub_client

HOUR = Granularity("1h", 3600)
BUCKET = TimeRange(datetime(2026, 8, 27, 10, tzinfo=UTC), datetime(2026, 8, 27, 11, tzinfo=UTC))


def row(metric="edgeapp_requests_total", route="/a", delta=5.0, is_counter=1.0) -> RollupRow:
    return RollupRow(
        signal="metrics",
        granularity="1h",
        bucket_start=BUCKET.start,
        metric=metric,
        dimensions=canonical_dimensions({"service_name": "svc", "route": route}),
        count=3,
        sum=60.0,
        min=10.0,
        max=30.0,
        first=10.0,
        last=30.0,
        delta=delta,
        extras=(("avg", 20.0), ("is_counter", is_counter)),
    )


class TestParquetSink:
    def test_writes_a_hive_partitioned_file_per_bucket(self, tmp_path):
        sink = ParquetSink(tmp_path)
        sink.write("metrics", HOUR, BUCKET, [row()])

        expected = (
            tmp_path
            / "signal=metrics"
            / "granularity=1h"
            / "date=2026-08-27"
            / "bucket=1787824800.parquet"
        )
        assert expected.exists()

    def test_rewriting_replaces_rather_than_appends(self, tmp_path):
        """The property the whole retry story depends on.

        Appending would make every retry additive -- the double-counting failure this
        pipeline exists to prevent, arriving through the back door.
        """
        sink = ParquetSink(tmp_path)
        for _ in range(3):
            sink.write("metrics", HOUR, BUCKET, [row()])

        table = pq.read_table(sink.path_for("metrics", HOUR, BUCKET))
        assert table.num_rows == 1

    def test_two_identical_writes_produce_identical_bytes(self, tmp_path):
        """Byte-identical, not merely equivalent.

        If the same input produced different bytes, "did this run change anything?"
        would be unanswerable without parsing and comparing semantically.
        """
        first_root, second_root = tmp_path / "a", tmp_path / "b"
        rows = [row(route="/a"), row(route="/b")]
        ParquetSink(first_root).write("metrics", HOUR, BUCKET, rows)
        ParquetSink(second_root).write("metrics", HOUR, BUCKET, rows)

        first = ParquetSink(first_root).path_for("metrics", HOUR, BUCKET).read_bytes()
        second = ParquetSink(second_root).path_for("metrics", HOUR, BUCKET).read_bytes()
        assert first == second

    def test_an_empty_bucket_still_writes_a_file(self, tmp_path):
        """ "We looked and there was nothing" must be distinguishable from "never ran".

        It also matters for a reprocess: replacing a previously non-empty file with an
        empty one is how an upstream deletion propagates.
        """
        sink = ParquetSink(tmp_path)
        sink.write("metrics", HOUR, BUCKET, [])
        path = sink.path_for("metrics", HOUR, BUCKET)
        assert path.exists()
        assert pq.read_table(path).num_rows == 0

    def test_a_non_empty_bucket_can_be_replaced_by_an_empty_one(self, tmp_path):
        sink = ParquetSink(tmp_path)
        sink.write("metrics", HOUR, BUCKET, [row()])
        sink.write("metrics", HOUR, BUCKET, [])
        assert pq.read_table(sink.path_for("metrics", HOUR, BUCKET)).num_rows == 0

    def test_no_temp_files_are_left_behind(self, tmp_path):
        sink = ParquetSink(tmp_path)
        sink.write("metrics", HOUR, BUCKET, [row()])
        leftovers = list(tmp_path.rglob(".tmp-*"))
        assert leftovers == []

    def test_dimensions_round_trip_as_a_map(self, tmp_path):
        """A map column, not one column per label.

        A column per label would need a schema migration every time upstream adds one --
        schema drift showing up as an outage.
        """
        sink = ParquetSink(tmp_path)
        sink.write("metrics", HOUR, BUCKET, [row(route="/orders")])
        table = pq.read_table(sink.path_for("metrics", HOUR, BUCKET))
        dims = dict(table.column("dimensions")[0].as_py())
        assert dims == {"service_name": "svc", "route": "/orders"}


class TestVictoriaMetricsSink:
    def _capture(self):
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(204)

        return seen, VictoriaMetricsSink("http://vm", stub_client(handler))

    def test_a_counter_projects_only_its_delta(self):
        seen, sink = self._capture()
        sink.write("metrics", HOUR, BUCKET, [row(is_counter=1.0)])
        body = seen[0].content.decode()
        assert "aea_rollup_edgeapp_requests_total_delta" in body
        assert "_avg" not in body and "_min" not in body

    def test_a_gauge_projects_avg_min_max_but_no_delta(self):
        seen, sink = self._capture()
        sink.write("metrics", HOUR, BUCKET, [row(metric="active_requests", is_counter=0.0)])
        body = seen[0].content.decode()
        for aggregate in ("_avg", "_min", "_max"):
            assert f"aea_rollup_active_requests{aggregate}" in body
        assert "_delta" not in body

    def test_the_aggregate_is_part_of_the_name_not_a_label(self):
        """Same reasoning as the tier prefix (D-003).

        With an `agg` label, `sum(aea_rollup_x)` would happily add a max to a sum and
        return a plausible, wrong number. Distinct names make that impossible rather
        than merely discouraged.
        """
        seen, sink = self._capture()
        sink.write("metrics", HOUR, BUCKET, [row()])
        body = seen[0].content.decode()
        assert 'agg="' not in body

    def test_every_series_carries_the_mandatory_cold_tier_labels(self):
        seen, sink = self._capture()
        sink.write("metrics", HOUR, BUCKET, [row()])
        body = seen[0].content.decode()
        assert 'tier="cold"' in body
        assert 'granularity="1h"' in body
        assert 'rollup_schema="1"' in body

    def test_samples_are_stamped_at_the_bucket_start(self):
        """Stamping at the end would place an hour's summary outside the hour it
        describes, so a query for [10:00,11:00) would miss it and pick up 09:00's."""
        seen, sink = self._capture()
        sink.write("metrics", HOUR, BUCKET, [row()])
        assert str(int(BUCKET.start.timestamp() * 1000)) in seen[0].content.decode()

    def test_an_empty_bucket_issues_no_request(self):
        seen, sink = self._capture()
        assert sink.write("metrics", HOUR, BUCKET, []) == 0
        assert seen == []

    def test_declares_that_it_does_not_replace_on_rewrite(self):
        """Measured against the real instance: the same sample written three times is
        stored three times, and a changed value at the same timestamp did not win.
        The flag records that so callers cannot assume otherwise."""
        _, sink = self._capture()
        assert sink.replaces_on_rewrite is False
        assert ParquetSink("/tmp").replaces_on_rewrite is True

    def test_an_http_failure_becomes_a_sink_error(self):
        def handler(request):
            return httpx.Response(500, text="nope")

        sink = VictoriaMetricsSink("http://vm", stub_client(handler))
        with pytest.raises(SinkError, match="import failed"):
            sink.write("metrics", HOUR, BUCKET, [row()])


class BrokenSink(Sink):
    name = "broken"

    def write(self, signal, granularity, bucket, rows):
        raise RuntimeError("disk on fire")


class RecordingSink(Sink):
    name = "recording"

    def __init__(self):
        self.calls = []

    def write(self, signal, granularity, bucket, rows):
        self.calls.append(rows)
        return len(rows)


class TestDualWriteRule:
    """Every sink must succeed before the bucket may be committed."""

    def _writer(self, sinks):
        return RollupWriter(MetricsRollup(("service_name", "route")), sinks)

    def test_a_failing_sink_raises_so_the_bucket_is_never_committed(self, tmp_path):
        """The rule, stated directly.

        There is deliberately no "committed except for one sink" state. Raising here
        means pipeline.py never reaches commit, so the bucket is retried in full.
        """
        from .test_boundary_gate import SilentSource  # noqa: F401 - shared record shape

        writer = self._writer([RecordingSink(), BrokenSink()])
        with pytest.raises(SinkError):
            writer("metrics", HOUR, BUCKET, [])

    def test_the_error_names_which_sinks_had_already_succeeded(self, tmp_path):
        """Whether a partial write left recoverable state depends on which sinks landed,
        and that should not have to be reconstructed from a traceback."""
        ok = RecordingSink()
        writer = self._writer([ok, BrokenSink()])
        with pytest.raises(SinkError, match="recording"):
            writer("metrics", HOUR, BUCKET, [])

    def test_sinks_are_written_in_order(self, tmp_path):
        """Parquet first: the authoritative copy must land before its projection."""
        first, second = RecordingSink(), RecordingSink()
        second.name = "second"
        order = []
        first.write = lambda *a: order.append("first")  # type: ignore[assignment]
        second.write = lambda *a: order.append("second")  # type: ignore[assignment]
        self._writer([first, second])("metrics", HOUR, BUCKET, [])
        assert order == ["first", "second"]

    def test_the_first_sink_failing_means_the_second_is_never_attempted(self):
        never = RecordingSink()
        writer = self._writer([BrokenSink(), never])
        with pytest.raises(SinkError):
            writer("metrics", HOUR, BUCKET, [])
        assert never.calls == []

    def test_returns_the_row_count_only_when_every_sink_succeeded(self, tmp_path):
        writer = self._writer([ParquetSink(tmp_path), RecordingSink()])
        assert writer("metrics", HOUR, BUCKET, []) == 0


class TestWriterVersion:
    def test_includes_the_schema_version_so_a_bump_reopens_buckets(self):
        assert writer_version("metrics") == "metrics-v1"

    def test_differs_per_signal(self):
        assert writer_version("metrics") != writer_version("logs")
