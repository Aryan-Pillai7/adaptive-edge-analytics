"""Trace aggregation, percentiles, and the error/ok partition."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from edgerollup.model import RawRecord, TimeRange, canonical_dimensions
from edgerollup.rollups.traces import (
    DEFAULT_PERCENTILES,
    METRIC_NAME,
    TracesRollup,
    percentile,
)
from edgerollup.sinks import VictoriaMetricsSink
from edgerollup.sources import TempoSource
from edgerollup.windows import Granularity

from .conftest import load_json, stub_client

HOUR = Granularity("1h", 3600)
DIMENSIONS = ("service_name", "root_service_name", "root_name", "status")
BUCKET = TimeRange(datetime(2026, 8, 27, 10, tzinfo=UTC), datetime(2026, 8, 27, 11, tzinfo=UTC))


def trace(duration: float, status: str = "ok", second: int = 0, root_name="GET /a") -> RawRecord:
    return RawRecord(
        signal="traces",
        timestamp=datetime(2026, 8, 27, 10, 0, second, tzinfo=UTC),
        dimensions=canonical_dimensions(
            {"root_service_name": "svc", "root_name": root_name, "status": status}
        ),
        value=duration,
        identity=f"{status}-{second}-{duration}-{root_name}",
        signal_kind="trace",
    )


class TestPercentile:
    def test_p50_of_an_odd_length_series_is_the_middle_value(self):
        assert percentile([1, 2, 3], 50) == 2

    def test_p0_and_p100_are_the_extremes(self):
        values = [10.0, 20.0, 30.0, 40.0]
        assert percentile(values, 0) == 10.0
        assert percentile(values, 100) == 40.0

    def test_interpolates_between_ranks(self):
        # rank = 3 * 0.5 = 1.5 -> halfway between 20 and 30
        assert percentile([10.0, 20.0, 30.0, 40.0], 50) == 25.0

    def test_a_single_value_is_every_percentile(self):
        """A one-trace bucket is a real and common case for a quiet service.

        `statistics.quantiles` raises below two data points, which is one of the reasons
        this is implemented directly.
        """
        for p in (0, 50, 99, 100):
            assert percentile([7.0], p) == 7.0

    def test_an_empty_series_is_zero_not_an_error(self):
        assert percentile([], 95) == 0.0

    def test_is_deterministic_for_the_same_multiset(self):
        """Byte-identical Parquet between runs depends on this."""
        values = [5.0, 1.0, 3.0, 1.0, 9.0]
        assert percentile(sorted(values), 90) == percentile(sorted(reversed(values)), 90)

    def test_percentiles_are_monotonic(self):
        values = sorted([3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0])
        results = [percentile(values, p) for p in (10, 25, 50, 75, 90, 99)]
        assert results == sorted(results)


class TestErrorPartition:
    """The asymmetry from F-008 is resolved in the source; the rollup must not redo it."""

    def setup_method(self):
        self.rollup = TracesRollup(DIMENSIONS)

    def test_error_and_ok_traces_land_in_separate_groups(self):
        rows = self.rollup.aggregate(HOUR, BUCKET, [trace(10, "ok"), trace(20, "error", second=1)])
        assert {r.dims()["status"] for r in rows} == {"ok", "error"}

    def test_the_groups_partition_the_bucket_exactly(self):
        """No trace counted twice, none dropped.

        This is the property that would break if grouping filtered by direct query while
        percentiles filtered by set difference -- the boundary case would land in both
        groups or neither.
        """
        records = [trace(i, "error" if i % 3 == 0 else "ok", second=i) for i in range(1, 13)]
        rows = self.rollup.aggregate(HOUR, BUCKET, records)
        assert sum(r.count for r in rows) == len(records)

    def test_percentiles_are_computed_within_a_status_group_not_across(self):
        """Each group's percentiles come from its own members only.

        Errors here are all slow and successes all fast; if the percentile were computed
        over the whole bucket both rows would show the same value.
        """
        records = [
            trace(1, "ok"),
            trace(2, "ok", second=1),
            trace(100, "error", second=2),
            trace(200, "error", second=3),
        ]
        rows = {r.dims()["status"]: r for r in self.rollup.aggregate(HOUR, BUCKET, records)}
        assert dict(rows["ok"].extras)["p50"] == 1.5
        assert dict(rows["error"].extras)["p50"] == 150.0

    def test_error_rate_is_derivable_from_the_counts(self):
        """Deliberately not stored: a derived ratio can silently disagree with the counts
        beside it, and computing it would need cross-group arithmetic -- the one thing
        that would reintroduce a second filtering path."""
        records = [
            trace(1, "error"),
            trace(1, "ok", second=1),
            trace(1, "ok", second=2),
            trace(1, "ok", second=3),
        ]
        rows = {r.dims()["status"]: r for r in self.rollup.aggregate(HOUR, BUCKET, records)}
        total = sum(r.count for r in rows.values())
        assert rows["error"].count / total == 0.25

    def test_a_bucket_of_only_errors_still_works(self):
        rows = self.rollup.aggregate(HOUR, BUCKET, [trace(5, "error")])
        assert len(rows) == 1
        assert rows[0].dims()["status"] == "error"
        assert rows[0].count == 1

    def test_a_missing_status_defaults_to_ok_rather_than_a_third_bucket(self):
        record = RawRecord(
            signal="traces",
            timestamp=BUCKET.start,
            dimensions=canonical_dimensions({"root_service_name": "svc", "root_name": "GET /a"}),
            value=3.0,
            identity="nostatus",
            signal_kind="trace",
        )
        rows = self.rollup.aggregate(HOUR, BUCKET, [record])
        assert rows[0].dims()["status"] == "ok"

    def test_an_unexpected_status_fails_loudly(self):
        """A third value would quietly become its own group, silently changing the
        denominator of every rate while each number still looked reasonable."""
        rows = [trace(1, "ok"), trace(1, "degraded", second=1)]
        with pytest.raises(ValueError, match="unexpected status"):
            self.rollup.aggregate(HOUR, BUCKET, rows)


class TestAggregation:
    def setup_method(self):
        self.rollup = TracesRollup(DIMENSIONS)

    def test_first_and_last_follow_time_not_duration(self):
        """ "The first trace in the bucket", not "the shortest"."""
        records = [trace(100, second=0), trace(5, second=1), trace(50, second=2)]
        row = self.rollup.aggregate(HOUR, BUCKET, records)[0]
        assert row.first == 100
        assert row.last == 50
        assert row.min == 5
        assert row.max == 100

    def test_traces_have_no_delta(self):
        row = self.rollup.aggregate(HOUR, BUCKET, [trace(10)])[0]
        assert row.delta == 0.0
        assert dict(row.extras)["is_counter"] == 0.0

    def test_the_metric_name_is_constant(self):
        row = self.rollup.aggregate(HOUR, BUCKET, [trace(10)])[0]
        assert row.metric == METRIC_NAME

    def test_different_root_names_stay_separate(self):
        rows = self.rollup.aggregate(
            HOUR, BUCKET, [trace(1, root_name="GET /a"), trace(1, root_name="GET /b", second=1)]
        )
        assert len(rows) == 2

    def test_configured_percentiles_are_emitted(self):
        rollup = TracesRollup(DIMENSIONS, percentiles=(75, 99))
        extras = dict(rollup.aggregate(HOUR, BUCKET, [trace(1), trace(2, second=1)])[0].extras)
        assert "p75" in extras and "p99" in extras
        assert "p50" not in extras

    def test_percentile_order_is_stable_regardless_of_config_order(self):
        """extras feed the Parquet file, which is compared byte-for-byte between runs."""
        forward = TracesRollup(DIMENSIONS, percentiles=(50, 90, 99))
        jumbled = TracesRollup(DIMENSIONS, percentiles=(99, 50, 90, 50))
        records = [trace(1), trace(2, second=1)]
        assert (
            forward.aggregate(HOUR, BUCKET, records)[0].extras
            == jumbled.aggregate(HOUR, BUCKET, records)[0].extras
        )

    def test_from_config_reads_percentiles(self):
        rollup = TracesRollup.from_config(DIMENSIONS, {"percentiles": [80]})
        assert rollup.percentiles == (80,)

    def test_from_config_falls_back_to_defaults(self):
        assert TracesRollup.from_config(DIMENSIONS, {}).percentiles == DEFAULT_PERCENTILES

    def test_an_empty_bucket_produces_no_rows(self):
        assert self.rollup.aggregate(HOUR, BUCKET, []) == []

    def test_aggregation_is_deterministic(self):
        records = [trace(3), trace(1, second=1), trace(2, "error", second=2)]
        assert self.rollup.aggregate(HOUR, BUCKET, records) == self.rollup.aggregate(
            HOUR, BUCKET, records
        )


class TestAgainstCapturedFixture:
    def test_aggregates_real_tempo_search_results(self):
        """End to end over recorded Tempo responses, including the two-search error split."""
        all_traces = load_json("tempo_search.json")
        errors = load_json("tempo_search_errors.json")

        def handler(request: httpx.Request) -> httpx.Response:
            query = request.url.params.get("q", "")
            return httpx.Response(200, json=errors if "status" in query else all_traces)

        source = TempoSource("http://tempo", stub_client(handler))
        moments = [
            datetime.fromtimestamp(int(t["startTimeUnixNano"]) // 10**9, tz=UTC)
            for t in all_traces["traces"]
        ]
        window = TimeRange(min(moments), max(moments) + HOUR.delta)
        records = source.read(window)

        rows = TracesRollup(DIMENSIONS).aggregate(HOUR, window, records)

        assert rows, "no rows from the fixture"
        # The partition, on real data: every trace counted exactly once.
        assert sum(r.count for r in rows) == len(records)
        assert {r.dims()["status"] for r in rows} <= {"ok", "error"}
        # The fixture has both errored and clean traces, so both must appear.
        assert {r.dims()["status"] for r in rows} == {"ok", "error"}
        for row in rows:
            extras = dict(row.extras)
            assert row.min <= extras["p50"] <= row.max
            assert extras["p50"] <= extras["p99"] <= row.max


class TestVictoriaMetricsProjectsPercentiles:
    def _capture(self):
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(204)

        return seen, VictoriaMetricsSink("http://vm", stub_client(handler))

    def test_percentiles_and_count_reach_the_queryable_tier(self):
        """Otherwise the trace rollup's headline numbers exist only in Parquet."""
        rows = TracesRollup(DIMENSIONS).aggregate(HOUR, BUCKET, [trace(1), trace(9, second=1)])
        seen, sink = self._capture()
        sink.write("traces", HOUR, BUCKET, rows)
        body = seen[0].content.decode()
        for suffix in ("_p50", "_p90", "_p95", "_p99", "_count", "_avg"):
            assert f"aea_rollup_{METRIC_NAME}{suffix}" in body, f"missing {suffix}"

    def test_status_is_carried_as_a_label(self):
        rows = TracesRollup(DIMENSIONS).aggregate(HOUR, BUCKET, [trace(1, "error")])
        seen, sink = self._capture()
        sink.write("traces", HOUR, BUCKET, rows)
        assert 'status="error"' in seen[0].content.decode()
