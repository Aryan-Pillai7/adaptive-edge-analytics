"""Metric aggregation. Pure functions, fixture data, nothing running."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from edgerollup.model import RawRecord, TimeRange, canonical_dimensions
from edgerollup.rollups.metrics import MetricsRollup, infer_metric_type, monotonic_delta
from edgerollup.schema import detect_drift
from edgerollup.sources import VictoriaMetricsSource
from edgerollup.windows import Granularity

from .conftest import load_text, stub_client, text_responder

HOUR = Granularity("1h", 3600)
DIMENSIONS = ("service_name", "route", "status")


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 8, 27, hour, minute, tzinfo=UTC)


def sample(value: float, minute: int, metric="edgeapp_requests_total", **dims) -> RawRecord:
    base = {"service_name": "svc", "route": "/a", "status": "2xx"}
    base.update(dims)
    return RawRecord(
        signal="metrics",
        timestamp=at(10, minute),
        dimensions=canonical_dimensions(base),
        value=value,
        identity=f"{metric}-{minute}-{sorted(base.items())}",
        signal_kind=metric,
    )


class TestMonotonicDelta:
    def test_a_plain_increase_is_last_minus_first(self):
        assert monotonic_delta([10, 20, 30]) == 20

    def test_a_single_sample_carries_no_increase(self):
        """Zero, not the value itself.

        A cumulative counter's value is a running total that mostly accrued in earlier
        buckets. Treating it as this bucket's increase would attribute the entire
        history of the process to whichever hour happened to hold one sample.
        """
        assert monotonic_delta([500]) == 0.0

    def test_no_samples_is_zero(self):
        assert monotonic_delta([]) == 0.0

    def test_a_reset_counts_the_post_reset_value_as_increase(self):
        """A process restart sends the counter to zero.

        `last - first` would report -70. Clamping that to zero would throw away the 30
        that happened after the restart. Both are wrong in ways that look plausible.
        """
        assert monotonic_delta([100, 120, 10, 30]) == 20 + 10 + 20

    def test_several_resets_in_one_bucket(self):
        assert monotonic_delta([50, 5, 20, 3]) == 5 + 15 + 3

    def test_a_flat_counter_has_zero_increase(self):
        assert monotonic_delta([7, 7, 7, 7]) == 0.0


class TestMetricTypeInference:
    @pytest.mark.parametrize(
        "name",
        [
            "edgeapp_requests_total",
            "http_server_duration_milliseconds_count",
            "http_server_duration_milliseconds_sum",
            "http_server_duration_milliseconds_bucket",
        ],
    )
    def test_prometheus_cumulative_suffixes_are_counters(self, name):
        assert infer_metric_type(name) == "counter"

    @pytest.mark.parametrize("name", ["http_server_active_requests", "queue_depth"])
    def test_everything_else_is_a_gauge(self, name):
        assert infer_metric_type(name) == "gauge"


class TestAggregation:
    def setup_method(self):
        self.rollup = MetricsRollup(DIMENSIONS)
        self.bucket = TimeRange(at(10), at(11))

    def aggregate(self, records):
        return self.rollup.aggregate(HOUR, self.bucket, records)

    def test_a_counter_reports_its_increase_not_the_sum_of_its_samples(self):
        """The headline correctness property of a metric rollup.

        Summing a cumulative counter's samples is the classic mistake here: three
        samples reading 10, 20, 30 sum to 60, which describes nothing. The increase is
        20.
        """
        rows = self.aggregate([sample(10, 0), sample(20, 20), sample(30, 40)])
        assert len(rows) == 1
        assert rows[0].delta == 20
        assert rows[0].sum == 60, "sum is still stored, it is just not the useful figure"

    def test_a_gauge_gets_no_delta(self):
        rows = self.aggregate(
            [
                sample(5, 0, metric="http_server_active_requests"),
                sample(9, 30, metric="http_server_active_requests"),
            ]
        )
        assert rows[0].delta == 0.0
        assert dict(rows[0].extras)["is_counter"] == 0.0
        assert dict(rows[0].extras)["avg"] == 7

    def test_records_are_ordered_before_first_last_and_delta(self):
        """Aggregation must not depend on the backend's response order.

        Fed in reverse, the result has to be identical -- otherwise `delta` becomes a
        property of how the data happened to be serialised.
        """
        forward = self.aggregate([sample(10, 0), sample(20, 20), sample(30, 40)])
        backward = self.aggregate([sample(30, 40), sample(20, 20), sample(10, 0)])
        assert forward[0].first == backward[0].first == 10
        assert forward[0].last == backward[0].last == 30
        assert forward[0].delta == backward[0].delta == 20

    def test_dimensions_outside_the_contract_are_dropped_and_collapsed(self):
        """The cardinality reduction, stated directly.

        Two raw series differing only by service_instance_id become ONE rollup series.
        Keeping that dimension would make cold-tier cardinality grow with restart count
        forever (D-005) -- the exact failure the upstream project exists to prevent.
        """
        rows = self.aggregate(
            [
                sample(10, 0, service_instance_id="uuid-a"),
                sample(20, 30, service_instance_id="uuid-b"),
            ]
        )
        assert len(rows) == 1
        assert "service_instance_id" not in rows[0].dims()
        assert rows[0].count == 2

    def test_different_retained_dimensions_stay_separate(self):
        rows = self.aggregate([sample(10, 0, route="/a"), sample(10, 0, route="/b")])
        assert len(rows) == 2
        assert {r.dims()["route"] for r in rows} == {"/a", "/b"}

    def test_different_metrics_stay_separate(self):
        rows = self.aggregate([sample(1, 0, metric="a_total"), sample(2, 0, metric="b_total")])
        assert {r.metric for r in rows} == {"a_total", "b_total"}

    def test_bucket_start_is_stamped_on_every_row(self):
        rows = self.aggregate([sample(1, 5)])
        assert rows[0].bucket_start == self.bucket.start

    def test_output_order_is_deterministic(self):
        """The idempotency test compares two runs' Parquet files byte-for-byte, so row
        order cannot depend on dict insertion order (which follows response order)."""
        records = [sample(1, 0, route=r) for r in ("/z", "/a", "/m")]
        first = [(r.metric, r.dimensions) for r in self.aggregate(records)]
        second = [(r.metric, r.dimensions) for r in self.aggregate(list(reversed(records)))]
        assert first == second

    def test_an_empty_bucket_produces_no_rows(self):
        assert self.aggregate([]) == []

    def test_aggregation_is_deterministic_across_repeated_calls(self):
        records = [sample(10, 0), sample(20, 30)]
        assert self.aggregate(records) == self.aggregate(records)


class TestAgainstCapturedFixture:
    def test_aggregates_real_exported_samples(self):
        """End-to-end over a recorded VictoriaMetrics response.

        Guards the seam between the read layer's `signal_kind` and the rollup's grouping
        -- a mismatch there would silently produce one row per sample.
        """
        body = load_text("vm_export.jsonl")
        source = VictoriaMetricsSource("http://vm", stub_client(text_responder(body)))
        series = [json.loads(x) for x in body.splitlines() if x.strip()]
        moments = [
            datetime.fromtimestamp(ts / 1000.0, tz=UTC) for s in series for ts in s["timestamps"]
        ]
        window = TimeRange(min(moments), max(moments) + (max(moments) - min(moments)))
        records = source.read(window)

        rows = MetricsRollup(DIMENSIONS).aggregate(HOUR, window, records)

        assert rows, "fixture produced no rollup rows"
        assert len(rows) < len(records), "aggregation did not reduce anything"
        assert sum(r.count for r in rows) == len(records), "records were lost or duplicated"
        for row in rows:
            assert row.metric == "edgeapp_requests_total"
            assert row.delta >= 0, "a counter reported a negative increase"


class TestDriftDetection:
    def test_a_missing_configured_dimension_is_reported(self):
        """The dangerous direction.

        Grouping silently collapses across an absent dimension, so a per-route rollup
        quietly becomes a single total and the number still looks plausible.
        """
        records = [sample(1, 0)]
        report = detect_drift(records, frozenset({"service_name", "route", "tenant"}))
        assert report.has_drift
        assert report.missing == {"tenant"}
        assert "tenant" in report.describe()

    def test_an_unexpected_dimension_is_reported_separately(self):
        records = [sample(1, 0, tenant="acme")]
        report = detect_drift(records, frozenset({"service_name", "route", "status"}))
        assert "tenant" in report.unexpected
        assert not report.missing

    def test_an_empty_bucket_is_not_drift(self):
        """Otherwise every quiet hour would raise a false alarm."""
        assert not detect_drift([], frozenset({"route"})).has_drift
