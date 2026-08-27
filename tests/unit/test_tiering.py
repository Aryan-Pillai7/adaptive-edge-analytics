"""Retention parsing, the safety-margin inequality, and the ratio measurement."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from edgerollup.config import Settings
from edgerollup.model import TimeRange, canonical_dimensions
from edgerollup.schema import RollupRow
from edgerollup.sinks import ParquetSink
from edgerollup.tiering import (
    BackendRetention,
    check_safety_margin,
    humanise,
    measure_ratio,
    parse_duration,
    probe_retentions,
)
from edgerollup.windows import Granularity

from .conftest import stub_client

HOUR = Granularity("1h", 3600)


class TestParseDuration:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("3d", timedelta(days=3)),
            ("72h", timedelta(hours=72)),
            ("2160h", timedelta(days=90)),
            ("30m", timedelta(minutes=30)),
            ("1w", timedelta(weeks=1)),
        ],
    )
    def test_parses_go_style_durations(self, text, expected):
        assert parse_duration(text) == expected

    def test_3d_and_72h_are_the_same_window(self):
        """The two configurable backends express the same retention differently.

        VictoriaMetrics reports `3d`, Loki reports `72h`. The audit compares them, so
        they must normalise to one value or it would report a mismatch that is not one.
        """
        assert parse_duration("3d") == parse_duration("72h")

    @pytest.mark.parametrize("text", ["", "forever", "3", "d", "3 days", None])
    def test_returns_none_rather_than_guessing(self, text):
        assert parse_duration(text) is None


class TestHumanise:
    def test_prefers_the_largest_whole_unit(self):
        assert humanise(timedelta(days=3)) == "3d"
        assert humanise(timedelta(hours=5)) == "5h"
        assert humanise(timedelta(minutes=20)) == "20m"

    def test_unknown_is_explicit_not_blank(self):
        assert humanise(None) == "unknown"


class TestSafetyMargin:
    """hot must exceed max_grace + job_interval + safety_margin."""

    def setup_method(self):
        self.settings = Settings(_env_file=None)

    def check(self, hot, interval=timedelta(hours=1), margin=timedelta(hours=2)):
        backends = [BackendRetention("test", hot=hot, cold=None, can_tier=False)]
        return check_safety_margin(backends, self.settings, interval, margin)[0]

    def test_ample_retention_passes(self):
        result = self.check(timedelta(days=3))
        assert result.ok
        assert "headroom" in result.detail

    def test_retention_shorter_than_the_requirement_fails(self):
        """The silent-data-loss condition: raw expires before the job reaches it, and
        the resulting empty bucket is indistinguishable from a quiet hour."""
        result = self.check(timedelta(hours=1))
        assert not result.ok
        assert "SHORT by" in result.detail

    def test_the_boundary_is_strict(self):
        # required = 20m grace + 1h interval + 2h margin = 3h20m
        required = timedelta(minutes=20) + timedelta(hours=1) + timedelta(hours=2)
        assert self.check(required).ok is False, "equal is not enough — it must exceed"
        assert self.check(required + timedelta(minutes=1)).ok is True

    def test_uses_the_WORST_grace_not_the_average(self):
        """Traces have the longest grace (20m). Using an average would understate the
        requirement for exactly the signal most at risk."""
        result = self.check(timedelta(hours=4))
        assert result.required == timedelta(minutes=20) + timedelta(hours=3)

    def test_unknown_retention_is_a_failure_not_a_pass(self):
        """A backend that will not say what it retains cannot be assumed safe."""
        result = self.check(None)
        assert not result.ok
        assert "unknown" in result.detail

    def test_a_longer_job_interval_raises_the_requirement(self):
        hourly = self.check(timedelta(days=1), interval=timedelta(hours=1))
        daily = self.check(timedelta(days=1), interval=timedelta(hours=24))
        assert daily.required > hourly.required
        assert hourly.ok and not daily.ok


class TestProbeRetentions:
    def _client(self, vm_body: str, loki_body: str):
        def handler(request: httpx.Request) -> httpx.Response:
            if "8428" in str(request.url):
                return httpx.Response(200, text=vm_body)
            return httpx.Response(200, text=loki_body)

        return stub_client(handler)

    def test_reads_victoriametrics_retention_from_its_flags(self):
        client = self._client('flag{name="retentionPeriod", value="3d", is_set="true"} 1', "")
        vm = next(
            b
            for b in probe_retentions(Settings(_env_file=None), client)
            if b.name == "victoriametrics"
        )
        assert vm.hot == timedelta(days=3)

    def test_victoriametrics_reports_that_it_cannot_tier(self):
        """F-019: a single global retention with no per-series override means cold
        rollups expire with the raw data. The report must say so rather than implying
        tiering works everywhere."""
        client = self._client('flag{name="retentionPeriod", value="3d", is_set="true"} 1', "")
        vm = next(
            b
            for b in probe_retentions(Settings(_env_file=None), client)
            if b.name == "victoriametrics"
        )
        assert vm.can_tier is False
        assert vm.cold == vm.hot, "cold cannot differ from hot when there is one setting"
        assert "expire WITH raw" in vm.note

    def test_reads_lokis_cold_stream_rule(self):
        loki_body = (
            "  retention_period: 3d\n"
            "  retention_stream:\n"
            "  - period: 90d\n"
            "    priority: 10\n"
            "    selector: '{tier=\"cold\"}'\n"
        )
        client = self._client("", loki_body)
        loki = next(
            b for b in probe_retentions(Settings(_env_file=None), client) if b.name == "loki"
        )
        assert loki.hot == timedelta(days=3)
        assert loki.cold == timedelta(days=90)
        assert loki.can_tier is True

    def test_loki_without_a_cold_rule_reports_that_it_cannot_tier(self):
        client = self._client("", "  retention_period: 3d\n")
        loki = next(
            b for b in probe_retentions(Settings(_env_file=None), client) if b.name == "loki"
        )
        assert loki.can_tier is False
        assert "expire with raw" in loki.note

    def test_an_unreachable_backend_reports_unknown_rather_than_crashing(self):
        def handler(request):
            raise httpx.ConnectError("refused")

        backends = probe_retentions(Settings(_env_file=None), stub_client(handler))
        assert {b.name for b in backends} == {"victoriametrics", "loki", "tempo"}
        assert next(b for b in backends if b.name == "loki").hot is None


def row(value: float = 1.0, count: int = 7) -> RollupRow:
    return RollupRow(
        signal="metrics",
        granularity="1h",
        bucket_start=datetime(2026, 8, 27, 10, tzinfo=UTC),
        metric="x_total",
        dimensions=canonical_dimensions({"service_name": "svc"}),
        count=count,
        sum=value,
        min=value,
        max=value,
        first=value,
        last=value,
        delta=value,
    )


class TestMeasureRatio:
    def _write(self, root, buckets: list[list[RollupRow]]):
        sink = ParquetSink(root)
        base = datetime(2026, 8, 27, 10, tzinfo=UTC)
        for index, rows in enumerate(buckets):
            bucket = TimeRange(base + HOUR.delta * index, base + HOUR.delta * (index + 1))
            sink.write("metrics", HOUR, bucket, rows)

    def test_counts_raw_records_from_the_stored_counts(self, tmp_path):
        """The raw total is recoverable from the archive alone -- no second backend read,
        and it stays correct after the raw data has expired."""
        self._write(tmp_path, [[row(count=10), row(count=5)]])
        ratio = measure_ratio("metrics", tmp_path)
        assert ratio.raw_records == 15
        assert ratio.rollup_rows == 2
        assert ratio.buckets == 1

    def test_reduction_is_raw_over_rows(self, tmp_path):
        self._write(tmp_path, [[row(count=100)]])
        assert measure_ratio("metrics", tmp_path).reduction == 100.0

    def test_empty_buckets_are_counted_separately(self, tmp_path):
        """A sparse signal's archive is mostly schema footers. Reporting one blended
        byte figure makes the rollup look ineffective when there was nothing to roll up."""
        self._write(tmp_path, [[row(count=4)], [], []])
        ratio = measure_ratio("metrics", tmp_path)
        assert ratio.buckets == 3
        assert ratio.empty_buckets == 2
        assert 0 < ratio.empty_bytes < ratio.cold_bytes

    def test_an_absent_archive_reports_zero_rather_than_failing(self, tmp_path):
        ratio = measure_ratio("metrics", tmp_path / "nothing-here")
        assert ratio.buckets == 0
        assert ratio.reduction == 0.0

    def test_reduction_of_an_empty_archive_does_not_divide_by_zero(self, tmp_path):
        self._write(tmp_path, [[]])
        assert measure_ratio("metrics", tmp_path).reduction == 0.0
