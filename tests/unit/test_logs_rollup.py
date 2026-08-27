"""Log aggregation, and the normalisation-order trap it has to avoid."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
import pytest

from edgerollup.model import RawRecord, TimeRange, canonical_dimensions
from edgerollup.rollups.base import Rollup
from edgerollup.rollups.logs import UNKNOWN_SEVERITY, LogsRollup
from edgerollup.schema import RollupRow
from edgerollup.sinks import LokiSink
from edgerollup.sinks.base import SinkError
from edgerollup.sources import LokiSource
from edgerollup.windows import Granularity
from edgerollup.writer import RollupWriter

from .conftest import json_responder, load_json, stub_client

HOUR = Granularity("1h", 3600)
DIMENSIONS = ("service_name", "deployment_environment", "severity_text")
BUCKET = TimeRange(datetime(2026, 8, 27, 10, tzinfo=UTC), datetime(2026, 8, 27, 11, tzinfo=UTC))


def entry(severity: str, events: float = 1.0, minute: int = 0, service="svc") -> RawRecord:
    return RawRecord(
        signal="logs",
        timestamp=datetime(2026, 8, 27, 10, minute, tzinfo=UTC),
        dimensions=canonical_dimensions(
            {"service_name": service, "deployment_environment": "local", "severity_text": severity}
        ),
        value=events,
        identity=f"{severity}-{minute}-{events}-{service}",
        signal_kind=severity,
    )


class TestSeverityNormalisationHappensBeforeGrouping:
    """The trap this phase exists to avoid.

    Normalising after grouping gives two groups (`ERROR`, `Error`) that both emit
    `severity="error"` -- two rows with an identical key, which is a duplicated row in
    Parquet and two samples at one timestamp in VictoriaMetrics, where the query layer
    picks one arbitrarily. The count silently halves and nothing raises.
    """

    def setup_method(self):
        self.rollup = LogsRollup(DIMENSIONS)

    def test_mixed_casing_collapses_into_one_row(self):
        rows = self.rollup.aggregate(
            HOUR,
            BUCKET,
            [entry("ERROR", 3), entry("Error", 4, minute=1), entry("error", 5, minute=2)],
        )
        assert len(rows) == 1, f"casing variants did not collapse: {[r.dims() for r in rows]}"
        assert rows[0].dims()["severity_text"] == "error"

    def test_the_collapsed_row_sums_every_variant(self):
        """The half-counting check. If normalisation happened after grouping, this row
        would carry only one variant's events."""
        rows = self.rollup.aggregate(
            HOUR,
            BUCKET,
            [entry("ERROR", 3), entry("Error", 4, minute=1), entry("error", 5, minute=2)],
        )
        assert rows[0].sum == 12
        assert rows[0].count == 3

    def test_distinct_severities_still_stay_apart(self):
        rows = self.rollup.aggregate(HOUR, BUCKET, [entry("ERROR", 1), entry("WARN", 2, minute=1)])
        assert {r.dims()["severity_text"] for r in rows} == {"error", "warn"}

    @pytest.mark.parametrize("blank", ["", "unknown", "UNKNOWN", "unspecified", "none"])
    def test_the_many_spellings_of_unknown_become_one(self, blank):
        """Loki says "unknown", OTel omits the field. Leaving them distinct would split
        one logical bucket into several."""
        rows = self.rollup.aggregate(HOUR, BUCKET, [entry(blank, 1)])
        if rows:
            assert rows[0].dims().get("severity_text", UNKNOWN_SEVERITY) == UNKNOWN_SEVERITY

    def test_whitespace_is_stripped(self):
        rows = self.rollup.aggregate(
            HOUR, BUCKET, [entry("  ERROR  ", 2), entry("error", 3, minute=1)]
        )
        assert len(rows) == 1
        assert rows[0].sum == 5


class TestEventCounting:
    def setup_method(self):
        self.rollup = LogsRollup(DIMENSIONS)

    def test_sum_is_events_represented_not_entries_stored(self):
        """The read layer sets value to dedup_count. Counting entries would undercount a
        flood by exactly the factor the upstream Collector achieved."""
        rows = self.rollup.aggregate(
            HOUR, BUCKET, [entry("error", 500), entry("error", 1, minute=1)]
        )
        assert rows[0].sum == 501, "events"
        assert rows[0].count == 2, "stored entries"

    def test_dedup_factor_is_recorded(self):
        rows = self.rollup.aggregate(
            HOUR, BUCKET, [entry("error", 100), entry("error", 0, minute=1)]
        )
        assert dict(rows[0].extras)["dedup_factor"] == 50.0

    def test_logs_have_no_delta(self):
        """There is no cumulative series to take an increase of."""
        rows = self.rollup.aggregate(HOUR, BUCKET, [entry("error", 5)])
        assert rows[0].delta == 0.0

    def test_services_stay_separate(self):
        rows = self.rollup.aggregate(
            HOUR, BUCKET, [entry("error", 1, service="a"), entry("error", 1, service="b")]
        )
        assert len(rows) == 2

    def test_an_empty_bucket_produces_no_rows(self):
        assert self.rollup.aggregate(HOUR, BUCKET, []) == []


class TestAgainstCapturedFixture:
    def test_aggregates_a_real_loki_response(self):
        payload = load_json("loki_query_range.json")
        source = LokiSource("http://loki", stub_client(json_responder(payload)))
        moments = [
            datetime.fromtimestamp(int(v[0]) // 10**9, tz=UTC)
            for s in payload["data"]["result"]
            for v in s["values"]
        ]
        window = TimeRange(min(moments), max(moments) + (max(moments) - min(moments) or HOUR.delta))
        records = source.read(window)

        rows = LogsRollup(DIMENSIONS).aggregate(HOUR, window, records)

        assert rows, "no rows from the fixture"
        assert sum(r.count for r in rows) == len(records), "entries lost or duplicated"
        # The captured fixture contains both ERROR and Error (F-009). The property is
        # that no two rows share a full KEY -- several rows may legitimately share a
        # severity if they differ by service, which the fixture does.
        keys = [(r.metric, r.dimensions) for r in rows]
        assert len(keys) == len(set(keys)), "two rows share a rollup key"

        severities = [r.dims()["severity_text"] for r in rows]
        assert all(s == s.lower() for s in severities), f"unfolded casing: {severities}"
        # Every row has one: a record with no severity at all is filled with `unknown`
        # rather than producing a row invisible to any severity filter.
        assert all(severities)


class NaiveLogsRollup(Rollup):
    """Deliberately wrong: groups on RAW severity, normalises afterwards.

    This is the bug the phase was warned about, written down so the guard that catches
    it is itself tested. Without a guard this produces plausible, halved numbers.
    """

    signal = "logs"

    def aggregate(self, granularity, bucket, records):
        rows = []
        for (kind, dimensions), group in self.group(records).items():
            fixed = canonical_dimensions(
                {k: (v.lower() if k == "severity_text" else v) for k, v in dimensions}
            )
            rows.append(
                RollupRow(
                    signal="logs",
                    granularity=granularity.name,
                    bucket_start=bucket.start,
                    metric="log_events",
                    dimensions=fixed,
                    count=len(group),
                    sum=sum(r.value for r in group),
                    min=0.0,
                    max=0.0,
                    first=0.0,
                    last=0.0,
                    delta=0.0,
                )
            )
            _ = kind
        return rows


class TestDuplicateKeyGuard:
    def test_normalising_after_grouping_is_caught_not_silently_accepted(self):
        """The guard exists precisely because this failure is otherwise invisible."""
        writer = RollupWriter(NaiveLogsRollup(DIMENSIONS), sinks=[])
        with pytest.raises(SinkError, match="more than one row"):
            writer("logs", HOUR, BUCKET, [entry("ERROR", 3), entry("Error", 4, minute=1)])

    def test_the_message_points_at_the_actual_cause(self):
        writer = RollupWriter(NaiveLogsRollup(DIMENSIONS), sinks=[])
        with pytest.raises(SinkError, match="normalised after grouping"):
            writer("logs", HOUR, BUCKET, [entry("ERROR", 1), entry("Error", 1, minute=1)])

    def test_the_correct_rollup_passes_the_same_guard(self):
        writer = RollupWriter(LogsRollup(DIMENSIONS), sinks=[])
        assert writer("logs", HOUR, BUCKET, [entry("ERROR", 3), entry("Error", 4, minute=1)]) == 1


def log_row(severity="error", events=12.0) -> RollupRow:
    return RollupRow(
        signal="logs",
        granularity="1h",
        bucket_start=BUCKET.start,
        metric="log_events",
        dimensions=canonical_dimensions({"service_name": "svc", "severity_text": severity}),
        count=3,
        sum=events,
        min=1.0,
        max=10.0,
        first=1.0,
        last=10.0,
        delta=0.0,
        extras=(("dedup_factor", 4.0), ("is_counter", 0.0)),
    )


class TestLokiSink:
    def _capture(self):
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(204)

        return seen, LokiSink("http://loki", stub_client(handler))

    def test_rollups_carry_the_cold_tier_labels(self):
        """`tier="cold"` is what keeps the source selector from reading this back in."""
        seen, sink = self._capture()
        sink.write("logs", HOUR, BUCKET, [log_row()])
        stream = json.loads(seen[0].content)["streams"][0]["stream"]
        assert stream["tier"] == "cold"
        assert stream["signal"] == "logs"
        assert stream["granularity"] == "1h"

    def test_grouping_dimensions_become_stream_labels(self):
        seen, sink = self._capture()
        sink.write("logs", HOUR, BUCKET, [log_row(severity="warn")])
        stream = json.loads(seen[0].content)["streams"][0]["stream"]
        assert stream["severity_text"] == "warn"
        assert stream["service_name"] == "svc"

    def test_the_entry_is_stamped_at_the_bucket_start(self):
        seen, sink = self._capture()
        sink.write("logs", HOUR, BUCKET, [log_row()])
        ts = json.loads(seen[0].content)["streams"][0]["values"][0][0]
        assert ts == str(int(BUCKET.start.timestamp()) * 1_000_000_000)

    def test_the_line_is_stable_json(self):
        """Loki deduplicates on exact line equality, so an unstable key order would make
        a retry look like a new entry and append instead of dedupe."""
        seen, sink = self._capture()
        sink.write("logs", HOUR, BUCKET, [log_row()])
        sink.write("logs", HOUR, BUCKET, [log_row()])
        first = json.loads(seen[0].content)["streams"][0]["values"][0][1]
        second = json.loads(seen[1].content)["streams"][0]["values"][0][1]
        assert first == second
        assert json.loads(first)["events"] == 12.0

    def test_one_stream_per_row(self):
        seen, sink = self._capture()
        sink.write("logs", HOUR, BUCKET, [log_row("error"), log_row("warn")])
        assert len(json.loads(seen[0].content)["streams"]) == 2

    def test_an_empty_bucket_issues_no_request(self):
        seen, sink = self._capture()
        assert sink.write("logs", HOUR, BUCKET, []) == 0
        assert seen == []

    def test_declares_that_it_does_not_replace_on_rewrite(self):
        _, sink = self._capture()
        assert sink.replaces_on_rewrite is False

    def test_an_http_failure_becomes_a_sink_error(self):
        sink = LokiSink("http://loki", stub_client(lambda r: httpx.Response(500, text="no")))
        with pytest.raises(SinkError, match="push failed"):
            sink.write("logs", HOUR, BUCKET, [log_row()])
