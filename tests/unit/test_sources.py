"""Each adapter against a recorded response from the backend it talks to.

Windows are derived from the fixture's own contents rather than hardcoded, so
re-capturing fixtures against a newer backend does not require editing every test --
only the assertions about *shape* should ever need to change.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from edgerollup.model import TimeRange
from edgerollup.sources import LokiSource, TempoSource, VictoriaMetricsSource
from edgerollup.sources.base import SourceError
from edgerollup.sources.loki import DEFAULT_ROW_LIMIT, LOKI_MAX_ENTRIES

from .conftest import json_responder, stub_client, text_responder

# Comfortably brackets the synthetic epoch used by the hand-built payloads below
# (1787787000 == 2026-08-26T23:30Z). Named rather than repeated, because getting it
# wrong shows up as an empty result that looks exactly like a parsing failure.
WIDE = TimeRange(datetime(2026, 8, 26, tzinfo=UTC), datetime(2026, 8, 29, tzinfo=UTC))


def spanning_window(moments: list[datetime]) -> TimeRange:
    """A window that comfortably contains every given instant."""
    return TimeRange(min(moments) - timedelta(seconds=1), max(moments) + timedelta(seconds=1))


# ---------------------------------------------------------------- VictoriaMetrics


class TestVictoriaMetricsSource:
    def _read_all(self, vm_export_text: str, record: list | None = None):
        source = VictoriaMetricsSource(
            "http://vm", stub_client(text_responder(vm_export_text, record))
        )
        series = [json.loads(x) for x in vm_export_text.splitlines() if x.strip()]
        moments = [
            datetime.fromtimestamp(ts / 1000.0, tz=UTC) for s in series for ts in s["timestamps"]
        ]
        return source.read(spanning_window(moments)), series

    def test_returns_one_record_per_stored_sample(self, vm_export_text):
        """No resampling, no interpolation, no gaps.

        This is why the adapter uses /api/v1/export rather than /api/v1/query_range:
        query_range evaluates at fixed steps and looks backwards, so one stored sample
        can be returned at several step points while another is never returned at all.
        """
        records, series = self._read_all(vm_export_text)
        expected = sum(len(s["timestamps"]) for s in series)
        assert len(records) == expected
        assert expected > 100, "fixture too small to be meaningful — re-capture it"

    def test_identities_are_unique_per_sample(self, vm_export_text):
        records, _ = self._read_all(vm_export_text)
        assert len({r.identity for r in records}) == len(records)

    def test_drops_prometheus_duplicate_labels(self, vm_export_text):
        """`job` and `instance` are remote-write duplicates of service_name and
        service_instance_id. Carrying both would let a rollup group by the same thing
        twice under two different names."""
        records, _ = self._read_all(vm_export_text)
        for key in ("__name__", "job", "instance"):
            assert all(key not in r.dims() for r in records)

    def test_metric_name_is_kept_as_signal_kind(self, vm_export_text):
        records, _ = self._read_all(vm_export_text)
        assert {r.signal_kind for r in records} == {"edgeapp_requests_total"}

    def test_requests_the_exact_window_bounds(self, vm_export_text):
        seen: list[httpx.Request] = []
        self._read_all(vm_export_text, seen)
        params = seen[0].url.params
        assert "start" in params and "end" in params
        assert float(params["end"]) > float(params["start"])

    def test_refuses_to_guess_when_values_and_timestamps_disagree(self):
        """Positional correspondence is the entire contract of this response format.

        If it breaks, every sample is attributed to the wrong instant — so there is
        nothing safe to salvage, and a partial parse would be worse than an error.
        """
        broken = json.dumps(
            {"metric": {"__name__": "x"}, "values": [1, 2, 3], "timestamps": [1000, 2000]}
        )
        source = VictoriaMetricsSource("http://vm", stub_client(text_responder(broken)))
        with pytest.raises(SourceError, match="refusing to guess"):
            source.read(WIDE)

    def test_series_without_a_name_is_skipped_not_fatal(self):
        """One malformed series must not abort a run over thousands of healthy ones."""
        body = "\n".join(
            [
                json.dumps({"metric": {}, "values": [1], "timestamps": [1787787000000]}),
                json.dumps(
                    {"metric": {"__name__": "good"}, "values": [2], "timestamps": [1787787000000]}
                ),
            ]
        )
        source = VictoriaMetricsSource("http://vm", stub_client(text_responder(body)))
        records = source.read(WIDE)
        assert [r.signal_kind for r in records] == ["good"]


# ---------------------------------------------------------------------------- Loki


class TestLokiSource:
    def _read_all(self, loki_response: dict, record: list | None = None):
        source = LokiSource("http://loki", stub_client(json_responder(loki_response, record)))
        moments = [
            datetime.fromtimestamp(int(v[0]) // 10**9, tz=UTC)
            for s in loki_response["data"]["result"]
            for v in s["values"]
        ]
        return source.read(spanning_window(moments))

    def test_row_limit_stays_below_lokis_hard_cap(self):
        """Loki REJECTS `limit` above max_entries_limit_per_query with a 400 — it does
        not clamp. Since the truncation probe asks for row_limit + 1, row_limit must sit
        one below the cap or every single query fails. Found by running it."""
        assert DEFAULT_ROW_LIMIT + 1 <= LOKI_MAX_ENTRIES

    def test_reads_every_entry(self, loki_response):
        records = self._read_all(loki_response)
        expected = sum(len(s["values"]) for s in loki_response["data"]["result"])
        assert len(records) == expected

    def test_value_is_events_represented_not_rows_returned(self, loki_response):
        """The upstream log_dedup processor collapses identical records and reports the
        count in `dedup_count`. Counting rows would undercount a flood by exactly the
        dedup factor — the number the upstream project works hardest to make large."""
        records = self._read_all(loki_response)
        total_events = sum(r.value for r in records)
        assert total_events > len(records), (
            "fixture has no deduplicated records — re-capture after a flood, "
            "or this assertion proves nothing"
        )
        assert any(r.value >= 20 for r in records)

    def test_missing_dedup_count_means_one_event(self):
        payload = {
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"service_name": "svc"},
                        "values": [["1787787000000000000", "hello"]],
                    }
                ],
            }
        }
        source = LokiSource("http://loki", stub_client(json_responder(payload)))
        assert source.read(WIDE)[0].value == 1.0

    def test_severity_is_read_from_structured_metadata(self, loki_response):
        """Severity is NOT an indexed label here — Loki merges structured metadata into
        the stream object, and only service_name / deployment_environment /
        service_instance_id are actually indexed."""
        records = self._read_all(loki_response)
        severities = {r.dims().get("severity_text") for r in records}
        assert severities - {None}, "no severity found — the parse or the fixture is wrong"

    def test_per_record_noise_is_not_a_dimension(self, loki_response):
        """service_instance_id is a restart-scoped UUID (D-005); trace_id and order_id
        are per-record. All would make cold-tier cardinality unbounded."""
        records = self._read_all(loki_response)
        for key in ("service_instance_id", "trace_id", "span_id", "order_id"):
            assert all(key not in r.dims() for r in records), f"{key} leaked into dimensions"

    def test_identities_are_unique(self, loki_response):
        """Loki stores multiple entries at the same nanosecond, so timestamp alone is
        not an identity — the line and full label set are needed too."""
        records = self._read_all(loki_response)
        assert len({r.identity for r in records}) == len(records)

    def test_queries_forward_so_truncation_keeps_the_oldest(self, loki_response):
        """With Loki's default `backward` direction a truncated page keeps the NEWEST
        entries, so bisecting would recurse on halves whose oldest data was already
        gone."""
        seen: list[httpx.Request] = []
        self._read_all(loki_response, seen)
        assert seen[0].url.params["direction"] == "forward"

    def test_rejects_a_non_stream_result_type(self):
        payload = {"data": {"resultType": "matrix", "result": []}}
        source = LokiSource("http://loki", stub_client(json_responder(payload)))
        with pytest.raises(SourceError, match="resultType"):
            source.read(WIDE)


# --------------------------------------------------------------------------- Tempo


def tempo_client(all_payload: dict, error_payload: dict, record: list | None = None):
    """Route the two searches the adapter makes to their respective fixtures."""

    def handler(request: httpx.Request) -> httpx.Response:
        if record is not None:
            record.append(request)
        query = request.url.params.get("q", "")
        return httpx.Response(200, json=error_payload if "status" in query else all_payload)

    return stub_client(handler)


class TestTempoSource:
    def _read_all(self, tempo_search, tempo_search_errors, record=None):
        source = TempoSource(
            "http://tempo", tempo_client(tempo_search, tempo_search_errors, record)
        )
        moments = [
            datetime.fromtimestamp(int(t["startTimeUnixNano"]) // 10**9, tz=UTC)
            for t in tempo_search["traces"]
        ]
        return source.read(spanning_window(moments))

    def test_one_record_per_trace(self, tempo_search, tempo_search_errors):
        records = self._read_all(tempo_search, tempo_search_errors)
        assert len(records) == len(tempo_search["traces"])

    def test_trace_ids_are_unique(self, tempo_search, tempo_search_errors):
        records = self._read_all(tempo_search, tempo_search_errors)
        assert len({r.identity for r in records}) == len(records)

    def test_error_status_comes_from_the_second_search(self, tempo_search, tempo_search_errors):
        """Error rate without fetching a single trace body.

        `q={ status = error }` returns the traces with at least one errored span, so a
        window costs two searches rather than one search plus N trace fetches — and
        trace fetches are the most expensive read path in the job.
        """
        records = self._read_all(tempo_search, tempo_search_errors)
        error_ids = {t["traceID"] for t in tempo_search_errors["traces"]}
        expected = len(error_ids & {t["traceID"] for t in tempo_search["traces"]})
        actual = sum(1 for r in records if r.dims()["status"] == "error")
        assert actual == expected
        assert {r.dims()["status"] for r in records} <= {"ok", "error"}

    def test_never_asks_tempo_for_the_error_complement(self, tempo_search, tempo_search_errors):
        """`{ status != error }` matches traces with ANY non-error span, so on this
        stack it returned all 9 traces while `{ status = error }` returned 6. Non-error
        traces must be computed by difference, never queried."""
        seen: list[httpx.Request] = []
        self._read_all(tempo_search, tempo_search_errors, seen)
        assert not any("!=" in r.url.params.get("q", "") for r in seen)

    def test_timestamp_is_the_root_span_start(self, tempo_search, tempo_search_errors):
        """The single choice that makes trace reads exactly-once.

        Tempo search matches any trace OVERLAPPING the window, so a trace straddling a
        boundary is returned on both sides. Giving each trace one owning instant — where
        it began — and filtering on that is what resolves it.
        """
        records = self._read_all(tempo_search, tempo_search_errors)
        by_id = {t["traceID"]: t for t in tempo_search["traces"]}
        for raw in list(by_id.values())[:5]:
            expected_ns = int(raw["startTimeUnixNano"])
            match = next(r for r in records if r.identity == _identity_of(raw["traceID"]))
            assert abs(match.timestamp.timestamp() - expected_ns / 1e9) < 0.001

    def test_widens_the_query_window_to_whole_seconds(self, tempo_search, tempo_search_errors):
        """Tempo takes seconds and truncates. Truncating `end` downwards would silently
        exclude traces in the final fractional second, so the query widens outwards and
        the precise filter is applied client-side."""
        seen: list[httpx.Request] = []
        source = TempoSource("http://tempo", tempo_client(tempo_search, tempo_search_errors, seen))
        start = datetime(2026, 8, 27, 10, 0, 0, 500_000, tzinfo=UTC)
        end = datetime(2026, 8, 27, 11, 0, 0, 500_000, tzinfo=UTC)
        source.read(TimeRange(start, end))
        params = seen[0].url.params
        assert int(params["start"]) <= start.timestamp()
        assert int(params["end"]) >= end.timestamp()

    def test_trace_without_a_start_time_is_skipped(self, tempo_search_errors):
        """Without a start time a trace has no owning bucket, and guessing one would
        make a re-run produce different numbers."""
        payload = {"traces": [{"traceID": "abc"}, *tempo_search_errors["traces"][:1]]}
        source = TempoSource("http://tempo", tempo_client(payload, {"traces": []}))
        records = source.read(WIDE)
        assert len(records) == 1


def _identity_of(trace_id: str) -> str:
    from edgerollup.model import stable_identity

    return stable_identity(trace_id)
