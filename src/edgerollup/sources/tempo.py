"""Tempo: trace summaries via /api/search.

Tempo is the awkward one, in three separate ways.

**Search matches OVERLAP, not containment.** `/api/search?start=&end=` returns any trace
with a span intersecting the window, not traces that *started* in it. Verified: splitting
a window returned one trace in both halves. So a trace straddling a bucket boundary
belongs to two buckets, and a naive read counts its latency twice.

The fix is to give every trace exactly one owning instant -- its ROOT SPAN start -- and
filter on that. Tempo's overlap matching is a superset of "started in this window", so
filtering client-side is both safe and sufficient: every trace we want is returned, and
the ones we do not want are discarded deterministically. A trace is then attributed to
the bucket it began in, which is also the only attribution that stays stable if the
trace is re-read later.

**Windows are in whole SECONDS.** Sub-second boundaries would be truncated by the API,
so the query window is widened outwards to second boundaries and the precise half-open
filter is applied by `Source.read()` against the real nanosecond start times. Widening
(never narrowing) keeps the result a superset.

**Silent truncation, again.** `limit` caps results with no flag. Detected the same way as
Loki, by asking for one more row than we accept.

One thing Tempo makes *easy*: error status comes from a second TraceQL search rather
than from fetching every trace body. `q={ status = error }` returns the traces having at
least one errored span, so an entire window costs two searches instead of one search
plus N trace fetches. That matters because trace fetches are the most expensive read
path in the job, and this is what keeps the trace rollup affordable.

Note that the complement is NOT a valid query: `{ status != error }` matches traces with
any non-error span, which on this stack returned all 9 traces while `{ status = error }`
returned 6. Non-error traces are computed by set difference, never by asking Tempo.
"""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime

from edgerollup.model import RawRecord, TimeRange, canonical_dimensions, stable_identity
from edgerollup.sources.base import Source, SourceError

log = logging.getLogger(__name__)

DEFAULT_ROW_LIMIT = 1000


class TempoSource(Source):
    signal = "traces"
    row_limit = DEFAULT_ROW_LIMIT

    def __init__(self, base_url: str, client, selector: str = "{}") -> None:
        super().__init__(base_url, client)
        # A TraceQL selector. "{}" means every trace.
        self.selector = selector

    def health(self) -> bool:
        try:
            response = self.client.get(f"{self.base_url}/api/echo")
            return response.status_code == 200
        except Exception:
            return False

    def _fetch(self, window: TimeRange) -> tuple[list[RawRecord], bool]:
        probe_limit = self.row_limit + 1

        # Widen outwards to whole seconds. Tempo truncates sub-second bounds, and
        # truncating `end` downwards would silently exclude traces in the final
        # fractional second of the window.
        start_s = math.floor(window.start.timestamp())
        end_s = math.ceil(window.end.timestamp())

        traces, truncated = self._search(self.selector, start_s, end_s, probe_limit)

        # Second search, not N trace fetches. See module docstring.
        error_query = (
            "{ status = error }"
            if self.selector == "{}"
            else (f"{self.selector.rstrip('}')} && status = error }}")
        )
        error_traces, error_truncated = self._search(error_query, start_s, end_s, probe_limit)
        error_ids = {t.get("traceID") for t in error_traces}

        records: list[RawRecord] = []
        for trace in traces:
            record = self._record_for_trace(trace, error_ids)
            if record is not None:
                records.append(record)

        return records, truncated or error_truncated

    def _search(self, query: str, start_s: int, end_s: int, limit: int) -> tuple[list[dict], bool]:
        response = self._get(
            "/api/search",
            {"q": query, "start": start_s, "end": end_s, "limit": limit},
        )
        payload = response.json()
        traces = payload.get("traces") or []
        return traces, len(traces) >= limit

    def _record_for_trace(self, trace: dict, error_ids: set) -> RawRecord | None:
        trace_id = trace.get("traceID")
        if not trace_id:
            log.warning("traces: search result with no traceID, skipping: %s", trace)
            return None

        raw_start = trace.get("startTimeUnixNano")
        if raw_start is None:
            # Without a start time the trace has no owning bucket, and guessing one
            # would attribute it arbitrarily -- the exact non-determinism that makes a
            # re-run produce different numbers.
            log.warning("traces: %s has no startTimeUnixNano, skipping", trace_id)
            return None

        try:
            start_nanos = int(raw_start)
        except (TypeError, ValueError) as exc:
            raise SourceError(f"traces: {trace_id} has unparseable start {raw_start!r}") from exc

        seconds, remainder = divmod(start_nanos, 1_000_000_000)
        timestamp = datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=remainder // 1_000)

        dimensions = canonical_dimensions(
            {
                "root_service_name": trace.get("rootServiceName") or "",
                "root_name": trace.get("rootTraceName") or "",
                # Derived from the error search, so it is a real dimension rather than
                # something a later stage has to re-derive from a duration.
                "status": "error" if trace_id in error_ids else "ok",
            }
        )

        # durationMs is absent on traces Tempo considers incomplete, which is a real
        # state near the write path rather than an error. Treated as zero-latency rather
        # than dropped: the trace still happened, and dropping it would understate
        # throughput while leaving percentiles unaffected anyway.
        duration_ms = float(trace.get("durationMs") or 0)

        return RawRecord(
            signal="traces",
            # The ROOT SPAN start. This is the single choice that makes trace reads
            # exactly-once across a boundary -- see module docstring.
            timestamp=timestamp,
            dimensions=dimensions,
            value=duration_ms,
            # A trace ID is globally unique by construction, so it needs no hashing.
            identity=stable_identity(trace_id),
            signal_kind="trace",
        )
