"""VictoriaMetrics: raw samples via /api/v1/export.

Two things about this adapter are deliberate and are the reason it does not use the
obvious endpoint.

**Why /api/v1/export and not /api/v1/query_range.** `query_range` *resamples*: it
evaluates the query at fixed `step` intervals and, at each step, looks backwards for the
most recent sample. With a 15s raw resolution and any step at all, one raw sample can be
returned at several consecutive step points, and a sample that falls between step points
is never returned. For a rollup that must sum its input exactly once, a resampling read
is wrong in both directions at once. `export` streams the actual stored samples with
their real timestamps and does no interpolation.

Measured on this stack: `export` over a one-hour window returned 374 samples with 374
distinct timestamps -- one row per stored sample, which is what exactness requires.

**Why the results get filtered afterwards.** VictoriaMetrics treats `start` AND `end` as
INCLUSIVE. Verified directly: querying `[T, T]` returns the sample at T, and splitting a
window at T returned that sample in *both* halves. Left uncorrected, every window seam
in the pipeline would double-count exactly one sample per series -- a small, plausible,
permanent overcount. `Source.read()` applies the half-open filter that fixes it; this
adapter's job is to make sure the raw page is a superset, never a subset.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from edgerollup.model import RawRecord, TimeRange, canonical_dimensions, stable_identity
from edgerollup.sources.base import Source, SourceError

log = logging.getLogger(__name__)

# VictoriaMetrics adds these to every remote-written series. `job` and `instance` are
# Prometheus-convention duplicates of service_name / service_instance_id, and carrying
# both would let a rollup group by the same thing twice under two names.
_NOISE_LABELS = frozenset({"__name__", "job", "instance"})


class VictoriaMetricsSource(Source):
    signal = "metrics"

    def __init__(self, base_url: str, client, selector: str = '{__name__!=""}') -> None:
        super().__init__(base_url, client)
        # A PromQL series selector. Defaults to everything; the rollup config narrows it
        # in Phase 3. Kept as a plain string because that is what the API takes, and
        # building a selector DSL here would be inventing a language to avoid a string.
        self.selector = selector

    def health(self) -> bool:
        try:
            return self.client.get(f"{self.base_url}/health").status_code == 200
        except Exception:
            return False

    def _fetch(self, window: TimeRange) -> tuple[list[RawRecord], bool]:
        # Sent as float seconds. The end instant is passed through unchanged even though
        # VM will include it -- read() drops it. Subtracting an epsilon here instead
        # would work today and break the moment VM's resolution or rounding changes,
        # and it would hide the half-open rule inside an arithmetic trick.
        response = self._get(
            "/api/v1/export",
            {
                "match[]": self.selector,
                "start": window.start.timestamp(),
                "end": window.end.timestamp(),
            },
        )

        records: list[RawRecord] = []
        # Streaming JSON-lines, one object per series, NOT a single JSON document.
        for line in response.text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                series = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SourceError(f"metrics: unparseable export line: {line[:200]}") from exc
            records.extend(self._records_for_series(series))

        # `export` streams the whole matching set with no server-side row cap, so unlike
        # Loki and Tempo there is no truncation to detect. The one real limit is memory,
        # and an hourly window at this volume is nowhere near it.
        return records, False

    def _records_for_series(self, series: dict) -> list[RawRecord]:
        metric = series.get("metric") or {}
        name = metric.get("__name__")
        if not name:
            # A sample with no metric name cannot be rolled up into anything nameable.
            # Skipped rather than fatal: one malformed series should not abort a run
            # over thousands of healthy ones.
            log.warning("metrics: series with no __name__, skipping: %s", metric)
            return []

        values = series.get("values") or []
        timestamps = series.get("timestamps") or []
        if len(values) != len(timestamps):
            # Positional correspondence is the entire contract of this response format.
            # If it is broken, every sample in the series is attributed to the wrong
            # instant, so there is nothing safe to salvage.
            raise SourceError(
                f"metrics: {name} returned {len(values)} values for {len(timestamps)} "
                f"timestamps; refusing to guess the alignment"
            )

        dimensions = canonical_dimensions(
            {k: v for k, v in metric.items() if k not in _NOISE_LABELS}
        )
        # Hashed once per series, not once per sample: it is identical for every sample
        # in the series and this is the hot loop of the whole read path.
        series_key = stable_identity(name, dimensions)

        out: list[RawRecord] = []
        for value, ts_millis in zip(values, timestamps, strict=True):
            if value is None:
                continue
            out.append(
                RawRecord(
                    signal="metrics",
                    timestamp=datetime.fromtimestamp(ts_millis / 1000.0, tz=UTC),
                    dimensions=dimensions,
                    value=float(value),
                    # (series, exact stored timestamp) is unique by construction: a
                    # timeseries cannot hold two samples at one instant.
                    identity=f"{series_key}:{ts_millis}",
                    signal_kind=name,
                )
            )
        return out
