"""Trace rollups: latency distribution and error split per service per bucket.

## The error asymmetry is resolved once, upstream, and must not be re-derived here

Tempo can answer "which traces errored" directly (`{ status = error }`) but cannot answer
the complement: `{ status != error }` matches any trace with a non-error span, which on
this stack returned all 9 traces where the error query returned 6 (F-008). So non-error
traces are a set difference, not a query.

That asymmetry is handled **entirely in the source adapter**, which runs the two searches
and stamps every record with a plain `status` dimension of `error` or `ok`. By the time
records arrive here the asymmetry is gone: `status` is an ordinary dimension forming a
clean partition of the bucket's traces.

**So this module never filters by status.** It groups by it like any other dimension, and
percentiles are computed per group from that group's own members. There is exactly one
place that decides which traces are errors, so grouping and percentile computation cannot
disagree about the boundary case -- which they easily could if one filtered by direct
query and the other by set difference.

Error *rate* is deliberately not stored. It would need cross-group arithmetic, which is
the one thing that would reintroduce a second filtering path. Because the status groups
partition the population, the rate is a division at query time:

    error_count / (error_count + ok_count)

or in PromQL, `count{status="error"} / sum without(status)(count)`. Storing a derived
ratio would add a number that can silently disagree with the counts beside it.

## Percentiles are computed client-side

Tempo's TraceQL metrics API returns `empty ring` here -- the sibling deliberately disables
`metrics_generator`, because it would cost RAM to re-derive downstream exactly what its
thesis says should be reduced upstream. So there is no server-side shortcut, and the
percentiles come from sorting the bucket's durations in process.

Linear interpolation between neighbouring ranks, implemented directly rather than via
`statistics.quantiles`: that helper needs at least two data points and expresses cut
points rather than "the value at percentile p", so single-trace buckets and the exact
definition would both need working around anyway.
"""

from __future__ import annotations

import logging
from math import ceil, floor

from edgerollup.model import Dimensions, RawRecord, TimeRange
from edgerollup.rollups.base import Rollup
from edgerollup.schema import RollupRow
from edgerollup.windows import Granularity

log = logging.getLogger(__name__)

METRIC_NAME = "trace_duration_ms"

#: The only two values the source layer produces. A third would mean Tempo or the source
#: adapter changed, and it must not silently become an extra bucket.
VALID_STATUS = frozenset({"error", "ok"})

DEFAULT_PERCENTILES = (50, 90, 95, 99)


def percentile(sorted_values: list[float], p: float) -> float:
    """The value at percentile `p`, by linear interpolation between ranks.

    Deterministic for a given multiset of values -- ties cannot reorder the result,
    because only the sorted values are read. That matters: the same sealed window
    re-aggregated must produce byte-identical Parquet.
    """
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]

    rank = (len(sorted_values) - 1) * (p / 100.0)
    lower, upper = floor(rank), ceil(rank)
    if lower == upper:
        return sorted_values[int(rank)]
    weight = rank - lower
    return sorted_values[lower] + weight * (sorted_values[upper] - sorted_values[lower])


class TracesRollup(Rollup):
    signal = "traces"

    def __init__(
        self, dimensions: tuple[str, ...], percentiles: tuple[int, ...] = DEFAULT_PERCENTILES
    ) -> None:
        super().__init__(dimensions)
        # Sorted and deduplicated so the extras tuple has a stable order regardless of
        # how the config lists them -- extras feed the Parquet file, which is compared
        # byte-for-byte between runs.
        self.percentiles = tuple(sorted(set(percentiles)))

    @classmethod
    def from_config(cls, dimensions: tuple[str, ...], entry: dict) -> TracesRollup:
        configured = entry.get("percentiles") or DEFAULT_PERCENTILES
        return cls(dimensions, tuple(int(p) for p in configured))

    def grouping_key(self, record: RawRecord) -> tuple[str, Dimensions]:
        """Every trace rollup is the same measurement; the kind carries nothing useful.

        `status` is left to flow through as an ordinary dimension. It is NOT recomputed
        here -- see the module docstring on why there must be exactly one place that
        decides what an error is.
        """
        return METRIC_NAME, self.retain(record.dimensions, defaults={"status": "ok"})

    def aggregate(
        self, granularity: Granularity, bucket: TimeRange, records: list[RawRecord]
    ) -> list[RollupRow]:
        self._check_status_values(records, bucket)

        rows: list[RollupRow] = []
        for (metric, dimensions), group in self.group(records).items():
            # Sorted once, used for every percentile.
            durations = sorted(record.value for record in group)

            extras = [(f"p{p}", percentile(durations, p)) for p in self.percentiles]
            extras.append(("avg", sum(durations) / len(durations)))
            # Duration is a point-in-time measurement per trace, never cumulative.
            extras.append(("is_counter", 0.0))

            rows.append(
                RollupRow(
                    signal="traces",
                    granularity=granularity.name,
                    bucket_start=bucket.start,
                    metric=metric,
                    dimensions=dimensions,
                    # Trace throughput for this group. With the status partition, this
                    # is also what an error rate is computed from at query time.
                    count=len(durations),
                    sum=sum(durations),
                    min=durations[0],
                    max=durations[-1],
                    # From the time-ordered group, not the duration-sorted list: "the
                    # first trace in the bucket", not "the shortest".
                    first=group[0].value,
                    last=group[-1].value,
                    # No cumulative series to take an increase of.
                    delta=0.0,
                    extras=tuple(extras),
                )
            )

        rows.sort(key=lambda row: (row.metric, row.dimensions))
        return rows

    def _check_status_values(self, records: list[RawRecord], bucket: TimeRange) -> None:
        """Fail on a status the source layer should never produce.

        The error/ok split is a partition, and everything downstream -- including any
        error rate computed at query time -- depends on that. An unexpected third value
        would quietly become its own group, so the denominator of every rate silently
        changes while every individual number still looks reasonable.
        """
        seen = {value for record in records for key, value in record.dimensions if key == "status"}
        unexpected = seen - VALID_STATUS
        if unexpected:
            raise ValueError(
                f"traces {bucket.start.isoformat()}: unexpected status value(s) "
                f"{sorted(unexpected)}; expected only {sorted(VALID_STATUS)}. The "
                f"error/ok split must stay a partition or any error rate derived from "
                f"it is wrong."
            )
