"""Metric rollups: raw ~10s samples into hourly and daily aggregates.

## Why every aggregate is stored, and why `delta` exists

Most of what this stack emits is **cumulative**: `edgeapp_requests_total` only ever goes
up, and so do the `_sum`, `_count` and `_bucket` components of a histogram. Summing the
raw samples of a cumulative counter is the classic mistake in this area -- 374 samples
each reading around 22 sum to about 8,200, a number that describes nothing at all. The
only meaningful hourly figure for a counter is its **increase** over the bucket.

So `delta` is computed reset-aware: it walks the samples in time order and adds each
positive increment, treating any decrease as a process restart and counting the new
value as the increment from zero. That is the standard reading, and it is why the
records have to be sorted before aggregating rather than taken in response order.

For a gauge, `delta` is meaningless and `avg`/`min`/`max` are what matter. Rather than
guess the type and store only the "right" aggregate, every aggregate is computed and
stored -- it is one pass over the samples, and it means a question asked later can be
answered from the cold tier instead of from raw data that has expired.

## Type inference is a labelled guess, not a fact

VictoriaMetrics' export API does not return metric types. The Prometheus naming
convention (`_total`, `_count`, `_sum`, `_bucket` are cumulative) is the only signal
available, so it is used -- but the inference is RECORDED on the row (as the
`is_counter` extra) rather than silently deciding what gets stored. Every aggregate is
written to Parquet regardless of the guess, so a wrong inference costs a misleading
VictoriaMetrics projection, never an aggregate that cannot be reconstructed.
"""

from __future__ import annotations

import logging
from itertools import pairwise

from edgerollup.model import RawRecord, TimeRange
from edgerollup.rollups.base import Rollup
from edgerollup.schema import RollupRow
from edgerollup.windows import Granularity

log = logging.getLogger(__name__)

# Prometheus convention for cumulative series. `_bucket`, `_sum` and `_count` are the
# three components a histogram is exported as, and all three are cumulative.
CUMULATIVE_SUFFIXES = ("_total", "_count", "_sum", "_bucket")


def infer_metric_type(name: str) -> str:
    """ "counter" or "gauge", by naming convention. See the module docstring."""
    return "counter" if name.endswith(CUMULATIVE_SUFFIXES) else "gauge"


def monotonic_delta(values: list[float]) -> float:
    """Total increase across `values`, treating any decrease as a counter reset.

    A process restart sends a cumulative counter back to zero. Naive `last - first` would
    then report a large negative increase, and clamping it to zero would discard
    everything that happened after the restart. Summing positive increments and counting
    a post-reset value as an increment from zero is the standard reading and is what
    Prometheus' own `increase()` approximates.
    """
    if len(values) < 2:
        # A single sample carries no increase information. Zero, not the value itself:
        # the value is a cumulative total that mostly accrued in earlier buckets.
        return 0.0

    total = 0.0
    for previous, current in pairwise(values):
        if current >= previous:
            total += current - previous
        else:
            # Reset. Everything from zero up to `current` happened inside this bucket.
            total += current
    return total


class MetricsRollup(Rollup):
    signal = "metrics"

    def aggregate(
        self, granularity: Granularity, bucket: TimeRange, records: list[RawRecord]
    ) -> list[RollupRow]:
        rows: list[RollupRow] = []

        for (metric, dimensions), group in self.group(records).items():
            # Already sorted by Rollup.group -- first/last/delta depend on it.
            values = [record.value for record in group]
            metric_type = infer_metric_type(metric)

            rows.append(
                RollupRow(
                    signal="metrics",
                    granularity=granularity.name,
                    bucket_start=bucket.start,
                    metric=metric,
                    dimensions=dimensions,
                    count=len(values),
                    sum=sum(values),
                    min=min(values),
                    max=max(values),
                    first=values[0],
                    last=values[-1],
                    delta=monotonic_delta(values) if metric_type == "counter" else 0.0,
                    extras=(
                        ("avg", sum(values) / len(values)),
                        # 1 for counter, 0 for gauge. Carried as a number because the
                        # extras channel is numeric; the sink turns it back into a
                        # label. Recorded so a bad inference is visible rather than
                        # silently deciding which aggregates were worth keeping.
                        ("is_counter", 1.0 if metric_type == "counter" else 0.0),
                    ),
                )
            )

        # Deterministic order. Not cosmetic: the Parquet file is written whole and
        # compared byte-for-byte by the idempotency test, and dict iteration order would
        # otherwise depend on insertion order, which depends on the backend's response
        # order.
        rows.sort(key=lambda row: (row.metric, row.dimensions))
        return rows
