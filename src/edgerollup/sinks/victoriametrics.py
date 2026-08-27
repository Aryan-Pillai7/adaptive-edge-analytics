"""VictoriaMetrics: a queryable projection of the cold tier, not the authoritative copy.

## The measured limitation this design works around

**VictoriaMetrics is not idempotent on re-write.** Verified directly against the running
instance:

  * the same sample written three times is stored three times;
  * the same (series, timestamp) written with a *different* value stores a fourth sample
    and an instant query returned the OLD value, not the new one.

`-dedup.minScrapeInterval` is not set on this instance, and there is no delete API that
takes a time range -- `/api/v1/admin/tsdb/delete_series` removes a series across all
time, which would take out every other bucket's rollup along with this one.

So a rewrite here cannot be made to replace. What it can be is *harmless*: the query
layer returns a single value per timestamp, so a retry that writes identical values reads
back correctly, and the only cost is duplicate storage. That covers the case this
pipeline actually produces, because a retry re-runs deterministic aggregation over a
sealed window and therefore writes the same numbers.

What it does NOT cover is a reprocess whose values legitimately change -- after an
aggregation fix, say. There the stale sample can win. That is why Parquet is the
authoritative copy and this is a projection: correcting VM means deleting the affected
rollup metric outright and re-writing it from Parquet, which is a deliberate operation
rather than something a `--reprocess` does by accident.

## Naming

Rollups are written as `aea_rollup_<metric>_<aggregate>` with `tier="cold"`. The
aggregate is part of the NAME, not a label, for the same reason the tier is (D-003): a
careless `sum(aea_rollup_edgeapp_requests_total)` across an `agg` label would silently
add a max to a sum and produce a plausible, wrong number. Separate names make that
impossible rather than merely discouraged.

Only a curated subset of aggregates is projected here -- the full set always goes to
Parquet. Sending every aggregate for every series would multiply cold-tier cardinality
for aggregates nobody queries, in the tier whose entire purpose is being cheap.
"""

from __future__ import annotations

import logging

import httpx

from edgerollup.model import TimeRange
from edgerollup.schema import RollupRow
from edgerollup.sinks.base import Sink, SinkError
from edgerollup.windows import Granularity

log = logging.getLogger(__name__)

ROLLUP_PREFIX = "aea_rollup_"

# Which aggregates reach VictoriaMetrics, by inferred metric type. A counter's only
# meaningful hourly figure is its increase; a gauge's are its average and extremes.
COUNTER_AGGREGATES = ("delta",)
GAUGE_AGGREGATES = ("avg", "min", "max")

# Labels VM would reject or that would collide with its own.
_RESERVED = frozenset({"__name__", "job", "instance"})


class VictoriaMetricsSink(Sink):
    name = "victoriametrics"
    # Measured, not assumed. See the module docstring.
    replaces_on_rewrite = False

    def __init__(self, base_url: str, client: httpx.Client) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = client

    def write(
        self,
        signal: str,
        granularity: Granularity,
        bucket: TimeRange,
        rows: list[RollupRow],
    ) -> int:
        if not rows:
            return 0

        lines = []
        # Samples are stamped at the bucket's START. The alternative -- the end -- would
        # put an hour's summary at an instant outside the hour it describes, so a query
        # for [10:00,11:00) would miss the 10:00 bucket and pick up the 09:00 one.
        timestamp_ms = int(bucket.start.timestamp() * 1000)

        for row in rows:
            extras = dict(row.extras)
            is_counter = extras.get("is_counter", 0.0) >= 0.5
            wanted = COUNTER_AGGREGATES if is_counter else GAUGE_AGGREGATES

            for aggregate in wanted:
                value = self._value_for(row, aggregate, extras)
                if value is None:
                    continue
                lines.append(self._line(row, granularity, aggregate, value, timestamp_ms))

        if not lines:
            return 0

        body = "\n".join(lines)
        try:
            response = self.client.post(
                f"{self.base_url}/api/v1/import/prometheus", content=body.encode("utf-8")
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise SinkError(f"victoriametrics: import failed: {exc}") from exc

        return len(lines)

    @staticmethod
    def _value_for(row: RollupRow, aggregate: str, extras: dict[str, float]) -> float | None:
        if aggregate in extras:
            return extras[aggregate]
        return getattr(row, aggregate, None)

    def _line(
        self,
        row: RollupRow,
        granularity: Granularity,
        aggregate: str,
        value: float,
        timestamp_ms: int,
    ) -> str:
        name = f"{ROLLUP_PREFIX}{row.metric}_{aggregate}"
        labels = {key: val for key, val in row.dimensions if key not in _RESERVED}
        # Mandatory on every rolled-up series (D-003). `tier` in particular is what lets
        # a query say "cold only" without relying on the name prefix.
        labels["tier"] = "cold"
        labels["granularity"] = granularity.name
        labels["rollup_schema"] = str(row.schema_version)

        rendered = ",".join(f'{key}="{_escape(val)}"' for key, val in sorted(labels.items()))
        return f"{name}{{{rendered}}} {value!r} {timestamp_ms}"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
