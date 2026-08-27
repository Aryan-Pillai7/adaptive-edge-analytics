"""Log rollups: event counts by severity per bucket.

Not literal log-line aggregation. A rolled-up log tier answers "how much, of what kind,
when" -- the individual lines are what the hot tier is for and exactly what expires.

## Two normalisation concerns, and the order between them is load-bearing

Both land here, and getting the ORDER wrong produces a bug that looks like success.

1. **Severity is Loki structured metadata, not an indexed label.** It arrives merged into
   the stream object alongside real labels, so the read layer surfaces it as an ordinary
   dimension. Nothing more is needed here -- that was handled at the source.

2. **Severity casing is inconsistent in the raw data.** Captured fixtures contain both
   `ERROR` and `Error` (decisions.md F-009).

The trap is normalising *after* grouping. Group by raw severity and you get two groups,
`ERROR` and `Error`; normalise their labels afterwards and both emit `severity="error"`
for the same bucket. That is two rollup rows with an identical key -- which in Parquet is
a duplicated row and in VictoriaMetrics is two samples at one timestamp, where the query
layer returns one of them arbitrarily. The count silently halves, and nothing errors.

Exactly the shape of F-013: not a crash, just numbers that quietly stop meaning anything.

So all of it happens inside `grouping_key`, which the base class calls while BUILDING the
key -- before any grouping occurs. `ERROR` and `Error` land in one group and the sum is
right. `RollupWriter` additionally asserts that no two rows in a bucket share a key, so a
regression fails loudly instead of under-reporting.

There is a third variant of the same trap, found by that guard firing on the first run of
this rollup: the record's *kind* also carried the raw severity, so folding only the
dimension left two groups anyway. Hence one override covering kind, casing and defaults
together, rather than a hook per concern.

## What `value` means here

The read layer sets each record's value to `dedup_count`: how many log EVENTS the record
represents, not how many rows Loki returned. The upstream Collector collapses identical
records, so counting rows would undercount a flood by exactly the dedup factor. Therefore:

    sum   = events represented   <- the headline number
    count = Loki entries stored  <- how much storage those events occupied

The ratio between them is the upstream dedup factor, which is worth keeping.
"""

from __future__ import annotations

import logging

from edgerollup.model import Dimensions, RawRecord, TimeRange
from edgerollup.rollups.base import Rollup
from edgerollup.schema import RollupRow
from edgerollup.windows import Granularity

log = logging.getLogger(__name__)

# Dimensions whose values are normalised to lowercase before grouping. Severity is the
# one that matters; `detected_level` is Loki's own inference and varies the same way.
_CASE_INSENSITIVE = frozenset({"severity_text", "detected_level", "level"})

# Loki reports an unparsed level as "unknown"; OTel omits it entirely. Both mean the same
# thing, and leaving them distinct would split one logical bucket in two.
_UNKNOWN = frozenset({"", "unknown", "unspecified", "none"})
UNKNOWN_SEVERITY = "unknown"

#: Every log rollup row carries this as its metric. Severity lives in the dimensions,
#: where it can be grouped and filtered.
METRIC_NAME = "log_events"


class LogsRollup(Rollup):
    signal = "logs"

    def grouping_key(self, record: RawRecord) -> tuple[str, Dimensions]:
        """Everything logs-specific about grouping, in one place and before any grouping.

        Three things happen here, and all three must happen together or the group splits:

        * The kind is discarded. The read layer sets it to the raw severity, which is
          ALSO a dimension -- leaving it in the key would keep `ERROR` and `Error` apart
          even after the dimension itself was folded. (The duplicate-key guard caught
          exactly this on the first run of this rollup.)
        * Severity casing is folded, since the raw data carries both `ERROR` and `Error`
          (F-009).
        * A missing severity is filled with `unknown`, so "absent" and "unparsed" do not
          become two buckets.
        """
        folded = {key: self._fold(key, value) for key, value in record.dimensions}
        return METRIC_NAME, self.retain(folded, defaults={"severity_text": UNKNOWN_SEVERITY})

    @staticmethod
    def _fold(key: str, value: str) -> str:
        if key not in _CASE_INSENSITIVE:
            return value
        lowered = value.strip().lower()
        return UNKNOWN_SEVERITY if lowered in _UNKNOWN else lowered

    def aggregate(
        self, granularity: Granularity, bucket: TimeRange, records: list[RawRecord]
    ) -> list[RollupRow]:
        rows: list[RollupRow] = []

        for (metric, dimensions), group in self.group(records).items():
            # Events represented, not rows returned.
            events = [record.value for record in group]

            rows.append(
                RollupRow(
                    signal="logs",
                    granularity=granularity.name,
                    bucket_start=bucket.start,
                    metric=metric,
                    dimensions=dimensions,
                    count=len(group),
                    sum=sum(events),
                    min=min(events),
                    max=max(events),
                    first=events[0],
                    last=events[-1],
                    # Meaningless for logs: there is no cumulative series to take an
                    # increase of. Zero rather than omitted, so the row shape stays
                    # identical across signals and one sink handles all of them.
                    delta=0.0,
                    extras=(
                        # Events per stored entry. 1.0 means nothing was deduplicated
                        # upstream; higher means the Collector collapsed a flood, and
                        # the gap between the two is worth being able to see.
                        ("dedup_factor", sum(events) / len(group)),
                        ("is_counter", 0.0),
                    ),
                )
            )

        rows.sort(key=lambda row: (row.metric, row.dimensions))
        return rows
