"""The Rollup contract, and the grouping every signal shares.

A rollup takes the raw records of one sealed bucket and returns the aggregated rows for
it. It performs no I/O and reads no clock, so given the same records it returns the same
rows -- the property the whole idempotency story rests on. If aggregation were
non-deterministic, keyed overwrites would still write, just something different each time.

## One extension point: `grouping_key`

Signals differ in how a record maps to a group, and there is exactly one place to say so.
That is deliberate, and it is the second shape this took. The first attempt grew a
separate hook per concern -- normalise a dimension value, normalise the kind, supply a
default for an absent dimension -- and each was added only after the previous one turned
out to be insufficient. Three hooks that must all agree is three chances for them to
disagree, and the failure when they disagree is silent: two groups collapsing to one
output key, which is a duplicated Parquet row and a halved count in VictoriaMetrics.

So everything deciding "which group does this record belong to" happens in one override,
and `RollupWriter` independently asserts that no two rows share a key.

**Whatever a subclass does here must happen BEFORE grouping, never after.** Normalising
an already-grouped result is the specific bug this design prevents: `ERROR` and `Error`
group separately, then both emit `severity="error"`, and the count silently halves with
nothing raised.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Mapping

from edgerollup.model import Dimensions, RawRecord, TimeRange, canonical_dimensions
from edgerollup.schema import RollupRow
from edgerollup.windows import Granularity


class Rollup(ABC):
    """Aggregates one signal's raw records into rollup rows."""

    #: Which signal this handles.
    signal: str = ""

    def __init__(self, dimensions: tuple[str, ...]) -> None:
        #: The dimension keys to group by. Everything else is discarded -- that is the
        #: cardinality reduction, and it is why the cold tier is small enough to keep.
        self.dimensions = tuple(dimensions)
        self.dimension_set = frozenset(dimensions)

    @classmethod
    def from_config(cls, dimensions: tuple[str, ...], entry: dict) -> Rollup:
        """Build from this signal's `rollup.yaml` entry.

        Default ignores the entry. Overridden where a signal needs more than its
        dimension list -- traces need their percentile set. Kept here rather than in the
        registry so that adding a signal with extra config does not require the registry
        to learn anything about it.
        """
        return cls(dimensions)

    @abstractmethod
    def aggregate(
        self, granularity: Granularity, bucket: TimeRange, records: list[RawRecord]
    ) -> list[RollupRow]:
        """Aggregate one bucket's records. Must be deterministic."""

    # --- grouping ---------------------------------------------------------------
    def grouping_key(self, record: RawRecord) -> tuple[str, Dimensions]:
        """Which (metric, dimensions) group this record belongs to.

        The default suits metrics: the kind is the metric name and genuinely
        distinguishes series, and dimensions are taken as they arrive. Signals whose raw
        data needs canonicalising override this -- see rollups/logs.py.
        """
        return record.signal_kind, self.retain(record.dimensions)

    def retain(
        self,
        dimensions: Mapping[str, str] | Dimensions,
        defaults: Mapping[str, str] | None = None,
    ) -> Dimensions:
        """Keep only the configured dimensions, filling in any that are absent.

        `defaults` exists because "the label is missing" and "the label is empty" must not
        become two different groups. A log record carrying no `severity_text` at all would
        otherwise produce a row with no severity dimension, sitting alongside the
        `unknown` row and invisible to any query filtering on severity.
        """
        source = dict(dimensions)
        merged = {key: source[key] for key in self.dimension_set if key in source}
        for key, value in (defaults or {}).items():
            if key in self.dimension_set and not merged.get(key):
                merged[key] = value
        return canonical_dimensions(merged)

    def group(self, records: list[RawRecord]) -> dict[tuple[str, Dimensions], list[RawRecord]]:
        """Group records by their grouping key.

        Records are kept in timestamp order within each group. `first`, `last` and `delta`
        are order-dependent, and relying on the backend's response order would make those
        aggregates a property of how the data happened to be serialised.
        """
        grouped: dict[tuple[str, Dimensions], list[RawRecord]] = defaultdict(list)
        for record in records:
            grouped[self.grouping_key(record)].append(record)

        for group in grouped.values():
            group.sort(key=lambda r: (r.timestamp, r.identity))
        return grouped
