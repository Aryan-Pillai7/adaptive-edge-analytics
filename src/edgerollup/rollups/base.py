"""The Rollup contract, and the grouping every signal shares.

A rollup takes the raw records of one sealed bucket and returns the aggregated rows for
it. It performs no I/O and reads no clock, so given the same records it returns the same
rows -- which is the property the whole idempotency story rests on. If aggregation were
non-deterministic, keyed overwrites would still write, just something different each
time.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict

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

    @abstractmethod
    def aggregate(
        self, granularity: Granularity, bucket: TimeRange, records: list[RawRecord]
    ) -> list[RollupRow]:
        """Aggregate one bucket's records. Must be deterministic."""

    def group(self, records: list[RawRecord]) -> dict[tuple[str, Dimensions], list[RawRecord]]:
        """Group records by (metric, retained dimensions).

        Dimensions outside `self.dimensions` are dropped BEFORE grouping, which is what
        collapses many raw series into one rollup series. `service_instance_id` is the
        motivating case (D-005): keeping it would make cold-tier cardinality grow with
        restart count forever.

        Records are kept in timestamp order within each group. `first`, `last` and
        `delta` are order-dependent, and relying on the backend's response order would
        make those aggregates a property of how the data happened to be serialised.
        """
        grouped: dict[tuple[str, Dimensions], list[RawRecord]] = defaultdict(list)
        for record in records:
            retained = canonical_dimensions(
                {key: value for key, value in record.dimensions if key in self.dimension_set}
            )
            grouped[(record.signal_kind, retained)].append(record)

        for group in grouped.values():
            group.sort(key=lambda r: (r.timestamp, r.identity))
        return grouped
