"""The rollup output row, and detection of drift in the raw shape feeding it.

One row type for all three signals. The aggregates below (count/sum/min/max/first/last/
delta) are meaningful for any numeric series, and `extras` carries whatever a particular
signal needs on top -- trace percentiles, for instance -- without every signal needing
its own row type and its own sink.

`SCHEMA_VERSION` is the lever from F-011. It forms part of the writer version, so
bumping it re-opens every previously committed bucket rather than leaving a cold tier
that silently mixes two output shapes.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime

from edgerollup.model import Dimensions, RawRecord, stable_identity

log = logging.getLogger(__name__)

#: Bump when the aggregate set or its meaning changes incompatibly.
SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class RollupRow:
    """One aggregated series for one bucket."""

    signal: str
    granularity: str
    bucket_start: datetime
    metric: str
    dimensions: Dimensions

    count: int
    sum: float
    min: float
    max: float
    first: float
    last: float
    # Monotonic increase across the bucket, reset-aware. For a cumulative counter this
    # is the only aggregate that means anything -- see rollups/metrics.py.
    delta: float

    extras: tuple[tuple[str, float], ...] = ()
    schema_version: int = SCHEMA_VERSION

    @property
    def dimension_hash(self) -> str:
        """Stable identity of this row's grouping.

        Part of the rollup key (signal, granularity, bucket_start, dimension_hash), which
        is what makes a re-write overwrite rather than append.
        """
        return stable_identity(self.metric, self.dimensions)

    def dims(self) -> dict[str, str]:
        return dict(self.dimensions)


@dataclass
class DriftReport:
    """What changed about the raw data's shape since the contract was written."""

    missing: set[str] = field(default_factory=set)
    unexpected: Counter = field(default_factory=Counter)
    total_records: int = 0

    @property
    def has_drift(self) -> bool:
        return bool(self.missing or self.unexpected)

    def describe(self) -> str:
        parts = []
        if self.missing:
            parts.append(f"configured dimensions absent from raw data: {sorted(self.missing)}")
        if self.unexpected:
            top = ", ".join(f"{k} ({n})" for k, n in self.unexpected.most_common(5))
            parts.append(f"raw dimensions not in config: {top}")
        return "; ".join(parts)


def detect_drift(records: list[RawRecord], expected: frozenset[str]) -> DriftReport:
    """Compare the raw records' dimension keys against what the config expects.

    Two directions, and they mean different things:

    * **Missing** -- a dimension the config groups by is absent from the raw data. This
      is the dangerous one. Grouping silently collapses across it, so a rollup that used
      to be per-route becomes a single total, and the number still looks plausible. That
      is exactly the schema-drift failure this pipeline has to avoid, so it is reported
      loudly rather than absorbed.
    * **Unexpected** -- the raw data carries dimensions the config does not group by.
      Usually benign (upstream added a label) but worth surfacing, because it is how you
      find out that a dimension worth keeping has appeared.

    Reporting only, deliberately. The read and rollup layers must not silently reshape
    data to match an expectation; that would hide the drift instead of exposing it.
    """
    report = DriftReport(total_records=len(records))
    if not records:
        # No records is not drift. An empty sealed bucket is a legitimate answer, and
        # calling it drift would fire on every quiet hour.
        return report

    seen: Counter = Counter()
    for record in records:
        seen.update(key for key, _ in record.dimensions)

    report.missing = set(expected) - set(seen)
    report.unexpected = Counter({key: n for key, n in seen.items() if key not in expected})
    return report
