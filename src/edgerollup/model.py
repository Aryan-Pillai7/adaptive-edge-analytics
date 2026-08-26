"""The normalised shape that raw records take between reading and aggregating.

Three backends with three entirely different wire formats collapse to one record type
here. That is what lets `rollups/` stay pure and backend-agnostic: an aggregation cares
that a record has a timestamp, a set of dimensions and a numeric contribution, not that
it arrived as a Prometheus sample, a Loki stream entry or a Tempo trace summary.

Nothing in this module performs I/O or reads the clock.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta

Dimensions = tuple[tuple[str, str], ...]


def canonical_dimensions(raw: Mapping[str, str]) -> Dimensions:
    """Freeze a label mapping into a canonical, hashable, deterministically ordered form.

    Sorting is not cosmetic. Dimension tuples are hashed into record identities and, in
    Phase 3, into rollup output keys -- so two runs that saw the same labels in a
    different order must produce byte-identical results, or idempotency silently fails
    the first time a backend changes its serialisation order.

    Empty and None-valued labels are dropped rather than kept as "": a label that is
    absent and a label that is present-but-empty must not produce two distinct rollup
    series for what is the same thing.
    """
    return tuple(sorted((str(k), str(v)) for k, v in raw.items() if v not in (None, "")))


@dataclass(frozen=True, slots=True)
class TimeRange:
    """A half-open interval ``[start, end)``.

    Half-open is the whole of the double-counting defence at the seam between two runs:
    adjacent ranges share a boundary instant, and exactly one of them owns it. Every
    window in this pipeline is half-open, without exception -- including the ones handed
    to backends whose own APIs are not (see sources/victoriametrics.py).
    """

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        for name, value in (("start", self.start), ("end", self.end)):
            if value.tzinfo is None:
                raise ValueError(f"TimeRange.{name} must be timezone-aware (UTC)")
            if value.utcoffset() != timedelta(0):
                raise ValueError(f"TimeRange.{name} must be UTC, got offset {value.utcoffset()}")
        if self.start >= self.end:
            # An empty or inverted range is always a bug in the caller's window maths,
            # and it is far cheaper to find here than as a mysteriously empty bucket.
            raise ValueError(f"TimeRange requires start < end, got {self.start} .. {self.end}")

    def contains(self, moment: datetime) -> bool:
        """Half-open membership: start is in, end is not."""
        return self.start <= moment < self.end

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    def bisect(self) -> tuple[TimeRange, TimeRange]:
        """Split into two adjacent half-open ranges covering exactly the same span.

        Used to subdivide a query whose result hit a backend's row limit. Because the
        halves are half-open and share a boundary, re-querying them cannot double-count
        the record sitting on that boundary.
        """
        midpoint = self.start + self.duration / 2
        if midpoint <= self.start or midpoint >= self.end:
            raise ValueError(f"cannot bisect a range this small: {self}")
        return TimeRange(self.start, midpoint), TimeRange(midpoint, self.end)

    def __str__(self) -> str:
        return f"[{self.start.isoformat()} .. {self.end.isoformat()})"


@dataclass(frozen=True, slots=True)
class RawRecord:
    """One raw datum read from a backend, normalised.

    ``value`` is the record's numeric contribution to whatever is being aggregated, and
    it means something different per signal -- deliberately, because the alternative is
    three record types and three parallel pipelines:

      metrics  the sample value at ``timestamp``
      logs     how many log events this record represents. NOT always 1: the upstream
               Collector's log_dedup processor collapses identical records and reports
               the count in ``dedup_count``, so counting rows here would undercount a
               log flood by the entire dedup factor -- which is precisely the number
               the upstream project exists to make large.
      traces   the trace's wall-clock duration in milliseconds
    """

    signal: str
    # The instant that decides which bucket this record falls into. For traces this is
    # the ROOT SPAN start, not any other span's -- see sources/tempo.py for why that
    # choice is what makes trace reads exactly-once.
    timestamp: datetime
    dimensions: Dimensions
    value: float
    # Stable, unique per source record, and reproducible across runs. Set membership on
    # this is how the read gate proves two adjacent windows return each record exactly
    # once, so it must be derived from the record's own content -- never from its
    # position in a response or from anything time-of-read.
    identity: str
    signal_kind: str = field(default="")

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError("RawRecord.timestamp must be timezone-aware (UTC)")

    def dims(self) -> dict[str, str]:
        """Dimensions as a plain dict, for readability at call sites."""
        return dict(self.dimensions)

    def without_dimensions(self, drop: frozenset[str]) -> RawRecord:
        """Return a copy with some dimensions removed.

        This is how `service_instance_id` and friends are excluded (decisions.md D-005).
        It deliberately does NOT touch ``identity``: dropping a dimension must not make
        two distinct source records look like one, or a rollup that groups after the
        drop would lose the records it was supposed to sum.
        """
        kept = tuple((k, v) for k, v in self.dimensions if k not in drop)
        return RawRecord(
            signal=self.signal,
            timestamp=self.timestamp,
            dimensions=kept,
            value=self.value,
            identity=self.identity,
            signal_kind=self.signal_kind,
        )


def stable_identity(*parts: object) -> str:
    """A short, deterministic content hash.

    blake2b over the parts, not `hash()`: Python's builtin hash is randomised per
    process by PYTHONHASHSEED, so identities built from it would differ between the run
    that writes a rollup and the run that checks whether it already exists -- turning
    idempotency into a coin flip that passes locally and fails in CI.
    """
    digest = hashlib.blake2b(digest_size=16)
    for part in parts:
        digest.update(repr(part).encode("utf-8"))
        # Separator prevents ("ab","c") and ("a","bc") hashing identically.
        digest.update(b"\x00")
    return digest.hexdigest()
