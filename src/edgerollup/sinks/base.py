"""The Sink contract.

Two properties every sink must provide, and they are what the checkpoint relies on:

**Durable on return.** `write` must not return until the data is safe. Not "issued", not
"buffered" -- safe. `pipeline.py` commits the bucket the moment the processor returns,
so a sink that returns early moves the commit before the write and reintroduces exactly
the failure D-010 exists to prevent.

**Replace, not append.** Writing the same bucket twice must leave the same state as
writing it once. A retry after a partial dual-write rewrites both sinks, and that is
only safe if rewriting is a no-op.

The second property is where the two sinks genuinely differ, and it is not papered over:
ParquetSink achieves it by construction (whole-partition atomic replace), while
VictoriaMetrics cannot -- see sinks/victoriametrics.py. That difference is the reason
Parquet is the authoritative copy and VM is a projection of it.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from edgerollup.model import TimeRange
from edgerollup.schema import RollupRow
from edgerollup.windows import Granularity


class SinkError(RuntimeError):
    """A sink could not durably store a bucket's rows."""


class Sink(ABC):
    #: Short name, used in error messages and in the partial-write report.
    name: str = ""

    #: Whether re-writing a bucket leaves the same state as writing it once. False means
    #: a retry can leave duplicate storage behind, which the writer surfaces rather than
    #: hides.
    replaces_on_rewrite: bool = True

    @abstractmethod
    def write(
        self,
        signal: str,
        granularity: Granularity,
        bucket: TimeRange,
        rows: list[RollupRow],
    ) -> int:
        """Durably store `rows`. Returns how many were written.

        Must not return until the data is safe.
        """
