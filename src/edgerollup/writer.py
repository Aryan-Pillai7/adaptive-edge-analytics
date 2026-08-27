"""The processor: aggregate a bucket and write it to every sink.

## The dual-write rule

**Every sink must succeed before the bucket is committed.** A partial write -- Parquet
lands, VictoriaMetrics fails -- raises, so `pipeline.py` never reaches the commit, the
bucket stays uncommitted, and the next run redoes it in full. There is deliberately no
"committed except for one sink" state.

The alternative -- commit on partial success and reconcile later -- needs per-sink
checkpoints, a reconciliation pass, and a way to express "done, mostly". That is a real
design for a system with many sinks and expensive re-writes. Here there are two sinks and
re-writing a bucket costs one HTTP POST and one small file, so buying that complexity
would only add places for a bucket to get lost.

**Order is Parquet first, then VictoriaMetrics**, and the order is load-bearing rather
than incidental. Parquet is the authoritative copy and replaces atomically, so writing it
first means the durable record of a bucket exists before its queryable projection does.
The reverse would leave windows where VM shows a rollup that nothing authoritative backs.

## What a partial write actually leaves behind

Not all of it is cleaned up by the retry, and pretending otherwise would be the lie this
whole project is about avoiding:

* **Parquet succeeded, VM failed.** Fully recovered. The retry rewrites the Parquet file
  atomically (same bytes, same path) and writes VM for the first time.
* **VM succeeded, Parquet failed.** Cannot happen in this order -- but if the sink order
  is ever changed, VM would be left holding samples that the retry then duplicates,
  because VM does not replace on rewrite (measured; see sinks/victoriametrics.py). The
  duplicates are query-harmless when the values match, which they do for a deterministic
  re-aggregation of a sealed window.

Either way the error names exactly which sinks succeeded, so the operator can tell a
clean failure from one that left something behind, instead of inferring it from a stack
trace.
"""

from __future__ import annotations

import logging

from edgerollup.model import RawRecord, TimeRange
from edgerollup.rollups.base import Rollup
from edgerollup.schema import SCHEMA_VERSION, detect_drift
from edgerollup.sinks.base import Sink, SinkError
from edgerollup.windows import Granularity

log = logging.getLogger(__name__)


def writer_version(rollup_name: str) -> str:
    """The identity stored alongside every checkpoint (F-011).

    Includes the schema version, so bumping SCHEMA_VERSION re-opens every previously
    committed bucket rather than leaving a cold tier that mixes two output shapes.
    """
    return f"{rollup_name}-v{SCHEMA_VERSION}"


class RollupWriter:
    """Aggregates a bucket and writes it to all sinks, all-or-nothing.

    Callable with the `Processor` signature, so `pipeline.py` needs no knowledge of
    aggregation or sinks.
    """

    def __init__(self, rollup: Rollup, sinks: list[Sink], strict_drift: bool = False) -> None:
        self.rollup = rollup
        # Ordered. See the module docstring: authoritative copy first.
        self.sinks = list(sinks)
        #: Whether a missing configured dimension aborts the bucket rather than warning.
        #: Off by default -- a rollup that silently collapses a dimension is bad, but a
        #: pipeline that stops dead the first time upstream renames a label is worse, and
        #: the warning plus the recorded dimension set makes it visible either way.
        self.strict_drift = strict_drift

    def __call__(
        self,
        signal: str,
        granularity: Granularity,
        bucket: TimeRange,
        records: list[RawRecord],
    ) -> int:
        drift = detect_drift(records, self.rollup.dimension_set)
        if drift.has_drift:
            message = f"{signal}/{granularity.name} {bucket.start.isoformat()}: {drift.describe()}"
            if drift.missing and self.strict_drift:
                raise SinkError(f"schema drift (strict): {message}")
            log.warning("schema drift: %s", message)

        rows = self.rollup.aggregate(granularity, bucket, records)

        written: list[str] = []
        for sink in self.sinks:
            try:
                sink.write(signal, granularity, bucket, rows)
            except Exception as exc:
                # Name what did land. Whether a partial write left recoverable state
                # depends on which sinks succeeded, and that is not something anyone
                # should have to reconstruct from a traceback at 3am.
                raise SinkError(
                    f"{signal}/{granularity.name} {bucket.start.isoformat()}: "
                    f"sink {sink.name!r} failed after {written or 'no'} sink(s) succeeded "
                    f"-- bucket NOT committed, will be retried in full: {exc}"
                ) from exc
            written.append(sink.name)

        # Every sink returned, which by the Sink contract means every sink is durable.
        # Only now may pipeline.py commit.
        return len(rows)
