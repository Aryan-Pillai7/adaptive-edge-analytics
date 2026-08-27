"""Parquet: the authoritative cold-tier copy.

Hive-partitioned by signal, granularity and date, one file per bucket:

    data/cold/signal=metrics/granularity=1h/date=2026-08-27/bucket=1787788800.parquet

One file per bucket is what makes this sink idempotent by construction. A bucket's
output is written to a temporary file, fsynced, and then atomically renamed over its
final path -- so a re-write REPLACES the bucket wholesale rather than appending to it,
and a crash mid-write leaves either the old complete file or no file, never a half-written
one. Appending to a shared file per day would make a retry additive, which is the
double-counting failure this pipeline is built to avoid.

It also means a reprocess that produces a *different* set of series for a bucket leaves
no orphans: the whole file is replaced, so series that no longer exist simply vanish.
VictoriaMetrics cannot offer that, which is the main reason this copy is the
authoritative one.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from edgerollup.model import TimeRange
from edgerollup.schema import RollupRow
from edgerollup.sinks.base import Sink, SinkError
from edgerollup.windows import Granularity

log = logging.getLogger(__name__)

SCHEMA = pa.schema(
    [
        ("signal", pa.string()),
        ("granularity", pa.string()),
        ("bucket_start", pa.timestamp("us", tz="UTC")),
        ("metric", pa.string()),
        ("dimension_hash", pa.string()),
        # Dimensions as a sorted key/value map rather than one column per label. A
        # column per label would need a schema migration every time upstream adds one,
        # which is the schema-drift failure mode showing up as an outage.
        ("dimensions", pa.map_(pa.string(), pa.string())),
        ("count", pa.int64()),
        ("sum", pa.float64()),
        ("min", pa.float64()),
        ("max", pa.float64()),
        ("first", pa.float64()),
        ("last", pa.float64()),
        ("delta", pa.float64()),
        ("extras", pa.map_(pa.string(), pa.float64())),
        ("schema_version", pa.int32()),
    ]
)


class ParquetSink(Sink):
    name = "parquet"
    replaces_on_rewrite = True

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def partition_dir(self, signal: str, granularity: Granularity, bucket: TimeRange) -> Path:
        return (
            self.root
            / f"signal={signal}"
            / f"granularity={granularity.name}"
            / f"date={bucket.start.strftime('%Y-%m-%d')}"
        )

    def path_for(self, signal: str, granularity: Granularity, bucket: TimeRange) -> Path:
        # Named by the bucket's epoch start: unique, sortable, and derivable from the
        # bucket alone, so a re-write always targets the same file.
        return self.partition_dir(signal, granularity, bucket) / (
            f"bucket={int(bucket.start.timestamp())}.parquet"
        )

    def write(
        self,
        signal: str,
        granularity: Granularity,
        bucket: TimeRange,
        rows: list[RollupRow],
    ) -> int:
        target = self.path_for(signal, granularity, bucket)
        target.parent.mkdir(parents=True, exist_ok=True)

        if not rows:
            # An empty sealed bucket is a real answer, and it has to be distinguishable
            # from "never processed". Writing an empty file with the right schema says
            # "we looked, there was nothing" -- and it also replaces any previous
            # non-empty file for this bucket, which a reprocess after an upstream
            # deletion depends on.
            table = SCHEMA.empty_table()
        else:
            table = pa.Table.from_pydict(
                {
                    "signal": [r.signal for r in rows],
                    "granularity": [r.granularity for r in rows],
                    "bucket_start": [r.bucket_start for r in rows],
                    "metric": [r.metric for r in rows],
                    "dimension_hash": [r.dimension_hash for r in rows],
                    "dimensions": [list(r.dimensions) for r in rows],
                    "count": [r.count for r in rows],
                    "sum": [r.sum for r in rows],
                    "min": [r.min for r in rows],
                    "max": [r.max for r in rows],
                    "first": [r.first for r in rows],
                    "last": [r.last for r in rows],
                    "delta": [r.delta for r in rows],
                    "extras": [list(r.extras) for r in rows],
                    "schema_version": [r.schema_version for r in rows],
                },
                schema=SCHEMA,
            )

        try:
            self._atomic_write(table, target)
        except OSError as exc:
            raise SinkError(f"parquet: could not write {target}: {exc}") from exc
        return len(rows)

    def _atomic_write(self, table: pa.Table, target: Path) -> None:
        """Write to a temp file in the same directory, fsync, then rename over target.

        Same directory because rename is only atomic within a filesystem. fsync before
        the rename because a rename that lands before the data reaches disk gives a
        file that exists and is empty after a power cut -- which is worse than no file,
        since the checkpoint would have been committed on the strength of it.
        """
        handle, temp_name = tempfile.mkstemp(dir=target.parent, prefix=".tmp-", suffix=".parquet")
        os.close(handle)
        temp_path = Path(temp_name)
        try:
            # Deterministic settings: the idempotency test compares two runs' files
            # byte-for-byte, and a timestamp or random id embedded in the file would
            # make identical data produce different bytes.
            pq.write_table(
                table,
                temp_path,
                compression="zstd",
                write_statistics=True,
                store_schema=True,
                coerce_timestamps="us",
                version="2.6",
            )
            # "r+b", not "rb": on Windows, fsync on a read-only handle fails with
            # EBADF. The read-only form works on Linux, which is exactly the kind of
            # difference that would have passed CI and failed on the dev machine.
            with open(temp_path, "r+b") as fh:
                fh.flush()
                os.fsync(fh.fileno())

            os.replace(temp_path, target)

            # fsync the directory too, or the rename itself may not survive a crash on
            # some filesystems. Not available on Windows, where the rename is already
            # ordered -- so a failure here is not fatal.
            try:
                dir_fd = os.open(target.parent, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except (OSError, AttributeError):
                pass
        finally:
            temp_path.unlink(missing_ok=True)
