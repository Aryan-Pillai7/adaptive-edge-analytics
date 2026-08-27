"""Loki: the cold-tier log rollup stream.

Rollups are pushed as a separate stream carrying `tier="cold"`, `signal="logs"` and the
granularity, alongside the grouping dimensions. Raw streams carry no `tier` label, so
`{tier="cold"}` and `{tier!="cold"}` separate the two cleanly -- which is what keeps the
source selector from reading this sink's output back in (F-013).

The line body is compact JSON holding the aggregates, so a LogQL query can extract any of
them with `| json` without a schema change here.

## The measured constraint: a write is not immediately queryable

An entry stamped more than ~45 minutes in the past is accepted (HTTP 204) but returns
NOTHING from a query issued seconds later. Entries stamped within the last ~45 minutes
are readable immediately.

It is not lost. Re-querying the same entries about five minutes later found all of them,
including one stamped three hours back -- the delay is the chunk flush plus index ship
(`chunk_idle_period: 2m` upstream), not a rejection. Measured twice, with `/ready`
healthy and no discard counters moving.

This is F-006's shape once more, and worth stating in those terms: when the writer
considers it done and when the reader can see it are different instants, and here it
applies to data *this pipeline itself writes*. It matters because a rollup is stamped at
its BUCKET START, which for an hourly job is always at least an hour old -- so every cold
log rollup lands in exactly the case that is briefly invisible.

**Consequently this sink cannot be verified by reading straight back.** A check that
writes and immediately queries reports a false failure, so the integration tests assert on
the accepted write plus the authoritative Parquet copy rather than on an immediate
round-trip. Parquet holding the authoritative copy is what makes the delay a non-issue.
"""

from __future__ import annotations

import json
import logging

import httpx

from edgerollup.model import TimeRange
from edgerollup.schema import RollupRow
from edgerollup.sinks.base import Sink, SinkError
from edgerollup.windows import Granularity

log = logging.getLogger(__name__)

# Loki label names must match [a-zA-Z_][a-zA-Z0-9_]*, and these would collide with the
# labels this sink sets itself.
_RESERVED = frozenset({"tier", "signal", "granularity", "rollup_schema"})


class LokiSink(Sink):
    name = "loki"
    # Loki drops an exact duplicate of (stream, timestamp, line) within a stream, so a
    # retry writing identical content does not accumulate. Values that CHANGE between
    # runs would append rather than replace, which is why Parquet stays authoritative --
    # the same conclusion as VictoriaMetrics, for a different underlying reason.
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

        # Nanoseconds, at the bucket START -- the instant the summary describes. Built
        # with integer arithmetic: float seconds cannot represent nanosecond precision
        # at current epoch values.
        timestamp_ns = str(int(bucket.start.timestamp()) * 1_000_000_000)

        # One Loki stream per distinct label set. Rows sharing a label set would collide
        # on (stream, timestamp), so the writer's duplicate-key guard upstream is what
        # makes this safe.
        streams: list[dict] = []
        for row in rows:
            labels = {key: value for key, value in row.dimensions if key not in _RESERVED}
            labels.update(
                {
                    "tier": "cold",
                    "signal": signal,
                    "granularity": granularity.name,
                    "rollup_schema": str(row.schema_version),
                }
            )
            streams.append(
                {
                    "stream": labels,
                    "values": [[timestamp_ns, self._line(row)]],
                }
            )

        try:
            response = self.client.post(
                f"{self.base_url}/loki/api/v1/push",
                headers={"Content-Type": "application/json"},
                json={"streams": streams},
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise SinkError(f"loki: push failed: {exc}") from exc

        return len(streams)

    @staticmethod
    def _line(row: RollupRow) -> str:
        """Compact, sorted JSON so identical input produces an identical line.

        Sorted because Loki deduplicates on exact line equality: an unstable key order
        would make a retry look like a different entry and append instead of dedupe.
        """
        body = {
            "events": row.sum,
            "entries": row.count,
            "min": row.min,
            "max": row.max,
            "bucket_start": row.bucket_start.isoformat(),
        }
        body.update({key: value for key, value in row.extras})
        return json.dumps(body, sort_keys=True, separators=(",", ":"))
