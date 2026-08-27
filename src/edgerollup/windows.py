"""Bucket alignment and the sealed/unsealed decision.

Two rules do all the work here, and both exist to make a bucket mean the same thing no
matter when or how often the job runs.

**Buckets are aligned to the epoch, never to the job's start time.** The hourly bucket
containing 10:37 is [10:00, 11:00), whether the job wakes at 10:59 or 14:02, whether it
runs hourly or once a week. If buckets were measured from "now minus an hour", two runs
at different times would produce overlapping, differently-shaped buckets covering the
same data -- and no amount of checkpointing would make that idempotent, because there
would be no stable definition of the thing being checkpointed.

**A bucket is sealed only when `bucket_end + grace <= now`.** Grace is per-signal and
covers both halves of the delay between an event happening and it being readable: what
the writer has to finish (SDK export, ingester flush) AND what the reader has to notice
(VM's latency offset, Tempo's blocklist refresh). Those are separate buffers, and the
trace grace was measurably wrong when only the first was counted -- see decisions.md
F-006.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from edgerollup.model import TimeRange

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class Granularity:
    """A bucket width, e.g. 1h or 1d."""

    name: str
    seconds: int

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise ValueError(f"granularity {self.name!r} must be positive, got {self.seconds}")
        # Day-aligned buckets only make sense if a day divides evenly into them, and
        # anything that does not divide 86400 produces buckets that drift relative to
        # calendar days -- which makes the Parquet date= partition ambiguous.
        if 86400 % self.seconds != 0 and self.seconds % 86400 != 0:
            raise ValueError(
                f"granularity {self.name!r} ({self.seconds}s) does not divide evenly into "
                f"a day; buckets would drift against the date partition"
            )

    @property
    def delta(self) -> timedelta:
        return timedelta(seconds=self.seconds)

    @classmethod
    def from_config(cls, entry: dict) -> Granularity:
        return cls(name=str(entry["name"]), seconds=int(entry["seconds"]))


def floor_to_bucket(moment: datetime, granularity: Granularity) -> datetime:
    """The start of the bucket containing `moment`.

    Computed from the Unix epoch, using integer arithmetic. Deliberately NOT via
    `replace(minute=0, second=0)`: that only works for granularities that happen to
    align with clock fields, and it silently produces something plausible-but-wrong for
    anything else.
    """
    if moment.tzinfo is None:
        raise ValueError("floor_to_bucket requires a timezone-aware instant")
    elapsed = int((moment.astimezone(UTC) - EPOCH).total_seconds())
    # Floor division, so it behaves correctly for pre-epoch instants too rather than
    # truncating toward zero.
    aligned = (elapsed // granularity.seconds) * granularity.seconds
    return EPOCH + timedelta(seconds=aligned)


def bucket_containing(moment: datetime, granularity: Granularity) -> TimeRange:
    start = floor_to_bucket(moment, granularity)
    return TimeRange(start, start + granularity.delta)


def is_sealed(bucket: TimeRange, now: datetime, grace: timedelta) -> bool:
    """Whether `bucket` is safe to roll up.

    Note this tests the bucket's END, not its start. A bucket is only complete once the
    period it covers is over AND every backend has had time to make that period
    readable. Testing the start would seal a bucket while events were still landing in
    it, which reads as a partial result and is indistinguishable from a quiet period.
    """
    return bucket.end + grace <= now


def sealed_buckets(
    granularity: Granularity,
    since: datetime,
    now: datetime,
    grace: timedelta,
    limit: int | None = None,
) -> Iterator[TimeRange]:
    """Every sealed bucket at or after `since`, oldest first.

    Oldest first is required, not stylistic: the checkpoint frontier advances
    contiguously, so processing newer buckets before older ones would leave the frontier
    stuck behind a hole it has already passed over.

    `since` is snapped down to its bucket start. Anything else would produce a first
    bucket narrower than the granularity -- a partial bucket that looks like a real one
    and would be indistinguishable from a genuinely sparse hour once aggregated.
    """
    cursor = floor_to_bucket(since, granularity)
    produced = 0
    while True:
        bucket = TimeRange(cursor, cursor + granularity.delta)
        if not is_sealed(bucket, now, grace):
            return
        yield bucket
        produced += 1
        if limit is not None and produced >= limit:
            return
        cursor = bucket.end


def watermark(now: datetime, grace: timedelta, granularity: Granularity) -> datetime:
    """The start of the newest bucket that is *not* yet sealed.

    Everything strictly before this is eligible; everything at or after it must wait.
    Exposed mainly so `edge-rollup status` can show the boundary as a concrete instant
    rather than as a policy someone has to re-derive in their head.
    """
    return floor_to_bucket(now - grace, granularity)


def cold_start_floor(now: datetime, granularity: Granularity, max_backfill: timedelta) -> datetime:
    """Where to begin when there is no checkpoint at all.

    Bounded so a first run cannot try to scan every backend's entire retention window in
    one pass. Deliberate backfills go through `edge-rollup backfill`, where the operator
    states the range explicitly and can see what they are asking for.
    """
    return floor_to_bucket(now - max_backfill, granularity)
