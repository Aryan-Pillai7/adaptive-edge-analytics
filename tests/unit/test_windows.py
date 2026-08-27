"""Bucket alignment and the sealed/unsealed decision."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest

from edgerollup.model import TimeRange
from edgerollup.windows import (
    Granularity,
    bucket_containing,
    cold_start_floor,
    floor_to_bucket,
    is_sealed,
    sealed_buckets,
    watermark,
)

HOUR = Granularity("1h", 3600)
DAY = Granularity("1d", 86400)


def at(hour: int, minute: int = 0, second: int = 0, day: int = 27) -> datetime:
    return datetime(2026, 8, day, hour, minute, second, tzinfo=UTC)


class TestGranularity:
    def test_rejects_widths_that_drift_against_a_day(self):
        """A 7-minute bucket does not tile a day, so the Parquet date= partition would
        contain a bucket straddling midnight — belonging to two dates at once."""
        with pytest.raises(ValueError, match="divide evenly"):
            Granularity("7m", 420)

    @pytest.mark.parametrize("seconds", [60, 300, 900, 3600, 21600, 86400])
    def test_accepts_widths_that_tile_a_day(self, seconds):
        Granularity("ok", seconds)

    def test_rejects_non_positive(self):
        with pytest.raises(ValueError, match="must be positive"):
            Granularity("zero", 0)


class TestBucketAlignment:
    def test_buckets_are_anchored_to_the_epoch_not_to_now(self):
        """The property that makes checkpoints meaningful at all.

        The hourly bucket containing 10:37 is [10:00, 11:00) regardless of when the job
        runs. If buckets were measured backwards from "now", two runs at different times
        would produce overlapping, differently-shaped buckets covering the same data,
        and nothing could make that idempotent.
        """
        for minute in (0, 1, 37, 59):
            assert floor_to_bucket(at(10, minute), HOUR) == at(10, 0)

    def test_a_bucket_start_floors_to_itself(self):
        # Otherwise iterating bucket-to-bucket would drift or repeat.
        assert floor_to_bucket(at(10), HOUR) == at(10)

    def test_daily_buckets_land_on_midnight(self):
        assert floor_to_bucket(at(13, 45), DAY) == datetime(2026, 8, 27, tzinfo=UTC)

    def test_bucket_containing_is_half_open(self):
        bucket = bucket_containing(at(10, 30), HOUR)
        assert bucket == TimeRange(at(10), at(11))
        assert bucket.contains(at(10))
        assert not bucket.contains(at(11))

    def test_consecutive_buckets_tile_without_gap_or_overlap(self):
        first = bucket_containing(at(10, 30), HOUR)
        second = bucket_containing(at(11, 30), HOUR)
        assert first.end == second.start

    def test_rejects_naive_input(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            floor_to_bucket(datetime(2026, 8, 27, 10), HOUR)


class TestSealing:
    def test_a_bucket_is_judged_on_its_END_not_its_start(self):
        """Testing the start would seal a bucket while events were still landing in it.

        The result reads as a partial hour, which is indistinguishable from a quiet one.
        """
        bucket = TimeRange(at(10), at(11))
        grace = timedelta(minutes=10)
        # 10:30 is well past the bucket's start plus grace, but the bucket is still open.
        assert is_sealed(bucket, now=at(10, 30), grace=grace) is False

    def test_seals_exactly_at_end_plus_grace_and_not_a_second_before(self):
        bucket = TimeRange(at(10), at(11))
        grace = timedelta(minutes=10)
        assert is_sealed(bucket, now=at(11, 9, 59), grace=grace) is False
        assert is_sealed(bucket, now=at(11, 10, 0), grace=grace) is True

    def test_zero_grace_seals_the_instant_the_bucket_closes(self):
        bucket = TimeRange(at(10), at(11))
        assert is_sealed(bucket, now=at(11), grace=timedelta(0)) is True


class TestSealedBuckets:
    def test_yields_oldest_first(self):
        """Required, not stylistic: the frontier advances contiguously, so processing
        newer buckets first would leave it stuck behind a hole it had already passed."""
        got = list(sealed_buckets(HOUR, at(8), at(13), timedelta(0)))
        assert [b.start for b in got] == [at(8), at(9), at(10), at(11), at(12)]

    def test_stops_before_the_first_unsealed_bucket(self):
        got = list(sealed_buckets(HOUR, at(8), at(12, 5), timedelta(minutes=10)))
        # [11:00,12:00) needs now >= 12:10, so it is not included.
        assert [b.start for b in got] == [at(8), at(9), at(10)]

    def test_snaps_since_down_to_a_bucket_boundary(self):
        """A partial first bucket would look like a real one but cover less time, and
        after aggregation would be indistinguishable from a sparse hour."""
        got = list(sealed_buckets(HOUR, at(8, 37), at(11), timedelta(0)))
        assert got[0].start == at(8)
        assert got[0].duration == timedelta(hours=1)

    def test_produces_a_contiguous_tiling(self):
        got = list(sealed_buckets(HOUR, at(0), at(12), timedelta(0)))
        for earlier, later in pairwise(got):
            assert earlier.end == later.start

    def test_returns_nothing_when_nothing_is_sealed_yet(self):
        assert list(sealed_buckets(HOUR, at(10), at(10, 30), timedelta(minutes=10))) == []

    def test_respects_a_limit(self):
        got = list(sealed_buckets(HOUR, at(0), at(23), timedelta(0), limit=3))
        assert len(got) == 3


class TestWatermarkAndColdStart:
    def test_watermark_is_the_first_unsealed_bucket_start(self):
        assert watermark(at(11, 30), timedelta(minutes=10), HOUR) == at(11)

    def test_cold_start_is_bounded_and_aligned(self):
        floor = cold_start_floor(at(10, 30), HOUR, timedelta(hours=24))
        assert floor == at(10, 0, day=26)
        assert floor_to_bucket(floor, HOUR) == floor
