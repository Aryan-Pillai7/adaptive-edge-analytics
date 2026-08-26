"""The two guarantees `Source.read()` enforces for every adapter.

These use a fake backend rather than fixtures, because the point is not "does this
parse" but "does the base class hold the line when a backend misbehaves". The fake is
built to misbehave in exactly the ways the real ones do: returning records outside the
requested window, and capping results without saying so.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from edgerollup.model import RawRecord, TimeRange
from edgerollup.sources.base import Source, SourceError, SourceTruncated


def at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 27, hour, minute, second, tzinfo=UTC)


def record(moment: datetime, identity: str, value: float = 1.0) -> RawRecord:
    return RawRecord(signal="fake", timestamp=moment, dimensions=(), value=value, identity=identity)


class FakeSource(Source):
    """A backend that behaves as badly as the real ones.

    `inclusive_end` reproduces VictoriaMetrics: the record sitting exactly on the
    window's end instant is returned as well. `row_limit` reproduces Loki and Tempo:
    responses are capped with no flag saying they were.
    """

    signal = "fake"

    def __init__(self, records: list[RawRecord], row_limit: int = 1000, inclusive_end=True):
        super().__init__("http://fake", client=None)  # type: ignore[arg-type]
        self.all_records = records
        self.row_limit = row_limit
        self.inclusive_end = inclusive_end
        self.windows_fetched: list[TimeRange] = []

    def _fetch(self, window: TimeRange) -> tuple[list[RawRecord], bool]:
        self.windows_fetched.append(window)
        if self.inclusive_end:
            matched = [r for r in self.all_records if window.start <= r.timestamp <= window.end]
        else:
            matched = [r for r in self.all_records if window.contains(r.timestamp)]
        matched.sort(key=lambda r: r.timestamp)
        page = matched[: self.row_limit]
        return page, len(page) >= self.row_limit


class TestHalfOpenEnforcement:
    def test_record_on_the_end_boundary_is_excluded(self):
        """VictoriaMetrics' inclusive end, corrected.

        Verified against the real backend: querying [T, T] returns the sample at T, and
        splitting a window at T returned it in BOTH halves. Uncorrected, every window
        seam permanently overcounts by one sample per series.
        """
        source = FakeSource([record(at(10), "a"), record(at(11), "boundary")])
        got = source.read(TimeRange(at(10), at(11)))
        assert [r.identity for r in got] == ["a"]

    def test_adjacent_windows_return_each_record_exactly_once(self):
        """The read gate, in miniature."""
        records = [record(at(10) + timedelta(minutes=m), f"r{m}") for m in range(0, 120, 5)]
        source = FakeSource(records)

        whole = source.read(TimeRange(at(10), at(12)))
        left = source.read(TimeRange(at(10), at(11)))
        right = source.read(TimeRange(at(11), at(12)))

        left_ids = {r.identity for r in left}
        right_ids = {r.identity for r in right}

        assert left_ids & right_ids == set(), "a record appeared in both windows"
        assert left_ids | right_ids == {r.identity for r in whole}
        assert len(left) + len(right) == len(whole)


class TestTruncationHandling:
    def test_a_capped_response_is_subdivided_until_complete(self):
        """Loki and Tempo cap results with no flag. Verified: asking Loki for 2 entries
        out of a larger window returns exactly 2, structurally identical to a complete
        answer. Returning that page would write a rollup silently missing most of its
        input."""
        records = [record(at(10) + timedelta(seconds=s), f"r{s}") for s in range(0, 3600, 60)]
        source = FakeSource(records, row_limit=10)

        got = source.read(TimeRange(at(10), at(11)))

        assert len(got) == len(records), "subdivision did not recover every record"
        assert len({r.identity for r in got}) == len(records), "subdivision duplicated records"
        assert len(source.windows_fetched) > 1, "expected the window to be bisected"

    def test_subdivision_does_not_double_count_the_split_boundary(self):
        # A record sitting exactly on a bisection midpoint is the one most likely to be
        # returned twice, since both halves query it from an inclusive-end backend.
        midpoint = at(10, 30)
        records = [record(at(10), "start"), record(midpoint, "mid"), record(at(10, 59), "end")]
        source = FakeSource(records, row_limit=2)

        got = source.read(TimeRange(at(10), at(11)))

        identities = [r.identity for r in got]
        assert sorted(identities) == ["end", "mid", "start"]
        assert len(identities) == len(set(identities))

    def test_gives_up_loudly_rather_than_truncating(self):
        """When the window cannot be subdivided further, failing is the correct outcome.

        Returning a partial page here would be the worst available option: a rollup that
        looks complete and is quietly missing an unknown fraction of its input.
        """
        dense = [record(at(10) + timedelta(microseconds=i), f"r{i}") for i in range(50)]
        source = FakeSource(dense, row_limit=2)
        with pytest.raises(SourceTruncated, match="minimum subdivision"):
            source.read(TimeRange(at(10), at(10, 0, 1)))


class TestIdentityCollisionGuard:
    def test_duplicate_identities_are_rejected(self):
        """An identity scheme that cannot tell two records apart is a silent
        double-count or a silent drop, depending on which way the dedup falls."""
        source = FakeSource([record(at(10), "same"), record(at(10, 1), "same")])
        with pytest.raises(SourceError, match="duplicate record identities"):
            source.read(TimeRange(at(10), at(11)))
