"""The read gate: exactly-once, non-overlapping reads against the real backends.

This is the gate that has to pass before any aggregation code exists. The reasoning is
that a read bug behind working aggregation does not look like a bug -- it looks like a
number. If a window seam double-counts one sample per series, the rollup is simply
slightly too high, forever, with nothing anywhere reporting a problem. Proving the read
layer first means that when a rollup number later looks wrong, the read layer is already
ruled out.

Each test states the property it is defending, because the properties are the point --
the specific counts vary with whatever the stack has been doing.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from edgerollup.model import TimeRange

pytestmark = pytest.mark.integration

SIGNALS = ("metrics", "logs", "traces")


def ids_of(records) -> set[str]:
    return {r.identity for r in records}


def window_for(settled_window, signal) -> TimeRange:
    """The populated, settled window for a signal.

    Skips only when the backend genuinely holds no data anywhere in the lookback --
    which is a stack that has been idle, not a property this suite can assert on. Any
    other emptiness is a bug and must not be papered over with a skip.
    """
    found = settled_window.get(signal)
    if found is None:
        pytest.skip(
            f"no {signal} data anywhere in the last 24h. Generate traffic against the "
            f"sibling app (curl http://localhost:8000/api/orders/x), wait out the "
            f"grace period, then re-run."
        )
    return TimeRange(*found)


@pytest.fixture(params=SIGNALS)
def signal(request) -> str:
    return request.param


class TestExactlyOnceAcrossAdjacentWindows:
    """The core property: splitting a window changes nothing about what it contains."""

    def test_union_of_halves_equals_the_whole(self, sources, settled_window, signal):
        whole = window_for(settled_window, signal)
        left, right = whole.bisect()

        source = sources[signal]
        all_records = source.read(whole)
        left_records = source.read(left)
        right_records = source.read(right)

        if not all_records:
            pytest.skip(
                f"no {signal} data in {whole}. The stack has been idle — generate "
                f"traffic against the sibling app, then re-run."
            )

        left_ids, right_ids = ids_of(left_records), ids_of(right_records)

        # The double-counting failure. VictoriaMetrics returns the boundary sample in
        # both halves unless it is filtered out; Tempo returns a straddling trace in
        # both unless it is attributed to its root-span start.
        assert left_ids & right_ids == set(), (
            f"{signal}: {len(left_ids & right_ids)} records appeared in BOTH halves — "
            f"a rollup would count them twice"
        )

        # The missing-data failure. Quieter and worse: nothing reports it.
        missing = ids_of(all_records) - left_ids - right_ids
        assert missing == set(), (
            f"{signal}: {len(missing)} records fell between the halves — "
            f"a rollup would silently omit them"
        )

        assert left_ids | right_ids == ids_of(all_records)
        assert len(left_records) + len(right_records) == len(all_records)

    def test_the_aggregate_value_is_conserved_by_splitting(self, sources, settled_window, signal):
        """Identity-level correctness is not enough on its own.

        Records could be counted once each and still carry the wrong value -- a log
        record's dedup_count read as 1, say. Summing what a rollup would actually sum
        catches that, where a set comparison would not.
        """
        whole = window_for(settled_window, signal)
        left, right = whole.bisect()
        source = sources[signal]

        total = sum(r.value for r in source.read(whole))
        halves = sum(r.value for r in source.read(left)) + sum(r.value for r in source.read(right))

        if total == 0:
            pytest.skip(f"no {signal} data in {whole}")
        assert halves == pytest.approx(total), (
            f"{signal}: values did not survive the split ({halves} vs {total})"
        )

    def test_many_adjacent_slices_still_tile_the_window(self, sources, settled_window, signal):
        """One split can pass by luck. Ten consecutive slices is a real tiling test.

        This is also the shape the pipeline actually runs in: a sequence of adjacent
        hourly buckets, each read by a separate job invocation.
        """
        whole = window_for(settled_window, signal)
        source = sources[signal]

        expected = ids_of(source.read(whole))
        if not expected:
            pytest.skip(f"no {signal} data in {whole}")

        slice_width = whole.duration / 10
        seen: set[str] = set()
        overlaps: set[str] = set()
        for index in range(10):
            piece = TimeRange(
                whole.start + slice_width * index,
                whole.start + slice_width * (index + 1),
            )
            piece_ids = ids_of(source.read(piece))
            overlaps |= seen & piece_ids
            seen |= piece_ids

        assert overlaps == set(), f"{signal}: {len(overlaps)} records appeared in two slices"
        assert seen == expected, (
            f"{signal}: tiling lost {len(expected - seen)} and invented {len(seen - expected)}"
        )


class TestRepeatability:
    def test_reading_the_same_window_twice_gives_the_same_answer(
        self, sources, settled_window, signal
    ):
        """A settled window must be stable.

        If it is not, no checkpoint scheme can be idempotent, because "the window I
        already processed" would not mean a fixed set of records. This is the assumption
        the entire Phase 2 checkpoint design rests on, so it is worth asserting rather
        than presuming.
        """
        window = window_for(settled_window, signal)
        source = sources[signal]

        first = source.read(window)
        second = source.read(window)

        if not first:
            pytest.skip(f"no {signal} data in {window}")

        assert ids_of(first) == ids_of(second)
        assert sum(r.value for r in first) == pytest.approx(sum(r.value for r in second))


class TestBoundaryOwnership:
    def test_the_shared_instant_belongs_to_exactly_one_window(
        self, sources, settled_window, signal
    ):
        """Directly targets the instant most likely to be double-counted.

        Rather than trusting a random split to land on a record, this finds a real
        record's timestamp and splits exactly there -- the worst case for an
        inclusive-end backend.
        """
        whole = window_for(settled_window, signal)
        source = sources[signal]
        records = source.read(whole)
        if len(records) < 3:
            pytest.skip(f"not enough {signal} data to split on a record boundary")

        # A real record's own timestamp, strictly inside the window.
        interior = sorted(r.timestamp for r in records)[len(records) // 2]
        if not (whole.start < interior < whole.end):
            pytest.skip("no interior record timestamp to split on")

        left = source.read(TimeRange(whole.start, interior))
        right = source.read(TimeRange(interior, whole.end))

        assert ids_of(left) & ids_of(right) == set(), (
            f"{signal}: splitting exactly on a record's timestamp double-counted it"
        )
        # The record AT the split instant must be in the right-hand window, since
        # [start, end) owns its start and disowns its end.
        at_boundary = {r.identity for r in records if r.timestamp == interior}
        assert at_boundary <= ids_of(right)
        assert at_boundary & ids_of(left) == set()


class TestWindowsOutsideTheData:
    def test_an_empty_window_reads_as_empty_not_as_an_error(self, sources, settled_window, signal):
        """An idle period is a legitimate answer, not a failure.

        Worth pinning because the pipeline must be able to tell "nothing happened" from
        "something went wrong" -- and Tempo in particular has a state where a busy
        window reads as empty, which is why the grace periods exist.
        """
        start = window_for(settled_window, signal).start
        # A one-minute window a year before this stack existed. Comfortably outside
        # every backend's retention, so any result at all would mean the window bounds
        # are not being applied.
        long_ago = start - timedelta(days=365)
        assert sources[signal].read(TimeRange(long_ago, long_ago + timedelta(minutes=1))) == []
