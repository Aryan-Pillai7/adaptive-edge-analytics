"""The time and identity primitives everything else rests on."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from edgerollup.model import (
    RawRecord,
    TimeRange,
    canonical_dimensions,
    stable_identity,
)


def at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 27, hour, minute, second, tzinfo=UTC)


class TestTimeRange:
    def test_rejects_naive_datetimes(self):
        # A naive datetime is the start of every timezone bug in a batch pipeline: it
        # silently means "local" on one machine and "UTC" on another.
        with pytest.raises(ValueError, match="timezone-aware"):
            TimeRange(datetime(2026, 8, 27, 10), at(11))

    def test_rejects_non_utc(self):
        ist = timezone(timedelta(hours=5, minutes=30))
        with pytest.raises(ValueError, match="must be UTC"):
            TimeRange(datetime(2026, 8, 27, 10, tzinfo=ist), at(11))

    def test_rejects_inverted_and_empty_ranges(self):
        with pytest.raises(ValueError, match="start < end"):
            TimeRange(at(11), at(10))
        with pytest.raises(ValueError, match="start < end"):
            TimeRange(at(10), at(10))

    def test_membership_is_half_open(self):
        window = TimeRange(at(10), at(11))
        assert window.contains(at(10)) is True, "start must be inside"
        assert window.contains(at(11)) is False, "end must be outside"
        assert window.contains(at(10, 30)) is True
        assert window.contains(at(9, 59, 59)) is False

    def test_adjacent_windows_share_a_boundary_owned_by_exactly_one(self):
        """The core anti-double-counting property, stated directly.

        Every record in the pipeline is placed by this rule, so if it ever stops
        holding, two runs will both claim the instant they share.
        """
        first = TimeRange(at(10), at(11))
        second = TimeRange(at(11), at(12))
        boundary = at(11)
        owners = [w for w in (first, second) if w.contains(boundary)]
        assert len(owners) == 1
        assert owners[0] is second

    def test_bisect_covers_the_parent_exactly_once(self):
        window = TimeRange(at(10), at(12))
        left, right = window.bisect()
        assert left.start == window.start
        assert right.end == window.end
        assert left.end == right.start, "halves must be adjacent, with no gap"

        # Every instant in the parent belongs to exactly one half.
        for offset in range(0, 120, 7):
            moment = window.start + timedelta(minutes=offset)
            in_halves = [h for h in (left, right) if h.contains(moment)]
            assert len(in_halves) == 1, f"{moment} landed in {len(in_halves)} halves"

    def test_bisect_refuses_when_it_would_not_make_progress(self):
        window = TimeRange(at(10), at(10, 0, 0) + timedelta(microseconds=1))
        with pytest.raises(ValueError, match="cannot bisect"):
            window.bisect()


class TestCanonicalDimensions:
    def test_order_does_not_affect_the_result(self):
        # Two runs must produce byte-identical output even if a backend changes the
        # order it serialises labels in.
        a = canonical_dimensions({"b": "2", "a": "1", "c": "3"})
        b = canonical_dimensions({"c": "3", "a": "1", "b": "2"})
        assert a == b
        assert a == (("a", "1"), ("b", "2"), ("c", "3"))

    def test_empty_values_are_dropped_not_kept(self):
        """An absent label and a present-but-empty label are the same thing.

        Keeping both would split one logical rollup series into two.
        """
        assert canonical_dimensions({"a": "1", "b": "", "c": None}) == (("a", "1"),)

    def test_is_hashable(self):
        assert {canonical_dimensions({"a": "1"})}


class TestStableIdentity:
    def test_is_deterministic_across_calls(self):
        assert stable_identity("a", 1) == stable_identity("a", 1)

    def test_distinguishes_different_inputs(self):
        assert stable_identity("a", 1) != stable_identity("a", 2)

    def test_is_not_fooled_by_concatenation(self):
        """('ab','c') and ('a','bc') must not collide.

        Without a separator between parts they would, and two genuinely different
        records would be silently deduplicated into one.
        """
        assert stable_identity("ab", "c") != stable_identity("a", "bc")

    def test_survives_a_subprocess(self):
        """Identity must not depend on PYTHONHASHSEED.

        Python randomises `hash()` per process. An identity built on it would differ
        between the run that writes a rollup and the run that checks whether that
        rollup already exists, making idempotency a coin flip.
        """
        import subprocess
        import sys

        code = (
            "from edgerollup.model import stable_identity;"
            "print(stable_identity('a', 1, ('x', 'y')))"
        )
        first = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        ).stdout.strip()
        second = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=True,
            env={"PYTHONHASHSEED": "12345", "PATH": ""},
        ).stdout.strip()
        assert first == second


class TestRawRecord:
    def test_rejects_naive_timestamps(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            RawRecord(
                signal="metrics",
                timestamp=datetime(2026, 8, 27, 10),
                dimensions=(),
                value=1.0,
                identity="x",
            )

    def test_without_dimensions_preserves_identity(self):
        """Dropping a dimension must not merge two distinct source records.

        If `identity` were recomputed after the drop, every record that differed only by
        `service_instance_id` would collapse into one -- and the rollup would sum a
        single record instead of all of them.
        """
        record = RawRecord(
            signal="metrics",
            timestamp=at(10),
            dimensions=canonical_dimensions({"service_instance_id": "uuid-1", "route": "/a"}),
            value=1.0,
            identity="original",
        )
        stripped = record.without_dimensions(frozenset({"service_instance_id"}))
        assert stripped.dims() == {"route": "/a"}
        assert stripped.identity == "original"
        assert stripped.value == record.value
