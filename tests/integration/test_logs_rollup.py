"""The log rollup end to end, against the real stack."""

from __future__ import annotations

from datetime import timedelta

import httpx
import pyarrow.parquet as pq
import pytest

from edgerollup.clock import SystemClock
from edgerollup.pipeline import run_signal
from edgerollup.registry import dimensions_for
from edgerollup.rollups import LogsRollup
from edgerollup.sinks import LokiSink, ParquetSink
from edgerollup.sinks.base import Sink
from edgerollup.state import COMMITTED, StateStore
from edgerollup.windows import Granularity
from edgerollup.writer import RollupWriter, writer_version

pytestmark = pytest.mark.integration

HOUR = Granularity("1h", 3600)
SIGNAL = "logs"


@pytest.fixture
def store(tmp_path) -> StateStore:
    with StateStore(tmp_path / "checkpoints.db") as s:
        yield s


@pytest.fixture
def parquet(tmp_path) -> ParquetSink:
    return ParquetSink(tmp_path / "cold")


def go(store, sources, settings, backfill_for, sinks: list[Sink], **kwargs):
    return run_signal(
        signal=SIGNAL,
        granularity=HOUR,
        source=sources[SIGNAL],
        store=store,
        clock=SystemClock(),
        grace=settings.grace(SIGNAL),
        max_backfill=backfill_for[SIGNAL],
        processor=RollupWriter(LogsRollup(dimensions_for(SIGNAL)), sinks),
        writer_version=writer_version(SIGNAL),
        **kwargs,
    )


def all_rows(parquet: ParquetSink):
    for path in parquet.root.rglob("*.parquet"):
        table = pq.read_table(path)
        for dims, total, count in zip(
            table.column("dimensions").to_pylist(),
            table.column("sum").to_pylist(),
            table.column("count").to_pylist(),
            strict=True,
        ):
            yield dict(dims), total, count


class TestSeverityNormalisationOnRealData:
    def test_no_two_rows_in_a_bucket_share_a_key(
        self, store, sources, settings, parquet, backfill_for
    ):
        """The guard runs inside the writer, so reaching here at all means it passed --
        but asserting it on real data is what makes that meaningful. Raw Loki carries
        both ERROR and Error (F-009), which is precisely the input that produces two
        colliding rows if casing is folded after grouping instead of before."""
        report = go(store, sources, settings, backfill_for, [parquet])
        if not report.processed:
            pytest.skip("no logs sealed to roll up")

        for path in parquet.root.rglob("*.parquet"):
            table = pq.read_table(path)
            keys = [
                (m, tuple(sorted(dict(d).items())))
                for m, d in zip(
                    table.column("metric").to_pylist(),
                    table.column("dimensions").to_pylist(),
                    strict=True,
                )
            ]
            assert len(keys) == len(set(keys)), f"{path.name}: duplicate rollup keys"

    def test_every_severity_is_folded_and_present(
        self, store, sources, settings, parquet, backfill_for
    ):
        report = go(store, sources, settings, backfill_for, [parquet])
        if not report.processed:
            pytest.skip("no logs sealed to roll up")

        severities = {dims.get("severity_text") for dims, _, _ in all_rows(parquet)}
        assert severities, "no rows produced"
        assert None not in severities, "a row has no severity — it would be invisible to filters"
        assert all(s == s.lower() for s in severities), f"unfolded casing: {severities}"

    def test_events_exceed_stored_entries_where_upstream_deduplicated(
        self, store, sources, settings, parquet, backfill_for
    ):
        """`sum` is events represented, `count` is Loki entries stored. Counting entries
        would undercount a flood by exactly the factor the upstream Collector achieved."""
        report = go(store, sources, settings, backfill_for, [parquet])
        if not report.processed:
            pytest.skip("no logs sealed to roll up")

        rows = list(all_rows(parquet))
        assert rows, "no rows produced"
        for _, total, count in rows:
            assert total >= count, "events cannot be fewer than the entries carrying them"


class TestIdempotency:
    def test_running_twice_writes_identical_parquet_bytes(
        self, store, sources, settings, parquet, backfill_for
    ):
        first = go(store, sources, settings, backfill_for, [parquet])
        if not first.processed:
            pytest.skip("no logs sealed to roll up")
        before = {p: p.read_bytes() for p in sorted(parquet.root.rglob("*.parquet"))}

        store.reset(SIGNAL)
        go(store, sources, settings, backfill_for, [parquet])

        after = {p: p.read_bytes() for p in sorted(parquet.root.rglob("*.parquet"))}
        assert after.keys() == before.keys()
        for path, content in before.items():
            assert after[path] == content, f"{path.name} changed on re-run"

    def test_a_second_run_does_nothing(self, store, sources, settings, parquet, backfill_for):
        go(store, sources, settings, backfill_for, [parquet])
        assert go(store, sources, settings, backfill_for, [parquet]).processed == []


class TestNoFeedbackLoop:
    def test_cold_tier_streams_are_never_read_back_as_raw(self, sources, settings):
        """The log sink writes `tier="cold"`, and the source selector excludes it. Same
        loop as F-013, one signal over."""
        from edgerollup.model import TimeRange

        now = SystemClock().now()
        window = TimeRange(now - timedelta(hours=3), now - settings.grace(SIGNAL) * 2)
        records = sources[SIGNAL].read(window)
        if not records:
            pytest.skip("no raw data in the window — nothing to check for feedback")
        for record in records:
            assert record.dims().get("tier") != "cold"


class TestLokiSinkAgainstRealLoki:
    def test_the_push_is_accepted(self, store, sources, settings, parquet, backfill_for):
        """Asserts on acceptance plus the authoritative Parquet copy, NOT on an immediate
        read-back.

        A rollup is stamped at its bucket start, which for an hourly job is over an hour
        old, and Loki does not serve an entry that old until its chunk flushes (measured:
        invisible immediately, all present about five minutes later). A write-then-query
        check here would report a false failure.
        """
        with httpx.Client(timeout=settings.http_timeout_seconds) as client:
            loki = LokiSink(settings.loki_url, client)
            report = go(store, sources, settings, backfill_for, [parquet, loki], max_buckets=2)
            if not report.processed:
                pytest.skip("no logs sealed to roll up")

            assert report.failed == []
            assert store.buckets(SIGNAL, COMMITTED), "committed despite a sink failure?"

    def test_a_loki_failure_leaves_the_bucket_uncommitted(
        self, store, sources, settings, parquet, backfill_for
    ):
        """An unreachable Loki must stop the commit -- but only for buckets that had
        something to write.

        An EMPTY bucket still commits, and that is correct: the sink short-circuits
        before issuing any request, so there is nothing to fail. The first version of
        this test capped the run at one bucket, drew a quiet hour, and asserted the
        wrong thing. The real property is that no bucket carrying rows is ever committed
        while a sink is down.
        """
        with httpx.Client(timeout=2.0) as client:
            broken = LokiSink("http://127.0.0.1:1", client)
            report = go(store, sources, settings, backfill_for, [parquet, broken])

        if not report.failed:
            pytest.skip("no non-empty log bucket in the window to fail on")

        _, message = report.failed[0]
        assert "parquet" in message, "the error should name the sink that had succeeded"
        assert "NOT committed" in message

        for record in store.buckets(SIGNAL, COMMITTED):
            assert record.record_count == 0, (
                "a bucket with rows was committed while a sink was unreachable"
            )
