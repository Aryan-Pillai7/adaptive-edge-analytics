"""Command-line entrypoint.

One binary, several verbs. `orchestration/run_rollup.sh` wraps this for cron; developers
call it directly.

Subcommands are declared here from Phase 0 even where they are not implemented yet, so
that the shape of the tool is fixed before the internals fill in -- and so that the
scheduling wrapper can be written and tested against a real, stable interface rather
than being retrofitted at the end.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime, timedelta

import httpx

from edgerollup import __version__
from edgerollup.clock import SystemClock
from edgerollup.config import SIGNALS, Settings, load_rollup_config
from edgerollup.model import TimeRange
from edgerollup.pipeline import NOOP_WRITER, run_signal
from edgerollup.registry import ROLLUPS, build_writer, open_sinks, open_sources
from edgerollup.sources import SourceError
from edgerollup.state import COMMITTED, FAILED, StateStore
from edgerollup.tiering import (
    check_safety_margin,
    humanise,
    measure_ratio,
    probe_retentions,
)
from edgerollup.windows import Granularity, watermark
from edgerollup.writer import writer_version

log = logging.getLogger("edgerollup")


class NotImplementedYet(Exception):
    """Raised by a declared-but-unbuilt subcommand.

    A distinct type rather than a bare NotImplementedError so the CLI can exit with a
    clear message and a dedicated exit code, instead of a traceback that reads like a
    crash to whoever is running it from cron.
    """


def _iso(value: str) -> datetime:
    """Parse an ISO-8601 timestamp, normalising to UTC.

    Naive input is *assumed* UTC rather than local. Local time is the wrong default for
    a batch job whose correctness depends on bucket alignment: a machine in a DST zone
    would silently produce a duplicated or missing hour twice a year.
    """
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="edge-rollup",
        description=(
            "Roll raw hot-tier telemetry in VictoriaMetrics, Loki and Tempo into "
            "coarser cold-tier summaries."
        ),
    )
    parser.add_argument("--version", action="version", version=f"edge-rollup {__version__}")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="debug logging, including every backend query issued",
    )

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # --- run ---------------------------------------------------------------------
    run = sub.add_parser(
        "run",
        help="roll up every sealed bucket since the last checkpoint (the cron verb)",
    )
    run.add_argument(
        "--signal",
        choices=SIGNALS,
        action="append",
        help="restrict to one signal; repeatable. Default: all three.",
    )
    run.add_argument(
        "--dry-run",
        action="store_true",
        help="read and aggregate, but write nothing and advance no checkpoint",
    )

    # --- backfill ----------------------------------------------------------------
    backfill = sub.add_parser(
        "backfill",
        help="roll up an explicit historical window, ignoring the checkpoint",
    )
    backfill.add_argument("--signal", choices=SIGNALS, action="append")
    backfill.add_argument("--from", dest="start", type=_iso, required=True)
    backfill.add_argument("--to", dest="end", type=_iso, required=True)
    backfill.add_argument(
        "--reprocess",
        action="store_true",
        help=(
            "overwrite buckets that were already rolled up. Deliberately manual: late "
            "data never triggers this automatically (decisions.md D-002)."
        ),
    )

    # --- probe -------------------------------------------------------------------
    probe = sub.add_parser(
        "probe",
        help="dump normalised raw records for a window without aggregating (Phase 1 gate)",
    )
    probe.add_argument("--signal", choices=SIGNALS, required=True)
    probe.add_argument("--from", dest="start", type=_iso, required=True)
    probe.add_argument("--to", dest="end", type=_iso, required=True)

    # --- status ------------------------------------------------------------------
    sub.add_parser("status", help="show checkpoints, watermarks and configured endpoints")

    # --- tiering -----------------------------------------------------------------
    tiering = sub.add_parser(
        "tiering",
        help="audit retention across the backends and report the hot/cold size ratio",
    )
    tiering.add_argument(
        "--job-interval-hours",
        type=float,
        default=1.0,
        help="how often the rollup job runs; used for the safety-margin check (default 1)",
    )
    tiering.add_argument(
        "--safety-margin-hours",
        type=float,
        default=2.0,
        help="extra headroom required on top of grace + interval (default 2)",
    )

    return parser


def cmd_status(args: argparse.Namespace) -> int:
    """Print resolved configuration.

    Phase 0 shows configuration only; checkpoints and watermarks join it in Phase 2,
    once there are any. Even in this reduced form it earns its place -- it answers "what
    is this job actually pointed at" without reading source or guessing at env
    precedence.
    """
    settings = Settings()
    now = datetime.now(UTC)

    print(f"edge-rollup {__version__}")
    print(f"now (UTC):  {now.isoformat(timespec='seconds')}")
    print()
    print("hot tier (sources)")
    print(f"  victoriametrics  {settings.victoriametrics_url}")
    print(f"  loki             {settings.loki_url}")
    print(f"  tempo            {settings.tempo_url}")
    print()
    print("cold tier (sinks)")
    print(f"  parquet root     {settings.parquet_root}")
    print(f"  checkpoint store {settings.state_dir}")
    granularities = _granularities()

    print()
    print("hot/cold boundary")
    for signal in SIGNALS:
        grace = settings.grace(signal)
        marks = ", ".join(
            f"{g.name} < {watermark(now, grace, g).isoformat(timespec='seconds')}"
            for g in granularities
        )
        print(f"  {signal:<8} grace {grace!s:>8}   sealed: {marks}")

    print()
    print("checkpoints")
    with StateStore(settings.state_dir / "checkpoints.db") as store:
        marks = store.frontiers()
        for signal_name, gran_name, writer, mark in marks:
            print(
                f"  {signal_name:<8} {gran_name:<4} {writer:<10} next bucket "
                f"{mark.isoformat(timespec='seconds')}"
            )
        if not marks:
            print("  none yet — nothing has been rolled up")

        # A stalled frontier is the one condition an operator must not have to go
        # looking for: newer buckets keep succeeding, so throughput looks healthy while
        # one bucket silently holds the checkpoint back.
        stuck = store.buckets(status=FAILED)
        if stuck:
            print()
            print(f"  {len(stuck)} FAILED bucket(s) blocking the frontier:")
            for record in stuck[:10]:
                print(
                    f"    {record.signal}/{record.granularity} "
                    f"{record.bucket_start.isoformat(timespec='seconds')} "
                    f"(attempt {record.attempts}): {record.last_error}"
                )

        done = store.buckets(status=COMMITTED)
        if done:
            writers = sorted({r.writer_version or "unknown" for r in done})
            print()
            print(f"  {len(done)} bucket(s) committed by: {', '.join(writers)}")
            if NOOP_WRITER in writers:
                # Worth saying plainly: these buckets are checkpointed but hold no
                # rollup output, and a real writer will redo them. Without this line,
                # "23 buckets committed" reads as work that was actually done.
                print(
                    f"  note: {NOOP_WRITER} writes no rollup output — those buckets are "
                    f"re-processed once a real writer runs"
                )
    return 0


def _granularities() -> list[Granularity]:
    return [Granularity.from_config(entry) for entry in load_rollup_config()["granularities"]]


def cmd_run(args: argparse.Namespace) -> int:
    """Roll up every sealed bucket since the last checkpoint. The cron verb."""
    settings = Settings()
    clock = SystemClock()
    signals = args.signal or list(SIGNALS)
    granularities = _granularities()

    failures = 0
    with (
        open_sources(settings) as sources,
        open_sinks(settings) as sinks,
        StateStore(settings.state_dir / "checkpoints.db") as store,
    ):
        for signal in signals:
            writer = build_writer(signal, sinks.get(signal, []))
            if writer is None:
                # A signal with no rollup yet is skipped explicitly and loudly, rather
                # than silently running the no-op processor and leaving checkpoints that
                # claim work was done.
                log.warning(
                    "%s: no rollup implemented yet — skipping (implemented: %s)",
                    signal,
                    ", ".join(sorted(ROLLUPS)),
                )
                continue

            for granularity in granularities:
                report = run_signal(
                    signal=signal,
                    granularity=granularity,
                    source=sources[signal],
                    store=store,
                    clock=clock,
                    grace=settings.grace(signal),
                    max_backfill=timedelta(hours=settings.max_backfill_hours),
                    processor=writer,
                    writer_version=writer_version(signal),
                    dry_run=args.dry_run,
                )
                log.info("%s%s", "[dry-run] " if args.dry_run else "", report.summary())
                failures += len(report.failed)

    # Exit 5 rather than 0-with-a-warning: under cron, a non-zero exit is the only thing
    # anyone will notice, and a bucket that failed is a hole in the cold tier.
    return 5 if failures else 0


def cmd_probe(args: argparse.Namespace) -> int:
    """Read one window and print the normalised records, without aggregating anything.

    This is the read gate made runnable. It exists so that "can we pull raw data out of
    this backend cleanly" is answerable on its own, before any aggregation is written
    that could mask a bad read as a plausible-looking number.

    Output is JSON lines so two windows can be diffed, counted or set-compared with
    ordinary shell tools.
    """
    settings = Settings()
    window = TimeRange(args.start, args.end)

    with open_sources(settings) as sources:
        source = sources[args.signal]
        records = source.read(window)

    for record in records:
        print(
            json.dumps(
                {
                    "identity": record.identity,
                    "timestamp": record.timestamp.isoformat(),
                    "kind": record.signal_kind,
                    "value": record.value,
                    "dimensions": record.dims(),
                },
                sort_keys=True,
            )
        )

    total = sum(record.value for record in records)
    log.info("%s: %d records in %s (value sum %g)", args.signal, len(records), window, total)
    return 0


def cmd_tiering(args: argparse.Namespace) -> int:
    """The tiering report: retention audit plus the hot/cold ratio table.

    Exits non-zero if any backend's hot retention is too short for the job's cadence,
    because that is a silent-data-loss condition rather than a warning: raw data expiring
    before the job reaches it produces an empty bucket indistinguishable from a quiet one.
    """
    settings = Settings()
    job_interval = timedelta(hours=args.job_interval_hours)
    safety_margin = timedelta(hours=args.safety_margin_hours)

    with httpx.Client(timeout=settings.http_timeout_seconds) as client:
        retentions = probe_retentions(settings, client)
    checks = check_safety_margin(retentions, settings, job_interval, safety_margin)

    print("RETENTION, as read from the running backends")
    print(f"  {'backend':<17}{'hot':>8}{'cold':>8}  {'tiers?':<8}note")
    print("  " + "-" * 84)
    for backend in retentions:
        print(
            f"  {backend.name:<17}{humanise(backend.hot):>8}{humanise(backend.cold):>8}  "
            f"{'yes' if backend.can_tier else 'NO':<8}{backend.note}"
        )

    worst = max(settings.grace(s) for s in SIGNALS)
    required = worst + job_interval + safety_margin
    print()
    print(
        f"SAFETY MARGIN — hot must exceed grace ({humanise(worst)}) + interval "
        f"({humanise(job_interval)}) + margin ({humanise(safety_margin)}) = {humanise(required)}"
    )
    for check in checks:
        mark = "ok " if check.ok else "FAIL"
        print(f"  [{mark}] {check.backend:<17}{humanise(check.hot):>8}   {check.detail}")

    print()
    print("HOT / COLD RATIO — measured from the Parquet archive")
    print(
        f"  {'signal':<10}{'buckets':>9}{'empty':>7}{'raw records':>14}{'rollup rows':>13}"
        f"{'reduction':>12}{'cold bytes':>12}"
    )
    print("  " + "-" * 77)
    totals = [measure_ratio(signal, settings.parquet_root) for signal in SIGNALS]
    for ratio in totals:
        if not ratio.buckets:
            print(f"  {ratio.signal:<10}{'—  no cold tier written yet':>60}")
            continue
        print(
            f"  {ratio.signal:<10}{ratio.buckets:>9,}{ratio.empty_buckets:>7,}"
            f"{ratio.raw_records:>14,}{ratio.rollup_rows:>13,}"
            f"{ratio.reduction:>11,.0f}x{ratio.cold_bytes:>12,}"
        )

    written = [r for r in totals if r.buckets]
    if written:
        raw = sum(r.raw_records for r in written)
        rows = sum(r.rollup_rows for r in written)
        cold = sum(r.cold_bytes for r in written)
        overhead = sum(r.empty_bytes for r in written)
        empty = sum(r.empty_buckets for r in written)
        print("  " + "-" * 77)
        print(
            f"  {'TOTAL':<10}{sum(r.buckets for r in written):>9,}{empty:>7,}"
            f"{raw:>14,}{rows:>13,}{(raw / rows if rows else 0):>11,.0f}x{cold:>12,}"
        )
        print()
        print(f"  {cold / raw * 1000:.2f} cold bytes per 1,000 raw records")
        if overhead:
            # Said plainly, because otherwise a sparse signal looks like the rollup
            # barely helped when in fact there was almost nothing to roll up.
            print(
                f"  of which {overhead:,} bytes ({overhead / cold:.0%}) is the Parquet "
                f"schema footer on {empty} empty bucket(s) — a fixed per-file floor, "
                f"not summary data"
            )

    # Non-zero on a failed margin: under cron this is the only signal anyone sees.
    return 6 if any(not c.ok for c in checks) else 0


def _not_yet(phase: str):
    def handler(args: argparse.Namespace) -> int:
        raise NotImplementedYet(f"lands in {phase}")

    return handler


HANDLERS = {
    "status": cmd_status,
    "tiering": cmd_tiering,
    "probe": cmd_probe,
    "run": cmd_run,
    "backfill": _not_yet("the aggregation milestone"),
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        # Timestamped and machine-greppable: this runs unattended under cron, where the
        # log is the only record of what happened.
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    if args.command is None:
        parser.print_help()
        return 2

    try:
        return HANDLERS[args.command](args)
    except NotImplementedYet as exc:
        # Exit 3, not 1: a cron wrapper must be able to tell "this verb is not built
        # yet" apart from "the rollup ran and failed".
        log.error("`%s` is not implemented yet (%s)", args.command, exc)
        return 3
    except SourceError as exc:
        # Exit 4: a backend was unreadable or returned something we refuse to guess at.
        # Distinct from a crash so the cron wrapper can treat "the stack is down" as a
        # retryable condition rather than paging about a broken job.
        log.error("%s", exc)
        return 4
    except ValueError as exc:
        # Almost always a bad window on the command line (end before start, naive
        # timestamp). A usage error, not a failure of the pipeline.
        log.error("%s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
