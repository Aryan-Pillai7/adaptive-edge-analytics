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
from datetime import UTC, datetime

from edgerollup import __version__
from edgerollup.config import SIGNALS, Settings
from edgerollup.model import TimeRange
from edgerollup.registry import open_sources
from edgerollup.sources import SourceError

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
    print()
    print("hot/cold boundary")
    for signal in SIGNALS:
        grace = settings.grace(signal)
        sealed_before = now - grace
        print(
            f"  {signal:<8} grace {grace!s:>8}"
            f"   sealed up to {sealed_before.isoformat(timespec='seconds')}"
        )
    print()
    print("checkpoints      (Phase 2)")
    return 0


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


def _not_yet(phase: str):
    def handler(args: argparse.Namespace) -> int:
        raise NotImplementedYet(f"lands in {phase}")

    return handler


HANDLERS = {
    "status": cmd_status,
    "probe": cmd_probe,
    "run": _not_yet("the aggregation milestone"),
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
