<h1 align="center">adaptive-edge-analytics</h1>

<p align="center">
  <strong>Keep observability data queryable after it stops being cheap to keep.</strong><br>
  A batch rollup pipeline that turns VictoriaMetrics, Loki and Tempo into a tiered
  data platform — full-resolution hot data, aggregated cold summaries, and a boundary
  between them that does not silently lose a window.
</p>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/python-3.11%2B-3776ab">
  <img alt="New containers" src="https://img.shields.io/badge/new%20containers-none-success">
  <img alt="Tests" src="https://img.shields.io/badge/tests-240%20unit%20%2B%2066%20integration-success">
</p>

---

The sibling project [`adaptive-edge-otel`](https://github.com/Aryan-Pillai7/adaptive-edge-otel)
proved that smart edge processing keeps backend storage lean **in the moment of
ingestion** — cutting stored telemetry by ~99% before it crosses the network.

This project asks the next question: once that data is in the backends, how do you keep
it queryable and cheap **over time**?

Raw data is a hot tier with short retention. An hourly batch job reads it, aggregates it
into coarser time buckets, and writes rolled-up summaries to a cold tier that survives far
longer because it is orders of magnitude smaller.

**No new infrastructure.** It runs against the sibling stack's existing three backends —
no Kafka, no MinIO, no ClickHouse, no new containers. Three Python libraries.

## Results

Measured over 24 hourly buckets against a live stack:

| signal | raw records | rollup rows | reduction | cold bytes |
|---|---|---|---|---|
| **metrics** | 577,461 | 253 | **2,282×** | 125,112 |
| logs | 59 | 5 | 12× | 84,290 |
| traces | 83 | 6 | 14× | 78,842 |
| **total** | **577,603** | **264** | **2,188×** | **288,244** |

**Read that table with two caveats, because the headline number flatters itself.**

**60% of those cold bytes are Parquet schema footers on 54 empty buckets**, not summary
data. An empty bucket still writes a file — "we looked and there was nothing" has to be
distinguishable from "never ran" — and each carries a few KB of footer regardless of
content. That is a fixed per-file floor.

**The logs and traces ratios are low because the window was quiet, not because the rollup
is weak.** 59 log records and 83 traces is not a workload. **2,282× on metrics is the
honest figure** for a signal with real volume in it, and it is the one to quote.

Reproduce with `bash scripts/verify-tiering.sh`, which prints this table alongside a
retention audit and exits non-zero if the hot tier is too short for the job's cadence.

## What the backends actually do

Every number below was measured against the running stack, not read from documentation.
Several contradict what the configuration appears to say.

| | VictoriaMetrics | Loki | Tempo |
|---|---|---|---|
| Window bounds | `start` **and** `end` inclusive | half-open, correct | matches **overlap**, not containment |
| Silent truncation | no (streams everything) | yes, no flag | yes, no flag |
| Rewrite is idempotent | **no** — 3 writes = 3 samples | no — changed content appends | n/a, never written to |
| Write visible immediately | yes | **no** if timestamp >45 min old | **no** for ~15 min after ingest |
| Can keep cold longer than raw | **no** — one global retention | yes, `retention_stream` | n/a |

Each of these produces a plausible wrong number rather than an error, which is why the
read layer enforces half-open windows and truncation-bisection for every backend rather
than trusting any of them.

## Why Parquet is the authoritative copy

This is the design decision the project kept arriving at, from three unrelated
directions. It is worth following, because it was **discovered rather than assumed** —
the first version of the architecture treated all three sinks as equals.

1. **Query ergonomics.** Rolled-up data has to be distinguishable from raw, or a careless
   query silently mixes a 15-second sample with an hourly average. That argued for an
   explicit archive with its own schema.
2. **Rewrite semantics.** VictoriaMetrics stores the same sample three times if you write
   it three times, and an instant query returned the *old* value after a rewrite with a
   changed value. Loki appends rather than replaces when content changes. Neither can be
   corrected in place. Parquet replaces a whole bucket atomically.
3. **Retention.** VictoriaMetrics OSS has a **single global retention** with no
   per-series override — so cold rollups written there expire at exactly the same moment
   as the raw data they summarise. Raising the global value would keep the raw data
   longer too, destroying the hot/cold distinction. There is no setting that fixes this;
   the limitation is structural.

So the honest architecture is:

| | tiers? | what it actually is |
|---|---|---|
| **Parquet** | yes, by construction | **the durable cold tier** — 90d hourly, 730d daily |
| **Loki** | yes, `retention_stream` | cold log rollups genuinely outlive raw (90d vs 3d) |
| **VictoriaMetrics** | **no** | a queryable projection with a 3-day horizon |

If VM cold data needs to outlive three days, the fix is a second VictoriaMetrics instance
with long retention, written to only by the rollup sink. That is a new container and is
therefore out of scope here — documented as a recommendation rather than left as a
silent gap.

## Architecture

```
   ┌──────────────────────────────────────────────────────────┐
   │  HOT TIER — raw, full resolution                          │
   │  VictoriaMetrics 3d   ·   Loki 3d   ·   Tempo 24h         │
   └───────────────────────────┬──────────────────────────────┘
                               │  native query APIs, host ports
                               ▼
              ┌──────────────────────────────────┐
              │   edge-rollup  (Python, hourly)  │
              │                                  │
              │   sources/  →  rollups/  →  sinks/
              │   read I/O     pure agg     write I/O
              └───────────────┬──────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        ▼                                           ▼
┌──────────────────────┐                 ┌─────────────────────┐
│ back into VM / Loki  │                 │  local Parquet      │
│ aea_rollup_*         │                 │  90d / 730d         │
│ tier="cold"          │                 │  AUTHORITATIVE      │
│ projection only      │                 │                     │
└──────────────────────┘                 └─────────────────────┘
```

## The part that is actually hard

Not the aggregation — the boundary. A naive batch job has two ways to be wrong, and both
are silent.

**Per-signal grace periods.** A bucket is eligible only when `bucket_end + grace <= now`.
The grace is not one number, because the delay between an event happening and it being
*queryable* differs by an order of magnitude per backend:

| Signal | What has to clear first | Grace |
|---|---|---|
| metrics | SDK export 10s + Collector batch 5s + VM `latencyOffset` 30s | **2m** |
| logs | …plus Loki `chunk_idle_period` 2m / `max_chunk_age` 5m | **10m** |
| traces | …plus Tempo block lifecycle **and `blocklist_poll`** | **20m** |

The trace figure started at 15 minutes, reasoned from Tempo's block settings. Measuring it
found a third buffer: traces ingested at 23:31:50 were searchable at 23:33, returned
**zero** from 23:38 to 23:46, then reappeared at 23:47 — on disk the whole time, invisible
because a flushed block is not queryable until Tempo refreshes its blocklist. A job
running in that gap would have recorded a confident, permanent, silently wrong zero.

The general form is worth naming: **when the writer considers it done and when the reader
can see it are two different instants**, in all three backends. Same distinction one layer
up decides when a checkpoint may be committed.

**Half-open intervals, everywhere.** Every window is `[start, end)`. That is the whole of
the double-counting defence at the seam between two runs.

**Commit last, always.** Per bucket: `claim → read → process → commit`, where the commit
happens only after every sink has durably accepted the data. A crash before it leaves the
bucket uncommitted and the next run redoes it — free, because writes are keyed. The bias
is deliberately toward doing work twice over skipping it: duplicated idempotent work costs
seconds, a skipped bucket is permanent and silent.

**A contiguous frontier.** If buckets 1, 2 and 4 commit but 3 fails, the checkpoint stays
at 3. Taking `max(committed)` would step over the hole and lose bucket 3 forever while
every run exited 0. A stalled frontier is the correct outcome, and `edge-rollup status`
reports it.

## Quick start

Requires the sibling stack running (`bash scripts/up.sh` in `adaptive-edge-otel`).

```bash
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -e ".[dev]"   # Linux/CI: .venv/bin/python

cp .env.example .env        # optional — every value defaults to the sibling stack

edge-rollup status          # what am I pointed at, and what is sealed right now?
edge-rollup run             # roll up every sealed bucket
edge-rollup tiering         # retention audit + the ratio table
```

### Commands

| | |
|---|---|
| `edge-rollup run` | roll up every sealed bucket since the last checkpoint — the cron verb |
| `edge-rollup probe --signal --from --to` | dump normalised raw records without aggregating |
| `edge-rollup status` | checkpoints, watermarks, stalled buckets |
| `edge-rollup tiering` | retention audit and the hot/cold ratio table |

### Scheduling

```bash
bash orchestration/run_rollup.sh          # what cron runs
cat orchestration/crontab.example         # hourly, at 20 past
```

Only one run happens at a time — the wrapper takes a `mkdir`-based lock (portable;
`flock` does not exist on Windows) and exits 0 without working if a previous run is still
going. Exit codes are actionable: `4` unreachable backend, `5` a bucket failed and the
frontier is held back.

### Tests

```bash
bash scripts/test.sh                 # unit only — no stack, no network, fast
bash scripts/test.sh --integration   # adds tests that need the sibling stack up
bash scripts/lint.sh                 # yaml + ruff + shellcheck
```

Unit tests run against **captured real API responses** in `tests/fixtures/`, refreshed
with `scripts/capture_fixtures.py`. Hand-written fixtures encode what you *think* an API
returns, which is exactly the assumption that broke five separate times here.

One standing rule, learned the hard way three times: **a test must not be satisfiable by
nothing.** Any test whose pass condition can be met by an empty result asserts on a count
first. See `docs/adr/0008-tests-must-not-pass-vacuously.md`.

## Layout

```
config/rollup.yaml      what gets rolled up, at what granularity, into which dimensions
src/edgerollup/
  sources/              read I/O — one adapter per backend
  rollups/              pure aggregation. no I/O, no network, no clock.
  sinks/                write I/O — Parquet (authoritative), VM, Loki
  windows.py            epoch-aligned buckets, per-signal grace, half-open intervals
  state.py              SQLite checkpoints, contiguous frontier, writer versioning
  writer.py             all-or-nothing dual write
  tiering.py            retention audit + ratio measurement
orchestration/          what cron runs unattended: lock, exit codes, logging
scripts/                what developers run by hand
docs/adr/               the decisions, and what forced them
```

`rollups/` performs no I/O, so aggregation is testable with no stack running and adding a
signal is one new file plus a config entry. That held: traces needed only
`rollups/traces.py` and reused the grouping hook logs had introduced.

## Decisions

Every non-obvious choice, and the measurement that forced it, is in
[`docs/adr/`](docs/adr/). The short version:

| | |
|---|---|
| [0001](docs/adr/0001-parquet-is-authoritative.md) | Parquet is authoritative; VM and Loki are projections |
| [0002](docs/adr/0002-per-signal-grace-periods.md) | Per-signal grace, because writer-done ≠ reader-sees |
| [0003](docs/adr/0003-commit-last-contiguous-frontier.md) | Commit last; the frontier advances contiguously |
| [0004](docs/adr/0004-checkpoints-record-their-writer.md) | Checkpoints record which writer made them |
| [0005](docs/adr/0005-cold-tier-is-never-read-back.md) | The cold tier is excluded from every source selector |
| [0006](docs/adr/0006-normalise-before-grouping.md) | Normalise while building the grouping key, never after |
| [0007](docs/adr/0007-counter-increase-not-sample-sum.md) | Counters roll up as increase, not sum |
| [0008](docs/adr/0008-tests-must-not-pass-vacuously.md) | A test must not be satisfiable by nothing |
