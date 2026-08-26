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
</p>

---

The sibling project [`adaptive-edge-otel`](https://github.com/Aryan-Pillai7/adaptive-edge-otel)
proved that smart edge processing keeps backend storage lean **in the moment of
ingestion** — tail sampling, cardinality control and log deduplication, cutting stored
telemetry by ~99% before it crosses the network.

This project asks the next question. Once that data is sitting in the backends, how do
you keep it queryable and cheap **over time**?

Every observability platform eventually has to answer "how long do we keep
full-resolution data, and what do we keep after that". This treats it as a data
engineering problem rather than an afterthought retention setting: raw data is a hot
tier with short retention, and a scheduled batch job reads it, aggregates it into
coarser time buckets, and writes rolled-up summaries back as a cold tier that survives
far longer because it is orders of magnitude smaller.

**No new infrastructure.** It runs against the sibling stack's existing three backends.
No Kafka, no MinIO, no ClickHouse, no new containers — the only new dependencies are
three Python libraries.

## Architecture

```
   ┌──────────────────────────────────────────────────────────┐
   │  HOT TIER — raw, full resolution, short retention         │
   │  VictoriaMetrics 3d   ·   Loki   ·   Tempo 24h            │
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
│ aea_rollup_*         │                 │  hive-partitioned   │
│ tier="cold"          │                 │  durable archive    │
└──────────────────────┘                 └─────────────────────┘
```

Rollups are **dual-written**: back into the original backends so cold data stays
queryable in the tools already running, and to Parquet as the cheap long-horizon
archive.

## The part that is actually hard

Not the aggregation — the boundary. A naive batch job has two ways to be wrong, and
both are silent:

- **Double-counting.** Two runs whose windows overlap, or a re-run after a partial
  failure, and a counter reads high with nothing in the logs to say so.
- **Missing a window.** Data that expired from the hot tier before a run reached it, or
  a bucket rolled up before the backend had finished making it queryable. The result is
  an empty bucket, which is indistinguishable from a genuinely quiet hour.

Three mechanisms handle it:

**Per-signal grace periods.** A bucket is eligible for rollup only when
`bucket_end + grace <= now`. The grace is not one number, because the buffers between
an event being emitted and being *queryable* differ by an order of magnitude per
backend:

| Signal | What has to clear first | Grace |
|---|---|---|
| metrics | SDK export 10s + Collector batch 5s + VM `latencyOffset` 30s | **2m** |
| logs | …plus Loki `chunk_idle_period` 2m / `max_chunk_age` 5m | **10m** |
| traces | …plus Tempo `max_block_duration` 5m + `complete_block_timeout` 5m | **15m** |

A single global grace would be either wrong for metrics (needless lag on the fastest
signal) or actively dangerous for traces.

**Half-open intervals, everywhere.** Every window is `[start, end)`. No exceptions. This
is the whole of the double-counting defence at the seam between two runs.

**Structural idempotency.** Rollup points are keyed on
`(signal, granularity, bucket_start, dimension_hash)`, so re-running a window
**overwrites** rather than appends. Running the job twice on the same window is a no-op,
by construction rather than by check.

Late-arriving data does **not** automatically re-roll a sealed bucket. `--reprocess`
exists, is deterministic, and is deliberately manual.

## Quick start

Requires the sibling stack to be running (`bash scripts/up.sh` in `adaptive-edge-otel`).

```bash
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -e ".[dev]"   # Linux/CI: .venv/bin/python

cp .env.example .env        # optional — every value already defaults to the sibling stack

edge-rollup status          # what am I pointed at, and what is sealed right now?
```

`edge-rollup status` is the fastest way to confirm the job can see your stack and to
see the hot/cold boundary as it currently stands.

### Commands

| | |
|---|---|
| `edge-rollup run` | roll up every sealed bucket since the last checkpoint — the cron verb |
| `edge-rollup backfill --from --to` | roll up an explicit historical window, ignoring the checkpoint |
| `edge-rollup probe --signal --from --to` | dump normalised raw records without aggregating |
| `edge-rollup status` | checkpoints, watermarks, configured endpoints |

### Tests

```bash
bash scripts/test.sh                 # unit only — no stack, no network, fast
bash scripts/test.sh --integration   # adds tests that need the sibling stack up
bash scripts/lint.sh                 # yaml + ruff + shellcheck
```

Unit tests run against **captured real API responses** in `tests/fixtures/`, not
hand-written JSON. Hand-written fixtures encode what you *think* an API returns, which
is exactly the assumption that breaks.

## Layout

```
config/rollup.yaml      what gets rolled up, at what granularity, into which dimensions
src/edgerollup/
  sources/              read I/O — one adapter per backend
  rollups/              pure aggregation. no I/O, no network, no clock.
  sinks/                write I/O — VM, Loki, Parquet
  windows.py            sealed buckets, per-signal grace, half-open intervals
  state.py              SQLite checkpoints, per (signal, granularity)
orchestration/          what cron runs unattended: lockfile, exit codes, logging
scripts/                what developers run by hand
```

The `sources → rollups → sinks` split is load-bearing. Because `rollups/` performs no
I/O, aggregation is testable with no stack running, and adding a new signal type or a
new granularity is one new file plus a config entry rather than a change to the
pipeline core.

## Status

Phase 0 (scaffolding) complete. See the build plan for what lands next.
