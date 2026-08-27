# 0001 — Parquet is authoritative; VictoriaMetrics and Loki are projections

**Status:** accepted

## Context

The obvious design treats all three sinks as equals: write the rollup back to the backend
it came from, plus a Parquet archive, and call any of them the cold tier. That is how this
started.

Three unrelated measurements pushed against it.

**Query ergonomics.** Rolled-up data has to be impossible to confuse with raw data. A
query matching both would silently average a 15-second sample with an hourly summary and
return something plausible. Solvable with naming discipline, but it argued for one place
with an explicit schema.

**Rewrite semantics.** VictoriaMetrics is not idempotent on rewrite — measured directly:
the same sample written three times is stored three times, and the same
`(series, timestamp)` written with a *different* value stored a fourth sample while an
instant query returned the **old** one. Loki deduplicates identical lines but appends when
content changes. Neither can be corrected in place, and neither has a time-bounded delete
to work around it with.

**Retention.** VictoriaMetrics OSS has a single global `-retentionPeriod` with no
per-series override (retention filters are an enterprise feature). Cold rollups written
there expire at exactly the same moment as the raw data they summarise. Raising the global
value would keep the raw data longer too — destroying the hot/cold distinction and
multiplying storage. There is no setting that fixes this; the limitation is structural.

## Decision

Parquet is the authoritative cold tier: one file per bucket, written temp-file → fsync →
atomic rename, so a rewrite replaces a bucket wholesale and leaves no orphaned series.
VictoriaMetrics and Loki hold *projections* of it, kept because they make cold data
queryable in tools that already exist.

`Sink.replaces_on_rewrite` records the difference on each sink, so no caller can assume
otherwise.

## Consequences

- Correcting a projection after an aggregation change means deleting the rollup metric or
  stream and rewriting from Parquet. That is a deliberate operation, not something a
  retry does by accident.
- A retry after a partial dual-write is safe: deterministic re-aggregation of a sealed
  window writes identical values, and both backends tolerate identical rewrites.
- If VM cold data must outlive its global retention, the fix is a second VictoriaMetrics
  instance written to only by the rollup sink. That is new infrastructure and was out of
  scope; it is recorded as a recommendation rather than left as a silent gap.

The decision reads as over-engineering until the third measurement. It is written up this
way — three independent paths to one conclusion — because that is how it was reached, not
how it was planned.
