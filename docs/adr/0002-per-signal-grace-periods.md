# 0002 — Per-signal grace periods, because writer-done ≠ reader-sees

**Status:** accepted

## Context

A bucket may only be rolled up once every backend has finished making that period
*queryable*. The obvious implementation is one global "wait N minutes" constant.

That is wrong in both directions here, because the delay differs by an order of magnitude
per backend. A value long enough for traces adds pointless lag to metrics; a value tuned
for metrics rolls up trace windows Tempo has not yet made searchable — and an empty result
is indistinguishable from a genuinely quiet hour.

The trace figure was initially set to 15 minutes, reasoned from Tempo's
`max_block_duration` (5m) and `complete_block_timeout` (5m). Measuring it found a third
buffer nobody had accounted for:

| time | state of traces ingested at 23:31:50 |
|---|---|
| 23:33 | searchable — held in the ingester |
| 23:38–23:46 | **returns zero** — block flushed, not yet in the queryable blocklist |
| 23:47 | searchable again — served from the backend block |

The block was on disk the entire time. `blocklist_poll` (5m default) is when Tempo
notices. While it was happening, overlapping windows disagreed with each other:
`[23:31,23:41)` returned 8 traces and `[23:26,23:36)` returned 0.

## Decision

Grace is per-signal, and each value is the sum of every buffer between an event happening
and it being readable:

| signal | buffers | grace |
|---|---|---|
| metrics | SDK export 10s + Collector batch 5s + VM `latencyOffset` 30s | 2m |
| logs | + Loki `chunk_idle_period` 2m / `max_chunk_age` 5m | 10m |
| traces | + Tempo block lifecycle + `blocklist_poll` | 20m |

## Consequences

The generalisable form matters more than the numbers: **when the writer considers
something done and when a reader can see it are two different instants.** All three
backends have both, and counting only the writer-side ones produces a constant that is
confidently too small.

The same distinction appears one layer up — "the job finished" and "it is safe to record
as committed" are also different instants — which is why the checkpoint in
[0003](0003-commit-last-contiguous-frontier.md) commits last.

It applies to writes too. A Loki entry stamped more than ~45 minutes in the past is
accepted with HTTP 204 and returns nothing from a query issued seconds later, becoming
readable about five minutes on. Every cold log rollup lands in that case, because a rollup
is stamped at its bucket start — so that sink is verified by the accepted write plus the
Parquet copy rather than by an immediate read-back.
