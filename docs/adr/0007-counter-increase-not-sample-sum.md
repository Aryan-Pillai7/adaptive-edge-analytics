# 0007 — Counters roll up as increase, not sum

**Status:** accepted

## Context

Most of what this stack emits is cumulative: `edgeapp_requests_total` only ever rises, and
so do the `_sum`, `_count` and `_bucket` components of every histogram.

Summing a cumulative counter's raw samples is the classic mistake in this area. 374
samples each reading around 22 sum to roughly 8,200 — a number that describes nothing at
all, and looks entirely reasonable sitting in a table.

## Decision

`delta` is the increase across the bucket, computed reset-aware: walk samples in time
order, add each positive increment, and treat a decrease as a process restart by counting
the new value as an increment from zero.

Every other aggregate — count, sum, min, max, first, last — is stored as well.

Metric type is inferred from the Prometheus naming convention, because VictoriaMetrics'
export API does not return types. That inference is **recorded on the row** rather than
used to decide what gets stored.

## Consequences

Naive `last - first` reports a large negative number after a restart, and clamping that to
zero discards everything that happened after it. Both are wrong in ways that look
plausible.

Storing every aggregate costs one pass over the samples, and means a question asked later
can be answered from the cold tier rather than from raw data that has since expired. It
also makes the type inference cheap to be wrong about: a bad guess costs a misleading
projection into VictoriaMetrics, never an aggregate nobody can reconstruct.

Records are sorted before aggregating. `first`, `last` and `delta` are order-dependent,
and taking the backend's response order would make them a property of how the data
happened to be serialised.

Trace rollups follow the same principle from the other side. Latency percentiles are
stored per status group, and **error rate is deliberately not stored**: deriving it needs
cross-group arithmetic, which would reintroduce a second filtering path over a split that
is otherwise resolved exactly once, and a stored ratio can silently disagree with the
counts sitting beside it.
