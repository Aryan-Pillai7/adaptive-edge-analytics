# 0003 — Commit last, and advance the frontier contiguously

**Status:** accepted

## Context

Two independent ways for a batch pipeline to be quietly wrong: process a window twice, or
skip one. Both are silent — the first inflates a number, the second leaves a gap that
looks like a quiet period.

## Decision

**Per bucket: `claim → read → process → commit`**, where a processor must not return until
its writes are durable, and every sink must succeed before the commit happens. There is
deliberately no "committed except for one sink" state.

**The frontier advances only across an unbroken run of committed buckets.** If buckets 1,
2 and 4 succeed but 3 fails, it stays at 3.

## Consequences

Committing last biases every failure toward doing work twice rather than skipping it. A
crash anywhere before the commit leaves the bucket claimed but uncommitted, so the next run
redoes it — free, because rollup writes are keyed and overwrite. The reverse order lets a
crash lose a bucket while the checkpoint asserts it was finished. Duplicated idempotent
work costs seconds; a skipped bucket is permanent.

Taking `max(committed)` for the frontier would step over a hole and lose that bucket
forever, while every run continued exiting 0. So a permanently failing bucket stalls the
frontier on purpose — newer buckets are still processed, and `edge-rollup status` names
the stall. Lateness is recoverable; a gap is not.

The all-or-nothing dual write is a deliberate simplification rather than an oversight.
Per-sink checkpoints and a reconciliation pass are the right design for many sinks with
expensive rewrites. With two sinks and a rewrite costing one HTTP POST and one small file,
that machinery would only add places for a bucket to get lost. The error names which sinks
had already succeeded, so a clean failure is distinguishable from one that left state
behind.
