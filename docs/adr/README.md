# Architecture decision records

The decisions that were not obvious, and the measurement that forced each one.

These are a public distillate of a longer working log. What is kept here is the subset
where a reasonable person would have chosen differently without the evidence — so each
one leads with what was measured, not with what was decided.

A recurring theme, stated once here rather than eight times below: **almost every failure
this project had to design around produces a plausible wrong number rather than an
error.** A double-counted sample, a silently truncated page, a rollup of a rollup, a
halved severity count — none of them raise. That is the whole reason the design is as
defensive as it is.

| | |
|---|---|
| [0001](0001-parquet-is-authoritative.md) | Parquet is authoritative; VM and Loki are projections |
| [0002](0002-per-signal-grace-periods.md) | Per-signal grace, because writer-done ≠ reader-sees |
| [0003](0003-commit-last-contiguous-frontier.md) | Commit last; the frontier advances contiguously |
| [0004](0004-checkpoints-record-their-writer.md) | Checkpoints record which writer made them |
| [0005](0005-cold-tier-is-never-read-back.md) | The cold tier is excluded from every source selector |
| [0006](0006-normalise-before-grouping.md) | Normalise while building the grouping key, never after |
| [0007](0007-counter-increase-not-sample-sum.md) | Counters roll up as increase, not sum |
| [0008](0008-tests-must-not-pass-vacuously.md) | A test must not be satisfiable by nothing |
