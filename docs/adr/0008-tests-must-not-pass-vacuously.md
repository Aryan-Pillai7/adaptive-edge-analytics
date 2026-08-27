# 0008 — A test must not be satisfiable by nothing

**Status:** accepted

## Context

Three separate times, a test suite reported green while asserting nothing. Each was found
by accident rather than by review.

1. **The read gate** skipped all four trace properties because the chosen window held no
   traces.
2. **The boundary gate's** no-op processor committed 23 real log buckets — the checkpoint
   recorded work that never happened.
3. **The rollup suites** used fixed backfill horizons that stopped reaching data as it
   aged, so they processed a run of empty buckets. Surfaced only because one assertion
   happened to be `assert rows`.

Every one of those tests was correct in what it asserted. The defect was in what they
never reached.

## Decision

Any test whose pass condition can be met by an empty or no-op result must assert on a
count or on non-emptiness explicitly. **Absence of an exception is not a pass.**

In practice:

- Assert `len(x) > 0` before asserting properties *of* `x` — a loop over an empty list
  passes vacuously.
- Skip on the **input** being absent, never on the output being empty. An empty output is
  a result, and must be asserted on or explained.
- For pipeline tests, skip on `records_written` rather than `processed`: a
  processed-but-empty bucket is exactly the case that makes an assertion vacuous.
- Run with `-rs` so skip reasons are visible. A silent skip is indistinguishable from a
  pass.

## Consequences

This is a review question as much as a coding rule: **"what would make this test pass
without doing anything?"** It is invisible otherwise, because the assertions themselves
look correct.

Writing it down after the third occurrence, rather than fixing a fourth instance, is the
point. The first two were treated as isolated bugs; only the third made the shape obvious.
