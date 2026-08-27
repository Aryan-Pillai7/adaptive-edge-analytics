# 0004 — Checkpoints record which writer made them

**Status:** accepted

## Context

Found by running the pipeline, not by reasoning about it.

The boundary gate deliberately runs a processor that writes nothing, so idempotency and
resume can be proven before any aggregation exists to mask a failure. After one real run
against the live stack, the checkpoint asserted that **23 log buckets were committed**.

Nothing had been written — that is what the no-op processor is for — but the checkpoint
did not record that. The first real rollup writer would have honoured those checkpoints
and skipped all 23 buckets. A permanent hole in the cold tier, produced by a run that
reported success.

## Decision

`bucket_state.writer_version` records what produced each bucket, and a commit is only
honoured by a writer identifying itself the same way.

**Three places had to become writer-scoped, not one**, and each was found by a test
failing after the previous one was fixed:

- `is_committed` — or the bucket is wrongly considered done;
- the `frontier` table — or a new writer starts past all the work and never reaches the
  bucket to check it;
- `claim` — or the new writer reaches the bucket and is then refused it.

## Consequences

The writer version includes the output schema version, so bumping it re-opens every
previously committed bucket. That makes it the schema-drift lever too: a change to the
aggregation output shape re-derives the cold tier rather than leaving it silently mixing
two formats.

`edge-rollup status` names the writer alongside each frontier, and says explicitly when
buckets were committed by a writer that produces no output.

The general lesson: **"done" is meaningless without "done by whom".** Any state recording
completion is likely to have the same defect, and it looks exactly like success until a
second writer disagrees with the first.
