# 0006 — Normalise while building the grouping key, never after

**Status:** accepted

## Context

Raw Loki data carries severity as both `ERROR` and `Error`.

Group on raw severity and fold the casing afterwards, and you get two groups that both
emit `severity="error"` for the same bucket — two rollup rows with an identical key. In
Parquet that is a duplicated row; in VictoriaMetrics it is two samples at one timestamp,
where the query layer returns one arbitrarily. The count silently halves.

This has a second entrance that is easy to miss. Folding only the *dimension* is not
enough: the record's **kind** also carried the raw severity and is part of the grouping
key, so the two variants still grouped apart. That was caught within minutes by the
duplicate-key guard below, rather than weeks later by someone noticing halved error counts.

## Decision

All grouping decisions happen in a single `Rollup.grouping_key` override, called while the
key is built. Whatever a signal needs — folding a value, discarding the kind, defaulting an
absent dimension — happens there, together.

`RollupWriter` independently asserts that no two rows in a bucket share
`(metric, dimensions)`, for every signal.

## Consequences

The first shape of this grew one hook per concern — fold a value, fold the kind, default a
missing label — each added only after the previous proved insufficient. Three hooks that
must all agree is three chances to disagree, and disagreement is silent. Collapsing them
into one override before moving to the next signal was cheaper than carrying the shape
forward; traces then reused it unchanged.

A related case: **an absent dimension is not an empty one.** A log record with no
`severity_text` produced a row with no severity dimension at all, sitting beside the
`unknown` row and invisible to any query filtering on severity. Defaults are applied while
the key is built, for the same reason.

The duplicate-key guard is the output-side mirror of the read layer's identity-collision
guard. Both exist because a duplicate key is never an error downstream — only a quietly
wrong number.
