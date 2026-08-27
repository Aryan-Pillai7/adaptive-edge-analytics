# 0005 — The cold tier is excluded from every source selector

**Status:** accepted

## Context

The metrics source originally used a selector matching every series. That was fine until
the first rollups landed in VictoriaMetrics — after which a second pass found
`aea_rollup_edgeapp_requests_total_delta` sitting in its own input, and began rolling up
its own rollups.

Nothing errored. No exception, no failed bucket, no warning. The numbers simply stopped
meaning anything.

## Decision

Every signal declares its source selector in `config/rollup.yaml`, and metrics and logs
exclude the cold tier **two independent ways**:

- `tier!="cold"` — catches anything correctly labelled;
- a name-prefix exclusion on `aea_rollup_*` — catches anything written before the label
  existed, or by hand.

A signal with no configured selector falls back and **warns**, because the fallback does
not exclude the cold tier.

## Consequences

Either guard alone is one edit away from reopening the loop, which is why there are two.
Tempo needs neither: nothing is ever written to it, so it cannot contain rollups.

This is the strongest argument for giving rolled-up series their own metric name rather
than only a `tier` label. The loop was *visible* in the logs precisely because rollups
carry a distinct name; with only a label it would have read as ordinary raw data.

A feedback loop that silently stops meaning anything, rather than erroring, is the failure
mode most likely to survive review — it produces no symptom at all until someone questions
a number.
