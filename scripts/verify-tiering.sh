#!/usr/bin/env bash
# The tiering gate: is the hot/cold boundary actually safe, and what does it buy?
#
# Two questions, and the first one matters more:
#
#   1. Does every backend keep raw data longer than the job needs to reach it?
#      If not, data expires unrolled and the resulting empty bucket is
#      indistinguishable from a genuinely quiet hour. Silent, permanent loss.
#
#   2. How much smaller is the cold tier than the raw data it summarises?
#
# Exits non-zero when (1) fails, so this is usable as a gate and not just a report.
#
#   bash scripts/verify-tiering.sh                     hourly cadence (the default)
#   bash scripts/verify-tiering.sh --interval 6        if the cron runs every 6 hours

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

interval=1
margin=2
while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval) interval="$2"; shift 2 ;;
    --margin)   margin="$2";   shift 2 ;;
    -h|--help)  sed -n '2,16p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

info "auditing retention and measuring the hot/cold ratio"
if py -m edgerollup.cli tiering \
     --job-interval-hours "$interval" \
     --safety-margin-hours "$margin"; then
  ok "tiering is safe: every backend outlives the job's reach"
else
  # Exit 6 from the CLI means a retention is too short for the cadence. That is a
  # data-loss condition, not a warning -- raw data can expire before the job reaches it.
  die "a backend's hot retention is too short for a ${interval}h job cadence"
fi
