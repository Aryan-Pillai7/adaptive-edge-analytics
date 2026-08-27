#!/usr/bin/env bash
# What cron runs. Unattended, so it has different obligations to scripts/ -- which is
# why this lives in its own directory rather than alongside the developer entrypoints.
#
#   * only one run at a time, enforced with a lock
#   * every line timestamped, because the log is the only record of what happened
#   * exit codes an operator can act on, not just zero and non-zero
#
# Install with orchestration/crontab.example.
#
#   bash orchestration/run_rollup.sh                  roll up every signal
#   bash orchestration/run_rollup.sh --signal logs    just one
#   bash orchestration/run_rollup.sh --dry-run        read and report, write nothing

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCK_DIR="${EDGEROLLUP_LOCK_DIR:-$REPO_ROOT/.rollup-state/run.lock}"
LOG_DIR="${EDGEROLLUP_LOG_DIR:-$REPO_ROOT/.rollup-state/logs}"

# A run that has held the lock longer than this is assumed dead. Generous: it must
# exceed the slowest plausible run, because breaking the lock on a LIVE run gets two
# processes rolling up the same buckets. That is survivable -- claims and keyed writes
# make it correct, just wasteful -- but it is still the wrong thing to cause.
STALE_LOCK_SECONDS="${EDGEROLLUP_STALE_LOCK_SECONDS:-7200}"

timestamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { printf '%s run_rollup: %s\n' "$(timestamp)" "$*"; }

# --- locking ----------------------------------------------------------------
# `mkdir` rather than flock: flock is absent on Windows/Git Bash, which is the primary
# machine here. mkdir is atomic on every filesystem this could run on, and the PID file
# inside makes a stale lock diagnosable rather than just old.
acquire_lock() {
  mkdir -p "$(dirname "$LOCK_DIR")"

  if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$$" > "$LOCK_DIR/pid"
    date -u +%s > "$LOCK_DIR/started_at"
    return 0
  fi

  local started held owner
  started="$(cat "$LOCK_DIR/started_at" 2>/dev/null || echo 0)"
  owner="$(cat "$LOCK_DIR/pid" 2>/dev/null || echo unknown)"
  held=$(( $(date -u +%s) - started ))

  if [[ "$started" != "0" && $held -gt $STALE_LOCK_SECONDS ]]; then
    log "WARN breaking a stale lock held ${held}s by pid $owner (limit ${STALE_LOCK_SECONDS}s)"
    rm -rf "$LOCK_DIR"
    acquire_lock
    return $?
  fi

  # Not an error. An overlapping run means the previous one is still working, and the
  # correct response is to let it finish -- cron will call again next hour. Exiting
  # non-zero here would page someone about a job that is functioning.
  log "another run is in progress (pid $owner, ${held}s) — exiting without work"
  return 1
}

# --- python -----------------------------------------------------------------
py() {
  if [[ -x "$REPO_ROOT/.venv/Scripts/python.exe" ]]; then
    "$REPO_ROOT/.venv/Scripts/python.exe" "$@"
  elif [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
    "$REPO_ROOT/.venv/bin/python" "$@"
  else
    python "$@"
  fi
}

# --- run --------------------------------------------------------------------
mkdir -p "$LOG_DIR"

if ! acquire_lock; then
  exit 0
fi
# Released on every exit path, including a signal. Without this a killed run leaves the
# lock behind and every subsequent run skips until the staleness timeout expires.
#
# Inline rather than a release_lock() function: a function called only from a trap looks
# unreachable to shellcheck, and the two versions in play disagree about which code to
# report it under (SC2329 locally, SC2317 in CI). One command needs no wrapper anyway.
# Single-quoted so $LOCK_DIR expands when the trap fires, not when it is installed.
trap 'rm -rf "$LOCK_DIR"' EXIT INT TERM

log "starting (args: ${*:-none})"
started_at=$(date -u +%s)

# Output passed through unmodified. The CLI already timestamps every line, and piping
# it through a formatter here printed two timestamps per line in two different formats
# and two different timezones -- which is worse than none.
set +e
py -m edgerollup.cli run "$@"
status=$?
set -e

elapsed=$(( $(date -u +%s) - started_at ))

case "$status" in
  0) log "finished ok in ${elapsed}s" ;;
  2) log "ERROR usage or window error (exit 2) after ${elapsed}s — check the arguments" ;;
  3) log "ERROR a requested verb is not implemented (exit 3)" ;;
  4) log "ERROR a backend was unreachable (exit 4) after ${elapsed}s — retryable, cron will call again" ;;
  # Exit 5 covers both a real defect and a backend outage: an unreachable backend during
  # a run is recorded per-bucket rather than aborting, so it surfaces here rather than as
  # exit 4. Either way the buckets stay uncommitted and the next run redoes them, so the
  # outage case self-heals -- but it is worth looking at, because a persistent 5 is a
  # hole in the cold tier that will not close on its own.
  5) log "ERROR one or more buckets failed (exit 5) after ${elapsed}s — run 'edge-rollup status' to see which" ;;
  *) log "ERROR unexpected exit ${status} after ${elapsed}s" ;;
esac

exit "$status"
