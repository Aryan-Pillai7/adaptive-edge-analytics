#!/usr/bin/env bash
# Runs the test suite.
#
# By default: unit tests only. They need no stack, no network, and must stay fast --
# that is what makes them worth running on every save.
#
#   bash scripts/test.sh                 unit only
#   bash scripts/test.sh --integration   unit + integration (needs the sibling stack up)
#   bash scripts/test.sh --all           same as --integration

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

want_integration=0
for arg in "$@"; do
  case "$arg" in
    --integration|--all) want_integration=1 ;;
    -h|--help) sed -n '2,10p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "unknown argument: $arg" ;;
  esac
done

info "unit tests"
py -m pytest "$REPO_ROOT/tests/unit" -q || die "unit tests failed"
ok "unit tests passed"

if [[ $want_integration -eq 1 ]]; then
  info "integration tests"
  # These assert against the RUNNING sibling stack. The suite skips itself loudly if the
  # stack is absent -- a missing stack is a setup problem, not a defect in the pipeline,
  # and a wall of red would bury the one line that says so.
  py -m pytest "$REPO_ROOT/tests/integration" -q -rs -m integration || die "integration tests failed"
  ok "integration tests passed"
else
  info "integration tests skipped (pass --integration to run them)"
fi
