#!/usr/bin/env bash
# Fast, dependency-light checks. Runs the same way locally and in CI.

# Tells shellcheck -x where to find the sourced file. The path is built at runtime, so
# without this it looks for ./lib.sh relative to the CWD, fails, and emits SC1091 --
# which is only "info" severity but still exits non-zero and fails the lint.
# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

failed=0

# --- YAML well-formedness -------------------------------------------------
# Covers config/rollup.yaml and the CI workflow.
info "yaml parse"
if py -c "import yaml" 2>/dev/null; then
  # No `|| true` on the git call: a failing git must fail the lint, not read as
  # "nothing to check".
  mapfile -t yamls < <(cd "$REPO_ROOT" && git ls-files '*.yaml' '*.yml')
  if [[ ${#yamls[@]} -eq 0 ]]; then
    warn "no tracked yaml files yet — skipping"
  else
    for y in "${yamls[@]}"; do
      if py -c "import sys,yaml;list(yaml.safe_load_all(open(sys.argv[1],encoding='utf-8')))" \
           "$REPO_ROOT/$y" >/dev/null; then
        ok "$y"
      else
        warn "$y is not valid yaml"; failed=1
      fi
    done
  fi
else
  warn "pyyaml unavailable — skipping yaml parse"
fi

# --- Python ---------------------------------------------------------------
info "ruff"
ruff="$(ruff_bin)"
if [[ -n "$ruff" ]]; then
  "$ruff" check "$REPO_ROOT/src" "$REPO_ROOT/tests" || failed=1
  "$ruff" format --check "$REPO_ROOT/src" "$REPO_ROOT/tests" || failed=1
else
  warn "ruff not installed — skipping (pip install -e \".[dev]\")"
fi

# --- Shell ----------------------------------------------------------------
info "shell syntax"
for s in "$REPO_ROOT"/scripts/*.sh "$REPO_ROOT"/orchestration/*.sh; do
  [[ -e "$s" ]] || continue
  bash -n "$s" || { warn "$s has a syntax error"; failed=1; }
done

# The linter below is not available as a binary on Windows/Git Bash. A lint you cannot
# run locally is a lint you cannot iterate on, so fall back to the pinned Docker image
# when the binary is missing and Docker is present.
#
# NOTE: never begin a prose comment with that tool's name immediately after the hash --
# it is parsed as a directive and fails with SC1072/SC1073. This comment used to.
sc_targets=("$REPO_ROOT"/scripts/*.sh)
[[ -d "$REPO_ROOT/orchestration" ]] && sc_targets+=("$REPO_ROOT"/orchestration/*.sh)

if command -v shellcheck >/dev/null 2>&1; then
  shellcheck -x "${sc_targets[@]}" || failed=1
elif command -v docker >/dev/null 2>&1; then
  info "shellcheck not installed — using ${SHELLCHECK_IMAGE:=koalaman/shellcheck:v0.11.0}"
  for dir in scripts orchestration; do
    [[ -d "$REPO_ROOT/$dir" ]] || continue
    compgen -G "$REPO_ROOT/$dir/*.sh" >/dev/null || continue
    # Basenames are expanded on the HOST: the shellcheck image has no shell, so a glob
    # passed through would arrive unexpanded and be treated as a literal filename.
    names=()
    for script in "$REPO_ROOT/$dir"/*.sh; do names+=("$(basename "$script")"); done
    # MSYS_NO_PATHCONV=1 is applied per-invocation, never exported: docker needs it ON
    # (Git Bash otherwise rewrites POSIX-looking paths in argv and corrupts -v), but
    # native git.exe needs it OFF -- it cannot resolve a /c/... path. Exporting it
    # globally would break every git call in these scripts.
    MSYS_NO_PATHCONV=1 docker run --rm -v "$(host_path "$REPO_ROOT/$dir"):/mnt" -w /mnt \
      "$SHELLCHECK_IMAGE" -x "${names[@]}" || failed=1
  done
else
  warn "shellcheck unavailable (no binary, no docker) — skipping"
fi

# CRLF in the working tree produces a wall of shellcheck parse errors that CI never
# sees, because .gitattributes normalises to LF on commit. Name it specifically rather
# than leaving someone to decode SC1017 line by line.
#
# Uses awk rather than `grep -lU`: the grep form prints nothing yet still exits 0 in
# Git Bash, so it reports "clean" on a tree that is entirely CRLF.
for script in "$REPO_ROOT"/scripts/*.sh; do
  if awk '/\r$/ { found = 1 } END { exit !found }' "$script"; then
    warn "CRLF line endings in the working tree (e.g. $(basename "$script"))"
    warn "fix with: git add --renormalize . && git checkout -- ."
    failed=1
    break
  fi
done

[[ $failed -eq 0 ]] || die "lint failed"
ok "lint clean"
