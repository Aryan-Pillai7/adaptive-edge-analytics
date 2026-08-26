#!/usr/bin/env bash
# Shared helpers for scripts/. Sourced, not executed.
#
# These scripts are the canonical entrypoints for this repo. `make` is not installed on
# the primary dev machine (Windows 11 + Git Bash), so there is no Makefile alias layer
# here at all -- unlike the sibling project, nothing in this repo runs inside a
# container, so there is no Linux-contributor case for one.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# --- paths ----------------------------------------------------------------
# Git Bash on Windows rewrites POSIX-looking paths in argv before handing them to
# docker.exe, which corrupts -v and container-side paths. `pwd -W` gives the
# Windows-native path docker actually wants. Written as an if/else rather than
# `A && B || C`, which is not if-then-else: a failing `pwd -W` would silently fall
# through to `pwd` and hand docker a path it cannot mount.
host_path() {
  if [[ "${OSTYPE:-}" == msys* || "${OSTYPE:-}" == cygwin* ]]; then
    (cd "$1" && pwd -W)
  else
    (cd "$1" && pwd)
  fi
}

# --- output ---------------------------------------------------------------
if [[ -t 1 ]]; then
  C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YLW=$'\033[33m'; C_DIM=$'\033[2m'; C_RST=$'\033[0m'
else
  C_RED=""; C_GRN=""; C_YLW=""; C_DIM=""; C_RST=""
fi

info() { printf '%s==>%s %s\n' "$C_DIM" "$C_RST" "$*"; }
ok()   { printf '%s  ok%s %s\n' "$C_GRN" "$C_RST" "$*"; }
warn() { printf '%s WARN%s %s\n' "$C_YLW" "$C_RST" "$*" >&2; }
die()  { printf '%sFAIL%s %s\n' "$C_RED" "$C_RST" "$*" >&2; exit 1; }

# --- python ---------------------------------------------------------------
# Prefer the repo venv, fall back to whatever `python` is on PATH. The venv is where
# the package is installed in editable mode, so the fallback will usually only work for
# lint, not for running the CLI -- which is the correct failure to have.
py() {
  if [[ -x "$REPO_ROOT/.venv/Scripts/python.exe" ]]; then
    "$REPO_ROOT/.venv/Scripts/python.exe" "$@"      # Windows / Git Bash
  elif [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
    "$REPO_ROOT/.venv/bin/python" "$@"              # Linux / CI
  else
    python "$@"
  fi
}

# Same idea as py(): the venv is where dev tooling is installed, and it is not on PATH
# in Git Bash. Without this, lint silently skips ruff locally and only ever runs it in
# CI -- and a check you cannot run locally is a check you cannot iterate on.
ruff_bin() {
  if [[ -x "$REPO_ROOT/.venv/Scripts/ruff.exe" ]]; then
    echo "$REPO_ROOT/.venv/Scripts/ruff.exe"
  elif [[ -x "$REPO_ROOT/.venv/bin/ruff" ]]; then
    echo "$REPO_ROOT/.venv/bin/ruff"
  elif command -v ruff >/dev/null 2>&1; then
    command -v ruff
  fi
}
