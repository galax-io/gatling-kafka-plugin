#!/usr/bin/env bash
# Install spec-kit extensions and presets for a project.
#
# Native extensions (agent-context, bug, git) install by name from the catalog.
# Community ones live in a discovery-only catalog (install_allowed: false), so
# they install via --from <github-archive-url> and trigger an "Untrusted Source"
# confirmation, which this script auto-answers (running it = you authorizing it).
#
# Requires the `specify` CLI on PATH (https://github.com/github/spec-kit).
#
# Usage:
#   bash setup-speckit.sh            # additive: install what's missing, skip existing
#   bash setup-speckit.sh --force    # reinstall extensions over existing (DANGER:
#                                     # overwrites customized files; presets have no --force)

set -euo pipefail

command -v specify >/dev/null 2>&1 || {
  echo "error: 'specify' CLI not found on PATH. Install spec-kit first." >&2; exit 2; }

FORCE_FLAG=""
[[ "${1:-}" == "--force" ]] && FORCE_FLAG="--force"

# Mirror all output to a log so failures can be inspected after the fact.
# Capture tee's PID and `wait` for it on exit — process-substitution is
# backgrounded, so without the trap the script can return before tee
# flushes its last buffered writes and its stdout can interleave with the
# caller's prompt after exit.
LOG="$(cd "$(dirname "$0")" && pwd)/speckit-install.log"
exec > >(tee "$LOG") 2>&1
TEE_PID=$!
trap 'exec >&- 2>&-; wait "$TEE_PID" 2>/dev/null || true' EXIT

# Extensions/presets need a base spec-kit project (a .specify/ dir); without it
# `specify extension add` fails with "Not a spec-kit project". If the base is
# missing, lay it down first. Idempotent: skipped once .specify/ exists.
#   --integration claude   this template targets Claude Code (.claude/, CLAUDE.md)
#   --force                merge into the non-empty generated project, no prompt
#   --ignore-agent-tools   init writes command/skill files even if the claude CLI
#                          is absent (e.g. CI / copier post-gen)
# init is additive: it preserves .claude/settings.json, hooks, and AGENTS.md, and
# only appends a marker block to CLAUDE.md.
if [[ ! -d .specify ]]; then
  echo
  echo "==> Base spec-kit not found — running 'specify init'"
  specify init --here --integration claude --script sh --force --ignore-agent-tools
fi

# A single failed install must NOT abort the rest (set -e would otherwise kill the
# script on the first ✗). Collect failures and report them at the end instead.
FAILED=()

run_ext() {     # id  <add-args...>
  local id="$1"; shift
  local out rc=0
  # specify prompts "Untrusted Source — Continue? [y/N]" for external --from URLs.
  # Auto-answer y so the script is non-interactive.
  out=$(printf 'y\n' | specify extension add "$@" ${FORCE_FLAG} 2>&1) || rc=$?
  if [[ $rc -eq 0 ]]; then
    echo "  ✓  $id"
  elif echo "$out" | grep -q "already installed"; then
    echo "  –  $id (already installed; --force to reinstall)"
  else
    echo "  ✗  $id"; echo "$out" | sed 's/^/      /'; FAILED+=("$id")
  fi
}

run_preset() {  # id  <add-args...>   (preset add has no --force)
  local id="$1"; shift
  local out rc=0
  out=$(printf 'y\n' | specify preset add "$@" 2>&1) || rc=$?
  if [[ $rc -eq 0 ]]; then
    echo "  ✓  $id"
  elif echo "$out" | grep -q "already installed"; then
    echo "  –  $id (already installed)"
  else
    echo "  ✗  $id"; echo "$out" | sed 's/^/      /'; FAILED+=("$id")
  fi
}

gh_zip() { echo "https://github.com/$1/archive/refs/tags/$2.zip"; }

echo
echo "==> Native extensions (catalog by name)"
run_ext agent-context  agent-context
run_ext bug            bug
run_ext git            git

echo
echo "==> Community extensions (--from github archive)"
run_ext worktrees  worktrees  --from "$(gh_zip dango85/spec-kit-worktree-parallel  v1.3.2)"
run_ext harness    harness    --from "$(gh_zip formin/spec-kit-harness            v1.0.0)"
# spectest + changelog: the original Quratulain-bilal v1.0.0 extensions are broken
# (manifests fail `specify`'s validator, and commands read the spec from
# .specify/spec.md instead of specs/<feature>/spec.md). We maintain fixed copies in
# our own forks — these forks are the canonical source for this template, not a
# temporary patch. Pinned tags below; bump when re-tagging a fork.
#   https://github.com/jigarkhwar/spec-kit-spectest   (tag v1.1.0)
#   https://github.com/jigarkhwar/spec-kit-changelog  (tag v1.2.0)
run_ext spectest   spectest   --from "$(gh_zip jigarkhwar/spec-kit-spectest  v1.1.0)"
run_ext changelog  changelog  --from "$(gh_zip jigarkhwar/spec-kit-changelog v1.2.0)"

echo
echo "==> Presets (--from github archive)"
run_preset claude-ask-questions  claude-ask-questions \
  --from "$(gh_zip 0xrafasec/spec-kit-preset-claude-ask-questions v1.0.0)"

echo
if [[ ${#FAILED[@]} -gt 0 ]]; then
  echo "Done with FAILURES: ${FAILED[*]}"
  exit 1
fi
echo "Done."
