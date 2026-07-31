#!/usr/bin/env bash
#
# install-hooks.sh — enable this repo's shared scalafmt pre-commit git hook.
# Points core.hooksPath at the tracked .githooks/ dir. Idempotent; run once per clone.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
chmod +x .githooks/pre-commit 2>/dev/null || true
git config core.hooksPath .githooks

# `git config core.hooksPath` writes the repo-shared config, but a linked worktree with
# extensions.worktreeConfig enabled can carry its own core.hooksPath that takes precedence
# and would silently shadow the value just written. Report instead of no-op'ing.
effective=$(git config --get core.hooksPath || true)
if [ "$effective" != ".githooks" ]; then
  echo "warning: core.hooksPath resolves to '${effective}', not '.githooks'." >&2
  echo "         A per-worktree config is overriding it. To enable the hook here, run:" >&2
  echo "           git config --worktree core.hooksPath .githooks" >&2
  exit 1
fi

echo "git hook enabled — core.hooksPath -> ${effective}"
echo "  pre-commit: sbt scalafmtAll scalafmtSbt (+ re-stage)"
echo "  bypass:     SKIP_SCALAFMT=1  (or git commit --no-verify)"
