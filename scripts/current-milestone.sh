#!/usr/bin/env bash
#
# current-milestone.sh — print the CURRENT release milestone as "<number>\t<title>".
# Prints nothing (exit 0) when no version milestone is open.
#
# "Current" = the single open milestone whose title starts with a semver `vX.Y.Z`
# (lowest version if several are open — full major.minor.patch, so a patch
# milestone like `v1.3.1` sorts correctly against `v1.3.0` and against `v1.4.0`).
# Single source of truth for "which milestone is current" — shared by
# scripts/check-linkage.sh and .claude/hooks/milestone-guard.sh.
#
# Env: REPO=owner/name overrides the repo (default: gh repo view of the checkout).
# Exit: 0 ok (may print empty) | 2 cannot determine repo.
set -uo pipefail

REPO="${REPO:-$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)}"
[ -n "$REPO" ] || { echo "current-milestone: cannot determine repo — set REPO=owner/name" >&2; exit 2; }

gh api "repos/$REPO/milestones?state=open&per_page=100" --jq '
  [ .[]
    | select(.title | test("^v[0-9]+\\.[0-9]+\\.[0-9]+"))
    | . + { _k: ((.title | capture("^v(?<a>[0-9]+)\\.(?<b>[0-9]+)\\.(?<c>[0-9]+)"))
                 | (.a|tonumber) * 100000000 + (.b|tonumber) * 100000 + (.c|tonumber)) }
  ]
  | sort_by(._k) | (.[0] // null)
  | if . == null then empty else "\(.number)\t\(.title)" end'
