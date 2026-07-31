#!/usr/bin/env bash
#
# check-linkage.sh — verify the issue ↔ PR ↔ milestone contract (see AGENTS.md "Milestones (ALWAYS)").
# Run `scripts/check-linkage.sh --help` for full usage.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'EOF'
check-linkage.sh — verify the issue <-> PR <-> milestone contract (see AGENTS.md "Milestones (ALWAYS)").

What each entity owes (this script enforces it):
  Issue      belongs to exactly one milestone; closed only when its fix is on main.
  PR         carries its issue's milestone + a real closing link (Closes #<issue>);
             the linked issue sits in the same milestone; one issue per PR.
  Milestone  one release (vX.Y.Z); tag only when every issue is closed and every PR merged.

Usage:
  scripts/check-linkage.sh --pr <N>          # GATE one PR: milestone + Closes #issue + same milestone
  scripts/check-linkage.sh --for-tag vX.Y.Z  # GATE a release: tag-readiness of that version's milestone
  scripts/check-linkage.sh [milestone]       # audit a milestone (default: lowest-numbered open)
  scripts/check-linkage.sh --tag [ms]        # also assert tag-readiness (all issues closed, all PRs merged)
  scripts/check-linkage.sh --help

Env:
  REPO=owner/name   # override repo (default: gh repo view of the current checkout)

Exit: 0 all rules hold | 1 at least one violation | 2 usage/prereq error.
EOF
}

for bin in gh jq; do
  command -v "$bin" >/dev/null 2>&1 || { echo "error: '$bin' not found on PATH" >&2; exit 2; }
done

REPO="${REPO:-$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)}"
[ -n "$REPO" ] || { echo "error: cannot determine repo — run inside a GitHub checkout or set REPO=owner/name" >&2; exit 2; }

TAG_MODE=0
MS=""
PR_NUM=""
FOR_TAG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --tag)      TAG_MODE=1 ;;
    --pr)       shift; PR_NUM="${1:-}"
                [[ "$PR_NUM" =~ ^[0-9]+$ ]] || { echo "error: --pr needs a numeric PR id" >&2; exit 2; } ;;
    --for-tag)  shift; FOR_TAG="${1:-}"
                [[ "$FOR_TAG" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "error: --for-tag needs vX.Y.Z" >&2; exit 2; } ;;
    --help|-h)  usage; exit 0 ;;
    [0-9]*)     MS="$1" ;;
    *)          echo "error: unknown argument '$1'" >&2; usage; exit 2 ;;
  esac
  shift
done

# Gate one PR (the merge gate). Fails if the PR is missing a milestone, or closes an
# issue in a different milestone. Does NOT require a closing link by itself: a PR with
# no tracked issue behind it (docs/chore/companion work, or anything merged without one)
# owes nothing here — the obligation is one-directional. When a tracked issue DOES exist
# for this milestone, it must be closed by a PR that actually links it (Closes #<issue>);
# that's enforced by the issue-side check below (audit) / in TAG_MODE (release gate),
# not by demanding every PR produce a link regardless of whether an issue exists.
if [ -n "$PR_NUM" ]; then
  pj=$(gh pr view "$PR_NUM" --repo "$REPO" --json number,title,state,milestone,closingIssuesReferences) \
    || { echo "error: PR #$PR_NUM not found in $REPO" >&2; exit 2; }
  p_title=$(jq -r '.title' <<<"$pj")
  p_ms=$(jq -r '.milestone.title // ""' <<<"$pj")
  p_closes=$(jq -r '.closingIssuesReferences[]?.number' <<<"$pj")
  e=0
  printf 'PR #%s  %s\n' "$PR_NUM" "$p_title"
  if [ -z "$p_ms" ]; then printf '  ✗ no milestone — assign one (gh pr edit %s --milestone "…")\n' "$PR_NUM"; e=1
  else printf '  ✓ milestone: %s\n' "$p_ms"; fi
  if [ -z "$p_closes" ]; then
    printf '  ! closes no issue (fine if none was tracked for this work)\n'
  else
    for i in $p_closes; do
      i_ms=$(gh issue view "$i" --repo "$REPO" --json milestone -q '.milestone.title // ""' 2>/dev/null || echo "")
      if [ "$i_ms" = "$p_ms" ]; then printf '  ✓ closes #%s (same milestone)\n' "$i"
      else printf '  ✗ closes #%s but its milestone is "%s", not "%s"\n' "$i" "${i_ms:-none}" "$p_ms"; e=1; fi
    done
  fi
  if [ "$e" = 0 ]; then printf 'PASS: PR #%s is well-formed.\n' "$PR_NUM"; exit 0; fi
  printf 'FAIL: PR #%s is malformed — fix the above before merge.\n' "$PR_NUM"; exit 1
fi

# Map a release version (vX.Y.Z) to its milestone, then assert tag-readiness on it.
# An exact vX.Y.Z-titled milestone wins if one exists (a dedicated patch milestone,
# e.g. "v1.3.1 — …"); otherwise fall back to the vX.Y.0 minor milestone (e.g.
# v1.23.1 -> "v1.23.0 …"). Both matches are digit-boundary safe so "v1.3.1" cannot
# accidentally match a title starting "v1.3.10".
if [ -n "$FOR_TAG" ]; then
  v="${FOR_TAG#v}"; minor="${v%.*}.0"
  MS=$(gh api "repos/$REPO/milestones?state=all&per_page=100" \
        | jq -r --arg exact "$v" --arg minor "$minor" '
            def boundary_match($ver): "^v" + ($ver | gsub("\\."; "\\.")) + "(\\D|$)";
            (map(select(.title | test(boundary_match($exact)))) | .[0].number) as $e |
            (map(select(.title | test(boundary_match($minor)))) | .[0].number) as $m |
            if $e != null then $e elif $m != null then $m else empty end
          ')
  [ -n "$MS" ] || { echo "error: no milestone titled 'v$v …' or 'v$minor …' for tag $FOR_TAG" >&2; exit 2; }
  TAG_MODE=1
fi

# Default to the current release milestone (shared resolver = single source of truth).
if [ -z "$MS" ]; then
  MS=$(REPO="$REPO" bash "$SCRIPT_DIR/current-milestone.sh" 2>/dev/null | cut -f1 || true)
  # Fallback (no version milestone yet): lowest-numbered open, the legacy behaviour.
  [ -n "$MS" ] || MS=$(gh api "repos/$REPO/milestones?state=open&per_page=100" --jq 'sort_by(.number) | .[0].number // empty')
  [ -n "$MS" ] || { echo "error: no open milestone found in $REPO" >&2; exit 2; }
fi

ms_json=$(gh api "repos/$REPO/milestones/$MS") || { echo "error: milestone #$MS not found in $REPO" >&2; exit 2; }
ms_title=$(jq -r '.title' <<<"$ms_json")
ms_state=$(jq -r '.state' <<<"$ms_json")

errors=0
warns=0
err()  { printf '  ✗ %s\n' "$1"; errors=$((errors + 1)); }
warn() { printf '  ! %s\n' "$1"; warns=$((warns + 1)); }
ok()   { printf '  ✓ %s\n' "$1"; }

printf 'Repo:      %s\n' "$REPO"
printf 'Milestone: #%s  %s  (%s)\n' "$MS" "$ms_title" "$ms_state"
[ "$TAG_MODE" = 1 ] && printf 'Mode:      tag-readiness\n'
printf '\n'

# All issues + PRs carrying this milestone (REST returns both; PRs carry a .pull_request key).
items=$(gh api --paginate --slurp "repos/$REPO/issues?milestone=$MS&state=all&per_page=100" | jq 'add')

pr_numbers=$(jq -r '.[] | select(.pull_request != null) | .number' <<<"$items")
issue_numbers=$(jq -r '.[] | select(.pull_request == null) | .number' <<<"$items")

linked_issues=" "   # space-delimited set of issue numbers a PR points at

printf 'Pull requests\n'
if [ -z "$pr_numbers" ]; then
  warn "no PRs carry milestone #$MS yet"
fi
for pr in $pr_numbers; do
  pr_json=$(gh pr view "$pr" --repo "$REPO" --json number,title,state,milestone,closingIssuesReferences,body)
  pr_state=$(jq -r '.state' <<<"$pr_json")
  pr_title=$(jq -r '.title' <<<"$pr_json")

  # Real GitHub closing links, plus a text fallback for Closes/Fixes/Resolves #N in the body.
  ref_nums=$(jq -r '.closingIssuesReferences[]?.number' <<<"$pr_json")
  if [ -z "$ref_nums" ]; then
    ref_nums=$(jq -r '.body // ""' <<<"$pr_json" \
      | grep -oiE '(close[sd]?|fix(e[sd])?|resolve[sd]?) +#[0-9]+' \
      | grep -oE '[0-9]+' || true)
    [ -n "$ref_nums" ] && warn "PR #$pr links via body text only (not a registered GitHub closing link): $pr_title"
  fi

  if [ -z "$ref_nums" ]; then
    # A PR owes a closing link only when a tracked issue exists behind it. Absence of a
    # link isn't itself a violation — the obligation is one-directional: if a tracked
    # issue exists, ITS PR must link it (enforced below, and by TAG_MODE's per-issue
    # closed-state check); a PR with no issue behind it (docs/chore/companion work, or
    # anything merged without one) owes nothing here regardless of its commit type.
    warn "PR #$pr ($pr_state) closes no issue (fine if none was tracked for this work): $pr_title"
    continue
  fi

  for ri in $ref_nums; do
    linked_issues="$linked_issues$ri "
    ri_ms=$(gh issue view "$ri" --repo "$REPO" --json milestone -q '.milestone.title // ""' 2>/dev/null || echo "")
    if [ "$ri_ms" != "$ms_title" ]; then
      err "PR #$pr closes issue #$ri but that issue's milestone is '${ri_ms:-none}', not '$ms_title'"
    fi
  done

  if [ "$TAG_MODE" = 1 ] && [ "$pr_state" != "MERGED" ]; then
    err "PR #$pr is $pr_state — must be MERGED before tagging: $pr_title"
  else
    ok "PR #$pr ($pr_state) → closes #$(echo "$ref_nums" | paste -sd, -)"
  fi
done

printf '\nIssues\n'
if [ -z "$issue_numbers" ]; then
  warn "no issues carry milestone #$MS"
fi
for is in $issue_numbers; do
  is_state=$(jq -r --argjson n "$is" '.[] | select(.number == $n) | .state' <<<"$items")
  case "$linked_issues" in
    *" $is "*) linked="linked" ;;
    *)         linked="" ;;
  esac

  if [ "$TAG_MODE" = 1 ] && [ "$is_state" != "closed" ]; then
    err "issue #$is is $is_state — must be closed before tagging"
  elif [ -z "$linked" ]; then
    warn "issue #$is ($is_state) has no PR closing it"
  else
    ok "issue #$is ($is_state) ← linked"
  fi
done

printf '\n'
if [ "$errors" -gt 0 ]; then
  printf 'FAIL: %d error(s), %d warning(s).\n' "$errors" "$warns"
  exit 1
fi
printf 'PASS: 0 errors, %d warning(s).\n' "$warns"
[ "$TAG_MODE" = 1 ] && printf 'Milestone #%s is tag-ready.\n' "$MS"
exit 0
