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
  Feature    an issue in the backlog repository whose milestone says "Release: business"
             on a line of its own. It sits in that business milestone, not in the version
             milestone of the PR that does a part of it, so a link to a feature
             ("Part of <backlog>#N") skips the same-milestone rule. The PR still carries
             its own version milestone.
  Feature link  every PR of a product repository links a feature of the backlog: "Part of
             <backlog>#N" (or a closing link) to an issue whose milestone says "Release: business".
             A bot's PR (a bot author, or a dependabot/ or update/ branch) and the backlog's own
             PRs are exempt. Maintenance with no feature of its own links the standing feature
             (the issue with the label "standing") of the active business milestone.
  Milestone  one release (vX.Y.Z); tag only when every issue is closed and every PR merged.

Usage:
  scripts/check-linkage.sh --pr <N>          # GATE one PR: milestone + Closes #issue + same milestone
  scripts/check-linkage.sh --for-tag vX.Y.Z  # GATE a release: tag-readiness of that version's milestone
  scripts/check-linkage.sh [milestone]       # audit a milestone (default: lowest-numbered open)
  scripts/check-linkage.sh --tag [ms]        # also assert tag-readiness (all issues closed, all PRs merged)
  scripts/check-linkage.sh --help

Env:
  REPO=owner/name          # override repo (default: gh repo view of the current checkout)
  BACKLOG_REPO=owner/name  # the repository whose business milestones hold features
                           # (default: galax-io/galaxio-sdlc-backlog)
  BACKLOG_READ_TOKEN=token # a read-only token for the backlog repository (Issues and Metadata). The
                           # backlog is private, so the job's own token cannot read it; this one is used
                           # for calls about the backlog and for nothing else.
  FEATURE_LINK=required|optional   # required (default): a PR without a feature of the backlog fails
                           # the gate. optional only for a repository not yet on the rule.

Exit: 0 all rules hold | 1 at least one violation | 2 usage/prereq error.
EOF
}

for bin in gh jq; do
  command -v "$bin" >/dev/null 2>&1 || { echo "error: '$bin' not found on PATH" >&2; exit 2; }
done

REPO="${REPO:-$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)}"
[ -n "$REPO" ] || { echo "error: cannot determine repo — run inside a GitHub checkout or set REPO=owner/name" >&2; exit 2; }

BACKLOG_REPO="${BACKLOG_REPO:-galax-io/galaxio-sdlc-backlog}"
FEATURE_LINK="${FEATURE_LINK:-required}"
case "$FEATURE_LINK" in required|optional) ;; *) echo "error: FEATURE_LINK must be required or optional, not '$FEATURE_LINK'" >&2; exit 2 ;; esac

gh_for() { # owner/repo, gh arguments: gh, with BACKLOG_READ_TOKEN when the repository is the backlog
  local repo="$1"; shift
  if [ "$repo" = "$BACKLOG_REPO" ] && [ -n "${BACKLOG_READ_TOKEN:-}" ]; then GH_TOKEN="$BACKLOG_READ_TOKEN" gh "$@"; else gh "$@"; fi
}
BACKLOG_SEEN=""
backlog_readable() { # whether this job can read the backlog repository (asked once)
  if [ -z "$BACKLOG_SEEN" ]; then
    if gh_for "$BACKLOG_REPO" api "repos/$BACKLOG_REPO" --silent >/dev/null 2>&1; then BACKLOG_SEEN=yes; else BACKLOG_SEEN=no; fi
  fi
  [ "$BACKLOG_SEEN" = yes ]
}
cannot_read_backlog() { # text of the failure that names what is missing
  printf 'cannot read the backlog %s with this job'"'"'s token, so a feature link cannot be checked: give the job BACKLOG_READ_TOKEN, a read-only token for %s (Issues and Metadata)' "$BACKLOG_REPO" "$BACKLOG_REPO"
}

# A feature is an issue of the backlog repository whose milestone description has the
# line "Release: business". Prints that milestone's title for a feature, nothing otherwise.
feature_ms() { # N (an issue of $BACKLOG_REPO) -> the business milestone's title
  gh_for "$BACKLOG_REPO" issue view "$1" --repo "$BACKLOG_REPO" --json milestone -q '
    select(.milestone != null and ((.milestone.description // "")
      | test("(^|\n)[ \t]*Release:[ \t]*business[ \t]*\r?(\n|$)"; "i"))) | .milestone.title' 2>/dev/null || true
}
# "Part of <backlog>#N" lines of a PR body -> the issue numbers N (backlog repository only).
backlog_part_of() { # body
  local re="^[[:space:]]*part of[[:space:]]+${BACKLOG_REPO//./\\.}#([0-9]+)"
  printf '%s\n' "$1" | tr -d '\r' | grep -iE "$re" | grep -oE '#[0-9]+' | tr -d '#' | sort -un || true
}

is_bot_pr() { # pr-json -> succeeds when a bot opened it: a bot author, or Dependabot's or Scala Steward's branch
  jq -e '(.author.is_bot == true) or ((.author.login // "") | test("\\[bot\\]$|^app/")) or ((.headRefName // "") | test("^(dependabot|update)/"))' <<<"$1" >/dev/null 2>&1
}
feature_refs() { # pr-json -> the backlog issue numbers it links (Part of lines, closing links into the backlog) that are features
  local i
  { backlog_part_of "$(jq -r '.body // ""' <<<"$1")"
    jq -r --arg b "$BACKLOG_REPO" '.closingIssuesReferences[]? | select((.repository.owner.login // "") + "/" + (.repository.name // "") == $b) | .number' <<<"$1"
  } | awk 'NF && !seen[$0]++' | while read -r i; do
    [ -z "$(feature_ms "$i")" ] || echo "$i"
  done
}

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
  pj=$(gh pr view "$PR_NUM" --repo "$REPO" --json number,title,state,milestone,closingIssuesReferences,body,author,headRefName) \
    || { echo "error: PR #$PR_NUM not found in $REPO" >&2; exit 2; }
  p_title=$(jq -r '.title' <<<"$pj")
  p_ms=$(jq -r '.milestone.title // ""' <<<"$pj")
  p_closes=$(jq -r '.closingIssuesReferences[]?.number' <<<"$pj")
  p_parts=$(backlog_part_of "$(jq -r '.body // ""' <<<"$pj")")
  e=0
  printf 'PR #%s  %s\n' "$PR_NUM" "$p_title"
  if [ -z "$p_ms" ]; then printf '  ✗ no milestone — assign one (gh pr edit %s --milestone "…")\n' "$PR_NUM"; e=1
  else printf '  ✓ milestone: %s\n' "$p_ms"; fi
  for n in $p_parts; do
    f_ms=$(feature_ms "$n")
    if [ -n "$f_ms" ]; then printf '  ✓ links %s#%s (feature of business milestone "%s")\n' "$BACKLOG_REPO" "$n" "$f_ms"; continue; fi
    if ! backlog_readable; then printf '  ✗ links %s#%s, but %s\n' "$BACKLOG_REPO" "$n" "$(cannot_read_backlog)"; e=1; continue; fi
    n_ms=$(gh_for "$BACKLOG_REPO" issue view "$n" --repo "$BACKLOG_REPO" --json milestone -q '.milestone.title // ""' 2>/dev/null || echo "")
    if [ -n "$p_ms" ] && [ "$n_ms" = "$p_ms" ]; then printf '  ✓ links %s#%s (same milestone)\n' "$BACKLOG_REPO" "$n"
    else printf '  ✗ links %s#%s but its milestone is "%s", not "%s"\n' "$BACKLOG_REPO" "$n" "${n_ms:-none}" "$p_ms"; e=1; fi
  done
  if [ -z "$p_closes" ] && [ -z "$p_parts" ]; then
    printf '  ! closes no issue (fine if none was tracked for this work)\n'
  else
    for i in $p_closes; do
      i_ms=$(gh issue view "$i" --repo "$REPO" --json milestone -q '.milestone.title // ""' 2>/dev/null || echo "")
      if [ "$i_ms" = "$p_ms" ]; then printf '  ✓ closes #%s (same milestone)\n' "$i"
      else printf '  ✗ closes #%s but its milestone is "%s", not "%s"\n' "$i" "${i_ms:-none}" "$p_ms"; e=1; fi
    done
  fi
  # Every PR belongs to a feature of the backlog; the backlog's own PRs and bots' are exempt.
  if [ "$FEATURE_LINK" = required ] && [ "$REPO" != "$BACKLOG_REPO" ]; then
    if is_bot_pr "$pj"; then
      printf '  ✓ a bot'"'"'s PR needs no feature of the backlog\n'
    else
      feats=$(feature_refs "$pj")
      if [ -z "$feats" ] && [ -n "$p_parts" ] && ! backlog_readable; then
        :   # said above, link by link
      elif [ -z "$feats" ]; then
        printf '  ✗ links no feature of the backlog — add "Part of %s#<feature>" (maintenance with no feature of its own: the standing feature of the active business milestone, the issue with the label standing)\n' "$BACKLOG_REPO"; e=1
      else
        for f in $feats; do
          case " $(printf '%s ' $p_parts)" in *" $f "*) ;; *) printf '  ✓ links %s#%s (feature of business milestone "%s")\n' "$BACKLOG_REPO" "$f" "$(feature_ms "$f")" ;; esac
        done
      fi
    fi
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
  pr_json=$(gh pr view "$pr" --repo "$REPO" --json number,title,state,milestone,closingIssuesReferences,body,author,headRefName)
  pr_state=$(jq -r '.state' <<<"$pr_json")
  pr_title=$(jq -r '.title' <<<"$pr_json")

  # A PR of a product repository belongs to a feature of the backlog (a warning here, the gate fails it).
  if [ "$FEATURE_LINK" = required ] && [ "$REPO" != "$BACKLOG_REPO" ] && ! is_bot_pr "$pr_json" \
     && [ -z "$(feature_refs "$pr_json")" ]; then
    warn "PR #$pr links no feature of the backlog (Part of $BACKLOG_REPO#N): $pr_title"
  fi

  # Real GitHub closing links, plus a text fallback for Closes/Fixes/Resolves #N in the body.
  ref_nums=$(jq -r '.closingIssuesReferences[]?.number' <<<"$pr_json")
  if [ -z "$ref_nums" ]; then
    ref_nums=$(jq -r '.body // ""' <<<"$pr_json" \
      | grep -oiE '(close[sd]?|fix(e[sd])?|resolve[sd]?) +#[0-9]+' \
      | grep -oE '[0-9]+' || true)
    [ -n "$ref_nums" ] && warn "PR #$pr links via body text only (not a registered GitHub closing link): $pr_title"
  fi

  # Links to the backlog: a feature sits in its business milestone, so only an ordinary
  # backlog issue has to match this milestone.
  part_refs=""
  for n in $(backlog_part_of "$(jq -r '.body // ""' <<<"$pr_json")"); do
    part_refs="${part_refs:+$part_refs,}$BACKLOG_REPO#$n"
    [ -z "$(feature_ms "$n")" ] || continue
    pn_ms=$(gh_for "$BACKLOG_REPO" issue view "$n" --repo "$BACKLOG_REPO" --json milestone -q '.milestone.title // ""' 2>/dev/null || echo "")
    [ "$pn_ms" = "$ms_title" ] || err "PR #$pr links $BACKLOG_REPO#$n but that issue's milestone is '${pn_ms:-none}', not '$ms_title'"
  done

  if [ -z "$ref_nums" ] && [ -n "$part_refs" ]; then
    if [ "$TAG_MODE" = 1 ] && [ "$pr_state" != "MERGED" ]; then
      err "PR #$pr is $pr_state — must be MERGED before tagging: $pr_title"
    else
      ok "PR #$pr ($pr_state) → $part_refs"
    fi
    continue
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
