#!/usr/bin/env bash
#
# check-linkage.sh — verify the issue ↔ PR ↔ milestone contract (see AGENTS.md "Milestones (ALWAYS)").
# Run `scripts/check-linkage.sh --help` for full usage.

set -euo pipefail

usage() {
  cat <<'EOF'
check-linkage.sh — verify the issue <-> PR <-> milestone contract (see AGENTS.md "Milestones (ALWAYS)").

What each entity owes (this script enforces it):
  Issue      an important task, not a technical one; belongs to exactly one milestone;
             closed only when its fix is on main.
  PR         carries a milestone. One that completes an important task links its
             issue with a real closing link (Closes owner/repo#N); one PR of several
             writes "Part of owner/repo#N". A linked issue sits in the PR's milestone.
             A technical PR (sync, pins, docs, CI) links no issue: its commit is the record.
  Feature    every PR of a product repository links a feature of the backlog: "Part of
             <backlog>#N" (or a closing link) to an issue whose milestone says "Release: business".
             A bot's PR (a bot author, or a dependabot/ or update/ branch) and the backlog's own
             PRs are exempt. A technical PR agreed with the maintainer may skip the feature: a
             line "No feature: <why, and who agreed>" of its own in the description (a reason of
             at least ten characters); the log prints it.
  Task       a PR of a feature names its task number, BCKLG-<N> for issue N of the backlog,
             in its body ("Refs: BCKLG-<N>"). A spec directory a PR adds is
             specs/BCKLG-<N>-<name>, and the spec.md, plan.md and tasks.md of one carry the
             line "**Задачи**: BCKLG-<N>[, …]" with its number ("**Tasks**:" in a repository
             whose files are English only). Bots' PRs are exempt.
  Milestone  one release (vX.Y.Z); tag only when every issue is closed, every PR that
             links one is merged, and no PR is still open.
  Feature    an issue in the backlog repository whose milestone says "Release: business"
             on a line of its own. It sits in that business milestone, not in the version
             milestone of the PR that does a part of it, so a link to a feature skips the
             same-milestone rule. The PR still carries its own version milestone.

Usage:
  scripts/check-linkage.sh --pr <N>          # GATE one PR: milestone + any linked issue in the same milestone
  scripts/check-linkage.sh --for-tag vX.Y.Z  # GATE a release: tag-readiness of that version's milestone
                                             # (a patch with no milestone of its own: its minor's, vX.Y.0)
  scripts/check-linkage.sh [milestone]       # audit a milestone (default: lowest-numbered open)
  scripts/check-linkage.sh --tag [ms]        # also assert tag-readiness (issues closed, linked PRs merged, none open)
  scripts/check-linkage.sh --specs [dir]     # every spec of a checkout (default specs/): named by its task,
                                             # its «Задачи» lines naming it, no task in two specs; no GitHub
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

# The task number (backlog#403): BCKLG-<N> for issue N of the backlog, the one number of a task in the
# organization. A spec names the tasks it carries on its «Задачи» line, under its title; a repository whose
# files are English only writes it «**Tasks**:», read alike.
TASK_RE='^BCKLG-([1-9][0-9]*)-[A-Za-z0-9]'
tasks_of() { # text on stdin -> the numbers on its first «Задачи» (or «Tasks») line, one per line; fails without the line
  local line
  line=$(grep -m1 -E '^[[:space:]]*\*\*(Задачи|Tasks)\*\*:') || return 1
  tr -c 'A-Za-z0-9_-' '\n' <<<"$line" | sed -n 's/^BCKLG-\([1-9][0-9]*\)$/\1/p'
}
names_task() { # text on stdin, number -> succeeds when the text names BCKLG-<number> as a whole word
  tr -c 'A-Za-z0-9_-' '\n' | grep -qx "BCKLG-$1"
}
line_problem() { # file path, number, its text on stdin -> what is wrong with its «Задачи» line; nothing when it names the number
  local nums
  if ! nums=$(tasks_of); then printf '%s has no line «**Задачи**: BCKLG-%s» (in English «**Tasks**: BCKLG-%s») under its title' "$1" "$2" "$2"
  elif ! grep -qx "$2" <<<"$nums"; then printf '%s: its «Задачи» line names no BCKLG-%s, the task of its directory' "$1" "$2"; fi
}

# Every spec of a checkout: its directory named by its task, the «Задачи» line of its spec, plan and tasks naming it,
# no task carried by two specs. Files only: no GitHub, so the backlog's CI runs it on every PR.
check_specs() { # dir
  local root="${1%/}" e=0 d name n f problem nums carried="" dup
  [ -d "$root" ] || { echo "error: no directory $root" >&2; return 2; }
  printf 'Specs: %s\n' "$root"
  for d in "$root"/*/; do
    [ -d "$d" ] || continue
    name=$(basename "$d")
    if ! [[ "$name" =~ $TASK_RE ]]; then printf '  ✗ %s/%s is not named BCKLG-<N>-<name>, N the number of its task\n' "$root" "$name"; e=1; continue; fi
    n="${BASH_REMATCH[1]}"
    [ -f "$d/spec.md" ] || { printf '  ✗ %s/%s has no spec.md\n' "$root" "$name"; e=1; }
    for f in spec.md plan.md tasks.md; do
      [ -f "$d/$f" ] || continue
      problem=$(line_problem "$root/$name/$f" "$n" < "$d/$f")
      if [ -n "$problem" ]; then printf '  ✗ %s\n' "$problem"; e=1
      elif [ "$f" = spec.md ]; then nums=$(tasks_of < "$d/$f"); carried+=$(sed "s|\$| $root/$name|" <<<"$nums")$'\n'; fi
    done
  done
  dup=$(awk 'NF { c[$1]++; d[$1] = d[$1] " " $2 } END { for (k in c) if (c[k] > 1) printf "BCKLG-%s is carried by %d specs:%s\n", k, c[k], d[k] }' <<<"$carried" | sort)
  if [ -n "$dup" ]; then sed 's/^/  ✗ /; s/$/ — a task may have one/' <<<"$dup"; e=1; fi
  if [ "$e" = 0 ]; then printf 'PASS: every spec is named by its task.\n'; return 0; fi
  printf 'FAIL: fix the above.\n'; return 1
}
if [ "${1:-}" = --specs ]; then
  [ $# -le 2 ] || { echo "error: --specs takes one directory" >&2; exit 2; }
  check_specs "${2:-specs}"; exit $?
fi

for bin in gh jq; do
  command -v "$bin" >/dev/null 2>&1 || { echo "error: '$bin' not found on PATH" >&2; exit 2; }
done

REPO="${REPO:-$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)}"
[ -n "$REPO" ] || { echo "error: cannot determine repo — run inside a GitHub checkout or set REPO=owner/name" >&2; exit 2; }

BACKLOG_REPO="${BACKLOG_REPO:-galax-io/galaxio-sdlc-backlog}"
FEATURE_LINK="${FEATURE_LINK:-required}"
case "$FEATURE_LINK" in required|optional) ;; *) echo "error: FEATURE_LINK must be required or optional, not '$FEATURE_LINK'" >&2; exit 2 ;; esac

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

# Issues may live in another repository (the SDLC backlog), so every reference is
# owner/repo#N and is looked up in its own repository. A bare #N means this one.
closing_refs() { # pr-json -> registered GitHub closing links
  jq -r '.closingIssuesReferences[]? | "\(.repository.owner.login)/\(.repository.name)#\(.number)"' <<<"$1"
}
body_refs() { # pr-json, regex of what precedes a link -> "[owner/repo]#N" links in the body
  jq -r '.body // ""' <<<"$1" \
    | grep -oiE "$2([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)?#[0-9]+" \
    | grep -oE '([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)?#[0-9]+$' \
    | sed "s|^#|$REPO#|" || true
}
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
issue_ms() { # owner/repo#N -> the issue's milestone title
  gh_for "${1%#*}" issue view "${1##*#}" --repo "${1%#*}" --json milestone -q '.milestone.title // ""' 2>/dev/null || echo ""
}
feature_ms() { # owner/repo#N -> the business milestone's title when the issue is a feature of the backlog
  [ "${1%#*}" = "$BACKLOG_REPO" ] || return 0
  gh_for "${1%#*}" issue view "${1##*#}" --repo "${1%#*}" --json milestone -q '
    select(.milestone != null and ((.milestone.description // "")
      | test("(^|\n)[ \t]*Release:[ \t]*business[ \t]*\r?(\n|$)"; "i"))) | .milestone.title' 2>/dev/null || true
}
no_feature_reason() { # pr-json -> the reason of a "No feature: <reason>" line of its own, when it is one (ten characters or more)
  jq -r '.body // ""' <<<"$1" | tr -d '\r' \
    | sed -n -E 's/^[[:space:]]*[Nn][Oo] [Ff][Ee][Aa][Tt][Uu][Rr][Ee]:[[:space:]]*(.*[^[:space:]])[[:space:]]*$/\1/p' \
    | awk 'length($0) >= 10 { print; exit }'
}
is_bot_pr() { # pr-json -> succeeds when a bot opened it: a bot author, or Dependabot's or Scala Steward's branch
  jq -e '(.author.is_bot == true) or ((.author.login // "") | test("\\[bot\\]$|^app/")) or ((.headRefName // "") | test("^(dependabot|update)/"))' <<<"$1" >/dev/null 2>&1
}
feature_candidates() { # pr-json -> the links it writes into the backlog (closing links and Part of lines)
  { closing_refs "$1"; body_refs "$1" "$PART_OF"; } | awk -v b="$BACKLOG_REPO" 'NF && index($0, b "#") == 1 && !seen[$0]++'
}
feature_refs() { # pr-json -> every issue it links (closing links and Part of lines) that is a feature of the backlog
  local i
  { closing_refs "$1"; body_refs "$1" "$PART_OF"; } | awk 'NF && !seen[$0]++' | while read -r i; do
    [ -z "$(feature_ms "$i")" ] || echo "$i"
  done
}
pr_links() { # pr-json -> its issue links: registered closing links, else "Part of" lines
  local refs
  refs=$(closing_refs "$1")
  [ -n "$refs" ] || refs=$(body_refs "$1" "$PART_OF")
  printf '%s' "$refs"
}
# "Part of owner/repo#N" marks one PR per repository of a multi-repository change. It is
# this project's convention, not a GitHub keyword, so it counts only at the start of a
# line: prose such as "part of #190's follow-up" links nothing.
PART_OF='^[[:space:]]*part of +'
# GitHub's closing keywords start a word and may take a colon: "prefix #5" closes nothing.
CLOSES='(^|[^[:alnum:]_])(close[sd]?|fix(e[sd])?|resolve[sd]?):? +'

# Gate one PR (the merge gate). Fails if the PR is missing a milestone or links an
# issue in a different milestone. A PR that links no issue passes: a technical PR
# needs none. A link is a registered GitHub closing link, or "Part of owner/repo#N"
# for one PR of several. A Closes in the body of a PR that does not target the
# default branch still fails: GitHub links closing keywords only there, so merging
# would not close that issue. On the default branch, a Closes GitHub did not link
# names no issue (a pull request, say) and is not a link (the lenient text
# fallback is audit-mode only).
if [ -n "$PR_NUM" ]; then
  pj=$(gh pr view "$PR_NUM" --repo "$REPO" --json number,title,state,milestone,closingIssuesReferences,body,baseRefName,author,headRefName,headRefOid) \
    || { echo "error: PR #$PR_NUM not found in $REPO" >&2; exit 2; }
  p_title=$(jq -r '.title' <<<"$pj")
  p_ms=$(jq -r '.milestone.title // ""' <<<"$pj")
  p_links=$(pr_links "$pj")
  e=0
  printf 'PR #%s  %s\n' "$PR_NUM" "$p_title"
  if [ -z "$p_ms" ]; then printf '  ✗ no milestone — assign one (gh pr edit %s --milestone "…")\n' "$PR_NUM"; e=1
  else printf '  ✓ milestone: %s\n' "$p_ms"; fi
  if [ -z "$p_links" ]; then
    p_text=$(body_refs "$pj" "$CLOSES")
    p_base=$(jq -r '.baseRefName' <<<"$pj")
    default=$p_base
    if [ -n "$p_text" ]; then
      default=$(gh api "repos/$REPO" --jq '.default_branch') \
        || { echo "error: cannot read the default branch of $REPO" >&2; exit 2; }
    fi
    if [ "$p_base" != "$default" ]; then
      printf '  ✗ the body closes %s, but the PR targets %s, not %s, and GitHub links closing keywords only on the default branch, so merging would not close it — retarget the PR, or write "Part of …"\n' \
        "$(paste -sd, - <<<"$p_text")" "$p_base" "$default"; e=1
    else
      printf '  ✓ links no issue (a technical PR needs none)\n'
    fi
  else
    for i in $p_links; do
      i_feature=$(feature_ms "$i")
      i_ms=$(issue_ms "$i")
      if [ -n "$i_feature" ]; then printf '  ✓ links %s (feature of business milestone "%s")\n' "$i" "$i_feature"
      elif [ "${i%#*}" = "$BACKLOG_REPO" ] && ! backlog_readable; then printf '  ✗ links %s, but %s\n' "$i" "$(cannot_read_backlog)"; e=1
      elif [ -z "$p_ms" ]; then printf '  - links %s (its milestone is "%s")\n' "$i" "${i_ms:-none}"
      elif [ "$i_ms" = "$p_ms" ]; then printf '  ✓ links %s (same milestone)\n' "$i"
      else printf '  ✗ links %s but its milestone is "%s", not "%s"\n' "$i" "${i_ms:-none}" "$p_ms"; e=1; fi
    done
  fi
  # Every PR belongs to a feature of the backlog; the backlog's own PRs and bots' are exempt.
  if [ "$FEATURE_LINK" = required ] && [ "$REPO" != "$BACKLOG_REPO" ]; then
    if is_bot_pr "$pj"; then
      printf '  ✓ a bot'"'"'s PR needs no feature of the backlog\n'
    else
      feats=$(feature_refs "$pj")
      if [ -z "$feats" ] && [ -n "$(feature_candidates "$pj")" ] && ! backlog_readable; then
        :   # said above, link by link
      elif [ -z "$feats" ] && [ -n "$(no_feature_reason "$pj")" ]; then
        printf '  ✓ no feature of the backlog, by agreement: %s\n' "$(no_feature_reason "$pj")"
      elif [ -z "$feats" ]; then
        printf '  ✗ links no feature of the backlog — add "Part of %s#<feature>", or for a technical PR agreed with the maintainer a line "No feature: <why, and who agreed>"\n' "$BACKLOG_REPO"; e=1
      else
        shown=" $(printf '%s ' $p_links)"
        for f in $feats; do
          case "$shown" in *" $f "*) ;; *) printf '  ✓ links %s (feature of business milestone "%s")\n' "$f" "$(feature_ms "$f")" ;; esac
        done
        # The feature's task number, which a search across the organization finds (backlog#403).
        for f in $feats; do
          if jq -r '.body // ""' <<<"$pj" | names_task "${f##*#}"; then printf '  ✓ names its task: BCKLG-%s\n' "${f##*#}"
          else printf '  ✗ names no task number BCKLG-%s — add the line "Refs: BCKLG-%s" to its body\n' "${f##*#}" "${f##*#}"; e=1; fi
        done
      fi
    fi
  fi
  # The spec directories the PR touches (backlog#403): a new one is named by its task, and its spec, plan
  # and tasks name it on the «Задачи» line. A directory from before the task number keeps its name.
  if ! is_bot_pr "$pj"; then
    p_files=$(gh api --paginate "repos/$REPO/pulls/$PR_NUM/files?per_page=100" --jq '.[] | "\(.status)\t\(.filename)"') \
      || { echo "error: cannot list the files of PR #$PR_NUM" >&2; exit 2; }
    p_head=$(jq -r '.headRefOid // ""' <<<"$pj")
    p_base=$(jq -r '.baseRefName' <<<"$pj")
    for d in $(awk -F'\t' '$2 ~ /^specs\/[^\/]+\// { split($2, a, "/"); print a[2] }' <<<"$p_files" | sort -u); do
      if [[ "$d" =~ $TASK_RE ]]; then
        n="${BASH_REMATCH[1]}"
        while IFS=$'\t' read -r st f; do
          case "$f" in "specs/$d/spec.md"|"specs/$d/plan.md"|"specs/$d/tasks.md") ;; *) continue ;; esac
          [ "$st" != removed ] || continue
          text=$(gh api "repos/$REPO/contents/$f?ref=$p_head" -H 'Accept: application/vnd.github.raw') \
            || { printf '  ✗ cannot read %s at the head of the PR\n' "$f"; e=1; continue; }
          problem=$(line_problem "$f" "$n" <<<"$text")
          if [ -n "$problem" ]; then printf '  ✗ %s\n' "$problem"; e=1; fi
        done <<<"$p_files"
      elif ! gh api "repos/$REPO/contents/specs/$d?ref=$p_base" --silent >/dev/null 2>&1; then
        printf '  ✗ adds specs/%s/: a new spec directory is specs/BCKLG-<N>-<name>, N the number of its task\n' "$d"; e=1
      fi
    done
  fi
  if [ "$e" = 0 ]; then printf 'PASS: PR #%s is well-formed.\n' "$PR_NUM"; exit 0; fi
  printf 'FAIL: PR #%s is malformed — fix the above before merge.\n' "$PR_NUM"; exit 1
fi

# Map a release version (vX.Y.Z) to its milestone, then assert tag-readiness on it. A milestone of the
# version itself ("v1.23.1", or "v1.23.1 — title") is the one; a patch with none falls back to its minor's
# (v1.23.0 -> "v1.23.0 …"). A tag without the leading v ("2.2.2") finds milestones with or without it.
if [ -n "$FOR_TAG" ]; then
  v="${FOR_TAG#v}"; minor="${v%.*}.0"
  MS=$(gh api "repos/$REPO/milestones?state=all&per_page=100" \
        | jq -r --arg v "$v" --arg minor "$minor" '
            map(. + {t: (.title | ltrimstr("v"))}) as $all
            | (($all | map(select(.t == $v or (.t | startswith($v + " "))))[0])
               // ($all | map(select(.t | startswith($minor)))[0])).number // empty')
  [ -n "$MS" ] || { echo "error: no milestone '$FOR_TAG' or '${FOR_TAG%%[0-9]*}${minor}…' (title starting with it) for tag $FOR_TAG" >&2; exit 2; }
  TAG_MODE=1
fi

# Default to the lowest-numbered open milestone (the "active" one).
if [ -z "$MS" ]; then
  MS=$(gh api "repos/$REPO/milestones?state=open&per_page=100" --jq 'sort_by(.number) | .[0].number // empty')
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

  # The PR's links, else a text fallback for Closes/Fixes/Resolves [owner/repo]#N
  # in the body.
  ref_nums=$(pr_links "$pr_json")
  if [ -z "$ref_nums" ]; then
    ref_nums=$(body_refs "$pr_json" "$CLOSES")
    [ -n "$ref_nums" ] && warn "PR #$pr links via body text only (not a registered GitHub closing link): $pr_title"
  fi

  # A technical PR (sync, pins, docs, CI — a bot's too) links no issue and needs
  # none. Before a tag it must still be merged or closed.
  if [ -z "$ref_nums" ]; then
    if [ "$TAG_MODE" = 1 ] && [ "$pr_state" = OPEN ]; then
      err "PR #$pr is OPEN — merge or close it before tagging: $pr_title"
    else
      ok "PR #$pr ($pr_state) → no issue (technical)"
    fi
    continue
  fi

  pr_ok=1
  for ri in $ref_nums; do
    linked_issues="$linked_issues$ri "
    [ -z "$(feature_ms "$ri")" ] || continue   # a feature sits in its business milestone
    ri_ms=$(issue_ms "$ri")
    if [ "$ri_ms" != "$ms_title" ]; then
      err "PR #$pr links issue $ri but that issue's milestone is '${ri_ms:-none}', not '$ms_title'"
      pr_ok=0
    fi
  done

  if [ "$TAG_MODE" = 1 ] && [ "$pr_state" != "MERGED" ]; then
    err "PR #$pr is $pr_state — must be MERGED before tagging: $pr_title"
  elif [ "$pr_ok" = 1 ]; then
    ok "PR #$pr ($pr_state) → $(echo "$ref_nums" | paste -sd, -)"
  fi
done

printf '\nIssues\n'
if [ -z "$issue_numbers" ]; then
  warn "no issues carry milestone #$MS"
fi
for is in $issue_numbers; do
  is_state=$(jq -r --argjson n "$is" '.[] | select(.number == $n) | .state' <<<"$items")
  case "$linked_issues" in
    *" $REPO#$is "*) linked="linked" ;;
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
