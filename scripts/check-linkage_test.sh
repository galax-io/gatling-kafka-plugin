#!/usr/bin/env bash
#
# check-linkage_test.sh — tests for scripts/check-linkage.sh.
#
# Hermetic: a stub `gh` sits first on PATH and answers, from fixture files, the
# only calls the script makes (milestone lookup, the milestone's issues and PRs,
# `pr view`, `issue view`), so there is no network and no credentials; REPO is
# set so the script never asks GitHub which repository it is in. What is under
# test is the rule the script enforces: a PR carries a milestone; it links an
# issue only when it does an important task (Closes owner/repo#N, or Part of
# owner/repo#N for one PR of several), and a linked issue sits in the PR's
# milestone, whichever repository it lives in; a technical PR links none and
# passes. Before a tag no PR may still be open and every linked one is merged.

# Every `check` condition is a $(... && echo 0 || echo 1) that yields exactly one
# token, so the unquoted substitution cannot split.
# shellcheck disable=SC2046
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$here/check-linkage.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

command -v jq >/dev/null 2>&1 || { echo "error: 'jq' not found on PATH" >&2; exit 2; }

pass=0; fail=0
ok()  { printf '  ✓ %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf '  ✗ %s\n     %s\n' "$1" "$2"; fail=$((fail + 1)); }
check() { # description, condition-exit-code, detail
  if [ "$2" -eq 0 ]; then ok "$1"; else bad "$1" "$3"; fi
}

# ---- the stub `gh` ---------------------------------------------------------------
# Fixtures live in $FIXTURES: items.ndjson (the milestone's issues and PRs as REST
# returns them), pr-<N>.json and issue-<owner>_<repo>-<N>.json. A call it does not
# know is logged to unexpected.log, so a test sees that the script reached for
# something else.
bin="$tmp/bin"; mkdir -p "$bin"
cat > "$bin/gh" <<'STUB'
#!/usr/bin/env bash
fx="${FIXTURES:?}"
unexpected() { echo "gh $*" >> "$fx/unexpected.log"; echo "stub gh: unexpected call: gh $*" >&2; exit 99; }
all="$*"
cmd="${1:-}"; [ $# -gt 0 ] && shift
expr=""; paginate=0; repo=""; positional=()
while [ $# -gt 0 ]; do
  case "$1" in
    --jq|-q)            shift; expr="${1:-}" ;;
    --paginate|--slurp) paginate=1 ;;
    --repo)             shift; repo="${1:-}" ;;
    --json)             shift ;;
    *)                  positional+=("$1") ;;
  esac
  shift
done
reply() { # file — the fixture, filtered through the --jq/-q expression when given
  if [ -n "$expr" ]; then jq -r "$expr" "$1"; else cat "$1"; fi
}
case "$cmd" in
  api)
    path="${positional[0]:-}"
    case "$path" in
      "repos/$REPO/milestones?state=open&per_page=100")
        echo '[{"number":8,"title":"v0.8.0"}]' | jq -r "${expr:-.}" ;;
      "repos/$REPO/milestones?state=all&per_page=100")
        echo '[{"number":7,"title":"v0.7.0"},{"number":8,"title":"v0.8.0"}]' ;;
      "repos/$REPO")
        echo '{"default_branch":"main"}' | jq -r "${expr:-.}" ;;
      "repos/$REPO/milestones/8")
        echo '{"title":"v0.8.0","state":"open"}' ;;
      "repos/$REPO/issues?milestone=8&state=all&per_page=100")
        [ "$paginate" = 1 ] || unexpected "$all"
        jq -sc '[.]' "$fx/items.ndjson" ;;   # --paginate --slurp: an array of pages
      *) unexpected "$all" ;;
    esac ;;
  pr)
    [ "${positional[0]:-}" = view ] && [ "$repo" = "$REPO" ] && [ -f "$fx/pr-${positional[1]:-}.json" ] || unexpected "$all"
    reply "$fx/pr-${positional[1]}.json" ;;
  issue)
    f="$fx/issue-${repo//\//_}-${positional[1]:-}.json"
    [ "${positional[0]:-}" = view ] && [ -f "$f" ] || unexpected "$all"
    reply "$f" ;;
  *) unexpected "$all" ;;
esac
STUB
chmod +x "$bin/gh"

# ---- fixtures --------------------------------------------------------------------
# One open milestone, v0.8.0 (#8), holding whatever a case adds to it.
REPO=galax-io/test
BACKLOG=galax-io/backlog
new_case() { # name
  fx="$tmp/fx-$1"; mkdir -p "$fx"; : > "$fx/items.ndjson"
}
issue_in() { # owner/repo, number, milestone title, [milestone description] — an issue the script may look up
  jq -nc --arg ms "$3" --arg desc "${4:-}" '{milestone: (if $ms == "" then null else {title: $ms, description: $desc} end)}' \
    > "$fx/issue-${1//\//_}-$2.json"
}
add_issue() { # number, state (open|closed) — an issue of this repository in v0.8.0
  printf '{"number":%s,"state":"%s"}\n' "$1" "$2" >> "$fx/items.ndjson"
  issue_in "$REPO" "$1" v0.8.0
}
pr_json() { # number, state (OPEN|CLOSED|MERGED), closing link owner/repo#N or "", body, milestone title or "", [base branch]
  jq -nc --argjson n "$1" --arg state "$2" --arg link "$3" --arg body "$4" --arg ms "$5" --arg base "${6:-main}" '{
    number: $n, title: "PR \($n)", state: $state, body: $body, baseRefName: $base,
    milestone: (if $ms == "" then null else {title: $ms} end),
    closingIssuesReferences: [$link | select(. != "")
      | capture("^(?<owner>[^/]+)/(?<name>[^#]+)#(?<n>[0-9]+)$")
      | {number: (.n | tonumber), repository: {name, owner: {login: .owner}}}]
  }' > "$fx/pr-$1.json"
}
add_pr() { # number, state, closing link or "", [body] — a PR in v0.8.0
  local rest=closed
  [ "$2" = OPEN ] && rest=open
  printf '{"number":%s,"state":"%s","pull_request":{}}\n' "$1" "$rest" >> "$fx/items.ndjson"
  pr_json "$1" "$2" "$3" "${4:-}" v0.8.0
}
run() { # the script's arguments; sets $out and $rc
  out="$(PATH="$bin:$PATH" REPO="$REPO" BACKLOG_REPO="$BACKLOG" FIXTURES="$fx" bash "$script" "$@" 2>&1)"; rc=$?
}
has() { grep -qF -- "$1" <<<"$out"; }
only_known_calls() { [ ! -s "$fx/unexpected.log" ]; }

echo "check-linkage.sh"

# 1. audit: a PR that closes an issue of the milestone, plus a technical PR that
#    links nothing — both are fine, and nothing is even a warning
new_case audit-technical
add_issue 1 open
add_pr 2 OPEN "$REPO#1"
add_pr 3 OPEN ""
run
check "audit: a technical PR without a link does not fail the audit" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "audit: it is listed as a technical PR" $(has '✓ PR #3 (OPEN) → no issue (technical)' && echo 0 || echo 1) "out=$out"
check "audit: the verdict is PASS without warnings" $(has 'PASS: 0 errors, 0 warning(s)' && echo 0 || echo 1) "out=$out"
check "audit: the linked PR is accepted" $(has "✓ PR #2 (OPEN) → $REPO#1" && echo 0 || echo 1) "out=$out"
check "audit: the script made only the calls the stub knows" $(only_known_calls && echo 0 || echo 1) "$(cat "$fx/unexpected.log" 2>&1)"

# 2. audit: a PR that closes an issue of another milestone is still an error
new_case audit-other-milestone
add_pr 2 OPEN "$REPO#1"
issue_in "$REPO" 1 v0.7.0
run
check "audit: a link to an issue of another milestone fails the audit" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "audit: it names both milestones" $(has "links issue $REPO#1 but that issue's milestone is 'v0.7.0', not 'v0.8.0'" && echo 0 || echo 1) "out=$out"
check "audit: the failing PR gets no ✓ line" $(! has '✓ PR #2' && echo 0 || echo 1) "out=$out"

# 3. tag: everything is merged and closed, but a technical PR is still OPEN
new_case tag-open
add_issue 1 closed
add_pr 2 MERGED "$REPO#1"
add_pr 3 OPEN ""
run --tag
check "tag: an open technical PR blocks the tag" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "tag: it says to merge or close the PR" $(has 'PR #3 is OPEN — merge or close it before tagging' && echo 0 || echo 1) "out=$out"
check "tag: it is the only error" $(has 'FAIL: 1 error(s)' && echo 0 || echo 1) "out=$out"

# 4. tag (through --for-tag, a patch of v0.8.0): technical PRs merged or closed
new_case tag-technical
add_issue 1 closed
add_pr 2 MERGED "$REPO#1"
add_pr 3 MERGED ""
add_pr 4 CLOSED ""
run --for-tag v0.8.1
check "for-tag: merged and closed technical PRs do not block the tag" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "for-tag: the patch maps to its minor's milestone" $(has 'Milestone: #8  v0.8.0' && echo 0 || echo 1) "out=$out"
check "for-tag: the milestone is tag-ready" $(has 'Milestone #8 is tag-ready.' && echo 0 || echo 1) "out=$out"
check "for-tag: the script made only the calls the stub knows" $(only_known_calls && echo 0 || echo 1) "$(cat "$fx/unexpected.log" 2>&1)"

# 5. tag: a PR that links an issue must be merged, not just closed
new_case tag-closed-linked
add_issue 1 closed
add_pr 2 CLOSED "$REPO#1"
run --tag
check "tag: a closed, unmerged PR that links an issue blocks the tag" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "tag: it says the PR must be merged" $(has 'PR #2 is CLOSED — must be MERGED before tagging' && echo 0 || echo 1) "out=$out"

# 6. gate: a technical PR with a milestone and no issue passes
new_case gate-technical
pr_json 3 OPEN "" "Bumps the pins." v0.8.0
run --pr 3
check "gate: a technical PR without an issue passes" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: it says no issue is needed" $(has '✓ links no issue (a technical PR needs none)' && echo 0 || echo 1) "out=$out"
check "gate: the script made only the calls the stub knows" $(only_known_calls && echo 0 || echo 1) "$(cat "$fx/unexpected.log" 2>&1)"

# 7. gate: a PR without a milestone fails, issue or not
new_case gate-no-milestone
pr_json 3 OPEN "" "" ""
run --pr 3
check "gate: a PR without a milestone fails" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: it says to assign one" $(has '✗ no milestone' && echo 0 || echo 1) "out=$out"

# 8. gate: Closes for an issue of the same milestone passes
new_case gate-closes
pr_json 3 OPEN "$REPO#1" "Closes #1" v0.8.0
issue_in "$REPO" 1 v0.8.0
run --pr 3
check "gate: closing an issue of the same milestone passes" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: it names the link" $(has "✓ links $REPO#1 (same milestone)" && echo 0 || echo 1) "out=$out"

# 9. gate: Closes for an issue of another milestone fails
new_case gate-closes-other
pr_json 3 OPEN "$REPO#1" "Closes #1" v0.8.0
issue_in "$REPO" 1 v0.7.0
run --pr 3
check "gate: closing an issue of another milestone fails" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: it names both milestones" $(has "✗ links $REPO#1 but its milestone is \"v0.7.0\", not \"v0.8.0\"" && echo 0 || echo 1) "out=$out"

# 10. gate: Part of an issue in another repository is looked up there
new_case gate-part-of
pr_json 3 OPEN "" "Part of $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 v0.8.0
run --pr 3
check "gate: Part of an issue in another repository passes" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: the issue is looked up in its own repository" $(has "✓ links $BACKLOG#30 (same milestone)" && echo 0 || echo 1) "out=$out"
check "gate: the script made only the calls the stub knows" $(only_known_calls && echo 0 || echo 1) "$(cat "$fx/unexpected.log" 2>&1)"

# 11. gate: off the default branch GitHub links no Closes, so merging would not close
#     the issue — it fails, the colon form too
new_case gate-unregistered
pr_json 3 OPEN "" "Closes: $BACKLOG#30" v0.8.0 feat/stack
run --pr 3
check "gate: a Closes off the default branch fails" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: it names the base branch and the default one" $(has "✗ the body closes $BACKLOG#30, but the PR targets feat/stack, not main" && echo 0 || echo 1) "out=$out"
check "gate: the script made only the calls the stub knows" $(only_known_calls && echo 0 || echo 1) "$(cat "$fx/unexpected.log" 2>&1)"

# 12. gate: a keyword inside another word is not a link
new_case gate-inside-word
pr_json 3 OPEN "" "Drops the prefix #5 and the counterpart of #6." v0.8.0
run --pr 3
check "gate: \"prefix #5\" and \"counterpart of #6\" are not links" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: the PR counts as technical" $(has '✓ links no issue' && echo 0 || echo 1) "out=$out"

# 13. gate: on the default branch, a Closes GitHub did not link names no issue
#     (here a pull request) — the PR is technical
new_case gate-closes-pr
pr_json 3 OPEN "" "Fixes #190's typo." v0.8.0
run --pr 3
check "gate: an unlinked Fixes on the default branch passes" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: the PR counts as technical" $(has '✓ links no issue' && echo 0 || echo 1) "out=$out"

# 14. gate: "part of" in prose is not the Part of convention
new_case gate-prose-part-of
pr_json 3 OPEN "" "Small cleanup that is part of #190's follow-up." v0.8.0
run --pr 3
check "gate: \"part of #190\" in prose is not a link" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: no issue is looked up" $(only_known_calls && echo 0 || echo 1) "$(cat "$fx/unexpected.log" 2>&1)"

# 15. gate: a registered closing link to an issue in another repository
new_case gate-closes-backlog
pr_json 3 OPEN "$BACKLOG#30" "Closes $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 v0.8.0
run --pr 3
check "gate: closing an issue in another repository passes" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: the issue is looked up in its own repository" $(has "✓ links $BACKLOG#30 (same milestone)" && echo 0 || echo 1) "out=$out"

# 16. gate: without a milestone, a linked issue is not "the same milestone"
new_case gate-no-milestone-link
pr_json 3 OPEN "$REPO#1" "Closes #1" ""
issue_in "$REPO" 1 ""
run --pr 3
check "gate: a PR without a milestone still fails" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: the link gets no ✓" $(! has '✓ links' && has "- links $REPO#1" && echo 0 || echo 1) "out=$out"

# 17. audit: a Closes in the body that GitHub did not register is a warning, and
#     the issue still counts as linked; "part of" in prose links nothing
new_case audit-text
add_issue 1 open
add_pr 2 OPEN "" "Closes #1"
add_pr 3 OPEN "" "Small cleanup that is part of #190's follow-up."
run
check "audit: an unregistered Closes does not fail the audit" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "audit: it is reported as a warning" $(has '! PR #2 links via body text only' && echo 0 || echo 1) "out=$out"
check "audit: the issue counts as linked" $(has '✓ issue #1 (open) ← linked' && echo 0 || echo 1) "out=$out"
check "audit: prose \"part of\" leaves the PR technical" $(has '✓ PR #3 (OPEN) → no issue (technical)' && echo 0 || echo 1) "out=$out"

BUSINESS='Release: business'

# 18. gate: a PR with a version milestone may link a feature of the backlog; the feature
#     sits in a business milestone of its own, so the milestones need not match
new_case gate-feature
pr_json 3 OPEN "" "Part of $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run --pr 3
check "gate: Part of a backlog feature in a business milestone passes" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: it names the business milestone" $(has "✓ links $BACKLOG#30 (feature of business milestone \"M1 — first run\")" && echo 0 || echo 1) "out=$out"
check "gate: the script made only the calls the stub knows" $(only_known_calls && echo 0 || echo 1) "$(cat "$fx/unexpected.log" 2>&1)"

# 19. gate: the marker may sit among other lines of the milestone description
new_case gate-feature-multiline
pr_json 3 OPEN "" "Part of $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 'M1 — first run' $'First real run of the process.\nRelease: business\nDue: autumn'
run --pr 3
check "gate: the marker on its own line of a longer description counts" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 20. gate: a backlog issue in an ordinary milestone is not a feature
new_case gate-backlog-plain
pr_json 3 OPEN "" "Part of $BACKLOG#31" v0.8.0
issue_in "$BACKLOG" 31 'v0.7.0' 'Tooling of the backlog itself.'
run --pr 3
check "gate: a backlog issue in an ordinary milestone still has to match" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: it names both milestones" $(has "✗ links $BACKLOG#31 but its milestone is \"v0.7.0\", not \"v0.8.0\"" && echo 0 || echo 1) "out=$out"

# 21. gate: the marker only counts in the backlog repository
new_case gate-marker-elsewhere
pr_json 3 OPEN "$REPO#1" "Closes #1" v0.8.0
issue_in "$REPO" 1 'M1 — first run' "$BUSINESS"
run --pr 3
check "gate: the marker in another repository does not make a feature" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 22. gate: a backlog issue without a milestone is not a feature
new_case gate-feature-no-milestone
pr_json 3 OPEN "" "Part of $BACKLOG#32" v0.8.0
issue_in "$BACKLOG" 32 ""
run --pr 3
check "gate: a backlog issue without a milestone fails" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 23. gate: the PR itself still needs its version milestone
new_case gate-feature-no-pr-milestone
pr_json 3 OPEN "" "Part of $BACKLOG#30" ""
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run --pr 3
check "gate: a feature link does not replace the PR's own milestone" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: it says to assign one" $(has '✗ no milestone' && echo 0 || echo 1) "out=$out"
check "gate: the feature link is still reported" $(has "✓ links $BACKLOG#30 (feature of business milestone" && echo 0 || echo 1) "out=$out"

# 24. audit: a PR of the milestone that links a backlog feature is not an error
new_case audit-feature
add_issue 1 open
add_pr 2 OPEN "" "Part of $BACKLOG#30"
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run
check "audit: linking a backlog feature passes the audit" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "audit: the PR is listed with its link" $(has "✓ PR #2 (OPEN) → $BACKLOG#30" && echo 0 || echo 1) "out=$out"

# 25. tag: a merged PR of a feature does not block the tag of its repository
new_case tag-feature
add_issue 1 closed
add_pr 2 MERGED "$REPO#1"
add_pr 3 MERGED "" "Part of $BACKLOG#30"
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run --tag
check "tag: a merged PR that links a feature does not block the tag" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "tag: the milestone is tag-ready" $(has 'Milestone #8 is tag-ready.' && echo 0 || echo 1) "out=$out"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
