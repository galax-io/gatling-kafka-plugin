#!/usr/bin/env bash
#
# check-linkage_feature_test.sh — tests for the feature rule of scripts/check-linkage.sh.
#
# A feature is an issue in the backlog repository (BACKLOG_REPO, default
# galax-io/galaxio-sdlc-backlog) whose milestone description has the line
# "Release: business". A PR links it with "Part of <backlog>#N" in its body and
# carries its own version milestone; the same-milestone rule is skipped for that link.
#
# Written against THIS repository's script (it reads Closes #N from GitHub's closing
# links and, in the audit, from the body). The sibling check-linkage_test.sh was copied
# from the bootstrap template and describes a different script; it is not run here.
#
# Hermetic: a stub `gh` first on PATH answers from fixture files; no network.
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
check() { if [ "$2" -eq 0 ]; then ok "$1"; else bad "$1" "$3"; fi; }

bin="$tmp/bin"; mkdir -p "$bin"
cat > "$bin/gh" <<'STUB'
#!/usr/bin/env bash
fx="${FIXTURES:?}"
unexpected() { echo "gh $*" >> "$fx/unexpected.log"; echo "stub gh: unexpected call: gh $*" >&2; exit 99; }
all="$*"
echo "GH_TOKEN=${GH_TOKEN:-} gh $all" >> "$fx/ghcalls.log"
# a private backlog: only its own token reads it (fixture file backlog-hidden)
backlog_denied() { [ -e "$fx/backlog-hidden" ] && [ "${GH_TOKEN:-}" != "backlog-secret" ]; }
not_found() { echo "gh: Not Found (HTTP 404)" >&2; exit 1; }
cmd="${1:-}"; [ $# -gt 0 ] && shift
expr=""; repo=""; positional=()
while [ $# -gt 0 ]; do
  case "$1" in
    --jq|-q)            shift; expr="${1:-}" ;;
    --paginate|--slurp) ;;
    --repo)             shift; repo="${1:-}" ;;
    --json)             shift ;;
    *)                  positional+=("$1") ;;
  esac
  shift
done
reply() { if [ -n "$expr" ]; then jq -r "$expr" "$1"; else cat "$1"; fi; }
case "$cmd" in
  api)
    case "${positional[0]:-}" in
      "repos/$REPO/milestones?state=open&per_page=100") echo '[{"number":8,"title":"v0.8.0"}]' | jq -r "${expr:-.}" ;;
      "repos/$BACKLOG_REPO") backlog_denied && not_found; echo '{"private":true}' ;;
      "repos/$REPO/milestones/8") echo '{"title":"v0.8.0","state":"open"}' ;;
      "repos/$REPO/issues?milestone=8&state=all&per_page=100") jq -sc '[.]' "$fx/items.ndjson" ;;
      *) unexpected "$all" ;;
    esac ;;
  pr)
    [ "${positional[0]:-}" = view ] && [ "$repo" = "$REPO" ] && [ -f "$fx/pr-${positional[1]:-}.json" ] || unexpected "$all"
    reply "$fx/pr-${positional[1]}.json" ;;
  issue)
    [ "$repo" = "$BACKLOG_REPO" ] && backlog_denied && not_found
    f="$fx/issue-${repo//\//_}-${positional[1]:-}.json"
    [ "${positional[0]:-}" = view ] && [ -f "$f" ] || unexpected "$all"
    reply "$f" ;;
  *) unexpected "$all" ;;
esac
STUB
chmod +x "$bin/gh"

REPO=galax-io/test
BACKLOG=galax-io/backlog
BUSINESS='Release: business'
new_case() { fx="$tmp/fx-$1"; mkdir -p "$fx"; : > "$fx/items.ndjson"; }
issue_in() { # owner/repo, number, milestone title, [milestone description]
  jq -nc --arg ms "$3" --arg desc "${4:-}" '{milestone: (if $ms == "" then null else {title: $ms, description: $desc} end)}' \
    > "$fx/issue-${1//\//_}-$2.json"
}
pr_json() { # number, state, closing issue number or "", body, milestone title or ""; PR_AUTHOR, PR_BOT=1, PR_HEAD, PR_CLOSE_REPO tune it
  jq -nc --argjson n "$1" --arg state "$2" --arg c "$3" --arg body "$4" --arg ms "$5" \
    --arg author "${PR_AUTHOR:-jigarkhwar}" --arg bot "${PR_BOT:-0}" --arg head "${PR_HEAD:-feat/x}" --arg crepo "${PR_CLOSE_REPO:-$REPO}" '{
    number: $n, title: "PR \($n)", state: $state, body: $body,
    author: {login: $author, is_bot: ($bot == "1")}, headRefName: $head,
    milestone: (if $ms == "" then null else {title: $ms} end),
    closingIssuesReferences: [$c | select(. != "") | {number: (. | tonumber),
      repository: {name: ($crepo | split("/")[1]), owner: {login: ($crepo | split("/")[0])}}}]
  }' > "$fx/pr-$1.json"
}
add_issue() { printf '{"number":%s,"state":"%s"}\n' "$1" "$2" >> "$fx/items.ndjson"; issue_in "$REPO" "$1" v0.8.0; }
add_pr() { # number, state, closing issue number or "", [body]
  local rest=closed; [ "$2" = OPEN ] && rest=open
  printf '{"number":%s,"state":"%s","pull_request":{}}\n' "$1" "$rest" >> "$fx/items.ndjson"
  pr_json "$1" "$2" "$3" "${4:-}" v0.8.0
}
# The cases above the "strict" block test the other rules, so the requirement of a feature
# link is off for them (run); the strict cases run with it in force, the default (run_strict).
run() { out="$(PATH="$bin:$PATH" REPO="$REPO" BACKLOG_REPO="$BACKLOG" FEATURE_LINK=optional FIXTURES="$fx" bash "$script" "$@" 2>&1)"; rc=$?; }
run_strict() { out="$(PATH="$bin:$PATH" REPO="$REPO" BACKLOG_REPO="$BACKLOG" FIXTURES="$fx" bash "$script" "$@" 2>&1)"; rc=$?; }
has() { grep -qF -- "$1" <<<"$out"; }
only_known_calls() { [ ! -s "$fx/unexpected.log" ]; }

echo "check-linkage.sh — feature rule"

# 1. gate: Part of a backlog feature passes although the milestones differ
new_case gate-feature
pr_json 3 OPEN "" "Part of $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run --pr 3
check "gate: Part of a backlog feature in a business milestone passes" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: it names the business milestone" $(has "✓ links $BACKLOG#30 (feature of business milestone \"M1 — first run\")" && echo 0 || echo 1) "out=$out"
check "gate: only known gh calls" $(only_known_calls && echo 0 || echo 1) "$(cat "$fx/unexpected.log" 2>&1)"

# 2. gate: the marker among other lines, any case
new_case gate-feature-multiline
pr_json 3 OPEN "" "Part of $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 'M1 — first run' $'First real run.\nrelease:  Business\nDue: autumn'
run --pr 3
check "gate: the marker on its own line of a longer description counts, case-insensitive" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 3. gate: a marker that is not on a line of its own is no feature
new_case gate-marker-inline
pr_json 3 OPEN "" "Part of $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 'v0.7.0' 'Not a Release: business milestone.'
run --pr 3
check "gate: a marker inside a sentence does not make a feature" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 4. gate: a backlog issue in an ordinary milestone has to match
new_case gate-backlog-plain
pr_json 3 OPEN "" "Part of $BACKLOG#31" v0.8.0
issue_in "$BACKLOG" 31 'v0.7.0' 'Tooling of the backlog itself.'
run --pr 3
check "gate: a backlog issue in an ordinary milestone still has to match" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: it names both milestones" $(has "✗ links $BACKLOG#31 but its milestone is \"v0.7.0\", not \"v0.8.0\"" && echo 0 || echo 1) "out=$out"

# 4b. gate: a backlog issue in the PR's own milestone name still passes
new_case gate-backlog-same
pr_json 3 OPEN "" "Part of $BACKLOG#31" v0.8.0
issue_in "$BACKLOG" 31 'v0.8.0' 'Tooling of the backlog itself.'
run --pr 3
check "gate: a backlog issue in the same milestone passes" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 5. gate: the marker only counts in the backlog repository
new_case gate-marker-elsewhere
pr_json 3 OPEN 1 "Closes #1" v0.8.0
issue_in "$REPO" 1 'M1 — first run' "$BUSINESS"
run --pr 3
check "gate: the marker in this repository does not make a feature" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 6. gate: a backlog issue without a milestone is not a feature
new_case gate-feature-no-milestone
pr_json 3 OPEN "" "Part of $BACKLOG#32" v0.8.0
issue_in "$BACKLOG" 32 ""
run --pr 3
check "gate: a backlog issue without a milestone fails" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 7. gate: the PR itself still needs its version milestone
new_case gate-feature-no-pr-milestone
pr_json 3 OPEN "" "Part of $BACKLOG#30" ""
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run --pr 3
check "gate: a feature link does not replace the PR's own milestone" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: it says to assign one" $(has '✗ no milestone' && echo 0 || echo 1) "out=$out"
check "gate: the feature link is still reported" $(has "✓ links $BACKLOG#30 (feature of business milestone" && echo 0 || echo 1) "out=$out"

# 8. gate: a local Closes #N of the same milestone is unchanged
new_case gate-closes-same
pr_json 3 OPEN 1 "Closes #1" v0.8.0
issue_in "$REPO" 1 v0.8.0
run --pr 3
check "gate: closing an issue of the same milestone still passes" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "gate: and one of another milestone still fails" $(issue_in "$REPO" 1 v0.7.0; run --pr 3; [ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 9. gate: a PR that links nothing is unchanged
new_case gate-technical
pr_json 3 OPEN "" "Bumps the pins." v0.8.0
run --pr 3
check "gate: a PR with no link still passes with a note" $([ "$rc" -eq 0 ] && has 'closes no issue' && echo 0 || echo 1) "rc=$rc out=$out"

# 10. audit: a PR that links a backlog feature is not an error, nor a "closes no issue" warning
new_case audit-feature
add_issue 1 open
add_pr 2 OPEN "" "Part of $BACKLOG#30"
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run
check "audit: linking a backlog feature passes the audit" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "audit: the PR is listed with its link" $(has "✓ PR #2 (OPEN) → $BACKLOG#30" && echo 0 || echo 1) "out=$out"
check "audit: it is not reported as closing no issue" $(! has 'PR #2 (OPEN) closes no issue' && echo 0 || echo 1) "out=$out"

# 11. audit: a link to an ordinary backlog milestone still has to match
new_case audit-backlog-plain
add_pr 2 OPEN "" "Part of $BACKLOG#31"
issue_in "$BACKLOG" 31 'v0.7.0' 'Tooling.'
run
check "audit: a link to an ordinary backlog milestone fails" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 12. audit: local Closes of another milestone is unchanged
new_case audit-closes-other
add_pr 2 OPEN 1 "Closes #1"
issue_in "$REPO" 1 v0.7.0
run
check "audit: a Closes of another milestone still fails" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 13. tag: a merged PR of a feature does not block, an open one does
new_case tag-feature
add_issue 1 closed
add_pr 2 MERGED 1
add_pr 3 MERGED "" "Part of $BACKLOG#30"
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run --tag
check "tag: a merged PR that links a feature does not block the tag" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "tag: the milestone is tag-ready" $(has 'Milestone #8 is tag-ready.' && echo 0 || echo 1) "out=$out"
new_case tag-feature-open
add_pr 3 OPEN "" "Part of $BACKLOG#30"
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run --tag
check "tag: an open PR that links a feature still blocks the tag" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 14. strict gate: every PR links a feature of the backlog; a PR with no link fails
new_case strict-none
pr_json 3 OPEN "" "Bumps the pins." v0.8.0
run_strict --pr 3
check "strict: a PR with no link to a feature fails" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "strict: it says which link to add" $(has "✗ links no feature of the backlog" && has "Part of $BACKLOG#" && echo 0 || echo 1) "out=$out"
check "strict: maintenance is pointed at the standing feature by its label" $(has 'label standing' && ! has 'Maintenance and tech debt' && echo 0 || echo 1) "out=$out"

# 15. strict gate: Part of a feature passes
new_case strict-feature
pr_json 3 OPEN "" "Part of $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run_strict --pr 3
check "strict: Part of a feature passes" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 16. strict gate: a closing link to an issue of this repository does not hide the feature
new_case strict-closes-and-part
pr_json 3 OPEN 1 $'Closes #1\nPart of '"$BACKLOG"'#30' v0.8.0
issue_in "$REPO" 1 v0.8.0
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run_strict --pr 3
check "strict: a closing link does not hide the Part of a feature" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "strict: the closed issue is still checked" $(has "✓ closes #1 (same milestone)" && echo 0 || echo 1) "out=$out"
new_case strict-closes-only
pr_json 3 OPEN 1 "Closes #1" v0.8.0
issue_in "$REPO" 1 v0.8.0
run_strict --pr 3
check "strict: a closing link to an issue of this repository alone is no feature" $([ "$rc" -eq 1 ] && has '✗ links no feature of the backlog' && echo 0 || echo 1) "rc=$rc out=$out"
new_case strict-closes-feature
PR_CLOSE_REPO="$BACKLOG" pr_json 3 OPEN 30 "Closes $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
issue_in "$REPO" 30 v0.8.0
run_strict --pr 3
check "strict: a closing link to a feature of the backlog counts" $([ "$rc" -eq 0 ] && has "feature of business milestone" && echo 0 || echo 1) "rc=$rc out=$out"

# 17. strict gate: a backlog issue that is not a feature does not satisfy it
new_case strict-not-feature
pr_json 3 OPEN "" "Part of $BACKLOG#31" v0.8.0
issue_in "$BACKLOG" 31 'v0.8.0' 'Tooling of the backlog itself.'
run_strict --pr 3
check "strict: Part of a backlog issue in an ordinary milestone is no feature" $([ "$rc" -eq 1 ] && has "✗ links no feature of the backlog" && echo 0 || echo 1) "rc=$rc out=$out"

# 18. strict gate: a bot's PR is exempt
new_case strict-bot
PR_AUTHOR='dependabot[bot]' PR_BOT=1 pr_json 3 OPEN "" "Bumps a library." v0.8.0
run_strict --pr 3
check "strict: a bot's PR needs no feature" $([ "$rc" -eq 0 ] && has 'bot' && has 'needs no feature' && echo 0 || echo 1) "rc=$rc out=$out"
new_case strict-steward
PR_HEAD='update/scalafmt-core-3.9.0' pr_json 3 OPEN "" "Update scalafmt-core to 3.9.0" v0.8.0
run_strict --pr 3
check "strict: a Scala Steward branch is exempt" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
new_case strict-dependabot-branch
PR_HEAD='dependabot/github_actions/x' pr_json 3 OPEN "" "Bump x" v0.8.0
run_strict --pr 3
check "strict: a dependabot branch is exempt" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
new_case strict-human-update
PR_HEAD='feat/not-a-bot' pr_json 3 OPEN "" "Update the docs" v0.8.0
run_strict --pr 3
check "strict: a human's branch is not exempt" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 19. strict gate: the backlog's own PRs link nothing above them
new_case strict-backlog-itself
pr_json 3 OPEN "" "Adds a script." v0.8.0
out="$(PATH="$bin:$PATH" REPO="$BACKLOG" BACKLOG_REPO="$BACKLOG" FIXTURES="$fx" bash "$script" --pr 3 2>&1)"; rc=$?
check "strict: a PR in the backlog repository itself needs no feature" $(! has 'links no feature' && echo 0 || echo 1) "rc=$rc out=$out"

# 20. strict audit: a PR of the milestone without a feature link is a warning, not an error
new_case strict-audit
add_issue 1 open
add_pr 2 OPEN 1 "Closes #1"
add_pr 3 OPEN "" "Bumps the pins."
add_pr 4 OPEN "" "Part of $BACKLOG#30"
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run_strict
check "strict audit: a PR without a feature is a warning" $([ "$rc" -eq 0 ] && has '! PR #3 links no feature of the backlog' && echo 0 || echo 1) "rc=$rc out=$out"
check "strict audit: a PR with a feature is not warned about" $(! has '! PR #4 links no feature' && echo 0 || echo 1) "out=$out"

# 21. the requirement can be switched off by name only
new_case strict-optional
pr_json 3 OPEN "" "Bumps the pins." v0.8.0
run --pr 3
check "optional: FEATURE_LINK=optional lets a PR without a feature pass" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"
out="$(PATH="$bin:$PATH" REPO="$REPO" BACKLOG_REPO="$BACKLOG" FEATURE_LINK=nonsense FIXTURES="$fx" bash "$script" --pr 3 2>&1)"; rc=$?
check "optional: an unknown FEATURE_LINK value is refused" $([ "$rc" -eq 2 ] && echo 0 || echo 1) "rc=$rc out=$out"

# 22. a private backlog the job's token cannot read is said so, not mistaken for "no feature"
new_case strict-hidden
pr_json 3 OPEN "" "Part of $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
: > "$fx/backlog-hidden"
run_strict --pr 3
check "hidden: a backlog the token cannot read fails the gate" $([ "$rc" -eq 1 ] && echo 0 || echo 1) "rc=$rc out=$out"
check "hidden: it names the missing token" $(has "cannot read the backlog $BACKLOG" && has 'BACKLOG_READ_TOKEN' && echo 0 || echo 1) "out=$out"
check "hidden: it does not claim the PR links no feature" $(! has 'links no feature of the backlog' && echo 0 || echo 1) "out=$out"
check "hidden: it does not call the milestone \"none\"" $(! has 'milestone is "none"' && echo 0 || echo 1) "out=$out"

# 23. with BACKLOG_READ_TOKEN the backlog is read with that token, and only the backlog
new_case strict-token
pr_json 3 OPEN "" "Part of $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
: > "$fx/backlog-hidden"
out="$(PATH="$bin:$PATH" REPO="$REPO" BACKLOG_REPO="$BACKLOG" GH_TOKEN=job-token BACKLOG_READ_TOKEN=backlog-secret FIXTURES="$fx" bash "$script" --pr 3 2>&1)"; rc=$?
check "token: with BACKLOG_READ_TOKEN the feature is seen and the gate passes" $([ "$rc" -eq 0 ] && has "✓ links $BACKLOG#30 (feature of business milestone" && echo 0 || echo 1) "rc=$rc out=$out"
check "token: the backlog token is used for calls about the backlog" $(grep -q "GH_TOKEN=backlog-secret gh .*$BACKLOG" "$fx/ghcalls.log" && echo 0 || echo 1) "$(cat "$fx/ghcalls.log")"
check "token: the backlog token never reaches a call about this repository" $(! grep "GH_TOKEN=backlog-secret" "$fx/ghcalls.log" | grep -v "$BACKLOG" | grep -q . && echo 0 || echo 1) "$(grep backlog-secret "$fx/ghcalls.log")"
check "token: the job's own token still reads the pull request" $(grep -q "GH_TOKEN=job-token gh pr view" "$fx/ghcalls.log" && echo 0 || echo 1) "$(cat "$fx/ghcalls.log")"

# 24. an unset BACKLOG_READ_TOKEN changes nothing for a readable backlog
new_case strict-readable
pr_json 3 OPEN "" "Part of $BACKLOG#30" v0.8.0
issue_in "$BACKLOG" 30 'M1 — first run' "$BUSINESS"
run_strict --pr 3
check "readable: without BACKLOG_READ_TOKEN a readable backlog is read with the job's token" $([ "$rc" -eq 0 ] && echo 0 || echo 1) "rc=$rc out=$out"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
