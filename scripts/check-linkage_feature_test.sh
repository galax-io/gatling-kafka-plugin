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
      "repos/$REPO/milestones/8") echo '{"title":"v0.8.0","state":"open"}' ;;
      "repos/$REPO/issues?milestone=8&state=all&per_page=100") jq -sc '[.]' "$fx/items.ndjson" ;;
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

REPO=galax-io/test
BACKLOG=galax-io/backlog
BUSINESS='Release: business'
new_case() { fx="$tmp/fx-$1"; mkdir -p "$fx"; : > "$fx/items.ndjson"; }
issue_in() { # owner/repo, number, milestone title, [milestone description]
  jq -nc --arg ms "$3" --arg desc "${4:-}" '{milestone: (if $ms == "" then null else {title: $ms, description: $desc} end)}' \
    > "$fx/issue-${1//\//_}-$2.json"
}
pr_json() { # number, state, closing issue number or "", body, milestone title or ""
  jq -nc --argjson n "$1" --arg state "$2" --arg c "$3" --arg body "$4" --arg ms "$5" '{
    number: $n, title: "PR \($n)", state: $state, body: $body,
    milestone: (if $ms == "" then null else {title: $ms} end),
    closingIssuesReferences: [$c | select(. != "") | {number: (. | tonumber)}]
  }' > "$fx/pr-$1.json"
}
add_issue() { printf '{"number":%s,"state":"%s"}\n' "$1" "$2" >> "$fx/items.ndjson"; issue_in "$REPO" "$1" v0.8.0; }
add_pr() { # number, state, closing issue number or "", [body]
  local rest=closed; [ "$2" = OPEN ] && rest=open
  printf '{"number":%s,"state":"%s","pull_request":{}}\n' "$1" "$rest" >> "$fx/items.ndjson"
  pr_json "$1" "$2" "$3" "${4:-}" v0.8.0
}
run() { out="$(PATH="$bin:$PATH" REPO="$REPO" BACKLOG_REPO="$BACKLOG" FIXTURES="$fx" bash "$script" "$@" 2>&1)"; rc=$?; }
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

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
