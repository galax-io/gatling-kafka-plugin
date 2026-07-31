#!/usr/bin/env bash
#
# test-linkage-guard.sh — regression suite for .claude/hooks/linkage-guard.sh.
#
# The guard must gate release tagging and nothing else. Two symmetric failures matter:
# blocking a command that merely *mentions* tagging, and waving through a real tag hidden
# in a compound command. Both are covered below.
#
# The real scripts/check-linkage.sh is replaced by a stub, so the suite is deterministic,
# offline, and independent of milestone state. It asserts only the guard's scope decision:
# exit 0 = passed through, exit 2 = gated.
#
# Usage: bash scripts/test-linkage-guard.sh   [GUARD=/path/to/linkage-guard.sh]
# Exit:  0 all green | 1 at least one case failed
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GUARD="${GUARD:-$root/.claude/hooks/linkage-guard.sh}"
[ -r "$GUARD" ] || { echo "guard not found: $GUARD" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

fake=$(mktemp -d)
trap 'rm -rf "$fake"' EXIT
mkdir -p "$fake/scripts"
cat >"$fake/scripts/check-linkage.sh" <<'STUB'
#!/usr/bin/env bash
echo "stub check-linkage: $*"
exit 1
STUB
chmod +x "$fake/scripts/check-linkage.sh"

pass=0
fail=0

run() { # run <expected rc> <label> <command>
  local want="$1" label="$2" cmd="$3" out rc
  out=$(jq -n --arg c "$cmd" '{tool_input:{command:$c}}' \
        | CLAUDE_PROJECT_DIR="$fake" bash "$GUARD" 2>&1)
  rc=$?
  if [ "$rc" = "$want" ]; then
    pass=$((pass + 1))
    printf '  ok   [%s] %s\n' "$rc" "$label"
  else
    fail=$((fail + 1))
    printf '  FAIL want=%s got=%s  %s\n       cmd: %s\n       out: %s\n' \
      "$want" "$rc" "$label" "$cmd" "$out"
  fi
}

echo "== must pass through (0) — not a release =="
run 0 'gh pr create, heredoc body mentions tagging' \
  "$(printf 'gh pr create --body-file - <<%sEOF%s\nRatified at v1.0.0.\nlinkage-guard blocks git tag vX.Y.Z until the milestone passes.\nEOF' "'" "'")"
run 0 'gh pr create, quoted body mentions tagging' \
  'rtk proxy gh pr create --title "x" --body "the guard blocks git tag v1.2.3"'
run 0 'gh issue create quoting a tag command' \
  'gh issue create --body "repro: git tag v2.3.4 then git push origin v2.3.4"'
run 0 'echo of a tag command'        'echo "git tag v9.9.9"'
run 0 'git status'                   'git status --short'
run 0 'git log over a tag range'     'git log --oneline v1.0.6..origin/main'
run 0 'git show a tag'               'git show v1.0.6'
run 0 'git push a feature branch'    'git push -u origin feat/some-work'
run 0 'git commit mentioning a tag'  'git commit -m "docs: describe git tag v1.0.7 flow"'
run 0 'git tag --list glob'          "git tag -l 'v1.*'"
run 0 'git tag --list long flag'     'git tag --list v1.0.0'
run 0 'git tag --delete'             'git tag -d v1.0.0'
run 0 'git tag --verify'             'git tag -v v1.0.0'
run 0 'git branch'                   'git branch -a'
run 0 'LINKAGE_OFF prefix bypass'    'LINKAGE_OFF=1 git tag v1.0.7'
run 0 'no git anywhere'              'ls -la'

echo "== must gate (2) — a real release =="
run 2 'lightweight tag'              'git tag v1.0.7'
run 2 'annotated tag'                'git tag -a v1.0.7 -m "release 1.0.7"'
run 2 'tag behind the rtk wrapper'   'rtk proxy git tag v1.0.7'
run 2 'push a version tag'           'git push origin v1.0.7'
run 2 'push --tags'                  'git push --tags'
run 2 'push refs/tags'               'git push origin refs/tags/v1.0.7'
run 2 'push a release branch'        'git push origin release/1.0'
run 2 'commit && tag'                'git commit -m "prep" && git tag v1.0.7'
run 2 'cd && tag'                    'cd /tmp && git tag v1.0.7'
run 2 'log && tag'                   'git log --oneline -1 && git tag v1.0.7'

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" = 0 ]
