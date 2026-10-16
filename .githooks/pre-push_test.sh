#!/usr/bin/env bash
#
# pre-push_test.sh — regression tests for the release-tag gate in pre-push
# (constitution Principle III: a bug fix ships with a test that fails without
# the fix).
#
# check-linkage.sh is replaced by a stub, so the tests are hermetic: no network,
# no milestones, same result on a laptop and on a runner. What is under test is
# the hook's own decisions — which refs it gates, which it lets by, and whether
# it fails closed.
#
# Note what is absent. The gate this replaced had to be tested against `xargs`,
# `sudo`, `bash -c`, subshells, line continuations and heredocs, because it
# guessed at command text. git reports the refs directly, so that entire class
# of test has nothing to assert: the same four fields arrive however the push
# was invoked.

set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
hook="$here/pre-push"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

pass=0; fail=0
ok()  { printf '  ✓ %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf '  ✗ %s\n     %s\n' "$1" "$2"; fail=$((fail + 1)); }

zero=0000000000000000000000000000000000000000
sha=a1b2c3d4e5f60718293a4b5c6d7e8f9012345678

# ---- a repository whose checker is a stub ----------------------------------
repo="$tmp/repo"
mkdir -p "$repo/scripts"
git -C "$repo" init -q 2>/dev/null
: > "$repo/f"; git -C "$repo" add f
git -C "$repo" -c user.email=t@t -c user.name=t commit -qm init

# The stub records what it was asked and answers from CHECKER_VERDICT.
cat > "$repo/scripts/check-linkage.sh" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$CHECKER_LOG"
if [ "${CHECKER_VERDICT:-ready}" = "ready" ]; then
  echo "PASS: milestone is tag-ready"
  exit 0
fi
echo "FAIL: issue #7 is open — must be closed before tagging"
exit 1
STUB
chmod +x "$repo/scripts/check-linkage.sh"

# run <verdict> <stdin lines> -> prints "rc|calls"
run() {
  local verdict="$1" input="$2"
  : > "$tmp/log"
  ( cd "$repo" && CHECKER_LOG="$tmp/log" CHECKER_VERDICT="$verdict" \
      bash "$hook" origin https://example.invalid/r.git <<< "$input" >/dev/null 2>&1 )
  printf '%s|%s' "$?" "$(wc -l < "$tmp/log" | tr -d ' ')"
}

check() { # check <name> <want rc|calls> <verdict> <stdin>
  local got; got=$(run "$3" "$4")
  if [ "$got" = "$2" ]; then ok "$1"; else bad "$1" "want rc|calls = $2, got $got"; fi
}

printf 'pre-push\n'

check "a ready tag is published" \
  "0|1" ready "refs/tags/v1.2.3 $sha refs/tags/v1.2.3 $zero"

check "an unready tag is refused" \
  "1|1" unready "refs/tags/v1.2.3 $sha refs/tags/v1.2.3 $zero"

check "a branch push is not a release, and asks nothing" \
  "0|0" unready "refs/heads/main $sha refs/heads/main $sha"

check "pushing the release branch is not a release" \
  "0|0" unready "refs/heads/release/0.0.0 $sha refs/heads/release/0.0.0 $zero"

check "deleting a tag publishes nothing" \
  "0|0" unready "(delete) $zero refs/tags/v1.2.3 $sha"

check "a tag that is not a release version is left alone" \
  "0|0" unready "refs/tags/nightly $sha refs/tags/nightly $zero"

# --tags sends one line per tag; each is judged, and one bad one refuses the push.
check "--tags checks every tag it carries" \
  "0|3" ready "refs/tags/v1.0.0 $sha refs/tags/v1.0.0 $zero
refs/tags/v1.1.0 $sha refs/tags/v1.1.0 $zero
refs/tags/v1.2.3 $sha refs/tags/v1.2.3 $zero"

check "one unready tag refuses the whole push" \
  "1|2" unready "refs/tags/v1.0.0 $sha refs/tags/v1.0.0 $zero
refs/tags/v1.2.3 $sha refs/tags/v1.2.3 $zero"

check "a tag riding along with a branch is still checked" \
  "1|1" unready "refs/heads/main $sha refs/heads/main $sha
refs/tags/v1.2.3 $sha refs/tags/v1.2.3 $zero"

check "nothing to push is not a failure" \
  "0|0" unready ""

# Fails closed: a missing checker must refuse, not wave the tag through.
mv "$repo/scripts/check-linkage.sh" "$repo/scripts/check-linkage.sh.away"
check "a missing checker refuses the tag" \
  "1|0" ready "refs/tags/v1.2.3 $sha refs/tags/v1.2.3 $zero"
mv "$repo/scripts/check-linkage.sh.away" "$repo/scripts/check-linkage.sh"

# The version handed to the checker is the tag's own, never guessed from a path
# or an earlier ref — the defect that let the old gate vet the wrong release.
: > "$tmp/log"
( cd "$repo" && CHECKER_LOG="$tmp/log" CHECKER_VERDICT=ready \
    bash "$hook" origin https://example.invalid/v9.9.9/r.git \
    <<< "refs/tags/v1.2.3 $sha refs/tags/v1.2.3 $zero" >/dev/null 2>&1 )
if grep -qx -- "--for-tag v1.2.3" "$tmp/log"; then
  ok "the checker is asked about the tag being pushed"
else
  bad "the checker is asked about the tag being pushed" "asked: $(cat "$tmp/log")"
fi

# LINKAGE_OFF=1 bypasses the gate entirely, before the checker is ever asked —
# the same escape hatch linkage-guard.sh already gives an in-session release.
: > "$tmp/log"
( cd "$repo" && CHECKER_LOG="$tmp/log" CHECKER_VERDICT=unready LINKAGE_OFF=1 \
    bash "$hook" origin https://example.invalid/r.git \
    <<< "refs/tags/v1.2.3 $sha refs/tags/v1.2.3 $zero" >/dev/null 2>&1 )
rc=$?
calls=$(wc -l < "$tmp/log" | tr -d ' ')
if [ "$rc" = 0 ] && [ "$calls" = 0 ]; then
  ok "LINKAGE_OFF=1 bypasses the gate without consulting the checker"
else
  bad "LINKAGE_OFF=1 bypasses the gate without consulting the checker" "rc=$rc calls=$calls"
fi

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
