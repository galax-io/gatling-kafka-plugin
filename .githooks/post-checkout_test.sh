#!/usr/bin/env bash
#
# post-checkout_test.sh — tests for what post-checkout gives a new worktree of AGENTS.md and CLAUDE.md.
#
# Hermetic: the "remote" is a bare repository in a temp directory, so there is no network; the hook runs as git
# runs it, through `git worktree add` and `git switch` with core.hooksPath pointing at it.

set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/hooks"
cp "$here/post-checkout" "$tmp/hooks/post-checkout"
chmod +x "$tmp/hooks/post-checkout"

pass=0; fail=0
ok()  { printf '  ✓ %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf '  ✗ %s\n     %s\n' "$1" "$2"; fail=$((fail + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "expected [$3], got [$2]"; fi; }

g() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main "$@"; }

# ---- origin: main with AGENTS.md v1; branch `old` forks there; main then moves to v2 -------------
g init -q --bare "$tmp/origin.git"
g clone -q "$tmp/origin.git" "$tmp/seed" 2>/dev/null
echo agents-v1 > "$tmp/seed/AGENTS.md"
g -C "$tmp/seed" add AGENTS.md; g -C "$tmp/seed" commit -qm v1
g -C "$tmp/seed" push -q origin HEAD:main
g -C "$tmp/seed" branch old
g -C "$tmp/seed" switch -q -c own
echo agents-own > "$tmp/seed/AGENTS.md"; g -C "$tmp/seed" commit -qam own
g -C "$tmp/seed" push -q origin old own
g -C "$tmp/seed" switch -q old 2>/dev/null

# The main checkout clones now, so its main stays at v1 when origin moves on: the stale checkout of the bug.
g clone -q "$tmp/origin.git" "$tmp/main" 2>/dev/null
g -C "$tmp/main" config core.hooksPath "$tmp/hooks"
echo claude-local > "$tmp/main/CLAUDE.md"

g -C "$tmp/seed" switch -q -c next origin/main 2>/dev/null || g -C "$tmp/seed" switch -q -c next main
echo agents-v2 > "$tmp/seed/AGENTS.md"; g -C "$tmp/seed" commit -qam v2
g -C "$tmp/seed" push -q origin HEAD:main

add() {  # <dir> <args...> -> stderr of the hook in $tmp/err
  local dir="$1"; shift
  ( cd "$tmp/main" && g worktree add -q "$tmp/$dir" "$@" ) 2>"$tmp/err" >/dev/null
}
status() { g -C "$tmp/$1" status --short --untracked-files=no | tr -d '\n'; }

echo "post-checkout:"

# 0. a new branch from a stale origin/main (the main checkout never fetched v2) is told it is behind
add wt-stale -b stale origin/main
check "branch from a stale origin/main: warned" "$(grep -c 'AGENTS.md is behind origin/main' "$tmp/err")" 1

# 1. a new branch from a fresh origin/main gets main's files, clean, and nothing is said
( cd "$tmp/main" && g fetch -q origin )
add wt-new -b feat origin/main
check "new branch from origin/main: AGENTS.md is main's" "$(cat "$tmp/wt-new/AGENTS.md")" agents-v2
check "new branch from origin/main: no tracked change" "$(status wt-new)" ""
check "new branch from origin/main: no warning" "$(grep -c 'is behind' "$tmp/err")" 0

# 2. a branch behind main keeps its own file, stays clean, and is told to rebase
add wt-old old
check "branch behind main: AGENTS.md stays the branch's" "$(cat "$tmp/wt-old/AGENTS.md")" agents-v1
check "branch behind main: no tracked change" "$(status wt-old)" ""
check "branch behind main: warned" "$(grep -c 'AGENTS.md is behind origin/main' "$tmp/err")" 1

# 3. a branch forked from today's main that changed the file itself: its own change, nothing to say
( cd "$tmp/seed" && g switch -q -c mine origin/main 2>/dev/null && echo agents-mine > AGENTS.md && g commit -qam mine && g push -q origin mine )
( cd "$tmp/main" && g fetch -q origin )
add wt-mine mine
check "branch that changed the file itself: kept" "$(cat "$tmp/wt-mine/AGENTS.md")" agents-mine
check "branch that changed the file itself: no warning" "$(grep -c 'is behind' "$tmp/err")" 0

# 4. a file no branch tracks comes from the main checkout
check "untracked CLAUDE.md copied from the main checkout" "$(cat "$tmp/wt-old/CLAUDE.md")" claude-local

# 5. hooks.worktreeRef writes that ref's file
( cd "$tmp/main" && g config hooks.worktreeRef origin/main )
add wt-ref -b t5 old
( cd "$tmp/main" && g config --unset hooks.worktreeRef )
check "hooks.worktreeRef=origin/main: main's file written" "$(cat "$tmp/wt-ref/AGENTS.md")" agents-v2

# 6. SDLC_WORKTREE_REF naming no commit: files as checked out, and said
( cd "$tmp/main" && SDLC_WORKTREE_REF=nosuch g worktree add -q "$tmp/wt-bad" -b t6 old ) 2>"$tmp/err" >/dev/null
check "unknown ref: AGENTS.md as checked out" "$(cat "$tmp/wt-bad/AGENTS.md")" agents-v1
check "unknown ref: said" "$(grep -c 'no commit nosuch' "$tmp/err")" 1

# 7. a branch switch inside a worktree does nothing
rm -f "$tmp/wt-new/CLAUDE.md"
( cd "$tmp/wt-new" && g switch -q -c t7 ) 2>/dev/null
check "branch switch: nothing copied" "$([ -e "$tmp/wt-new/CLAUDE.md" ] && echo copied || echo none)" none

# 8. no remote to fetch: it says so and still compares with what it has
g -C "$tmp/main" remote set-url origin "$tmp/missing.git"
add wt-off -b t8 old
check "fetch fails: said" "$(grep -c 'could not fetch origin main' "$tmp/err")" 1
check "fetch fails: still warned from the last fetch" "$(grep -c 'AGENTS.md is behind origin/main' "$tmp/err")" 1

echo "post-checkout: $pass passed, $fail failed"
[ "$fail" = 0 ]
