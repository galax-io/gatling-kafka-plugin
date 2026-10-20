#!/usr/bin/env bash
#
# check-speckit_test.sh — tests for scripts/check-speckit.sh.
#
# Hermetic: each case builds a small repository with a manifest of its own and runs the script
# in it. What is under test is the promise of the script: the kit is the template's, byte for
# byte — a changed file, a missing one and an extra one in a directory the kit owns are found;
# what is the repository's own (the constitution, the CLI's state) is not.

set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$here/check-speckit.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

pass=0; fail=0
ok()  { printf '  ✓ %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf '  ✗ %s\n     %s\n' "$1" "$2"; fail=$((fail + 1)); }
check() { if [ "$2" -eq 0 ]; then ok "$1"; else bad "$1" "$3"; fi; }

if command -v sha256sum >/dev/null 2>&1; then sha256() { sha256sum "$@"; }; else sha256() { shasum -a 256 "$@"; }; fi

new_repo() { # name — a repository whose kit matches its manifest
  local d="$tmp/$1"
  mkdir -p "$d/.claude/skills/speckit-plan" "$d/.agents/skills/speckit-plan" "$d/.specify/scripts/bash" "$d/.specify/memory"
  printf 'plan skill\n' > "$d/.claude/skills/speckit-plan/SKILL.md"
  printf 'plan skill for codex\n' > "$d/.agents/skills/speckit-plan/SKILL.md"
  printf '#!/bin/sh\n' > "$d/.specify/scripts/bash/common.sh"
  printf 'the repository own constitution\n' > "$d/.specify/memory/constitution.md"
  ( cd "$d" && sha256 .claude/skills/speckit-plan/SKILL.md .agents/skills/speckit-plan/SKILL.md .specify/scripts/bash/common.sh > .specify/speckit-kit.sha256 )
  echo "$d"
}
run() { ( cd "$1" && bash "$script" 2>&1 ); }

echo "check-speckit.sh"

d="$(new_repo same)"
out="$(run "$d")"; rc=$?
check "a kit equal to the manifest passes" "$rc" "rc=$rc: $out"

d="$(new_repo constitution)"
printf 'rewritten by the maintainer\n' > "$d/.specify/memory/constitution.md"
printf '{}\n' > "$d/.specify/feature.json"
mkdir -p "$d/.specify/extensions/git/.cache" "$d/.specify/extensions/.cache"
printf 'x\n' > "$d/.specify/extensions/.registry"; printf 'x\n' > "$d/.specify/extensions/.cache/c.json"
printf 'x\n' > "$d/.specify/integrations.manifest.json"
out="$(run "$d")"; rc=$?
check "the constitution, feature.json and the CLI's state are the repository's own" "$rc" "rc=$rc: $out"

d="$(new_repo modified)"
printf 'edited by hand\n' > "$d/.claude/skills/speckit-plan/SKILL.md"
out="$(run "$d")"; rc=$?
check "a changed skill fails" "$([ "$rc" = 1 ] && echo 0 || echo 1)" "rc=$rc: $out"
check "  and is named MODIFIED" "$(grep -q 'MODIFIED .claude/skills/speckit-plan/SKILL.md' <<<"$out" && echo 0 || echo 1)" "$out"

d="$(new_repo codex-modified)"
printf 'edited by hand\n' > "$d/.agents/skills/speckit-plan/SKILL.md"
out="$(run "$d")"; rc=$?
check "a changed Codex skill fails too" "$([ "$rc" = 1 ] && echo 0 || echo 1)" "rc=$rc: $out"

d="$(new_repo missing)"
rm "$d/.specify/scripts/bash/common.sh"
out="$(run "$d")"; rc=$?
check "a missing file fails" "$([ "$rc" = 1 ] && echo 0 || echo 1)" "rc=$rc: $out"
check "  and is named MISSING" "$(grep -q 'MISSING .specify/scripts/bash/common.sh' <<<"$out" && echo 0 || echo 1)" "$out"

d="$(new_repo extra-skill)"
mkdir -p "$d/.claude/skills/speckit-old"; printf 'a skill the template no longer has\n' > "$d/.claude/skills/speckit-old/SKILL.md"
out="$(run "$d")"; rc=$?
check "a skill the template does not list fails" "$([ "$rc" = 1 ] && echo 0 || echo 1)" "rc=$rc: $out"
check "  and is named EXTRA" "$(grep -q 'EXTRA .claude/skills/speckit-old/SKILL.md' <<<"$out" && echo 0 || echo 1)" "$out"

d="$(new_repo extra-script)"
printf '#!/bin/sh\n' > "$d/.specify/scripts/bash/mine.sh"
out="$(run "$d")"; rc=$?
check "a script added to .specify/scripts fails" "$([ "$rc" = 1 ] && echo 0 || echo 1)" "rc=$rc: $out"

d="$(new_repo pycache)"
mkdir -p "$d/.specify/extensions/git/scripts/python/__pycache__"; printf 'x\n' > "$d/.specify/extensions/git/scripts/python/__pycache__/a.cpython-312.pyc"
out="$(run "$d")"; rc=$?
check "Python's __pycache__ under the kit is not an extra file" "$rc" "rc=$rc: $out"

d="$(new_repo extra-link)"
mkdir -p "$d/.claude/skills/speckit-old"; ln -s ../../../.specify/gone "$d/.claude/skills/speckit-old/SKILL.md"
out="$(run "$d")"; rc=$?
check "a symlink in a directory the kit owns is an extra file" "$([ "$rc" = 1 ] && echo 0 || echo 1)" "rc=$rc: $out"

d="$(new_repo link-in-kit)"
rm "$d/.agents/skills/speckit-plan/SKILL.md"; ln -s ../../../x "$d/.agents/skills/speckit-plan/SKILL.md"
out="$(run "$d")"; rc=$?
check "a listed file that is a dangling link is missing" "$([ "$rc" = 1 ] && echo 0 || echo 1)" "rc=$rc: $out"

d="$(new_repo other-skill)"
mkdir -p "$d/.claude/skills/galaxio-own"; printf 'the repository own skill\n' > "$d/.claude/skills/galaxio-own/SKILL.md"
out="$(run "$d")"; rc=$?
check "a skill that is not a speckit-* one is the repository's own" "$rc" "rc=$rc: $out"

d="$(new_repo no-manifest)"
rm "$d/.specify/speckit-kit.sha256"
out="$(run "$d")"; rc=$?
check "no manifest is an error (2), not a pass" "$([ "$rc" = 2 ] && echo 0 || echo 1)" "rc=$rc: $out"

d="$(new_repo from-subdir)"
mkdir -p "$d/src/deep"; ( cd "$d" && git init -q . )
out="$( cd "$d/src/deep" && bash "$script" 2>&1 )"; rc=$?
check "it runs from a subdirectory of the repository" "$rc" "rc=$rc: $out"

echo
echo "$pass passed, $fail failed"
[ "$fail" = 0 ]
