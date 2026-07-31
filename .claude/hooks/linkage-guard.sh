#!/usr/bin/env bash
# PreToolUse(Bash) guard — gate ONLY release tagging. ~0 tokens. Обычный push/merge не трогает.
set -uo pipefail
input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // ""' 2>/dev/null || true)
[ -n "$cmd" ] || exit 0
case "$cmd" in *"git "*) ;; *) exit 0 ;; esac
root="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
checker="$root/scripts/check-linkage.sh"
block() { printf 'BLOCKED by linkage-guard:\n%s\n' "$1" >&2; exit 2; }
is_tag=0
# Any `git tag …` carrying a vX.Y.Z, in any flag order — upstream required the version
# to sit immediately after `tag`, so the documented annotated form (`git tag -a vX.Y.Z -m …`)
# slipped through. Read-only/destructive subcommands (-l/--list, -d/--delete, -v/--verify,
# -n) are not release creation and stay out of scope.
if printf '%s' "$cmd" | grep -qE '\bgit[[:space:]]+tag\b' \
   && printf '%s' "$cmd" | grep -qE '\bv?[0-9]+\.[0-9]+\.[0-9]+' \
   && ! printf '%s' "$cmd" | grep -qE '\bgit[[:space:]]+tag[[:space:]]+(-l\b|--list\b|-d\b|--delete\b|-v\b|--verify\b|-n)'; then
  is_tag=1
fi
if printf '%s' "$cmd" | grep -qE '\bgit[[:space:]]+push\b' \
   && printf '%s' "$cmd" | grep -qE '(--tags|refs/tags/|[[:space:]](origin[[:space:]]+)?v[0-9]+\.[0-9]+\.[0-9]+([[:space:]]|$)|release/[0-9])'; then
  is_tag=1
fi
printf '%s' "$cmd" | grep -qE '\bgit[[:space:]]+(commit|log|show)\b' && is_tag=0
[ "$is_tag" = 1 ] || exit 0
[ -x "$checker" ] || block "checker missing ($checker) — release cannot be verified"
ver=$(printf '%s' "$cmd" | grep -oE 'v?[0-9]+\.[0-9]+\.[0-9]+' | head -1)
[ -n "$ver" ] || block "tag/release push without explicit vX.Y.Z — verify: scripts/check-linkage.sh --for-tag <version>"
out=$("$checker" --for-tag "$ver" 2>&1) || block "$out"
exit 0
