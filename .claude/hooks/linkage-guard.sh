#!/usr/bin/env bash
# PreToolUse(Bash) guard — gate ONLY release tagging. ~0 tokens. Обычный push/merge не трогает.
# Fires ONLY on a real git tag/push command, never on text that merely mentions one.
# Bypass a deliberate release: LINKAGE_OFF=1 <cmd>
set -uo pipefail
input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // ""' 2>/dev/null || true)
[ -n "$cmd" ] || exit 0
[ "${LINKAGE_OFF:-}" = "1" ] && exit 0
case "$cmd" in *LINKAGE_OFF=1*) exit 0 ;; esac
case "$cmd" in *git*) ;; *) exit 0 ;; esac
root="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
checker="$root/scripts/check-linkage.sh"
block() { printf 'BLOCKED by linkage-guard:\n%s\n' "$1" >&2; exit 2; }

# Scope the guard to what the shell would actually RUN. Matching the raw command string
# made any *argument* mentioning a tag look like a release — a `gh pr create` whose heredoc
# body documented this hook was blocked, reporting a version that came from prose.
# So: drop heredoc bodies, split on shell separators, keep only segments that invoke git.
strip_heredocs() {
  local line trimmed term="" re='<<-?[[:space:]]*["'"'"']?([A-Za-z_][A-Za-z0-9_]*)["'"'"']?'
  while IFS= read -r line; do
    if [ -n "$term" ]; then
      trimmed="${line#"${line%%[![:space:]]*}"}"
      [ "$trimmed" = "$term" ] && term=""
      continue
    fi
    printf '%s\n' "$line"
    [[ $line =~ $re ]] && term="${BASH_REMATCH[1]}"
  done
}

split_segments() {
  local s="$1"
  s="${s//&&/$'\n'}"; s="${s//||/$'\n'}"; s="${s//;/$'\n'}"; s="${s//|/$'\n'}"
  printf '%s\n' "$s"
}

# Peel leading env assignments and wrappers so `rtk proxy git tag …` still counts as git.
peel() {
  local s="$1" head
  s="${s#"${s%%[![:space:]]*}"}"
  while :; do
    head="${s%% *}"
    case "$head" in
      [A-Za-z_]*=*) s="${s#"$head"}" ;;
      rtk|proxy|sudo|env|nohup|time|command|builtin|exec|xargs) s="${s#"$head"}" ;;
      *) break ;;
    esac
    s="${s#"${s%%[![:space:]]*}"}"
  done
  printf '%s' "$s"
}

is_tag=0
hit=""
segments=$(split_segments "$(printf '%s\n' "$cmd" | strip_heredocs)")
while IFS= read -r seg; do
  seg=$(peel "$seg")
  case "$seg" in git|git\ *) ;; *) continue ;; esac
  # Non-release subcommands are out of scope. Scoped to THIS segment: the old global
  # reset let `git commit -m … && git tag vX.Y.Z` through ungated.
  printf '%s' "$seg" | grep -qE '^git[[:space:]]+(commit|log|show)\b' && continue
  # Any `git tag …` carrying a vX.Y.Z, in any flag order — upstream required the version
  # to sit immediately after `tag`, so the documented annotated form (`git tag -a vX.Y.Z -m …`)
  # slipped through. Read-only/destructive subcommands (-l/--list, -d/--delete, -v/--verify,
  # -n) are not release creation and stay out of scope.
  if printf '%s' "$seg" | grep -qE '^git[[:space:]]+tag\b' \
     && printf '%s' "$seg" | grep -qE '\bv?[0-9]+\.[0-9]+\.[0-9]+' \
     && ! printf '%s' "$seg" | grep -qE '^git[[:space:]]+tag[[:space:]]+(-l\b|--list\b|-d\b|--delete\b|-v\b|--verify\b|-n)'; then
    is_tag=1; hit="$seg"; break
  fi
  if printf '%s' "$seg" | grep -qE '^git[[:space:]]+push\b' \
     && printf '%s' "$seg" | grep -qE '(--tags|refs/tags/|[[:space:]](origin[[:space:]]+)?v[0-9]+\.[0-9]+\.[0-9]+([[:space:]]|$)|release/[0-9])'; then
    is_tag=1; hit="$seg"; break
  fi
done <<<"$segments"
[ "$is_tag" = 1 ] || exit 0
[ -x "$checker" ] || block "checker missing ($checker) — release cannot be verified"
ver=$(printf '%s' "$hit" | grep -oE 'v?[0-9]+\.[0-9]+\.[0-9]+' | head -1)
[ -n "$ver" ] || block "tag/release push without explicit vX.Y.Z — verify: scripts/check-linkage.sh --for-tag <version>"
out=$("$checker" --for-tag "$ver" 2>&1) || block "$out"
exit 0
