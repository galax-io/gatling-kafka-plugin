#!/usr/bin/env bash
# PreToolUse(Bash): an issue/PR milestone may only be the current vX.Y.0 release
# milestone. Fires ONLY on a real gh assignment command (not text that mentions one);
# reads and everything else pass. Bypass a backlog move: MILESTONE_GUARD_OFF=1 <cmd>
set -uo pipefail
cmd=$(jq -r '.tool_input.command // ""' 2>/dev/null) || exit 0
case "$cmd" in *MILESTONE_GUARD_OFF=1*) exit 0 ;; esac

# In scope only when gh actually assigns a milestone: `issue|pr edit|create --milestone`,
# or `api … -F milestone=`. Anchored to the subcommand so a commit message or echo that
# merely contains "--milestone" does not trigger it.
is_assign=0
{ printf '%s' "$cmd" | grep -qE '\bgh +(issue|pr) +(edit|create)\b' && printf '%s' "$cmd" | grep -qE -- '--milestone[= ]'; } && is_assign=1
{ printf '%s' "$cmd" | grep -qE '\bgh +api\b'                       && printf '%s' "$cmd" | grep -qE -- '-[fF] +milestone='; } && is_assign=1
[ "$is_assign" = 1 ] || exit 0

val=$(printf '%s' "$cmd" | grep -oE -- '--milestone[= ]+("[^"]*"|[^ ]+)|-[fF] +milestone=[^ ]+' | head -1 \
      | sed -E 's/^--milestone[= ]+//; s/^-[fF] +milestone=//; s/^"//; s/"$//')
[ -n "$val" ] || exit 0   # clearing a milestone → fine

cur=$(bash "${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null)}/scripts/current-milestone.sh" 2>/dev/null || true)
[ -n "$cur" ] || exit 0   # no current milestone to compare against → stay out of the way
num=$(printf '%s' "$cur" | cut -f1)
ver=$(printf '%s' "$cur" | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1)

# Pass if the target is the current number, or a title carrying the current version.
[ "$val" = "$num" ] && exit 0
[ -n "$ver" ] && printf '%s' "$val" | grep -qF "$ver" && exit 0

printf 'BLOCKED by milestone-guard: assign to the current milestone #%s (%s), not "%s".\nDeliberate backlog move? prefix MILESTONE_GUARD_OFF=1\n' "$num" "$ver" "$val" >&2
exit 2
