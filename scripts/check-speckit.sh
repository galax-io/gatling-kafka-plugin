#!/usr/bin/env bash
#
# check-speckit.sh — the Spec Kit kit of this repository is the template's, byte for byte.
#
# Every repository of the organization carries the same skills (.claude/skills for Claude Code,
# .agents/skills for Codex) and the same Spec Kit files under .specify/. They are generated once, in
# spec-kit-galaxio-bootstrap, and listed with their hashes in .specify/speckit-kit.sha256. This
# script compares the repository with that list:
#
#   MODIFIED  a file of the kit differs from the template's
#   MISSING   a file of the kit is not there
#   EXTRA     a file in a directory the kit owns that the kit does not list
#
# Run from anywhere in the repository. It changes nothing.
#
# A repository may carry a layer on top of the kit (spec-kit-galaxio-bootstrap, layers/<name>/): a few
# kit files replaced by ones the template's generator made, listed with their hashes in
# .specify/speckit-overlay.sha256. A file listed there is compared with that hash instead of the kit's,
# and nothing else changes:
#
#   OVERLAY   a path of the overlay manifest that the kit does not list
#
# Not checked, because they are the repository's own: .specify/memory/ (its constitution),
# .specify/feature.json, and the state the Specify CLI keeps (.registry, .cache, .backup,
# integration manifests), which the kit does not ship.
#
# To change the kit, change spec-kit-galaxio-bootstrap (scripts/build-speckit-kit.sh) and take the
# new release with `copier update` or `scripts/adopt.sh --speckit`. Do not run `specify extension`,
# `preset` or `integration` commands here: they rewrite the kit differently in each repository.
#
# Exit: 0 the kit is the template's | 1 it is not | 2 no manifest or no hash tool.

set -uo pipefail

root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$root" || exit 2
MANIFEST=".specify/speckit-kit.sha256"
OVERLAY=".specify/speckit-overlay.sha256"

[ -f "$MANIFEST" ] || { echo "check-speckit: $MANIFEST is missing; bring the kit in with scripts/adopt.sh --speckit from spec-kit-galaxio-bootstrap" >&2; exit 2; }
if command -v sha256sum >/dev/null 2>&1; then sha256() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then sha256() { shasum -a 256 "$1" | cut -d' ' -f1; }
else echo "check-speckit: neither sha256sum nor shasum is installed" >&2; exit 2; fi

problems=0
report() { printf '%s %s\n' "$1" "$2"; problems=$((problems + 1)); }

listed="$(mktemp)"; trap 'rm -f "$listed"' EXIT

# The hashes to meet: the kit's, with the layer's in place of the kit's for the paths the layer lists.
# A path the layer lists that the kit does not is reported once, here.
expected="$(mktemp)"; trap 'rm -f "$listed" "$expected"' EXIT
if [ -f "$OVERLAY" ]; then
  awk 'NR == FNR { if (NF >= 2) over[$2] = $1; next }
       { if ($2 in over) { $1 = over[$2]; seen[$2] = 1 } print }
       END { for (p in over) if (!(p in seen)) print "OVERLAY", p > "/dev/stderr" }' "$OVERLAY" "$MANIFEST" > "$expected" 2> "$expected.problems"
  while read -r _ path; do report OVERLAY "$path"; done < "$expected.problems"
  rm -f "$expected.problems"
else
  cp "$MANIFEST" "$expected"
fi

while read -r hash path; do
  [ -n "$path" ] || continue
  printf '%s\n' "$path" >> "$listed"
  if [ -L "$path" ] && [ ! -e "$path" ]; then report MISSING "$path"
  elif [ ! -f "$path" ]; then report MISSING "$path"
  elif [ "$(sha256 "$path")" != "$hash" ]; then report MODIFIED "$path"
  fi
done < "$expected"
printf '%s\n' "$MANIFEST" >> "$listed"
[ -f "$OVERLAY" ] && printf '%s\n' "$OVERLAY" >> "$listed"
sort -o "$listed" "$listed"

# What the kit owns: its two skill directories and Spec Kit's own directories.
owned=(.claude/skills/speckit-* .agents/skills/speckit-* .specify/scripts .specify/templates \
       .specify/extensions .specify/presets .specify/workflows .specify/integrations)
for d in "${owned[@]}"; do
  [ -e "$d" ] || continue
  find "$d" \( -type f -o -type l \) \
    ! -path '*/__pycache__/*' ! -name '*.pyc' ! -path '.specify/extensions/.registry' ! -path '.specify/extensions/.cache/*' ! -path '.specify/extensions/.backup/*' \
    ! -path '.specify/extensions/*/local-config.yml' ! -path '.specify/presets/.registry' \
    ! -path '.specify/integrations/*.manifest.json' ! -path '.specify/integrations/packages/*' \
    ! -path '.specify/integrations/packages.json' ! -path '.specify/workflows/workflow-registry.json'
done | LC_ALL=C sort | LC_ALL=C comm -23 - "$listed" | while read -r extra; do printf 'EXTRA %s\n' "$extra"; done > "$listed.extra"
if [ -s "$listed.extra" ]; then
  problems=$((problems + $(wc -l < "$listed.extra")))
  cat "$listed.extra"
fi
rm -f "$listed.extra"

if [ "$problems" -gt 0 ]; then
  echo "check-speckit: $problems difference(s) from the template's Spec Kit kit; take the template's with scripts/adopt.sh --speckit (spec-kit-galaxio-bootstrap), do not edit the kit here" >&2
  exit 1
fi
echo "check-speckit: the Spec Kit kit is the template's"
