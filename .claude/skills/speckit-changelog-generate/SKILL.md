---
name: speckit-changelog-generate
description: Generate a full changelog from spec git history showing all requirement
  changes over time
compatibility: Requires spec-kit project structure with .specify/ directory
metadata:
  author: github-spec-kit
  source: changelog:commands/speckit.changelog.generate.md
---

# Changelog Generate

Generate a comprehensive changelog from the git history of spec artifacts. Tracks every requirement added, modified, removed, and reworded — producing a human-readable changelog that maps spec evolution to project milestones.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty). The user may specify:
- Date range (e.g., "last 2 weeks", "since v1.0")
- Format (e.g., "keep-a-changelog", "conventional", "timeline")
- Scope filter (e.g., "requirements only", "include tasks")
- Output style (e.g., "developer", "stakeholder", "executive")

## Prerequisites

1. Confirm you are inside a git repository.
2. Resolve the active feature by running `.specify/scripts/bash/check-prerequisites.sh --json` from the repo root and parsing `FEATURE_DIR` and `AVAILABLE_DOCS`. The spec artifacts live in `$FEATURE_DIR/` (under `specs/<feature>/`), not under `.specify/`. Always quote `"$FEATURE_DIR"` in shell snippets — the resolved value is absolute and may contain spaces.
3. Retrieve the full git log for `"$FEATURE_DIR/spec.md"` using `git log --oneline --follow -- "$FEATURE_DIR/spec.md"`.
4. If `plan.md` exists, retrieve its history too.
5. If `tasks.md` exists, retrieve its history too.

## Outline

1. **Collect History**: Get all committed versions of spec artifacts.

   ```bash
   git log --oneline --follow --format="%H %ad %s" --date=short -- "$FEATURE_DIR/spec.md"
   ```

2. **Diff Each Version**: For each consecutive pair of commits, extract what changed.

   For each commit pair, compare:
   - Requirements added, removed, or modified
   - Scenarios added or changed
   - Success criteria changes
   - Integration changes
   - Phase/task structure changes

3. **Categorize Changes**: Group changes using changelog categories.

   ```markdown
   ## Changelog

   ### [Current] — 2026-04-11

   #### Added
   - REQ-008: Email verification flow
   - REQ-009: Rate limiting per account
   - Scenario: Admin bulk user management

   #### Changed
   - REQ-003: JWT expiry changed from 24h to 1h (security hardening)
   - Phase 2 split into Phase 2a (auth) and Phase 2b (permissions)

   #### Removed
   - REQ-006: Social login (deferred to v2)

   ### [v0.2] — 2026-03-28

   #### Added
   - REQ-006: Social login (Google, GitHub)
   - REQ-007: Admin dashboard
   - Phase 3: Admin features

   #### Changed
   - REQ-002: Login now supports MFA
   ```

4. **Generate Statistics**: Summarize the changelog numerically.

   ```markdown
   ## Changelog Statistics

   | Metric | Value |
   |--------|-------|
   | Total versions | 6 |
   | Date range | Mar 15 — Apr 11 (27 days) |
   | Requirements added | +7 |
   | Requirements removed | -1 |
   | Requirements modified | 3 |
   | Net requirement growth | +6 (+120%) |
   | Most active period | Mar 22–28 (3 changes) |
   ```

5. **Requirement Lifecycle**: Show the full history of each requirement.

   ```markdown
   ## Requirement Lifecycle

   | Requirement | Added | Modified | Status |
   |-------------|-------|----------|--------|
   | REQ-001: Signup | Mar 15 | — | Active |
   | REQ-002: Login | Mar 15 | Mar 28 (MFA) | Active |
   | REQ-003: JWT | Mar 15 | Apr 2 (expiry) | Active |
   | REQ-006: Social login | Mar 28 | — | Removed (Apr 8) |
   | REQ-008: Email verify | Apr 2 | — | Active |
   ```

6. **Output**: Deliver the complete changelog.

## Rules

- **Read-only** — never modify any files, only analyze git history and report
- **Git-authoritative** — all data comes from git commits, not assumptions
- **Keep-a-Changelog format** — use Added/Changed/Removed/Fixed categories by default
- **Chronological** — newest changes first, oldest last
- **Requirement-level granularity** — track individual requirements, not just file-level diffs
- **Statistics included** — always include numerical summary of changes
- **Lifecycle tracking** — show the full history of requirements that were modified or removed