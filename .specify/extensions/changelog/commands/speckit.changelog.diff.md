---
description: "Generate a human-readable diff between two spec versions showing exactly what changed"
---

# Changelog Diff

Generate a clear, human-readable diff between two versions of a spec. Instead of raw git diffs, produces a structured comparison that shows requirement-level changes, rewording, additions, and removals in plain language.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty). The user may specify:
- Two versions to compare (e.g., "v1.0 vs v2.0", "HEAD vs main")
- A specific commit or branch (e.g., "compare against abc123")
- Scope filter (e.g., "requirements only", "tasks only")
- Detail level (e.g., "summary", "detailed", "line-by-line")

## Prerequisites

1. Confirm you are inside a git repository.
2. Resolve the active feature by running `.specify/scripts/bash/check-prerequisites.sh --json` from the repo root and parsing `REPO_ROOT`, `FEATURE_DIR`, and `AVAILABLE_DOCS`. The spec artifacts live in `$FEATURE_DIR/` (under `specs/<feature>/`), not under `.specify/`. `FEATURE_DIR` is absolute; derive the repo-relative form for any `git show <ref>:<path>` pathspec (which rejects absolute paths): `REL_FEATURE_DIR="${FEATURE_DIR#$REPO_ROOT/}"`.
3. Identify the two versions to compare:
   - If two refs specified: use those directly
   - If one ref specified: compare that ref against current HEAD
   - If no ref: compare current against the most recent commit of spec.md
4. Read both versions of spec.md using `git show "<ref>:$REL_FEATURE_DIR/spec.md"`.

## Outline

1. **Identify Versions**: Show what's being compared.

   ```markdown
   ## Spec Diff

   | Field | Version A | Version B |
   |-------|-----------|-----------|
   | Ref | `a1b2c3d` (v0.9) | `HEAD` (current) |
   | Date | 2026-03-15 | 2026-04-11 |
   | Author | @developer | @developer |
   ```

2. **Requirement Diff**: Show requirement-level changes.

   ```markdown
   ## Requirement Changes

   ### Added (+3)
   - ➕ REQ-007: Admin dashboard — manage users and view analytics
   - ➕ REQ-008: Email verification — verify email before access
   - ➕ REQ-009: Account rate limiting — per-account rate limits

   ### Modified (2)
   - ✏️ REQ-002: Login
     - Before: "Users can log in with email and password"
     - After: "Users can log in with email/password and optional MFA"
     - Change: Added MFA support
   - ✏️ REQ-003: JWT tokens
     - Before: "JWT tokens expire after 24 hours"
     - After: "JWT tokens expire after 1 hour with refresh token support"
     - Change: Security hardening — shorter expiry + refresh tokens

   ### Removed (-1)
   - ❌ REQ-006: Social login (Google, GitHub OAuth)
     - Reason: Deferred to v2.0

   ### Unchanged (4)
   - REQ-001: Signup ✓
   - REQ-004: Password reset ✓
   - REQ-005: Profile management ✓
   ```

3. **Scenario Diff**: Show scenario-level changes.

   ```markdown
   ## Scenario Changes

   | Status | Scenario | Detail |
   |--------|----------|--------|
   | ➕ Added | Admin bulk management | New admin workflow |
   | ➕ Added | Email verification flow | New onboarding step |
   | ✏️ Modified | User login | Added MFA branch |
   | ✓ Unchanged | User signup | — |
   ```

4. **Structure Diff**: Show phase and task structure changes.

   ```markdown
   ## Structure Changes

   | Element | Version A | Version B | Change |
   |---------|-----------|-----------|--------|
   | Phases | 2 | 4 | +2 new phases |
   | Tasks | 8 | 18 | +10 new tasks |
   | Integrations | 1 | 3 | +2 new integrations |
   ```

5. **Impact Summary**: Quantify the overall diff.

   ```markdown
   ## Diff Summary

   | Metric | Version A | Version B | Delta |
   |--------|-----------|-----------|-------|
   | Requirements | 6 | 8 | +2 net (+33%) |
   | Complexity score | 34 | 66 | +32 (+94%) |
   | Estimated effort | 25h | 50h | +25h (+100%) |
   | Risk level | 🟢 Low | 🟠 Medium | ⬆️ Increased |
   ```

6. **Output**: Deliver the complete diff report.

## Rules

- **Read-only** — never modify any files, only analyze and report
- **Git-authoritative** — both versions come from git history
- **Human-readable** — never show raw git diffs; always translate to structured, plain-language comparisons
- **Requirement-level** — track changes at requirement granularity, not line-level
- **Categorized** — always group into Added, Modified, Removed, Unchanged
- **Before/After** — for modified requirements, always show both the old and new wording
- **Quantified** — include numerical impact summary with percentages
