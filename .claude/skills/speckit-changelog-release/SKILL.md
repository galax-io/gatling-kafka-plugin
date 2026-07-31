---
name: speckit-changelog-release
description: Generate release notes for a specific version or tag from spec changes
compatibility: Requires spec-kit project structure with .specify/ directory
metadata:
  author: github-spec-kit
  source: changelog:commands/speckit.changelog.release.md
---

# Changelog Release

Generate polished release notes for a specific version, tag, or date range. Produces stakeholder-ready release documentation that maps spec changes to user-facing features, breaking changes, and migration notes.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty). The user may specify:
- Version or tag (e.g., "v1.0", "v2.0-beta")
- Date range (e.g., "since last release", "March 15 to April 1")
- Audience (e.g., "developer", "product manager", "end user")
- Comparison base (e.g., "compare against v0.9")

## Prerequisites

1. Confirm you are inside a git repository.
2. Resolve the active feature by running `.specify/scripts/bash/check-prerequisites.sh --json` from the repo root and parsing `FEATURE_DIR` and `AVAILABLE_DOCS`. The spec artifacts live in `$FEATURE_DIR/` (under `specs/<feature>/`), not under `.specify/`.
3. Identify the version boundaries:
   - If tag specified: `git log <previous-tag>..<tag> -- "$FEATURE_DIR"`
   - If date range: `git log --after="<start>" --before="<end>" -- "$FEATURE_DIR"`
   - If no range: use all commits since the previous tag or first commit
4. Read the current spec.md and the baseline version.

## Outline

1. **Identify Release Scope**: Determine what commits fall within this release.

   ```markdown
   ## Release: v1.0.0

   | Field | Value |
   |-------|-------|
   | Tag | v1.0.0 |
   | Date | 2026-04-11 |
   | Previous release | v0.9.0 (2026-03-15) |
   | Commits included | 12 |
   | Days since last release | 27 |
   ```

2. **Extract Features**: List new capabilities added in this release.

   ```markdown
   ## What's New

   ### New Features
   - **Email verification** — Users must verify email before accessing protected resources
   - **Admin dashboard** — Administrators can manage users, view analytics, and configure settings
   - **Account-based rate limiting** — Rate limits now tracked per account instead of per IP

   ### Improvements
   - **MFA support on login** — Login flow now supports multi-factor authentication
   - **JWT security hardening** — Token expiry reduced from 24h to 1h with refresh token support

   ### Removed
   - **Social login** — Deferred to v2.0 (Google and GitHub OAuth will return in next major release)
   ```

3. **Breaking Changes**: Highlight anything that requires migration or attention.

   ```markdown
   ## Breaking Changes

   | Change | Impact | Migration |
   |--------|--------|-----------|
   | JWT expiry reduced to 1h | Clients must handle token refresh | Implement refresh token flow |
   | Rate limiting per account | IP-only clients need account headers | Add X-Account-ID header |
   ```

4. **Scope Summary**: Quantify the release.

   ```markdown
   ## Release Scope

   | Metric | v0.9.0 | v1.0.0 | Delta |
   |--------|--------|--------|-------|
   | Requirements | 5 | 8 | +3 |
   | Scenarios | 2 | 4 | +2 |
   | Complexity score | 28 | 66 | +38 (+136%) |
   | Estimated effort | 20h | 50h | +30h |
   ```

5. **Audience-Specific Notes**: Tailor the message if audience was specified.

   - **Developer notes**: API changes, dependency updates, migration steps
   - **Product notes**: Feature list, user impact, business value
   - **End-user notes**: What's new, what changed, what to expect

6. **Output**: Deliver the complete release notes.

## Rules

- **Read-only** — never modify any files, only analyze and report
- **Git-authoritative** — all data comes from git tags, commits, and diffs
- **Audience-aware** — tailor language and detail level to the specified audience
- **Breaking changes prominent** — always highlight breaking changes separately and clearly
- **Migration-oriented** — for every breaking change, suggest a migration path
- **Quantified** — include numerical scope summary comparing to previous release
- **Polished** — release notes should be ready to publish without editing