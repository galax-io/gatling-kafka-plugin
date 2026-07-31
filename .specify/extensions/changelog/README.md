# spec-kit-changelog

A [Spec Kit](https://github.com/github/spec-kit) extension that auto-generates changelogs, release notes, human-readable diffs, and stakeholder notifications from spec git history — turning silent spec evolution into visible, documented change communication.

## Problem

Specs change constantly, but nobody tracks what changed or tells the team:

- Requirements get added, modified, or removed with no record
- Stakeholders find out about scope changes in standup, not in advance
- Release notes are written from memory days after the fact
- PR descriptions don't explain what spec changes mean for the project
- There's no structured way to compare spec versions in plain language
- Breaking requirement changes surprise downstream teams

Git tracks file diffs, but file diffs don't explain what changed at the requirement level.

## Solution

The Spec Changelog extension adds four commands that turn spec git history into structured change documentation:

| Command | Purpose | Modifies Files? |
|---------|---------|-----------------|
| `/speckit.changelog.generate` | Generate full changelog from spec history | No — read-only |
| `/speckit.changelog.release` | Generate release notes for a specific version | No — read-only |
| `/speckit.changelog.diff` | Human-readable diff between spec versions | No — read-only |
| `/speckit.changelog.notify` | Generate stakeholder notifications from changes | No — read-only |

## Installation

```bash
specify extension add --from https://github.com/Quratulain-bilal/spec-kit-changelog/archive/refs/tags/v1.0.0.zip
```

## How It Works

### The Communication Gap

```
/speckit.specify    → spec.md changes    Requirements evolve
/speckit.plan       → plan.md changes    Architecture adapts
/speckit.tasks      → tasks.md changes   Work shifts

                    → ???               Who knows what changed?

/speckit.changelog  → changelog          ← Spec Changelog answers this
```

### What Gets Generated

**`/speckit.changelog.generate`** produces a structured changelog:

```markdown
## Changelog

### [Current] — 2026-04-11

#### Added
- REQ-008: Email verification flow
- REQ-009: Account-based rate limiting

#### Changed
- REQ-003: JWT expiry reduced from 24h to 1h (security hardening)

#### Removed
- REQ-006: Social login (deferred to v2)

## Statistics
| Metric | Value |
|--------|-------|
| Requirements added | +4 |
| Requirements removed | -1 |
| Net growth | +3 (+60%) |
```

**`/speckit.changelog.release`** generates polished release notes:

```markdown
## Release: v1.0.0

### What's New
- **Email verification** — verify before accessing protected resources
- **Admin dashboard** — manage users and view analytics

### Breaking Changes
| Change | Impact | Migration |
|--------|--------|-----------|
| JWT expiry → 1h | Clients need refresh tokens | Implement refresh flow |
```

**`/speckit.changelog.diff`** creates human-readable spec diffs:

```markdown
## Requirement Changes

### Added (+2)
- ➕ REQ-008: Email verification
- ➕ REQ-009: Account rate limiting

### Modified (1)
- ✏️ REQ-003: JWT tokens
  - Before: "expire after 24 hours"
  - After: "expire after 1 hour with refresh token"
```

**`/speckit.changelog.notify`** generates ready-to-send notifications:

```markdown
📋 Spec Update: User Auth System

➕ Added: Email verification requirement
✏️ Changed: JWT expiry 24h → 1h
❌ Removed: Social login (deferred)

📊 Impact: +8h effort | Complexity: 52 → 66
⚠️ Breaking: JWT clients need refresh token support
```

## Workflow

```
/speckit.specify                        ← Define the feature
       │
       ▼
/speckit.implement                      ← Build it
       │
       ▼
/speckit.changelog.generate             ← "What changed overall?"
/speckit.changelog.release              ← "What's in this release?"
/speckit.changelog.diff                 ← "What's different from v1?"
/speckit.changelog.notify               ← "Tell the team"
```

## Commands

### `/speckit.changelog.generate`

Full changelog generation from spec git history:

- Tracks every requirement added, modified, removed across all commits
- Uses Keep-a-Changelog format (Added/Changed/Removed/Fixed)
- Includes statistics: total changes, growth rate, most active periods
- Requirement lifecycle tracking: when each requirement was born, changed, or removed

### `/speckit.changelog.release`

Release notes for specific versions or tags:

- Maps spec changes to user-facing features
- Highlights breaking changes with migration paths
- Audience-aware: developer, product manager, end-user formats
- Quantified scope comparison against previous release

### `/speckit.changelog.diff`

Human-readable diff between any two spec versions:

- Requirement-level comparison (not line-level git diffs)
- Shows before/after wording for modified requirements
- Categorized: Added, Modified, Removed, Unchanged
- Impact summary with complexity and effort deltas

### `/speckit.changelog.notify`

Stakeholder notification generation in multiple formats:

- Slack: Short, scannable, emoji-enhanced
- Email: Detailed, context-rich, action-oriented
- PR description: Technical, diff-focused, reviewer-tagged
- Standup: Ultra-brief, verbal-friendly

## Hooks

The extension registers one optional hook:

- **after_implement**: Offers to generate a changelog entry after implementation completes

## Design Decisions

- **All commands are read-only** — never modifies any files, only analyzes and reports
- **Git-authoritative** — all change data comes from git history, not assumptions
- **Requirement-level granularity** — tracks individual requirements, not just file diffs
- **Multi-format output** — notifications ready for Slack, email, PR, and standup
- **Keep-a-Changelog standard** — uses the widely adopted changelog format by default
- **Audience-aware** — adjusts language and detail for developers, product, or leadership

## Requirements

- Spec Kit >= 0.4.0
- Git >= 2.0.0

## License

MIT
