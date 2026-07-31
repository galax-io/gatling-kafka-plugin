---
name: speckit-spectest-gaps
description: Find spec requirements with no matching tests and suggest what to test
compatibility: Requires spec-kit project structure with .specify/ directory
metadata:
  author: github-spec-kit
  source: spectest:commands/speckit.spectest.gaps.md
---

# Find Untested Requirements

Scan the codebase for spec requirements that have no corresponding tests. For each gap, provide a specific suggestion for what to test, which test type to use, and where to add the test file. Designed for quick post-implementation audits and CI integration.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty). The user may specify:
- Severity filter (e.g., "critical only", "all gaps")
- Scope (e.g., "requirements only", "include scenarios", "include decisions")
- Output format (e.g., "markdown", "json", "checklist")
- Specific phase to check (e.g., "Phase 2 only")

## Prerequisites

1. Confirm you are inside a git repository.
2. Resolve the active feature by running `.specify/scripts/bash/check-prerequisites.sh --json` from the repo root and parsing `FEATURE_DIR` and `AVAILABLE_DOCS`. The spec lives at `$FEATURE_DIR/spec.md` (under `specs/<feature>/`), not under `.specify/`.
3. Read `spec.md` to extract all requirements, scenarios, and success criteria.
4. Locate existing test files in the project.
5. If `tasks.md` exists, read it to understand which phases are complete (only flag gaps for completed phases).

## Outline

1. **Extract Requirements**: Parse spec.md for all testable items.

   - Requirements from `## Requirements`
   - Scenarios from `## User Scenarios & Testing`
   - Success criteria from `## Success Criteria`
   - Technical decisions from `plan.md` (if present)

2. **Scan Existing Tests**: Find all test files and extract their coverage targets.

   - Parse test names, describe blocks, and comments
   - Build a set of "covered topics" from existing tests

3. **Identify Gaps**: Compare requirements against covered topics.

   - For each requirement, search for matching tests
   - If no match found → it's a gap
   - Classify gap severity based on requirement importance

4. **Generate Gap Report**: List every untested requirement with actionable suggestions.

   ```markdown
   ## Untested Requirements

   | # | Requirement | Severity | Suggested Test | File |
   |---|------------|----------|----------------|------|
   | 1 | Rate limiting on login endpoint | 🔴 Critical | Integration test: verify 429 after N attempts | `tests/integration/rateLimit.test.ts` |
   | 2 | Email verification flow | 🟡 Medium | Unit test: verify token generation + expiry | `tests/unit/auth/verify.test.ts` |
   | 3 | Admin can deactivate users | 🟡 Medium | E2E test: admin dashboard deactivation flow | `tests/e2e/admin.test.ts` |

   **3 gaps found across 8 requirements (62.5% covered)**
   ```

5. **Prioritized Action List**: Rank gaps by severity and suggest implementation order.

   ```markdown
   ## Recommended Test Order

   1. 🔴 **Rate limiting** — security requirement, test first
      - What to test: send N+1 requests, verify 429 response
      - Framework: supertest + jest
      - Estimated: 1 test file, 3-4 test cases

   2. 🟡 **Email verification** — user-facing flow
      - What to test: token generation, token expiry, invalid token handling
      - Framework: jest unit tests
      - Estimated: 1 test file, 4-5 test cases

   3. 🟡 **Admin deactivation** — admin feature
      - What to test: deactivate user, verify login blocked, verify reactivation
      - Framework: playwright or cypress E2E
      - Estimated: 1 test file, 3 test cases
   ```

6. **Quick Summary**: One-line pass/fail suitable for CI output.

   ```
   SPECTEST GAPS: 3 untested requirements (2 critical, 1 medium) — 62.5% coverage
   ```

## Rules

- **Read-only** — never create or modify test files, only identify gaps and suggest
- **Phase-aware** — only flag gaps for completed implementation phases; don't flag gaps for work not yet started
- **Severity-rated** — classify each gap as Critical (🔴 security, data integrity), Medium (🟡 user-facing), or Low (🔵 internal/cosmetic)
- **Actionable** — every gap must include a concrete suggestion: what to test, which framework, where to put the file
- **Honest** — if a requirement is ambiguous or hard to test, say so rather than suggesting a meaningless test
- **CI-friendly** — include a one-line summary suitable for CI pipeline output
- **No duplicates** — if a requirement is partially tested, report it as a partial gap with what's missing, not as a full gap