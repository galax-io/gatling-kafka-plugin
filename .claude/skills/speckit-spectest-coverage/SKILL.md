---
name: speckit-spectest-coverage
description: Map spec requirements to test files and calculate requirement-level coverage
compatibility: Requires spec-kit project structure with .specify/ directory
metadata:
  author: github-spec-kit
  source: spectest:commands/speckit.spectest.coverage.md
---

# Spec Requirement Coverage

Analyze the relationship between spec requirements and existing test files. Unlike code coverage tools that measure line/branch coverage, this command measures **requirement coverage** — what percentage of spec requirements have at least one corresponding test.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty). The user may specify:
- A specific requirement to check (e.g., "REQ-001")
- Output format (e.g., "markdown", "json", "summary")
- Coverage threshold (e.g., "minimum 80%")
- Test directory override (e.g., "src/__tests__/")

## Prerequisites

1. Confirm you are inside a git repository.
2. Resolve the active feature by running `.specify/scripts/bash/check-prerequisites.sh --json` from the repo root and parsing `FEATURE_DIR` and `AVAILABLE_DOCS`. The spec lives at `$FEATURE_DIR/spec.md` (under `specs/<feature>/`), not under `.specify/`.
3. Read `spec.md` to extract all requirements, scenarios, and success criteria.
4. Locate test files in the project by searching common test directories and patterns (`**/*.test.*`, `**/*.spec.*`, `**/test_*`, `tests/`, `__tests__/`).
5. If no test files are found, report 0% coverage and suggest running `/speckit.spectest.generate`.

## Outline

1. **Extract All Testable Requirements**: Build the complete requirement list from spec artifacts.

   - Requirements from `spec.md` `## Requirements` section
   - User scenarios from `spec.md` `## User Scenarios & Testing` section
   - Success criteria from `spec.md` `## Success Criteria` section
   - Technical decisions from `plan.md` (if exists)
   - Assign stable IDs: REQ-001, SCENARIO-001, CRITERIA-001, DECISION-001

2. **Discover Test Files**: Find all test files in the project.

   - Search for files matching test naming patterns
   - Parse test file contents to extract test suite names, describe blocks, and test case names
   - Build an inventory of what each test file covers

3. **Map Requirements to Tests**: For each requirement, determine if matching tests exist.

   - Match by keyword: requirement text keywords against test names and descriptions
   - Match by file reference: if test file name maps to implementation file referenced in spec
   - Match by explicit traceability: if test comments reference requirement IDs
   - Classify each mapping as Strong (explicit reference), Medium (keyword match), or Weak (file name match)

4. **Calculate Coverage Metrics**: Produce quantitative coverage scores.

   ```markdown
   ## Requirement Coverage Summary

   | Category | Total | Covered | Percentage |
   |----------|-------|---------|------------|
   | Requirements | 8 | 6 | 75.0% |
   | User Scenarios | 3 | 2 | 66.7% |
   | Success Criteria | 5 | 4 | 80.0% |
   | Technical Decisions | 4 | 3 | 75.0% |
   | **Overall** | **20** | **15** | **75.0%** |
   ```

5. **Generate Detailed Mapping**: Show each requirement and its test status.

   ```markdown
   ## Detailed Coverage Map

   | ID | Requirement | Test File | Confidence | Status |
   |----|------------|-----------|------------|--------|
   | REQ-001 | User signup | `signup.test.ts` | Strong | ✅ Covered |
   | REQ-002 | User login | `login.test.ts` | Strong | ✅ Covered |
   | REQ-003 | Rate limiting | — | — | ❌ No tests |
   | SCENARIO-001 | Registration flow | `auth-flow.test.ts` | Medium | ⚠️ Partial |
   ```

6. **Coverage by Test Type**: Break down by unit, integration, and E2E.

   ```markdown
   ## Coverage by Test Type

   | Requirement | Unit | Integration | E2E |
   |-------------|------|-------------|-----|
   | REQ-001 | ✅ | ✅ | ❌ |
   | REQ-002 | ✅ | ❌ | ❌ |
   | REQ-003 | ❌ | ❌ | ❌ |
   ```

7. **Output Report**: Deliver the complete coverage analysis.

## Rules

- **Read-only** — never modify any files, only analyze and report
- **Requirement-centric** — measure coverage by spec requirement, not by code line or branch
- **Confidence-rated** — classify each test mapping as Strong, Medium, or Weak with explanation
- **Threshold-aware** — compare against configured threshold and report pass/fail
- **Actionable** — for each uncovered requirement, suggest which test type and file would cover it
- **No false positives** — a test must demonstrably relate to a requirement to count as coverage; don't inflate scores with weak matches
- **Framework-agnostic** — work with any test framework by analyzing file contents and naming patterns