# spec-kit-spectest

A [Spec Kit](https://github.com/github/spec-kit) extension that auto-generates test scaffolds from spec acceptance criteria, maps requirement-level coverage, finds untested requirements, and produces structured test plans — the missing testing layer in the SDD workflow.

## Problem

Spec Kit's workflow produces detailed acceptance criteria, user scenarios, and success criteria in spec.md — but testing is completely manual. After implementation:

- Developers write tests from memory instead of tracing back to spec requirements
- Acceptance criteria exist in spec.md but have no corresponding test cases
- There's no way to measure "spec coverage" — what percentage of requirements are actually tested
- Test plans are written ad-hoc instead of being derived from the spec
- Requirements slip through without any test coverage and nobody notices until production

The spec says what to build. The code builds it. But nothing ensures the tests verify what the spec actually requires.

## Solution

The SpecTest extension adds four commands that connect specs to tests:

| Command | Purpose | Modifies Files? |
|---------|---------|-----------------|
| `/speckit.spectest.generate` | Generate test scaffolds from spec acceptance criteria and scenarios | Yes — creates test files |
| `/speckit.spectest.coverage` | Map spec requirements to test files with requirement-level coverage | No — read-only |
| `/speckit.spectest.gaps` | Find requirements with no matching tests and suggest what to test | No — read-only |
| `/speckit.spectest.plan` | Generate a structured test plan document from spec scenarios | Yes — writes test-plan.md |

## Installation

```bash
specify extension add --from https://github.com/Quratulain-bilal/spec-kit-spectest/archive/refs/tags/v1.0.0.zip
```

## How It Works

### The Testing Gap

```
/speckit.specify    → spec.md      Acceptance criteria defined
/speckit.plan       → plan.md      Technical decisions made
/speckit.tasks      → tasks.md     Implementation planned
/speckit.implement  → code         Feature built
                    → ???          Are the criteria actually tested?

/speckit.spectest.generate → tests     ← SpecTest fills this gap
```

### What Gets Generated

**`/speckit.spectest.generate`** reads spec artifacts and produces framework-native test scaffolds:

```typescript
/**
 * Tests for REQ-001: User signup with email/password
 * Source: spec.md → Requirements
 * Implementation: src/auth/signup.ts
 */
describe('User Signup', () => {
  describe('successful registration', () => {
    it('should create a new user with valid email and password', () => {
      // Arrange: prepare valid signup data
      // Act: call signup function/endpoint
      // Assert: user is created, returns success
      throw new Error('TODO: implement test');
    });

    it('should hash the password before storing', () => {
      // Arrange: prepare signup data
      // Act: call signup, inspect stored password
      // Assert: stored password is bcrypt hash, not plaintext
      throw new Error('TODO: implement test');
    });
  });

  describe('duplicate email handling', () => {
    it('should reject signup with existing email', () => {
      // Arrange: create user, attempt second signup
      // Act: call signup with same email
      // Assert: returns error, no duplicate created
      throw new Error('TODO: implement test');
    });
  });
});
```

**`/speckit.spectest.coverage`** maps every requirement to its test status:

```markdown
## Requirement Coverage

| ID | Requirement | Test File | Confidence | Status |
|----|------------|-----------|------------|--------|
| REQ-001 | User signup | `signup.test.ts` | Strong | ✅ Covered |
| REQ-002 | User login | `login.test.ts` | Strong | ✅ Covered |
| REQ-003 | Rate limiting | — | — | ❌ No tests |

## Coverage Summary

| Category | Total | Covered | Percentage |
|----------|-------|---------|------------|
| Requirements | 8 | 6 | 75.0% |
| Scenarios | 3 | 2 | 66.7% |
| **Overall** | **11** | **8** | **72.7%** |
```

**`/speckit.spectest.gaps`** finds what's missing and tells you exactly what to test:

```markdown
## Untested Requirements

| # | Requirement | Severity | Suggested Test |
|---|------------|----------|----------------|
| 1 | Rate limiting on login | 🔴 Critical | Integration: verify 429 after N attempts |
| 2 | Email verification flow | 🟡 Medium | Unit: token generation + expiry |

## Recommended Order
1. 🔴 Rate limiting — security requirement, test first
2. 🟡 Email verification — user-facing flow
```

**`/speckit.spectest.plan`** generates a complete test plan document:

```markdown
## Test Cases: REQ-001 User Signup

| ID | Test Case | Type | Priority | Expected Result |
|----|-----------|------|----------|-----------------|
| TC-001 | Valid signup → account created | Unit | P1 | 201 Created |
| TC-002 | Duplicate email → rejected | Unit | P1 | 409 Conflict |
| TC-003 | Invalid email → validation error | Unit | P2 | 400 Bad Request |
| TC-004 | Full signup → login → access | E2E | P1 | Success |

Acceptance: All P1 tests must pass.
```

## Workflow

```
/speckit.specify                     ← Define acceptance criteria
       │
       ▼
/speckit.plan                        ← Plan the architecture
       │
       ▼
/speckit.tasks                       ← Generate task list
       │
       ▼
/speckit.implement                   ← Build it
       │
       ▼
/speckit.spectest.plan                   ← Generate test plan from spec
/speckit.spectest.generate               ← Generate test scaffolds
       │
       ▼
Fill in test implementations
       │
       ▼
/speckit.spectest.coverage               ← Verify requirement coverage
/speckit.spectest.gaps                   ← Find what's still untested
```

## Commands

### `/speckit.spectest.generate`

Generates framework-native test scaffolds from spec artifacts:

- Every acceptance criterion becomes a test case with Arrange-Act-Assert hints
- Every user scenario becomes a describe/context block
- Tests organized by requirement, not by implementation file
- Supports Jest, Vitest, pytest, Go test, JUnit, and more
- Auto-detects language, framework, and existing test conventions
- Never overwrites existing test files
- Includes traceability comments linking each test to its spec source

### `/speckit.spectest.coverage`

Maps spec requirements to existing tests and calculates coverage:

- Requirement-level coverage (not line/branch coverage)
- Confidence rating: Strong (explicit reference), Medium (keyword match), Weak (file name match)
- Coverage breakdown by test type (unit, integration, E2E)
- Coverage by category (requirements, scenarios, criteria, decisions)
- Threshold comparison with pass/fail status

### `/speckit.spectest.gaps`

Finds untested requirements with actionable suggestions:

- Phase-aware: only flags gaps for completed implementation phases
- Severity classification: Critical (security/data), Medium (user-facing), Low (internal)
- Each gap includes: what to test, which framework, where to put the file
- Prioritized action list with estimated effort
- CI-friendly one-line summary output

### `/speckit.spectest.plan`

Generates a structured test plan document:

- Test scope definition (in-scope vs out-of-scope)
- Test strategy by level (unit, integration, E2E, manual)
- Detailed test cases per requirement with priority and expected results
- Test environment requirements
- Risk assessment with mitigations
- Acceptance thresholds per requirement

## Hooks

The extension registers one optional hook:

- **after_implement**: Offers to scan for untested requirements after implementation is complete

## Design Decisions

- **Scaffold, don't implement** — generates test structure with TODO placeholders, never writes assertion logic that could be wrong
- **Requirement-centric** — organizes tests by spec requirement, not by code file, ensuring traceability
- **Framework-native** — detects and uses the project's actual test framework, not a generic format
- **Three coverage levels** — tracks unit, integration, and E2E coverage separately per requirement
- **Phase-aware gaps** — only flags untested requirements for phases marked complete in tasks.md
- **Non-destructive** — never overwrites existing test files, only creates new ones or reports gaps
- **Confidence-rated** — rates every test-to-requirement mapping as Strong, Medium, or Weak to prevent false coverage

## Requirements

- Spec Kit >= 0.4.0
- Git >= 2.0.0

## License

MIT
