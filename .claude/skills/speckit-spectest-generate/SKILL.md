---
name: speckit-spectest-generate
description: Generate test scaffolds from spec acceptance criteria and user scenarios
compatibility: Requires spec-kit project structure with .specify/ directory
metadata:
  author: github-spec-kit
  source: spectest:commands/speckit.spectest.generate.md
---

# Generate Test Scaffolds from Spec

Read spec artifacts (spec.md, plan.md, tasks.md) and generate ready-to-fill test scaffolds that cover every acceptance criterion, user scenario, and success criterion. Tests are organized by requirement and include descriptive names, setup hints, and assertion placeholders.

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty). The user may specify:
- A target test framework (e.g., "jest", "pytest", "vitest", "go test", "junit")
- A specific requirement to generate tests for (e.g., "REQ-003 only")
- Test type preference (e.g., "unit only", "integration only", "e2e only")
- Output directory (e.g., "src/__tests__/", "tests/")
- Language or runtime (e.g., "typescript", "python", "go")

## Prerequisites

1. Confirm you are inside a git repository.
2. Resolve the active feature by running `.specify/scripts/bash/check-prerequisites.sh --json` from the repo root and parsing `FEATURE_DIR` and `AVAILABLE_DOCS`. The spec lives at `$FEATURE_DIR/spec.md` (under `specs/<feature>/`), not under `.specify/`. If missing, stop with error: "Cannot generate tests without spec.md — run `/speckit.specify` first."
3. Read `spec.md` completely to extract acceptance criteria, user scenarios, requirements, and success criteria.
4. If `plan.md` exists, read it to understand architecture, file structure, and technical decisions.
5. If `tasks.md` exists, read it to understand implementation phases and completed work.
6. Detect the project's language and test framework by examining `package.json`, `pyproject.toml`, `go.mod`, `pom.xml`, `Cargo.toml`, or existing test files.

## Outline

1. **Extract Testable Requirements**: Parse spec artifacts to build a complete list of things to test.

   - From `spec.md` → `## Requirements`: Each requirement becomes a test suite
   - From `spec.md` → `## User Scenarios & Testing`: Each scenario becomes a describe/context block
   - From `spec.md` → `## Success Criteria`: Each criterion becomes one or more test cases
   - From `plan.md` → Technical decisions: Each decision becomes a verification test
   - Assign each testable item a stable ID (TEST-001, TEST-002, etc.)

2. **Detect Test Environment**: Identify the project's testing setup.

   - Detect language: TypeScript, JavaScript, Python, Go, Java, Rust, etc.
   - Detect framework: Jest, Vitest, Mocha, pytest, unittest, go test, JUnit, etc.
   - Detect existing test directory structure and naming conventions
   - If no test setup exists, recommend one based on the project's language

3. **Generate Test File Structure**: Create test files organized by requirement.

   ```
   tests/
   ├── unit/
   │   ├── auth/
   │   │   ├── signup.test.ts          ← REQ-001: User signup
   │   │   ├── login.test.ts           ← REQ-002: User login
   │   │   └── token.test.ts           ← REQ-003: JWT generation
   │   └── middleware/
   │       └── rateLimit.test.ts       ← REQ-004: Rate limiting
   ├── integration/
   │   └── auth-flow.test.ts           ← Scenario: Full auth flow
   └── e2e/
       └── user-registration.test.ts   ← Scenario: New user registration
   ```

4. **Generate Test Scaffolds**: For each requirement, produce a test file with:

   **Example (TypeScript/Jest):**

   ```typescript
   /**
    * Tests for REQ-001: User signup with email/password
    * Source: spec.md → Requirements → "Users can sign up with email and password"
    * Implementation: src/auth/signup.ts
    */
   describe('User Signup', () => {
     // Acceptance Criteria: Valid email and password creates account
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

     // Acceptance Criteria: Duplicate email is rejected
     describe('duplicate email handling', () => {
       it('should reject signup with existing email', () => {
         // Arrange: create user with email, attempt second signup
         // Act: call signup with same email
         // Assert: returns error, no duplicate created
         throw new Error('TODO: implement test');
       });
     });

     // Acceptance Criteria: Invalid email format is rejected
     describe('email validation', () => {
       it('should reject invalid email formats', () => {
         // Arrange: prepare invalid emails
         // Act: call signup with each
         // Assert: validation error returned
         throw new Error('TODO: implement test');
       });
     });
   });
   ```

   **Example (Python/pytest):**

   ```python
   """
   Tests for REQ-001: User signup with email/password
   Source: spec.md → Requirements → "Users can sign up with email and password"
   Implementation: src/auth/signup.py
   """
   import pytest

   class TestUserSignup:
       """Acceptance Criteria: Valid email and password creates account"""

       def test_creates_user_with_valid_credentials(self):
           # Arrange: prepare valid signup data
           # Act: call signup function
           # Assert: user is created
           raise NotImplementedError("TODO: implement test")

       def test_hashes_password_before_storing(self):
           # Arrange: prepare signup data
           # Act: call signup, inspect stored password
           # Assert: password is hashed
           raise NotImplementedError("TODO: implement test")

       def test_rejects_duplicate_email(self):
           # Arrange: create existing user
           # Act: signup with same email
           # Assert: raises appropriate error
           raise NotImplementedError("TODO: implement test")
   ```

5. **Generate Traceability Header**: Each test file includes a header comment linking back to the spec.

   ```
   Requirement: REQ-001
   Spec Section: ## Requirements → "Users can sign up with email/password"
   Implementation: src/auth/signup.ts
   Plan Reference: plan.md → "Use bcrypt with cost factor 12"
   ```

6. **Output Summary**: Report what was generated.

   ```markdown
   ## Generated Test Scaffolds

   | Requirement | Test File | Test Cases | Type |
   |-------------|-----------|------------|------|
   | REQ-001: User signup | tests/unit/auth/signup.test.ts | 4 | Unit |
   | REQ-002: User login | tests/unit/auth/login.test.ts | 3 | Unit |
   | REQ-003: JWT tokens | tests/unit/auth/token.test.ts | 5 | Unit |
   | Scenario: Auth flow | tests/integration/auth-flow.test.ts | 3 | Integration |
   | Scenario: Registration | tests/e2e/user-registration.test.ts | 2 | E2E |

   **Total: 17 test cases across 5 files**
   ```

## Rules

- **Scaffold, don't implement** — generate test structure with descriptive names and TODO placeholders, never write actual assertion logic that could be wrong
- **Spec-traceable** — every test must reference its source requirement, scenario, or criterion from spec.md
- **Framework-native** — use the project's detected test framework conventions, not a generic format
- **Organize by requirement** — group tests by the spec requirement they verify, not by implementation file
- **Include all three levels** — generate unit, integration, and E2E scaffolds where the spec provides enough context
- **Follow existing conventions** — match the project's existing test directory structure, naming patterns, and style
- **Never overwrite** — if a test file already exists, report it and skip rather than overwriting existing tests
- **Arrange-Act-Assert** — every test case uses the AAA pattern with comment hints