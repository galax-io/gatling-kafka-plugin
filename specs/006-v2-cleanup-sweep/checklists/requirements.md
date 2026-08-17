# Specification Quality Checklist: v2.0.0 Cleanup — Validated Removal Sweep

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-08-09
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs)
- [x] Focused on user value and business needs
- [x] Written for non-technical stakeholders
- [x] All mandatory sections completed

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Success criteria are technology-agnostic (no implementation details)
- [x] All acceptance scenarios are defined
- [x] Edge cases are identified
- [x] Scope is clearly bounded
- [x] Dependencies and assumptions identified

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria
- [x] User scenarios cover primary flows
- [x] Feature meets measurable outcomes defined in Success Criteria
- [x] No implementation details leak into specification

## Notes

**On "no implementation details".** This is a removal feature: *what* is removed is the requirement,
so the specification cannot avoid naming code. The separation used here is deliberate — the
*Audit Verdicts* tables name files and symbols and are labelled as evidence, while every functional
requirement is stated behaviourally ("MUST NOT expose any way to build a send action without a
producer topic", not "delete lines 53–101"). A reviewer can therefore judge the requirements without
the tables, and audit the tables without re-reading the code.

**On evidence.** Verdicts A1–A12 and B1 were each confirmed against the current sources during
specification, not carried over from the issue text. Where they disagree with the issues, the
specification records the disagreement rather than silently following either:

- **A10** supersedes the issue's estimates. The real figures — 22 unused imports across 12 files,
  zero unused locals or pattern variables — come from a compiler run with the unused-code warnings
  enabled.
- **B1** is dead published surface that no issue in the milestone names. Found during this audit.
- **C1** contradicts an issue's stated reason for a deletion. The specification keeps the code and
  says why.

**On the response-code scope limit.** An early draft of FR-003 implied reports would lose their
response-code column. Verified and corrected: only the always-empty field on the message is removed;
the reporting slot is populated from a different source on failure paths and is untouched.

**Clarification session 2026-08-09 — three decisions, no open questions remain.**

1. *Release sequencing*: this feature lands independently of the eleven open 1.x milestones. It does
   not wait for the binary-compatibility guard, so FR-027 adds a hand-authored, checked-in record of
   the break surface in its place, and FR-022 gained an explicit reason the race-pinning tests stay
   (their redesign has not happened).
2. *Guard breadth*: the unused-code guard covers imports, private members, locals and pattern
   variables — not parameters or implicits. FR-010 now states the exclusion and its reason, and
   SC-003 requires the guard to pass with no suppressions anywhere.
3. *Kotlin examples*: they stay exactly where they are, with no Kotlin toolchain added and no
   relocation. US5 was reframed from "be honest about what they are" to "be correct"; FR-023 now
   forbids the moves an earlier draft permitted. The accepted tradeoff — nothing automated will catch
   the next drift — is recorded in Assumptions rather than left implicit.

**Consistency re-check after integration.** Scanned for language the earlier draft used that the
answers invalidate: no remaining either/or on Kotlin, and no remaining text assuming a
binary-compatibility guard is in place. Verified during clarification that all four Kotlin examples
already use only entry points this release keeps, so FR-024's third clause is satisfiable without
restructuring any of them.
