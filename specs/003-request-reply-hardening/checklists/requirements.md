# Specification Quality Checklist: Request-Reply Reliability Hardening

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-08-04
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

## Validation Notes

Two review iterations were run against the draft. Findings and resolutions:

**Iteration 1 — implementation leakage.** The draft named source-level constructs (the consumer's
poll call, the tracker actor's mailbox, the discarded cancellation handle, the concurrent map
proposed in #191). Replaced throughout with the domain vocabulary already established in spec `002`:
*reply channel*, *reply-tracking registration*, *shared reply-receiving machinery*, plus two terms
this feature needs — *pending-request record* (#191) and *background timeout watch* (#166). The
mechanism for #191 is deliberately left unstated; the issue does not prescribe one and it is a
planning decision.

**Iteration 2 — testability of the negative requirements.** FR-002 ("unmatched replies are still
discarded") and FR-010 ("bounded by channels held, not created") read as prohibitions with no
observable signal. Each is now paired with a measurable outcome: SC-005/SC-006 bound background
activity and retained state against a 20-channel churn run, and FR-002 is stated explicitly as a
constraint on FR-001's solution rather than as standalone prose. SC-007 was added to make "not by
coincidence" (#196) directly checkable: delete the produce-only scenarios and the request-reply
scenarios must still pass.

**Deliberate non-clarifications.** Three points were resolved by informed guess rather than a
[NEEDS CLARIFICATION] marker, and are recorded in the spec's Assumptions section:

1. *Scope* — the milestone URL was read as "the work still open in it" (#143, #166, #191, #196),
   since its other three issues have already landed under specs `001` and `002`. (The milestone's
   `closed_issues: 7` counts four merged PRs alongside those three issues.)
2. *Fix mechanism for #191 and #143* — both issues explicitly decline to prescribe one. The spec
   states required behaviour and leaves mechanism to `/speckit-plan`.
3. *Sequencing of #196 against #192* — both touch the same CI broker definition and topic list. The
   spec requires only that they be sequenced and that the two topic lists stay identical, not which
   lands first.

Both quantitative baselines cited in SC-001 (0–2 of ~6,760 on current code; 14–17 of ~6,500 before
idle release) are carried from measurements already recorded in the repository, not estimated.
