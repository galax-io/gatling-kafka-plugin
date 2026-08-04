# Specification Quality Checklist: Run-Scoped Reply Channels for Request-Reply

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

## Notes

- Items marked incomplete require spec updates before `/speckit-clarify` or `/speckit-plan`
- Validation pass 1 (2026-08-04): two corrections applied, then all items pass.
  1. The problem statement originally claimed a reply published during re-establishment "can be
     missed entirely". That is only demonstrable for the *first* assignment gap under the default
     read position — a separate concern tracked as point 3 of #193 — and is not supported for a
     later teardown/re-establish cycle, where committed progress may well cover the gap. Replaced
     with the reliability claim #165 itself makes: each teardown reopens an absent-channel window,
     and that window is what #164 and #143 turn into failures.
  2. Transport vocabulary ("record", "subscription") replaced with the spec's own terms in two
     places, so requirements stay readable without knowledge of the message broker's model.
- No [NEEDS CLARIFICATION] markers were needed.
  The one decision that genuinely branches — hold reply channels for the run vs. expire them after
  an idle period, both offered by issue #165 — already has a recorded default in the #193 target
  design ("never unsubscribe during a run"), so it is documented in Assumptions with its rationale
  and its cost rather than raised as a question.
- Terminology: the spec uses "reply channel" and "reply-tracking registration" throughout, matching
  [001's](../../001-nonblocking-tracker-acquisition/spec.md) vocabulary, so that neither transport
  internals nor type names leak into requirements.
- "Key Entities" section is included here (unlike 001): this feature is defined by the lifetime of
  two named things, so naming them is what makes FR-001/FR-002/FR-009 unambiguous.
- Scope boundary vs. sibling milestone issues (#143, #164, #166, #191) and vs. the wider redesign
  (#193) is stated explicitly in Assumptions; none are prerequisites, and none are claimed as fixed.
- FR-008 and the "third-party traffic" edge case exist because holding a channel open for the whole
  run is a genuine behavior change in what the plugin receives, not only in what it tears down.
- Validation pass 2 (2026-08-04, post-`/speckit-analyze`): two success criteria were rewritten
  because they were measurable on paper but not assertable in practice.
  1. **SC-003** demanded median *and p95* within 10% of a baseline. At the sample size a broker
     integration test can afford, p95 is effectively the maximum and a 10% gate would flake on CI
     scheduling noise. Restated as median within 1.5× plus a ceiling on the slowest request — which
     is still unambiguously red pre-change, where the inflation is orders of magnitude rather than
     percent. The looser number is the honest one: it fails for the right reason and passes for the
     right reason.
  2. **SC-002** specified an induced establishment cost of 1 s, but the test design induces ~5 s via
     the broker's initial rebalance delay, and there is no second knob. Restated to the cost the
     verification actually uses, rather than leaving the spec quoting a benchmark nothing runs.
  SC-001's and SC-004's volumes (50 and 100 requests) were left as written and the tasks raised to
  meet them instead.
- Validation pass 3 (2026-08-04, post-code-review): the feature's central decision was reversed.
  Holding reply channels for the whole run was found to revert issue #78, closed and released since
  v0.22.10. Replaced with release-on-idleness, which satisfies both #165 and #78. spec.md, plan.md,
  research.md and contracts/internal-api.md are rewritten accordingly, and plan.md's Complexity
  Tracking now records the two deviations (the SC-003 forward guard under Principle IV, and narrowing
  #143's trigger inside a PR scoped to #165) that it previously claimed did not exist.
