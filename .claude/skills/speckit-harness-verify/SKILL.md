---
name: speckit-harness-verify
description: Adversarially verify claims in spec/plan artifacts against primary sources
  and record verdicts
compatibility: Requires spec-kit project structure with .specify/ directory
metadata:
  author: github-spec-kit
  source: harness:commands/speckit.harness.verify.md
---

# Claim Verification

Turn unexamined assertions into verification records. Following Harness-1
(arXiv:2606.02373), verification is part of the externalized harness state:
every checked claim leaves a durable record with a verdict, a method, and an
evidence pointer — so later phases (and later sessions) know what is proven,
what is refuted, and what is merely assumed.

## User Input

```text
$ARGUMENTS
```

Optional. May name target artifacts (e.g. `plan.md`, `spec.md`,
`curated`) and/or specific claims to check. Default targets: the active
feature's `spec.md` and `plan.md`, plus all `critical`-importance entries in
`curated.md` that have no verification record yet.

## Prerequisites

Resolve `HARNESS_DIR` as in `/speckit.harness.init`. Require an initialized
harness (`budget.md` present) — otherwise instruct the user to run
`/speckit.harness.init` and stop. Load the verification budget (Remaining in
`budget.md`) and existing `verification.md` records.

## Steps

### 1. Extract claims

Read the target artifacts and extract **load-bearing factual claims**:
statements about the codebase, dependencies, APIs, data, or environment that,
if wrong, would change the design or break the implementation. Typical
shapes: "X is handled by Y", "library Z supports W", "there is no existing
implementation of V", "endpoint U returns T".

Exclude: requirements (decisions, not facts), pure opinions, and claims that
already have a `verified` record at high confidence in `verification.md`
(re-verify only if the user asks, or the underlying source changed).

Rank claims: `critical` curated links and architectural assumptions first.
Cap the list at the remaining verification budget; say explicitly which
claims were deferred for budget reasons.

### 2. Verify each claim — adversarially

For each claim, in rank order:

1. **Try to refute it, not confirm it.** Ask: what would be true if this claim
   were false? Check that.
2. **Go to the primary source** — open the actual file/API/doc at its current
   state. Never accept the curated summary or the artifact's own citation as
   proof; `evidence.md` tells you where to look, not what is true.
3. Decide the verdict:
   - `verified` — the primary source confirms it now, and your refutation
     attempt failed.
   - `refuted` — the primary source contradicts it (record what is actually true).
   - `unverifiable` — cannot be checked with available access/budget (record
     what would be needed).
   Assign confidence `high | medium | low`.
4. **Record** a row in `verification.md` (claim, method — e.g. "re-read
   src/auth/session.ts:40-80", verdict, confidence, evidence ID, date). Add or
   update the `evidence.md` entry so the record has a pointer. Decrement the
   verification budget and append an action-log row in `budget.md`.
5. **Propagate refutations**: if a curated entry is refuted, change its
   importance row in `curated.md` to `refuted (see V-xxx)` — do not delete it;
   a recorded dead end prevents re-deriving the same error later.

### 3. Report

Output, in order:

1. Verdict summary table: claim → verdict → confidence → evidence/record ID.
2. **Refuted claims**, each with: where it appears (artifact + section), what
   is actually true, and a concrete suggested edit. Do NOT edit spec/plan
   files yourself — present the corrections and let the user (or
   `/speckit.clarify`) apply them.
3. Unverifiable claims and what access/budget would resolve them.
4. Remaining verification budget, and whether any `critical` claims are still
   unverified (these block the stop condition in `budget.md`).

## Guardrails

- Confirmation bias is the failure mode: a verification pass that only re-reads
  what the author cited is not verification. Always attempt refutation.
- One primary-source check per budget unit; no skimming five claims off one read.
- Verdicts must cite evidence pointers that another agent could follow cold.
- Never mark `verified` at `low` confidence — investigate further or record
  `unverifiable`.