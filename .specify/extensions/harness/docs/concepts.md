# Concepts: from Harness-1 to Spec Kit

This document explains how the extension maps the mechanisms of
*"Harness-1: Reinforcement Learning for Search Agents with State-Externalizing
Harnesses"* (Jiang et al., [arXiv:2606.02373](https://arxiv.org/abs/2606.02373);
code at [pat-jj/harness-1](https://github.com/pat-jj/harness-1)) onto
spec-driven development — and where it deliberately differs.

## The core idea

Harness-1 observes that search agents conventionally carry their entire
working state — which documents were found, which matter, which claims were
checked — inside the policy's context. That forces one model to do two jobs:
**semantic decision-making** (what to search, what to retain, when to stop)
and **routine bookkeeping** (dedup, tracking, compression). The paper's
harness moves the second job out of the model entirely: the environment
maintains a candidate pool, an importance-tagged curated set, compact evidence
links, verification records, and compressed deduplicated observations, and
renders the policy a compact, budget-aware view each step. The policy is left
with only the decisions that need intelligence. On eight retrieval benchmarks
this separation yields 0.730 average curated recall, +11.4 points over
comparable open-source agents.

The research phase of spec-driven development has the same structure: a
long-horizon evidence-seeking loop whose state outlives any context window.
So we give it the same harness — with files in the repo playing the role of
environment-side memory.

## Mechanism mapping

### 1. Environment-side working memory → repo files

The paper's harness state lives outside the policy. Here it lives outside the
conversation: `specs/<feature>/harness/*.md`. The choice of plain markdown in
the feature directory is deliberate — it makes the working memory durable
across sessions, diffable in code review, and shared between multiple agents
(or humans) on the same feature, which is the SDD equivalent of the paper's
"recoverable search state".

### 2. Candidate pool → `candidates.md`

Everything discovered enters the pool exactly once (dedup key: source +
topic), with append-only IDs and a status lifecycle
(`new → inspected → curated | discarded`). The pool is the frontier: the
explore loop's INSPECT action draws from it, so search breadth is never lost
just because the agent got distracted.

### 3. Importance-tagged curated set → `curated.md`

Retention is a *decision*, so it stays with the policy: the agent assigns
`critical/high/medium/low` when promoting a candidate. The *mechanics* are
harness rules: a hard cap (default 25), an eviction policy
(lowest-importance-first), findings limited to two sentences, and refuted
entries marked rather than deleted. The cap is what keeps the curated set a
working set instead of a scrapbook.

### 4. Compact evidence links → `evidence.md`

The paper stores links, not documents. We cap excerpts at 25 words and require
a locator (function name, section anchor) that survives small edits. The rule
"pointers, never content" is what keeps harness state cheap to render and
forces verification to return to primary sources instead of trusting cached
prose.

### 5. Verification records → `verification.md`

Each load-bearing claim gets a durable record: verdict
(`verified/refuted/unverifiable`), method, confidence, evidence pointer, date.
Two design points beyond simple logging:

- **Adversarial stance** — `/speckit.harness.verify` instructs the agent to
  attempt refutation against the primary source, not to confirm the citation.
- **Refutations propagate** — a refuted claim demotes its curated entry and
  surfaces as a suggested artifact correction, but the record is kept;
  remembered dead ends are how the harness prevents re-deriving old errors.

### 6. Compressed, deduplicated observations → `observations.md`

Raw tool output never enters the state. Each action is compressed to ≤3 lines
with explicit `dup-of` marking. This is the paper's observation
compression/dedup applied at the file layer — and it is what makes the action
log auditable after the fact.

### 7. Budget-aware context rendering → slices everywhere

The ledger in `budget.md` gives explicit budgets (searches, inspections,
verifications) plus a render cap in tokens. Every command loads *slices*, not
files: top-K curated by importance, the open frontier, the last N
observations. `/speckit.harness.status` is the rendering function exposed as a
command — and doubles as the session-resume entry point.

### 8. Policy decisions → the four verbs

Inside `/speckit.harness.explore`, the agent's discretion is intentionally
narrowed to the paper's decision surface: **SEARCH** (what query), **INSPECT**
(which candidate), **CURATE** (what to retain, at what importance), **STOP**
(is marginal gain exhausted / is the mission answered). Everything else is
mandated bookkeeping. Stop rules combine budget exhaustion, a marginal-gain
window (default: 3 consecutive actions yielding no new curated evidence), and
mission coverage with verified criticals.

### 9. Curated recall → requirement coverage

The paper's headline metric is curated recall: did the evidence that matters
end up in the curated set? `/speckit.harness.report` translates this into SDD
terms: every requirement in `spec.md` is classified
`covered-verified / covered-unverified / contradicted / uncovered`, and the
coverage fraction is printed in `research.md`. Planning then starts from a
quantified evidence base instead of vibes.

## Deliberate differences from the paper

| | Harness-1 | This extension |
|---|---|---|
| Policy | 20B model trained with RL | Your coding agent, frozen; the harness is pure protocol |
| Harness executor | Environment code | The agent itself, following mechanical bookkeeping rules |
| Domain | Web-scale retrieval (BrowseComp+) | Codebase/docs/web research for specs and plans |
| State store | Harness process memory | Markdown files in the feature directory |
| Metrics | Recall/precision vs qrels | Requirement coverage vs `spec.md` |

The second row is the honest caveat: without environment-side code, the
bookkeeping is enforced by instruction rather than by construction. The
commands mitigate this by making the rules mechanical, single-purpose, and
verifiable after the fact (ledger rows, append-only IDs, dup markers) — drift
is visible in the diff, which is the strongest guarantee a prompt-only
extension can offer.

## Relation to core Spec Kit artifacts

The harness never edits `spec.md`, `plan.md`, or `tasks.md`. It feeds them:

```
/speckit.specify ──▶ spec.md
                      │ (hook: after_specify → harness.init)
        harness/ ◀── /speckit.harness.explore / .verify
                      │
                      ▼
                  research.md ◀── /speckit.harness.report
                      │
/speckit.plan ──▶ plan.md   (hook: after_plan → harness.verify)
```

Corrections flow as *suggested edits* through the report/verify outputs, so
authorship of the spec remains with the user and the core workflow.
