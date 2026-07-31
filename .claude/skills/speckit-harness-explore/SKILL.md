---
name: speckit-harness-explore
description: Run a budget-aware exploration loop that externalizes every finding into
  the harness state files
compatibility: Requires spec-kit project structure with .specify/ directory
metadata:
  author: github-spec-kit
  source: harness:commands/speckit.harness.explore.md
---

# Budget-Aware Exploration Loop

Research the given question with a strict separation of concerns, following
Harness-1 (arXiv:2606.02373):

- **Policy (your reasoning)** makes only semantic decisions: *what to search,
  which candidate to inspect, what to retain at which importance, when to stop.*
- **Harness (the state files + the bookkeeping rules below)** owns everything
  else: deduplication, compression, eviction, budget accounting.

Keep policy and bookkeeping separate. Spend your reasoning on search strategy
and relevance judgments; execute bookkeeping mechanically, exactly as specified.

## User Input

```text
$ARGUMENTS
```

`$ARGUMENTS` is the research question for this session. If empty, use the
mission from `budget.md`. If both are empty, ask the user for a question and
stop.

## Prerequisites

Resolve `HARNESS_DIR` as in `/speckit.harness.init` (feature directory →
`harness/` subdirectory, else the configured fallback). If `budget.md` does
not exist there, run the initialization procedure from
`/speckit.harness.init` first (with `$ARGUMENTS` as the mission), then continue.

## Steps

### 1. Render the working slice (never the full state)

Read configuration (config file → env vars, as in init) for budgets and slice
sizes, then load ONLY:

- `budget.md`: the budget table, stop conditions, and the last 5 action-log rows.
- `curated.md`: top `rendering.curated_slice` entries, ordered
  critical → high → medium → low.
- `candidates.md`: up to `rendering.candidates_slice` rows with status `new`
  or `inspected` (the open frontier).
- `observations.md`: the last `rendering.observations_slice` entries.

This rendered slice — not your memory of earlier turns, and not the full
files — is your working state. If the slice plus your plan would exceed the
`context_tokens` cap, shrink the slices further, dropping `low` importance
entries first.

### 2. Iterate: decide → act → bookkeep

Repeat until a stop condition fires.

**a. Policy decision (semantic).** Based on the rendered slice, choose ONE:

- `SEARCH <query>` — a new search (code search, grep/glob, docs, web — whatever
  fits the project). Prefer queries that discriminate between competing
  hypotheses over queries that confirm what is already curated.
- `INSPECT <candidate-id>` — open one candidate from the pool and read the
  relevant part.
- `CURATE` — promote inspected candidates into the curated set / demote or
  discard stale entries. Costs no budget.
- `STOP <reason>` — stop per the rules in step 3.

**b. Act.** Execute the chosen action with normal tools.

**c. Bookkeeping (mechanical — do all of these every iteration).**

1. **Compress the observation**: append one entry (≤ 3 lines) to
   `observations.md`: action, yield, and `dup-of O-xxx` if it substantially
   repeats an earlier observation. Never paste raw tool output.
2. **Update the candidate pool**: add each newly discovered source as a row in
   `candidates.md` with status `new`. Dedup key is source + topic — if a row
   with the same key exists, do not add another; note the duplicate in the
   observation instead. Mark inspected candidates `inspected`.
3. **Curate**: for findings worth retaining, add a row to `curated.md`
   (finding ≤ 2 sentences, importance tag) and a pointer entry to
   `evidence.md` (source, locator, ≤ 25-word excerpt, what it supports).
   Update the candidate's status to `curated:<E-id>`. If the curated set
   exceeds `curation.max_curated`, evict per `evict_policy` and log the
   eviction as an observation.
4. **Account**: increment Spent / decrement Remaining in `budget.md` for the
   resource used (SEARCH → searches, INSPECT → inspections) and append an
   action-log row, recording whether the action produced new curated evidence.

### 3. Stop rules

Stop the loop when ANY of:

- The needed budget resource hits 0 (say which).
- **Marginal gain**: the last `stop_conditions.marginal_gain_window`
  (default 3) consecutive budgeted actions produced no new curated evidence.
- The question is answered: every part of the mission/question maps to at
  least one curated entry, and no `critical` curated entry contradicts another.
- The user interrupts.

On stop, append a 3-line closing observation: questions answered, questions
still open, and why the loop ended.

### 4. Report

Output, in order:

1. Verdict on the research question — 2–5 sentences, citing curated IDs
   (e.g. "JWT refresh is gateway-only [E003, E007]").
2. The budget table (after this session).
3. New curated entries from this session, grouped by importance.
4. Open questions / unverified `critical` claims, with the suggested follow-up:
   `/speckit.harness.verify` for claims, `/speckit.harness.explore` for gaps,
   `/speckit.harness.report` when research is complete.

## Guardrails

- One action per iteration; no batching searches to dodge the budget ledger.
- All durable knowledge goes to the state files at the moment it is learned —
  assume the conversation context can be lost after any iteration.
- Excerpts in `evidence.md` are capped at 25 words; state files store pointers,
  not content.
- Do not edit `spec.md`/`plan.md`/`tasks.md` from this command; research and
  authoring stay separate (`/speckit.harness.report` bridges them).
- If you notice missing or corrupt state files, stop and tell the user to run
  `/speckit.harness.init` — do not silently recreate state mid-loop.