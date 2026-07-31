---
name: speckit-harness-init
description: Initialize externalized harness state (budget, candidates, curated set,
  evidence, verification, observations) for the active feature
compatibility: Requires spec-kit project structure with .specify/ directory
metadata:
  author: github-spec-kit
  source: harness:commands/speckit.harness.init.md
---

# Initialize Research Harness

Set up the externalized working memory for long-horizon research on the active
feature. After this command, exploration state lives in files — not in the
conversation context — so research survives context compaction, session
restarts, and agent handoffs.

This implements the environment-side working memory of Harness-1
(arXiv:2606.02373): a candidate pool, an importance-tagged curated set,
compact evidence links, verification records, and compressed deduplicated
observations, governed by an explicit budget ledger.

## User Input

```text
$ARGUMENTS
```

If provided, treat `$ARGUMENTS` as the **research mission** (the question this
harness exists to answer) and/or budget overrides in the form `key=value`
(e.g. `searches=50 inspections=60`). Both may appear together; the free text
is the mission, `key=value` tokens are overrides.

## Steps

### 1. Resolve the harness directory

1. Load configuration: read `.specify/extensions/harness/harness-config.yml`
   if it exists; otherwise use the extension defaults (budgets: 30 searches,
   40 inspections, 20 verifications, 4000 context tokens; curated cap 25).
   Apply any `SPECKIT_HARNESS_*` environment variable overrides, then any
   `key=value` overrides from `$ARGUMENTS` (highest precedence).
2. Determine `FEATURE_DIR`:
   - If the current git branch matches a feature directory under `specs/`
     (e.g. branch `003-user-auth` → `specs/003-user-auth/`), use it.
   - Otherwise, use the most recently modified directory under `specs/`.
   - If no feature directory exists, fall back to the configured
     `state.fallback_directory` (default `.specify/harness/global/`).
3. `HARNESS_DIR = FEATURE_DIR/<state.directory>` (default
   `specs/<feature>/harness/`).

### 2. Idempotency check

If `HARNESS_DIR/budget.md` already exists, **do not overwrite anything**.
Report that the harness is already initialized, then behave exactly like
`/speckit.harness.status` (render the current state slice) and stop. If the
user supplied a new mission in `$ARGUMENTS`, append it to the `## Mission`
section of `budget.md` as an additional numbered mission instead.

### 3. Create the state files

Create `HARNESS_DIR` and write each file below exactly once.

`budget.md` — the budget ledger and stop conditions:

```markdown
# Harness Budget Ledger

## Mission
1. <mission from $ARGUMENTS, or "(not set — pass a question to /speckit.harness.init or /speckit.harness.explore)">

## Budget
| Resource | Budget | Spent | Remaining |
|----------|-------:|------:|----------:|
| searches | <budget.searches> | 0 | <budget.searches> |
| inspections | <budget.inspections> | 0 | <budget.inspections> |
| verifications | <budget.verifications> | 0 | <budget.verifications> |

Context render cap: <budget.context_tokens> tokens per iteration.

## Stop conditions
- Budget exhausted in any resource required for the next action.
- Marginal gain: <stop_conditions.marginal_gain_window> consecutive actions
  produced no new curated evidence.
- Mission answered AND every `critical` claim has a `verified` record.

## Action log
| # | Action | Target | Cost | New evidence? |
|---|--------|--------|------|---------------|
```

`candidates.md` — the candidate pool (everything discovered, deduplicated):

```markdown
# Candidate Pool

Dedup key: source + topic. One row per candidate, append-only IDs (C001, C002…).
Status: `new` → `inspected` → `curated:<E-id>` | `discarded(<reason>)`.

| ID | Source | Type | Topic | Status | First seen |
|----|--------|------|-------|--------|------------|
```

`curated.md` — the importance-tagged curated set:

```markdown
# Curated Set

Cap: <curation.max_curated> entries. Importance: critical | high | medium | low.
When full, evict per `<curation.evict_policy>` and log the eviction in
observations.md. Findings are ≤ 2 sentences; details live behind the evidence link.

| ID | Importance | Finding | Source candidate | Evidence |
|----|------------|---------|------------------|----------|
```

`evidence.md` — compact evidence links (pointers, never bulk content):

```markdown
# Evidence Links

Pointers only. An entry records WHERE proof lives, not the proof itself.
Excerpts are capped at 25 words. IDs match curated.md (E001, E002…).

<!-- Entry format:
## E001
- Claim: <one sentence>
- Source: <path:lines | URL | command>
- Locator: <function/section/anchor that survives small edits>
- Excerpt: "<= 25 words"
- Supports: <spec.md FR-x / plan.md section / mission #n>
-->
```

`verification.md` — verification records:

```markdown
# Verification Records

A claim is `verified` only after re-checking the PRIMARY source (not the
curated summary). Verdicts: verified | refuted | unverifiable.

| ID | Claim | Method | Verdict | Confidence | Evidence | Date |
|----|-------|--------|---------|------------|----------|------|
```

`observations.md` — compressed, deduplicated observation log:

```markdown
# Compressed Observations

Append-only. Each entry ≤ 3 lines: what was done, what it yielded, what it
duplicates (if anything). Never paste raw tool output here.

<!-- Format: - [O-001] (action summary) → outcome; dup-of O-xxx if applicable -->
```

Replace every `<...>` placeholder with the resolved configuration value and
today's date where applicable.

### 4. Report

Output a short confirmation:

- The resolved `HARNESS_DIR` and the six files created.
- The active budgets and curated cap.
- The mission (or a note that none is set).
- Next step: `/speckit.harness.explore <question>` to start budgeted research,
  `/speckit.harness.status` at any time — including from a brand-new session —
  to resume from externalized state.

## Guardrails

- Never delete or truncate an existing harness; this command only creates.
- Do not copy spec/plan content into the state files — link to it.
- Keep this command's own context usage minimal: do not read spec.md/plan.md
  here; later commands read what they need.