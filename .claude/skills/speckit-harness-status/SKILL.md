---
name: speckit-harness-status
description: Render a compact, budget-aware slice of the harness state with a recommended
  next action
compatibility: Requires spec-kit project structure with .specify/ directory
metadata:
  author: github-spec-kit
  source: harness:commands/speckit.harness.status.md
---

# Harness Status — Budget-Aware Context Rendering

Render the externalized harness state as a compact working slice. This is the
Harness-1 (arXiv:2606.02373) "budget-aware context rendering" step exposed as
a command: instead of re-reading transcripts or whole files, the agent (and
the user) get a bounded snapshot that is sufficient to decide the next action.

Run it at any time — especially **at the start of a fresh session** to resume
research with zero context carried over, or before deciding whether more
exploration is worth the budget.

This command is **read-only**: it writes nothing and consumes no budget.

## User Input

```text
$ARGUMENTS
```

Optional. `full` renders larger slices (3× the configured sizes); a topic
string filters curated/candidate rows to those matching it.

## Steps

### 1. Locate state

Resolve `HARNESS_DIR` as in `/speckit.harness.init`. If no harness exists,
say so and point to `/speckit.harness.init` — do not create anything.

### 2. Load slices only

Read configuration for slice sizes (`rendering.*`), then load:

- `budget.md` — mission(s), budget table, stop conditions, last 5 action-log rows.
- `curated.md` — top `curated_slice` entries by importance (critical first);
  count the rest.
- `candidates.md` — up to `candidates_slice` rows with status `new`/`inspected`;
  count the rest.
- `verification.md` — all `refuted` rows, plus counts by verdict; list
  `critical` curated entries lacking a `verified` record.
- `observations.md` — last `observations_slice` entries.

Do not read `evidence.md` bodies, spec/plan files, or anything beyond the
slices. Keep the rendered output within the configured `context_tokens` cap;
truncate lowest-importance material first and say what was truncated.

### 3. Render the snapshot

```markdown
# Harness Status — <feature> (<date>)

**Mission**: <mission lines>

**Budget**: searches <r>/<b> · inspections <r>/<b> · verifications <r>/<b> remaining

**Curated set** (<shown>/<total>, cap <cap>):
| ID | Importance | Finding | Verified? |
...

**Open frontier** (<shown>/<total> candidates): ...

**Verification**: <n> verified · <n> refuted · <n> unverifiable
⚠ Unverified critical claims: <list or "none">
⚠ Refuted (do not rely on): <list or "none">

**Recent activity**: <observation slice, one line each>
```

### 4. Recommend exactly one next action

Close with a single recommendation, derived from the snapshot:

- Unverified `critical` claims → `/speckit.harness.verify`.
- Mission gaps with budget remaining → `/speckit.harness.explore <question>`.
- Marginal gain exhausted or budget empty → `/speckit.harness.report`.
- Coverage complete and verified → `/speckit.harness.report`, then proceed to
  `/speckit.plan` or `/speckit.tasks`.
- Empty harness (no curated entries, no actions logged) →
  `/speckit.harness.explore <mission>`.

State the reason in one sentence (e.g. "2 critical claims unverified and 12
verification budget remaining → verify before planning").

## Guardrails

- Read-only. No file writes, no budget changes, not even timestamp updates.
- Slices, never full files; counts stand in for what is not shown.
- The recommendation must follow from the rendered state alone — if the state
  files are missing or malformed, report that instead of guessing.