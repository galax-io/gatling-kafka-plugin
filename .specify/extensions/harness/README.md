# Research Harness — a Spec Kit extension

**State-externalizing research harness for spec-driven development**: budgeted
exploration, importance-tagged evidence curation, and adversarial claim
verification — all persisted as files, not context.

Based on **Harness-1** — *"Harness-1: Reinforcement Learning for Search Agents
with State-Externalizing Harnesses"* (Jiang et al.,
[arXiv:2606.02373](https://arxiv.org/abs/2606.02373)) and its reference
implementation [pat-jj/harness-1](https://github.com/pat-jj/harness-1).
This community extension adapts the paper's *harness* design to
[Spec Kit](https://github.com/github/spec-kit) workflows. It is not affiliated
with the paper authors or GitHub.

## Why

The research that feeds `/speckit.specify` and `/speckit.plan` — exploring a
codebase, evaluating libraries, checking API behavior — is long-horizon work,
and the agent's conversation context is a terrible place to keep its state:
findings silently fall out of the window, searches get repeated, claims are
written into the plan without ever being checked, and a new session starts
from zero.

Harness-1's diagnosis is that this is a separation-of-concerns failure: the
model is forced to do **bookkeeping** (tracking what was found, what it
supports, what was verified, what is duplicate) with the same machinery it
uses for **semantic decisions** (what to search, what to retain, when to
stop). Its fix is a stateful harness that holds the working memory
*environment-side* — a candidate pool, an importance-tagged curated set,
compact evidence links, verification records, compressed deduplicated
observations — and renders the model only a compact, budget-aware slice.

This extension applies the same split to spec-driven development. Your coding
agent is the policy; a set of per-feature markdown state files is the harness.

| Harness-1 (paper) | This extension |
|---|---|
| Environment-side working memory | `specs/<feature>/harness/` state files |
| Candidate pool | `candidates.md` — deduplicated, append-only IDs |
| Importance-tagged curated set | `curated.md` — capped, critical→low tags, eviction policy |
| Compact evidence links | `evidence.md` — pointers + ≤25-word excerpts, never bulk content |
| Verification records | `verification.md` — verdict, method, confidence per claim |
| Compressed, deduplicated observations | `observations.md` — ≤3-line entries, `dup-of` marking |
| Budget-aware context rendering | `/speckit.harness.status` + slice-only loading in every command |
| Policy decides: search / retain / verify / stop | The agent's only jobs inside `/speckit.harness.explore` |
| Recoverable search state | Resume any session from files via `/speckit.harness.status` |

## Installation

```bash
specify extension add harness --from https://github.com/formin/spec-kit-harness/archive/refs/tags/v1.0.0.zip
```

Or for development:

```bash
git clone https://github.com/formin/spec-kit-harness
specify extension add --dev ./spec-kit-harness
```

Requires Spec Kit `>=0.2.0`. Works with any agent Spec Kit supports (Claude
Code, GitHub Copilot, Cursor, Gemini CLI, …) — commands are plain prompt
files; no external tools, MCP servers, or network access required.

## Commands

| Command | What it does |
|---|---|
| `/speckit.harness.init [mission] [key=value…]` | Create the six state files with budgets and stop conditions |
| `/speckit.harness.explore <question>` | Budgeted decide→act→bookkeep loop; every finding externalized as it is learned |
| `/speckit.harness.verify [targets]` | Adversarial verification of spec/plan claims against primary sources |
| `/speckit.harness.status [full \| topic]` | Read-only, budget-aware snapshot + one recommended next action |
| `/speckit.harness.report [scope]` | Synthesize evidence + verdicts into `research.md` with a requirement-coverage table |

## Quickstart

```bash
/speckit.specify Build a session-revocation feature for the admin console
/speckit.harness.init How is session state currently handled, and what are the revocation options?
/speckit.harness.explore                      # spends search/inspection budget, fills the state files
/speckit.harness.verify                       # checks the load-bearing claims, records verdicts
/speckit.harness.report                       # writes research.md with a coverage table
/speckit.plan                                 # planning now starts from verified evidence
```

Interrupted? Open a fresh session and run `/speckit.harness.status` — the
harness files are the memory; nothing depended on the old context window.

## State files

Created under `specs/<feature>/harness/` (falls back to
`.specify/harness/global/` outside a feature):

| File | Role | Invariants |
|---|---|---|
| `budget.md` | Mission, budget ledger, stop conditions, action log | every budgeted action accounted |
| `candidates.md` | Everything discovered | dedup by source+topic; statuses `new/inspected/curated/discarded` |
| `curated.md` | What matters | hard cap; importance tags; refuted entries marked, not deleted |
| `evidence.md` | Where proof lives | pointers + locators; excerpts ≤ 25 words |
| `verification.md` | What was checked | verdict + method + confidence; primary sources only |
| `observations.md` | What happened | append-only; ≤3 lines each; duplicates flagged |

The files are ordinary markdown in your repo: diffable, reviewable in PRs, and
shared by every agent and teammate working on the feature.

## Hooks

Both optional (you are prompted):

- `after_specify` → `speckit.harness.init` — set up the harness when a spec is created.
- `after_plan` → `speckit.harness.verify` — verify the plan's claims before they harden into tasks.

## Configuration

Copy `config-template.yml` to
`.specify/extensions/harness/harness-config.yml` and adjust budgets, the
curated-set cap, slice sizes, state location, and stop conditions. Environment
variables (`SPECKIT_HARNESS_*`) override file values; per-invocation
`key=value` arguments to `init` override both. Defaults: 30 searches,
40 inspections, 20 verifications, curated cap 25, 4000-token render budget.

See [docs/concepts.md](docs/concepts.md) for the full design mapping and the
deliberate differences from the paper.

## License

[MIT](LICENSE) © 2026 formin

Credits: Harness-1 by Pengcheng Jiang et al. ([arXiv:2606.02373](https://arxiv.org/abs/2606.02373),
[pat-jj/harness-1](https://github.com/pat-jj/harness-1)); [Spec Kit](https://github.com/github/spec-kit) by GitHub.
