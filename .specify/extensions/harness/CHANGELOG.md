# Changelog

All notable changes to the Research Harness extension are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-06-11

### Added

- Initial release, implementing the state-externalizing harness pattern from
  Harness-1 ([arXiv:2606.02373](https://arxiv.org/abs/2606.02373),
  [pat-jj/harness-1](https://github.com/pat-jj/harness-1)) for spec-driven development.
- `/speckit.harness.init` — create per-feature harness state files
  (`budget.md`, `candidates.md`, `curated.md`, `evidence.md`,
  `verification.md`, `observations.md`).
- `/speckit.harness.explore` — budget-aware exploration loop with a strict
  policy/bookkeeping separation, candidate dedup, importance-tagged curation,
  observation compression, and a marginal-gain stop rule.
- `/speckit.harness.verify` — adversarial claim verification against primary
  sources with persistent verification records.
- `/speckit.harness.status` — budget-aware context rendering (compact state
  slices, never full files) plus a recommended next action; safe session resume.
- `/speckit.harness.report` — synthesis of curated evidence and verification
  records into the feature's `research.md`, including a requirement-coverage table.
- Optional hooks: `after_specify` → `speckit.harness.init`,
  `after_plan` → `speckit.harness.verify`.
- `config-template.yml` for budgets, curation caps, and rendering slice sizes.
