# Claude AskUserQuestion Preset

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![Spec Kit](https://img.shields.io/badge/spec--kit-%E2%89%A50.6.0-blue.svg)](https://github.com/github/spec-kit)
[![Claude Code](https://img.shields.io/badge/Claude%20Code-compatible-5A3FFF.svg)](https://claude.com/claude-code)

> A [Spec Kit](https://github.com/github/spec-kit) preset that replaces the
> Markdown-table question rendering in `/speckit.clarify` and `/speckit.checklist`
> with [Claude Code's](https://claude.com/claude-code) native `AskUserQuestion`
> structured picker — with a recommended option and reasoning for every prompt.

---

## Why

Spec Kit's default `clarify` and `checklist` commands surface multiple-choice
questions as Markdown tables. You then have to read the table and type back a
row letter.

On Claude Code, there's a native UI for exactly this: `AskUserQuestion`.
It renders an interactive picker, enforces one-of-N selection, and keeps the
spec conversation flowing without copy-pasting letters.

This preset swaps the table rendering for that picker — and, because we're
already there, bakes in an **always-on recommendation**: before showing options,
Claude picks the most suitable one and explains its reasoning in one or two
sentences, so the user can one-click accept or deliberately override.

### Before

```markdown
| Option | Description                                   |
| ------ | --------------------------------------------- |
| A      | Reject duplicates with an error               |
| B      | Merge duplicates silently                     |
| C      | Prompt the user on conflict                   |
| Short  | Provide a different short answer (≤5 words)   |

Please reply with A, B, C, or Short.
```

### After

A native Claude Code picker with:

- **Recommendation** surfaced first: *"Recommended: Option A — rejecting
  duplicates keeps the data model deterministic and surfaces ingestion bugs
  early."*
- Each option rendered as a selectable row with its description.
- A final **"Short"** escape hatch for free-form answers (≤5 words).
- One click → answered → integrated back into the spec.

---

## What it changes

| Command                | Behavior                                                                                                                |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `/speckit.clarify`     | Asks up to 5 clarification questions via `AskUserQuestion`, each with a pre-computed recommendation and reasoning.      |
| `/speckit.checklist`   | Presents the Option / Candidate / Why-It-Matters selection via `AskUserQuestion` instead of a Markdown table.           |

**Untouched:** every other Spec Kit template and command. This preset is
scoped to the two commands above and has zero effect on `specify`, `plan`,
`tasks`, `implement`, `analyze`, or any template file.

---

## Requirements

- [Spec Kit](https://github.com/github/spec-kit) `>= 0.6.0`
- [Claude Code](https://claude.com/claude-code) — `AskUserQuestion` is
  Claude-specific; other agents will fall back to their default behavior
  because this preset only rewires commands generated for Claude.

---

## Installation

### From a tagged release (recommended)

```bash
specify preset add --from https://github.com/0xrafasec/spec-kit-preset-claude-ask-questions/archive/refs/tags/v1.0.0.zip
```

### From the community catalog

Once merged into [`presets/catalog.community.json`](https://github.com/github/spec-kit/blob/main/presets/catalog.community.json):

```bash
specify preset add claude-ask-questions
```

### From a local clone (for development)

```bash
git clone https://github.com/0xrafasec/spec-kit-preset-claude-ask-questions
specify preset add --dev ./spec-kit-preset-claude-ask-questions
```

### Verify

```bash
specify preset list
specify preset info claude-ask-questions
ls .claude/commands/speckit.clarify.md .claude/commands/speckit.checklist.md
```

### Remove

```bash
specify preset remove claude-ask-questions
```

---

## How it works

Spec Kit resolves command files from installed presets before writing them to
the agent's command directory (`.claude/commands/` for Claude Code). This
preset ships two command overrides:

```
commands/
├── speckit.clarify.md       # replaces speckit.clarify
└── speckit.checklist.md     # replaces speckit.checklist
```

Each override is a drop-in replacement for the upstream command, with the
Markdown-table question block rewritten to use `AskUserQuestion`. The rest of
the command logic — the ambiguity taxonomy, the 5-question cap, the spec
integration rules — is preserved verbatim, so the preset is a **pure
presentation-layer change**.

No Python hooks, no core patches, no extension registration. The preset is
file-for-file compatible with anything else in the Spec Kit preset catalog.

---

## Compatibility

- ✅ **Stacks cleanly** with template-only presets (e.g. `lean`, `toc-navigation`).
- ⚠️ **Conflicts** with any other preset that also overrides
  `speckit.clarify` or `speckit.checklist`. Only the last preset applied wins
  for a given command.
- ❌ **Not useful** for non-Claude agents (Copilot, Gemini, Codex, etc.) —
  those agents don't expose an `AskUserQuestion` tool.

---

## Versioning

This preset follows [Semantic Versioning](https://semver.org/):

- **MAJOR** — breaking change to the command contract (e.g. removing the
  "Short" escape hatch, changing the recommendation format).
- **MINOR** — additive change (new optional behavior, new command override).
- **PATCH** — wording tweaks, typo fixes, doc updates.

See [CHANGELOG.md](./CHANGELOG.md) for release history.

---

## Contributing

Issues and PRs welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md) for the
development loop, testing workflow, and release process.

If you find a bug or have a suggestion, please
[open an issue](https://github.com/0xrafasec/spec-kit-preset-claude-ask-questions/issues).

---

## License

[MIT](./LICENSE) © 0xrafasec

Spec Kit is a project of GitHub, Inc., licensed under MIT. This preset is an
independent community contribution and is not officially affiliated with or
endorsed by GitHub.
