# Contributing

Thanks for your interest in improving this preset. The goal is to keep it
tiny, focused, and a reliable drop-in replacement for two Spec Kit commands.

## Scope

This preset only overrides:

- `commands/speckit.clarify.md`
- `commands/speckit.checklist.md`

Changes that expand scope (new commands, template overrides, core patches)
are generally out of scope here — open an issue first to discuss whether a
separate preset would be a better home.

## Development loop

1. Clone the repo and make your changes.
2. Install it into a throwaway Spec Kit project in dev mode:

   ```bash
   specify preset add --dev /path/to/spec-kit-preset-claude-ask-questions
   ```

3. Open the project in Claude Code and exercise the affected command(s):
   - `/speckit.clarify` — run on a feature spec with intentional ambiguity,
     confirm the `AskUserQuestion` picker shows up, the recommendation is
     surfaced first, the "Short" escape hatch works, and answers integrate
     back into `spec.md`.
   - `/speckit.checklist` — run against a checklist and confirm the picker
     replaces the Option / Candidate / Why-It-Matters table.
4. Remove the dev install when done:

   ```bash
   specify preset remove claude-ask-questions
   ```

## Guidelines

- **Preserve upstream semantics.** The question flow, 5-question cap,
  taxonomy, and spec integration rules come from upstream Spec Kit. Only
  the rendering layer is this preset's concern.
- **Keep the recommendation contract.** Every multi-choice question should
  surface a single recommended option with one or two sentences of
  reasoning, and every short-answer question should include a suggested
  answer with reasoning.
- **Always keep an escape hatch.** The "Short" / "Custom" free-form option
  must remain as the final entry so users can override without fighting the
  UI.
- **No new dependencies.** The preset must stay as two Markdown files plus
  `preset.yml`.
- **Match Spec Kit conventions.** When upstream changes the command
  structure (new sections, new hooks), sync the override so it stays a
  clean superset of the original rendering behavior.

## Pull requests

- One concern per PR.
- Include a short description of what you tested and how.
- If the change affects user-visible behavior, update `README.md` and
  `CHANGELOG.md` in the same PR.

## Releasing (maintainers)

1. Bump `version` in `preset.yml`.
2. Add a new section to `CHANGELOG.md`.
3. Commit, tag, and push:

   ```bash
   git commit -am "release: vX.Y.Z"
   git tag vX.Y.Z
   git push origin main vX.Y.Z
   gh release create vX.Y.Z --generate-notes
   ```

4. Open a PR to [github/spec-kit](https://github.com/github/spec-kit)
   updating `version` and `download_url` in
   `presets/catalog.community.json`.

## Reporting issues

Please include:

- Spec Kit version (`specify --version`)
- Claude Code version
- The exact command you ran
- What you expected vs. what happened
- Any relevant snippet from the generated `.claude/commands/speckit.*.md`
