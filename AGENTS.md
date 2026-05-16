# AGENTS.md

## Repository Rules

- Before every `git push`, run formatting and make sure formatter checks pass.
- Minimum required command before push:

```bash
sbt --batch scalafmtAll scalafmtCheckAll
```

- If the change touches tests or production Scala code, prefer the fuller pre-push check:

```bash
sbt --batch "Test / compile" scalafmtAll scalafmtCheckAll
```
