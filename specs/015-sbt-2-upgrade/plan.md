# Implementation Plan: Build the galax-io sbt projects on sbt 2.0.9

**Spec**: [spec.md](spec.md)

## Summary

Pin sbt 2.0.9 in five builds, prove each move with one reusable parity gate
hosted in gatling-picatinny, and cross-build sbt-schema-registry-plugin with
projectMatrix from an sbt 2 launcher. Each repository gets one pull request;
its commits follow [tasks.md](tasks.md).

## Delivery order

The repositories land in dependency order. Where a task needs a fact that only
an earlier delivery creates, it names it as a reference that is filled in when
the repository's own change is prepared:

- `{{head:galax-io/gatling-picatinny}}` — the commit of gatling-picatinny's
  default branch once its change, which adds the gate, is merged;
- `{{release:galax-io/sbt-schema-registry-plugin}}` — the version of
  sbt-schema-registry-plugin's first release after its change is merged.

| Order | Repository | Waits for |
|---|---|---|
| 1 | gatling-picatinny — the gate, then its own move to 2.0.9 | — |
| 2 | sbt-schema-registry-plugin, gatling-amqp-plugin, gatling-jdbc-plugin | gatling-picatinny merged |
| 3 | release of sbt-schema-registry-plugin (by the maintainer) | sbt-schema-registry-plugin merged |
| 4 | gatling-kafka-plugin — drop sbt-avro, move to 2.0.9, adopt the release | gatling-picatinny merged; the release |

gatling-kafka-plugin's two steps of the approved plan (the move to 2.0.9, and
the later adoption of the new sbt-schema-registry-plugin release) land as one
pull request after that release, because each repository gets one change per
delivery.

## Approach per repository

- **gatling-picatinny.** Add `sbt-upgrade-parity.yml` (workflow_call; also on
  its own pull requests touching the build). Then move to 2.0.9: sbt 2 DSL
  changes in `build.sbt`, `ci.yml` on JDK 17+ with no sbt 2 cache restore,
  `sbt2-compat.yml` deleted and its example overlay moved into `ci.yml`, stale
  1.12.15 comments fixed, MiMa against 1.27.0 kept advisory.
- **sbt-schema-registry-plugin.** Move the sources and scripted tests into
  `plugin/`, rewrite `build.sbt` as a projectMatrix over Scala 3.8.4 and
  2.12.21, pin 2.0.9, keep the sbt 1 axis on the same sbt 1 API, move MiMa to
  1.9.0 per axis, run per-axis CI and scripted legs, and call the gate in
  `sbt-plugin` mode. Fallback: pluginCrossBuild on a 1.13.0 launcher with the
  Scala 3 axis on 2.0.9, reason recorded.
- **gatling-amqp-plugin, gatling-jdbc-plugin.** Pin 2.0.9, apply the sbt 2
  DSL changes, keep Scala 2.13.18, plugins and blocking MiMa, update
  `target/` paths, restore no sbt 2 cache, call the gate in `library` mode.
- **gatling-kafka-plugin.** Remove sbt-avro (not enabled, no Avro sources,
  only sbt 2.0.0-milestone artifacts) on sbt 1.13.0 first; then pin 2.0.9 at
  the root and in `examples/scala`, check that example on sbt 1.13.0 too, keep
  blocking MiMa against 2.1.0, call the gate; then move to the new
  sbt-schema-registry-plugin release.

## Risks

- The projectMatrix restructure could change `_2.12_1.0` or stop the sbt 1
  scripted leg from running on an sbt 2 launcher; the gate blocks the merge
  and the fallback applies.
- sbt 2's behaviour changes (`exportJars`, bare settings reaching every
  subproject, eviction errors in Test, cached and incremental tests) can fail
  or silently skip tests; CI starts clean, and test counts are compared.
- Porting homepage, scm and licenses to URI and `Seq[License]` could change
  POM metadata; the gate diffs the normalized POM.
- JDK 17 becomes the floor for building from source; each README states it.
