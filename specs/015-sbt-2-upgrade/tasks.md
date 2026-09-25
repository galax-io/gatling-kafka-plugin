# Tasks: Build the galax-io sbt projects on sbt 2.0.9

**Input**: [spec.md](spec.md), [plan.md](plan.md)

Each task names its repository with a `[repo:owner/name]` label and becomes one
commit in that repository's pull request, in this order. A task that uses a
fact from an earlier delivery writes it as `{{head:owner/name}}` or
`{{release:owner/name}}`; the value is filled in when that repository's change
is prepared (plan.md, "Delivery order").

## gatling-picatinny

- [ ] T-001 [repo:galax-io/gatling-picatinny] [US3] Add the reusable parity gate `.github/workflows/sbt-upgrade-parity.yml`
  AC: `.github/workflows/sbt-upgrade-parity.yml` runs on `workflow_call` with the inputs `mode` (`library` or `sbt-plugin`) and `artifact-suffixes`, and on `pull_request` when `project/**`, `build.sbt` or `.github/workflows/**` change.
  AC: The gate fails when the head's `project/build.properties` names a pre-release sbt version (`-M` or `-RC`) or an sbt 2 version below 2.0.7.
  AC: The gate publishes the base commit with that commit's own launcher under version `0.0.0-parity-base` and the head under `0.0.0-parity-head`, into an isolated Ivy and Coursier home, both cross axes in `sbt-plugin` mode.
  AC: The gate fails on any difference in the published artifact set, the normalized POMs (dependencies with scopes and versions, the Scala library, licenses, scm and developers), the jar entry lists or the class-file major versions, and on any MiMa issue of the head against the base.
  AC: The gate uploads both POM sets and the diff, runs on JDK 17, declares `permissions: contents: read`, passes no secrets, and pins every action by full commit SHA.
- [ ] T-002 [repo:galax-io/gatling-picatinny] [US1] Move the build to sbt 2.0.9 (`project/build.properties`, `build.sbt`, `.github/workflows/ci.yml`)
  AC: `project/build.properties` pins exactly `sbt.version=2.0.9`, while Scala 2.13.18 and gatling-sbt 4.19.1 stay as they are.
  AC: `build.sbt` loads on sbt 2: URI-typed keys, `Seq[License]`, bare settings scoped where they must not reach every subproject, and `Def.uncached` for tasks whose result has no JsonFormat.
  AC: `.github/workflows/ci.yml` runs on sbt 2.0.9 with JDK 17 or newer, restores no sbt 2 task cache or `target/` directory, and runs the example overlay that `.github/workflows/sbt2-compat.yml` ran; `.github/workflows/sbt2-compat.yml` is deleted.
  AC: The MiMa check against 1.27.0 stays advisory and runs under the sbt 2 build of sbt-mima-plugin 1.2.1.
  AC: No workflow comment still names sbt 1.12.15, and `specs/012-cross-build-sbt/research.md` names sbt-mima-plugin 1.2.1 with its sbt 2.0.7 floor.
  AC: The repository's own verification passes on sbt 2.0.9.

## sbt-schema-registry-plugin

- [ ] T-003 [repo:galax-io/sbt-schema-registry-plugin] [US2] Cross-build the plugin with projectMatrix from an sbt 2.0.9 launcher (`plugin/`, `build.sbt`, `project/build.properties`)
  AC: The plugin sources and scripted tests live under `plugin/`, moved with `git mv`, and `build.sbt` defines a projectMatrix with `jvmPlatform(scalaVersions = Seq("3.8.4", "2.12.21"))`.
  AC: `project/build.properties` pins exactly `sbt.version=2.0.9`.
  AC: The `_2.12_1.0` artifact keeps Scala 2.12.21 and compiles against the same sbt 1 API as before, so the minimum sbt 1 version of plugin users stays the same.
  AC: The `_sbt2_3` artifact keeps its sbt 2.0.0 minimum when the matrix can express it; otherwise `README.md` states the new minimum.
  AC: When the projectMatrix layout cannot keep `_2.12_1.0` unchanged, the change instead keeps pluginCrossBuild on an sbt 1.13.0 launcher with the Scala 3 axis on sbt 2.0.9, and records why in `project/build.properties`.
  AC: The repository's own verification passes on both axes.
- [ ] T-004 [repo:galax-io/sbt-schema-registry-plugin] [US2] Check both axes with MiMa 1.9.0, scripted legs and the parity gate (`build.sbt`, `.github/workflows/ci.yml`, `README.md`)
  AC: `mimaPreviousArtifacts` moves from 1.8.0 to 1.9.0 on each axis that 1.9.0 was published for, and keeps the newest existing baseline on any other axis.
  AC: `.github/workflows/ci.yml` builds per-axis matrix subprojects instead of `++` legs, and MiMa stays blocking on both axes, with the sbt 2 axis checked by the sbt 2 MiMa plugin.
  AC: Scripted tests run on sbt 2.0.9 and on the declared sbt 2 minimum, and on sbt 1.13.0 and the current sbt 1 minimum; `README.md` documents the sbt 2 skip list for fixtures that add external plugins.
  AC: `.github/workflows/ci.yml` calls `galax-io/gatling-picatinny/.github/workflows/sbt-upgrade-parity.yml@{{head:galax-io/gatling-picatinny}}` in `sbt-plugin` mode, and that job must show `_2.12_1.0` identical to the base build and zero MiMa issues on `_sbt2_3`.
  AC: The repository's own verification passes.

## gatling-amqp-plugin

- [ ] T-005 [repo:galax-io/gatling-amqp-plugin] [US1] Move the build to sbt 2.0.9 and call the parity gate (`project/build.properties`, `build.sbt`, `.github/workflows/ci.yml`)
  AC: `project/build.properties` pins exactly `sbt.version=2.0.9`; plugins, library dependencies and Scala 2.13.18 stay as they are.
  AC: `build.sbt` loads on sbt 2: URI-typed keys, `Seq[License]`, scoped bare settings, and `Def.uncached` where a task result has no JsonFormat.
  AC: `.github/workflows/ci.yml` keeps the Java 17 and 21 matrix on sbt 2.0.9, keeps `mimaReportBinaryIssues` against 1.3.0 blocking under the sbt 2 MiMa plugin, uses the new `target/` layout, and restores no sbt 2 task cache or `target/` directory.
  AC: `.github/workflows/ci.yml` calls `galax-io/gatling-picatinny/.github/workflows/sbt-upgrade-parity.yml@{{head:galax-io/gatling-picatinny}}` in `library` mode.
  AC: The repository's own verification passes on sbt 2.0.9.

## gatling-jdbc-plugin

- [ ] T-006 [repo:galax-io/gatling-jdbc-plugin] [US1] Move the build to sbt 2.0.9 and call the parity gate (`project/build.properties`, `build.sbt`, `.github/workflows/ci.yml`)
  AC: `project/build.properties` pins exactly `sbt.version=2.0.9`; plugins, library dependencies and Scala 2.13.18 stay as they are.
  AC: `build.sbt` loads on sbt 2: URI-typed keys, `Seq[License]`, scoped bare settings, and `Def.uncached` where a task result has no JsonFormat.
  AC: `.github/workflows/ci.yml` on Temurin 17 runs the scalafmt check, `mimaReportBinaryIssues` against 1.5.0 as a blocking step under the sbt 2 MiMa plugin, and the coverage tests including the Gatling H2 `DebugTest` on sbt 2.0.9, with the new `target/` paths and no restored sbt 2 cache.
  AC: `.github/workflows/ci.yml` calls `galax-io/gatling-picatinny/.github/workflows/sbt-upgrade-parity.yml@{{head:galax-io/gatling-picatinny}}` in `library` mode.
  AC: The repository's own verification passes on sbt 2.0.9.

## gatling-kafka-plugin

- [ ] T-007 [repo:galax-io/gatling-kafka-plugin] [US1] Remove the unused sbt-avro plugin (`project/plugins.sbt`)
  AC: `project/plugins.sbt` no longer declares sbt-avro 4.0.2, which is not enabled, has no Avro sources to compile, and is published only for sbt 2.0.0 milestones.
  AC: The avro-compiler meta-build dependency goes too, unless something under `project/` still uses it.
  AC: A comment in `project/build.properties` records that sbt-avro was removed as unused rather than kept as an sbt 2 blocker.
  AC: The repository's own verification passes.
- [ ] T-008 [repo:galax-io/gatling-kafka-plugin] [US1] Move the build and its Scala example to sbt 2.0.9 and call the parity gate (`project/build.properties`, `examples/scala/project/build.properties`, `build.sbt`, `.github/workflows/ci.yml`)
  AC: The root `project/build.properties` and `examples/scala/project/build.properties` pin exactly `sbt.version=2.0.9`, and `.github/workflows/ci.yml` also runs `examples/scala` with `-Dsbt.version=1.13.0` against the `publishM2` output when its build loads on both launchers.
  AC: `build.sbt` loads on sbt 2, while Scala 2.13.18 and every other plugin stay as they are.
  AC: `mimaReportBinaryIssues` against 2.1.0 stays blocking under the sbt 2 MiMa plugin, and the Maven and Gradle examples and the service-container tests run as before.
  AC: `.github/workflows/ci.yml` calls `galax-io/gatling-picatinny/.github/workflows/sbt-upgrade-parity.yml@{{head:galax-io/gatling-picatinny}}` in `library` mode.
  AC: The repository's own verification passes on sbt 2.0.9.
- [ ] T-009 [repo:galax-io/gatling-kafka-plugin] [US2] Move to the first sbt-schema-registry-plugin release built by its sbt 2 launcher (`project/plugins.sbt`)
  AC: `project/plugins.sbt` pins sbt-schema-registry-plugin {{release:galax-io/sbt-schema-registry-plugin}} instead of 1.8.0.
  AC: The build loads that release's `_sbt2_3` artifact on sbt 2.0.9, and the parity gate shows the published POMs and jars unchanged.
  AC: The repository's own verification passes.
