import Dependencies.*

// sbt-git's default JGit reader throws NoWorkTreeException in linked git worktrees
// (where `.git` is a file, not a directory), which breaks project loading there.
// Shell out to the git CLI for read-only git ops so GitVersioning loads from
// worktrees too. (sbt-git helper; sets ThisBuild / useConsoleForROGit := true.)
useReadableConsoleGit

val scalaV = "2.13.18"

lazy val root = (project in file("."))
  .enablePlugins(GitVersioning, GatlingPlugin)
  .settings(
    name                                   := "gatling-kafka-plugin",
    scalaVersion                           := scalaV,
    libraryDependencies ++= gatling,
    libraryDependencies ++= gatlingTest,
    libraryDependencies ++= kafka,
    libraryDependencies ++= Seq(avro4s, avroCore, avroSerdes, avroSerializers),
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.20" % Test,
    libraryDependencies ++= testcontainers,
    dependencyOverrides ++= kafkaOverrides,
    resolvers ++= Seq(
      "Confluent" at "https://packages.confluent.io/maven/",
    ),
    // Do not publish artifacts for Gatling-configured scopes (this is a library)
    Gatling / publishArtifact              := false,
    GatlingIt / publishArtifact            := false,
    scalacOptions ++= Seq(
      "-encoding",
      "UTF-8",            // Option and arguments on same line
      "-Xfatal-warnings", // New lines for each options
      "-deprecation",
      "-feature",
      "-unchecked",
      // Residue guard. Every audit of this repository has re-derived the same finding — unused imports
      // and a private member nobody reads — so the compiler asserts it instead. Fatal by virtue of
      // -Xfatal-warnings above, and it must stay satisfiable with no @nowarn anywhere: a suppression is
      // evidence the wrong warning class was chosen, not a fix.
      //
      // `params` and `implicits` are deliberately absent. This codebase is dense with their classic
      // false-positive sources — `override`s implementing Gatling's and Kafka's interfaces cannot drop a
      // parameter, and KafkaCheckSupport is a wall of implicit conversions — so including them would buy
      // coverage with annotations and turn a self-enforcing guard into a suppression habit.
      "-Wunused:imports,privates,locals,patvars",
      "-language:implicitConversions",
      "-language:higherKinds",
      "-language:existentials",
      "-language:postfixOps",
    ),
  )

// ---------------------------------------------------------------------------------------------
// Published-POM contract.
//
// A dependency a consumer inherits MUST be fetchable from Maven Central, because the published POM
// deliberately carries no repository list (publish.sbt sets `pomIncludeRepository := { _ => false }`,
// which is correct Sonatype hygiene). A coordinate a consumer can neither fetch nor be told where to
// fetch from is unusable, and that is exactly what shipped: four inherited dependencies resolvable
// only from packages.confluent.io.
//
// Nothing asserted this, so routine dependency updates kept the broken coordinates current and green
// across several releases — they advanced 7.9.5-ce -> 7.9.9-ce while the defect sat untouched. Hence a
// standing gate rather than a one-time correction to a version string. Assertions are written over the
// coordinate *pattern*, never over specific versions, so the next bump cannot silence them.
//
// Contracts C1-C3 and C5 of specs/005-classpath-dependency-shedding/contracts/published-pom.md.
// The deny-list is deliberately offline: a network probe would report a network outage as a pass.
// ---------------------------------------------------------------------------------------------

// group:artifact -> why a consumer inherits it. An inherited dependency missing from this map fails
// C3. Every entry names a code path that uses it: the `deprecated:` escape hatch, and the rule that
// bounded it to one entry, went with the Kafka Streams surface it was written for in 2.0.0. A
// dependency held by nothing but a deprecation is now simply unjustified.
val inheritedDependencyJustification: Map[String, String] = Map(
  "org.scala-lang:scala-library"   -> "used-by: every Scala source",
  "org.apache.kafka:kafka-clients" -> "used-by: client/KafkaSender, client/DynamicKafkaConsumer, protocol settings",
  "org.apache.avro:avro"           -> "used-by: checks/AvroBodyCheckBuilder, avro4s serde derivation",
)

/** Why this coordinate cannot be fetched from Maven Central, or None if it can. */
def vendorOnlyReason(group: String, version: String): Option[String] =
  if (group == "io.confluent")
    Some("io.confluent artifacts are published only to packages.confluent.io")
  else if (group == "org.apache.kafka" && (version.endsWith("-ce") || version.endsWith("-ccs")))
    Some(s"$version is a Confluent rebuild, published only to packages.confluent.io")
  else None

lazy val checkPublishedPom =
  taskKey[Unit]("Assert the published POM declares only Maven Central-resolvable dependencies in scopes consumers inherit")

checkPublishedPom := {
  val log    = streams.value.log
  val pom    = (root / makePom).value
  val xml    = scala.xml.XML.loadFile(pom)
  val report = (root / update).value

  // sbt-scoverage adds its instrumentation artifacts as compile-scope dependencies while `coverage` is
  // on, so the POM generated during a coverage run declares four org.scoverage entries that a released
  // POM never carries. CI runs `sbt coverage ... test`, so without this the gate fails on artifacts
  // that are not part of any publish. Every other contract still runs; only these are filtered.
  val instrumented = (root / coverageEnabled).value

  // Cross-published artifacts carry a Scala binary suffix in the POM (kafka-streams-scala_2.13) that
  // the justification map should not have to repeat, or it would need editing on every Scala bump.
  val scalaSuffix = "_" + scalaBinaryVersion.value

  final case class Dep(group: String, artifact: String, version: String, scope: String) {
    def ga: String         = s"$group:${artifact.stripSuffix(scalaSuffix)}"
    def gav: String        = s"$group:$artifact:$version"
    def inherited: Boolean = scope == "compile" || scope == "runtime"
  }

  val deps      = (xml \ "dependencies" \ "dependency").map { d =>
    Dep(
      (d \ "groupId").text.trim,
      (d \ "artifactId").text.trim,
      (d \ "version").text.trim,
      Option((d \ "scope").text.trim).filter(_.nonEmpty).getOrElse("compile"),
    )
  }
    .filterNot(d => instrumented && d.group == "org.scoverage")
  val inherited = deps.filter(_.inherited)
  val failures  = List.newBuilder[String]

  // C1 - inherited scopes carry only Maven Central coordinates.
  inherited.foreach { d =>
    vendorOnlyReason(d.group, d.version).foreach { why =>
      failures += s"C1 ${d.gav} is inherited (scope=${d.scope}) but $why"
    }
  }

  // C2 - optional capabilities are not inherited.
  inherited.foreach { d =>
    if (d.group == "io.confluent" || d.group == "com.sksamuel.avro4s")
      failures += s"C2 ${d.gav} serves an optional capability and must not be inherited"
  }

  // C3 - every inherited dependency is justified by a code path that uses it.
  inherited.foreach { d =>
    if (!inheritedDependencyJustification.contains(d.ga))
      failures += s"C3 ${d.gav} is inherited with no recorded justification in inheritedDependencyJustification"
  }

  // C5 - publication identity is unchanged.
  def text(tag: String): String = (xml \ tag).text.trim
  val expectedArtifactId        = s"gatling-kafka-plugin$scalaSuffix"
  if (text("groupId") != "org.galaxio") failures += s"C5 groupId is '${text("groupId")}', expected 'org.galaxio'"
  // Guards a rename of `name` or a flip of `crossPaths`, either of which would publish under a new
  // coordinate — silently, and irrecoverably on Sonatype.
  if (text("artifactId") != expectedArtifactId)
    failures += s"C5 artifactId is '${text("artifactId")}', expected '$expectedArtifactId'"
  if (text("packaging") != "jar") failures += s"C5 packaging is '${text("packaging")}', expected 'jar'"
  if (text("url") != "https://github.com/galax-io/gatling-kafka-plugin") failures += s"C5 url is '${text("url")}'"
  if ((xml \ "licenses" \ "license").isEmpty) failures += "C5 no license declared"
  if ((xml \ "developers" \ "developer").isEmpty) failures += "C5 no developer declared"
  if ((xml \ "scm").isEmpty) failures += "C5 no scm block declared"
  if ((xml \ "organization").isEmpty) failures += "C5 no organization declared"
  Seq("gatling-core", "gatling-core-java").foreach { a =>
    deps.find(d => d.group == "io.gatling" && d.artifact == a) match {
      case Some(d) if d.scope != "provided" => failures += s"C5 io.gatling:$a is scope=${d.scope}, expected provided"
      case None                             => failures += s"C5 io.gatling:$a is missing from the POM"
      case _                                => ()
    }
  }

  // C1 (transitive) - the POM lists only what this build DECLARES, but a consumer inherits the whole
  // closure. Checking declarations alone would miss a vendor-only coordinate arriving through a Central
  // -published dependency, which is the same class of defect one level down. The Runtime configuration
  // is the right graph to ask: it excludes Provided, so it is exactly what a consumer ends up with.
  val inheritedClosure = report
    .configuration(ConfigRef("runtime"))
    .toSeq
    .flatMap(_.modules)
    .filterNot(_.evicted)
    .map(_.module)
    .filterNot(m => instrumented && m.organization == "org.scoverage")
  inheritedClosure.foreach { m =>
    vendorOnlyReason(m.organization, m.revision).foreach { why =>
      failures += s"C1 ${m.organization}:${m.name}:${m.revision} is inherited transitively but $why"
    }
  }

  val problems = failures.result()
  if (problems.nonEmpty) {
    problems.foreach(p => log.error(p))
    sys.error(
      s"${problems.size} published-POM contract violation(s) in ${pom.getName}. " +
        "See specs/005-classpath-dependency-shedding/contracts/published-pom.md",
    )
  }
  log.info(
    s"checkPublishedPom: ${inherited.size} declared and ${inheritedClosure.size} transitively inherited dependencies, " +
      "all Central-resolvable and justified",
  )
}

// Run on every `sbt test`, not only in CI: this defect was introduced by a dependency bump, and a
// dependency bump is exactly the change whose author will not think to run a bespoke check.
Test / test := (Test / test).dependsOn(checkPublishedPom).value

// And gate the tasks that actually produce the artifact. Relying on `test` running first is a property
// of one workflow file, not of the build: `publishSigned` from a laptop, or a workflow edit that
// parallelises the test and release steps, would otherwise publish an unchecked POM — and a Sonatype
// release cannot be withdrawn.
// `publishSigned` is what `ci-release` actually calls, so gating it is the point of this block; it
// comes from sbt-pgp rather than sbt core, hence the qualified key.
publish                                   := publish.dependsOn(checkPublishedPom).value
publishLocal                              := publishLocal.dependsOn(checkPublishedPom).value
publishM2                                 := publishM2.dependsOn(checkPublishedPom).value
com.jsuereth.sbtpgp.PgpKeys.publishSigned := com.jsuereth.sbtpgp.PgpKeys.publishSigned.dependsOn(checkPublishedPom).value

// Every spec under `integration/` starts its own single-node Kafka through Testcontainers, and sbt
// runs suites in parallel by default — so the peak broker count is the suite count, not anything the
// machine agreed to. This feature adds two such specs, and at seven the suite reliably outruns a
// laptop-sized Docker daemon: KafkaIntegrationSpec, TrackerAcquisitionIsolationSpec and
// TrackerLifetimeSpec all died on `Timed out waiting for log output ... RECOVERY to RUNNING`, and all
// three passed on a re-run with nothing else contending (KafkaIntegrationSpec: 32 s alone, timed out
// at ~100 s under contention). A red suite that has nothing to do with the code under test is worse
// than a slower one — here it cost about a minute of wall clock.
//
// Set with the first spec this feature adds rather than the last, so no commit in the series is left
// running an unbounded suite. The deep fix is one shared broker across the integration specs instead
// of one per suite; that is a refactor of seven files and does not belong in a correctness feature.
// This bounds the damage in one line and keeps the unit specs — which vastly outnumber them —
// running in parallel.
Global / concurrentRestrictions += Tags.limit(Tags.Test, 2)

Gatling / javaOptions := overrideDefaultJavaOptions(
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
)

// ---------------------------------------------------------------------------------------------
// Running the example simulations.
//
// The examples are not in this build at all: they live in examples/{scala,java,kotlin}, one consumer
// project per language, each depending on the published artifact. `sbt Gatling/test` here runs the
// three test harnesses.
//
// The Java and Kotlin examples are NOT here: `Gatling/testOnly` cannot run them. io.gatling.javaapi
// .core.Simulation does not extend io.gatling.core.scenario.Simulation, and gatling-test-framework
// declares exactly one sbt fingerprint, matching only the Scala superclass, so a Java simulation
// named there selects nothing and the build goes green having run nothing. This is a product
// boundary, not a repo defect: Gatling's sbt plugin supports Scala only and directs Java and Kotlin
// users to Maven or Gradle. Each language has its own consumer project under examples/, run the way
// its users run it.
//
// Sequentially, not in parallel: the simulations share one broker, and two of them running at once
// is exactly the cross-attribution hazard the request-reply matcher exists to prevent.
Gatling / parallelExecution := false

// One forked JVM per simulation. sbt's default puts every test of a configuration in a single group, so
// `Gatling / test` would otherwise run all three harnesses in one JVM — letting KafkaConcurrencyLoadTest
// (30 users held for 100 s) share Gatling's static configuration, Netty allocators and Kafka client
// statics with the two that follow it. A leaked consumer or an exhausted allocator would then surface in
// an unrelated simulation, and be diagnosed there.
//
// Derived from `(Gatling / forkOptions).value`, not a fresh `ForkOptions()`: the default carries the
// working directory and the other fork settings, and building one from scratch would silently drop them
// — including anything a later `Gatling / envVars` or scoped `javaHome` adds.
Gatling / testGrouping := (Gatling / definedTests).value.map { test =>
  Tests.Group(
    name = test.name,
    tests = Seq(test),
    runPolicy = Tests.SubProcess((Gatling / forkOptions).value.withRunJVMOptions((Gatling / javaOptions).value.toVector)),
  )
}
