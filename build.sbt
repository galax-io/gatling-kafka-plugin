import Dependencies.*
//import org.galaxio.performance.avro.RegistrySubject

// sbt-git's default JGit reader throws NoWorkTreeException in linked git worktrees
// (where `.git` is a file, not a directory), which breaks project loading there.
// Shell out to the git CLI for read-only git ops so GitVersioning loads from
// worktrees too. (sbt-git helper; sets ThisBuild / useConsoleForROGit := true.)
useReadableConsoleGit

val scalaV      = "2.13.18"
val avroSchemas = Seq() // for example Seq(RegistrySubject("test-hello-schema", 1))

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
    schemaRegistrySubjects ++= avroSchemas,
//    schemaRegistryUrl := "http://test-schema-registry:8081",
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
      "-language:implicitConversions",
      "-language:higherKinds",
      "-language:existentials",
      "-language:postfixOps",
    ),
  )

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
