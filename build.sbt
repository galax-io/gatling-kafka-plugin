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

Gatling / javaOptions := overrideDefaultJavaOptions(
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
)
