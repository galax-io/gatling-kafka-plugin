// Scala example simulations for gatling-kafka-plugin.
//
// A plain consumer project: it depends on the published artifact exactly as your own project does,
// and runs the examples with sbt's own Gatling task. Nothing here is specific to this repository.
//
//   sbt "Gatling / test"                                                  # every example
//   sbt "Gatling / testOnly org.galaxio.examples.scalaapi.BasicSimulation"  # one
//
// Publish the plugin from the repo root first:
//   sbt 'set ThisBuild / version := "0.0.0-EXAMPLES-SNAPSHOT"' publishM2
name         := "gatling-kafka-examples-scala"
scalaVersion := "2.13.18"

// The same guards the plugin build runs. This is a separate sbt build, so the root's scalafmt and
// scalacOptions cannot reach it — and these files are the ones most likely to be copied, so unused
// residue and unformatted code are least affordable here.
scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-Xfatal-warnings",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Wunused:imports,privates,locals,patvars",
)

enablePlugins(GatlingPlugin)

resolvers ++= Seq(
  Resolver.mavenLocal,
  // Schema-Registry-backed Avro only. Confluent publishes these artifacts nowhere else, which is why
  // the plugin declares them `provided` and a consumer adds them. Skip this if you do not use Schema
  // Registry.
  "Confluent" at "https://packages.confluent.io/maven/",
)

libraryDependencies ++= Seq(
  "org.galaxio"           % "gatling-kafka-plugin_2.13"  % "0.0.0-EXAMPLES-SNAPSHOT" % Test,
  "io.gatling.highcharts" % "gatling-charts-highcharts"  % "3.13.5"                  % Test,
  "io.gatling"            % "gatling-test-framework"     % "3.13.5"                  % Test,
  // Avro: avro4s for the derived-serde example, the Confluent serializers for the custom-serde one.
  "com.sksamuel.avro4s"  %% "avro4s-core"                % "4.1.2"                   % Test,
  "io.confluent"          % "kafka-avro-serializer"      % "7.9.9"                   % Test,
  "io.confluent"          % "kafka-streams-avro-serde"   % "7.9.9"                   % Test,
).map(_.exclude("org.apache.kafka", "kafka-clients")) ++ Seq(
  "org.apache.kafka"      % "kafka-clients"              % "3.9.2"                   % Test,
)

// One forked JVM per simulation, and never two at once: they share one broker.
Gatling / parallelExecution := false
Gatling / javaOptions := overrideDefaultJavaOptions(
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
)
Gatling / testGrouping := (Gatling / definedTests).value.map { test =>
  Tests.Group(
    name = test.name,
    tests = Seq(test),
    runPolicy = Tests.SubProcess((Gatling / forkOptions).value.withRunJVMOptions((Gatling / javaOptions).value.toVector)),
  )
}
