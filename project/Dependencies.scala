import sbt.*

object Dependencies {
  private object Versions {
    // Apache release, not Confluent's `-ce`/`-ccs` rebuild: those are published only to
    // packages.confluent.io, and the published POM cannot carry a resolver. Confluent Platform 7.9.x
    // is built from Apache Kafka 3.9.x, so this is the equivalent line rather than an upgrade, and it
    // matches the confluentinc/cp-kafka:7.9.5 brokers the test suites run against. The plugin uses
    // only core client API (clients.consumer, clients.producer, common.header, common.serialization,
    // TopicPartition, WakeupException), so nothing here depends on the vendor build.
    val kafka          = "3.9.2"
    val gatling        = "3.13.5"
    val avro4s         = "4.1.2"
    val avro           = "1.12.1"
    val kafkaAvroSerde = "7.9.9"
    val testcontainers = "0.44.1"
  }

  lazy val gatling: Seq[ModuleID] = Seq(
    "io.gatling" % "gatling-core"      % Versions.gatling % "provided",
    "io.gatling" % "gatling-core-java" % Versions.gatling % "provided",
  )

  lazy val gatlingTest: Seq[ModuleID] = Seq(
    "io.gatling.highcharts" % "gatling-charts-highcharts" % Versions.gatling % "it,test",
    "io.gatling"            % "gatling-test-framework"    % Versions.gatling % "it,test",
    "org.scalameta"        %% "munit"                     % "1.3.5"          % Test,
  )

  lazy val kafka: Seq[ModuleID] = Seq(
    ("org.apache.kafka"  % "kafka-clients"       % Versions.kafka)
      .exclude("org.slf4j", "slf4j-api"),
    ("org.apache.kafka" %% "kafka-streams-scala" % Versions.kafka)
      .exclude("org.slf4j", "slf4j-api"),
  )

  // The Confluent Avro artifacts drag in kafka-schema-registry-client, which depends on the
  // vendor-rebuilt kafka-clients. Left alone, sbt picks the higher version and evicts the Apache one:
  // the build would compile and test against 7.9.x-ccs while publishing a POM that declares 3.9.2, so
  // a green suite would say nothing about the artifact consumers actually run. Pin what we ship.
  lazy val kafkaOverrides: Seq[ModuleID] = Seq(
    "org.apache.kafka"  % "kafka-clients"       % Versions.kafka,
    "org.apache.kafka"  % "kafka-streams"       % Versions.kafka,
    "org.apache.kafka" %% "kafka-streams-scala" % Versions.kafka,
  )

  lazy val avro4s: ModuleID = "com.sksamuel.avro4s" %% "avro4s-core" % Versions.avro4s % "provided"

  lazy val avroCore: ModuleID        = "org.apache.avro" % "avro"                     % Versions.avro
  // `provided`, matching avro4s above: neither artifact is published to Maven Central, and the released
  // POM carries no resolver, so inheriting them makes the plugin unresolvable for anyone building
  // against Maven Central alone. Consumers who want Schema-Registry-backed Avro declare them together
  // with the Confluent resolver — see the Avro Support section of README.md.
  // The hand-managed `.exclude("org.apache.kafka", "kafka-streams-scala")` that used to sit here existed
  // only to stop Confluent's rebuild of kafka-streams-scala colliding with the one declared above.
  // `dependencyOverrides` now pins that coordinate for the whole build, so the exclusion had nothing
  // left to prevent — verified with `sbt evicted`.
  lazy val avroSerdes: ModuleID      = "io.confluent"    % "kafka-streams-avro-serde" % Versions.kafkaAvroSerde % "provided"
  lazy val avroSerializers: ModuleID = "io.confluent"    % "kafka-avro-serializer"    % Versions.kafkaAvroSerde % "provided"

  lazy val testcontainers: Seq[ModuleID] = Seq(
    "com.dimafeng" %% "testcontainers-scala-munit" % Versions.testcontainers % Test,
    "com.dimafeng" %% "testcontainers-scala-kafka" % Versions.testcontainers % Test,
  )

}
