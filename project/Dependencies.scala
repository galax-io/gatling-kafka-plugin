import sbt.*

object Dependencies {
  private object Versions {
    val kafka          = "7.9.5-ce"
    val gatling        = "3.11.5"
    val avro4s         = "4.1.2"
    val avro           = "1.12.1"
    val kafkaAvroSerde = "7.9.5"
    val scalapb        = "0.11.17"
    val scalaTest      = "3.2.19"
    val testcontainers = "1.21.0"
  }

  lazy val gatling: Seq[ModuleID] = Seq(
    "io.gatling" % "gatling-core"      % Versions.gatling % "provided",
    "io.gatling" % "gatling-core-java" % Versions.gatling % "provided",
  )

  lazy val gatlingTest: Seq[ModuleID] = Seq(
    "io.gatling.highcharts" % "gatling-charts-highcharts" % Versions.gatling % "it,test",
    "io.gatling"            % "gatling-test-framework"    % Versions.gatling % "it,test",
  )

  lazy val unitTest: Seq[ModuleID] = Seq(
    "org.scalatest"     %% "scalatest"      % Versions.scalaTest      % Test,
    "org.testcontainers" % "testcontainers" % Versions.testcontainers % Test,
    "org.testcontainers" % "kafka"          % Versions.testcontainers % Test,
  )

  lazy val kafka: Seq[ModuleID] = Seq(
    ("org.apache.kafka"  % "kafka-clients"       % Versions.kafka)
      .exclude("org.slf4j", "slf4j-api"),
    ("org.apache.kafka" %% "kafka-streams-scala" % Versions.kafka % "provided")
      .exclude("org.slf4j", "slf4j-api"),
  )

  lazy val avro4s: ModuleID = "com.sksamuel.avro4s" %% "avro4s-core" % Versions.avro4s % "provided"

  lazy val avroCompiler: ModuleID = "org.apache.avro" % "avro-compiler" % Versions.avro
  lazy val avroCore: ModuleID     = "org.apache.avro" % "avro"          % Versions.avro
  lazy val avroSerdes: ModuleID   =
    ("io.confluent" % "kafka-streams-avro-serde" % Versions.kafkaAvroSerde).exclude("org.apache.kafka", "kafka-streams-scala")
  lazy val avroSerializers: ModuleID = "io.confluent" % "kafka-avro-serializer" % Versions.kafkaAvroSerde

  lazy val scalapbRuntime: ModuleID =
    "com.thesamet.scalapb" %% "scalapb-runtime" % Versions.scalapb % "provided"
  lazy val protobufSerializer: ModuleID =
    ("io.confluent" % "kafka-protobuf-serializer" % Versions.kafkaAvroSerde % "provided")
      .exclude("com.google.protobuf", "protobuf-java")

}
