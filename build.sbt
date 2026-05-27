import Dependencies.*
import com.typesafe.tools.mima.core._
//import org.galaxio.performance.avro.RegistrySubject

val scalaV                             = "2.13.16"
val avroSchemas                        = Seq() // for example Seq(RegistrySubject("test-hello-schema", 1))
val binaryCompatibilityBaselineVersion =
  settingKey[Option[String]]("Released artifact used as the MiMa compatibility baseline.")
val binaryCompatibilityExceptions      =
  settingKey[Seq[ProblemFilter]]("Approved MiMa problem filters for intentional binary breaks.")

ThisBuild / binaryCompatibilityBaselineVersion := Some("0.22.2")
ThisBuild / binaryCompatibilityExceptions      := Seq(
  // KafkaProtocol.producerTopic: String -> Option[String] (intentional: topic deprecation, PR #93)
  ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.galaxio.gatling.kafka.protocol.KafkaProtocol.producerTopic"),
  ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.galaxio.gatling.kafka.protocol.KafkaProtocol.copy$default$1"),
  ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.galaxio.gatling.kafka.protocol.KafkaProtocol.apply"),
  ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.galaxio.gatling.kafka.protocol.KafkaProtocol.copy"),
  ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.galaxio.gatling.kafka.protocol.KafkaProtocol.this"),
  // KafkaLogging trait: new internal fields Utf8/BinaryPreviewLength (intentional: binary logging, PR #94)
  ProblemFilters.exclude[ReversedMissingMethodProblem](
    "org.galaxio.gatling.kafka.package#KafkaLogging.org$galaxio$gatling$kafka$KafkaLogging$$Utf8",
  ),
  ProblemFilters.exclude[ReversedMissingMethodProblem](
    "org.galaxio.gatling.kafka.package#KafkaLogging.org$galaxio$gatling$kafka$KafkaLogging$$BinaryPreviewLength",
  ),
  ProblemFilters.exclude[ReversedMissingMethodProblem](
    "org.galaxio.gatling.kafka.package#KafkaLogging.org$galaxio$gatling$kafka$KafkaLogging$_setter_$org$galaxio$gatling$kafka$KafkaLogging$$Utf8_=",
  ),
  ProblemFilters.exclude[ReversedMissingMethodProblem](
    "org.galaxio.gatling.kafka.package#KafkaLogging.org$galaxio$gatling$kafka$KafkaLogging$_setter_$org$galaxio$gatling$kafka$KafkaLogging$$BinaryPreviewLength_=",
  ),
)
ThisBuild / mimaFailOnNoPrevious               := false
ThisBuild / mimaPreviousArtifacts              := (ThisBuild / binaryCompatibilityBaselineVersion).value
  .map(version => organization.value %% moduleName.value % version)
  .toSet
ThisBuild / mimaBinaryIssueFilters ++= (ThisBuild / binaryCompatibilityExceptions).value

lazy val root = (project in file("."))
  .enablePlugins(GitVersioning, GatlingPlugin)
  .settings(
    name                        := "gatling-kafka-plugin",
    scalaVersion                := scalaV,
    libraryDependencies ++= gatling,
    libraryDependencies ++= gatlingTest,
    libraryDependencies ++= kafka,
    libraryDependencies ++= Seq(avro4s, avroCore, avroSerdes, avroSerializers),
    schemaRegistrySubjects ++= avroSchemas,
//    schemaRegistryUrl := "http://test-schema-registry:8081",
    resolvers ++= Seq(
      "Confluent" at "https://packages.confluent.io/maven/",
    ),
    // Do not publish artifacts for Gatling-configured scopes (this is a library)
    Gatling / publishArtifact   := false,
    GatlingIt / publishArtifact := false,
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

addCommandAlias("checkBinaryCompatibility", "mimaReportBinaryIssues")
