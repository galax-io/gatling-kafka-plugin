// Kotlin example simulations for gatling-kafka-plugin.
//
// A plain consumer project: it depends on the published artifact exactly as your own project does,
// and runs the examples with the Gradle plugin's own task. Nothing here is specific to this
// repository.
//
//   ./gradlew gatlingRun                      # every example
//   ./gradlew gatlingRun --simulation org.galaxio.examples.kotlinapi.BasicSimulation
//
// The Gatling plugin version's first three digits ARE the Gatling version it pins, so 3.13.5.4 is the
// release that matches this plugin's Gatling. It supports Gradle up to 8.12, which is why the wrapper
// pins 8.12 — a pairing Gatling tests. Do not "upgrade" the wrapper alone: on Gradle 9 this plugin
// fails at configuration time, because it calls Project#javaexec and the Convention API, both removed
// in Gradle 9.0.
plugins {
    kotlin("jvm") version "2.0.21"
    id("io.gatling.gradle") version "3.13.5.4"
}

repositories {
    mavenLocal()
    mavenCentral()
    // Schema-Registry-backed Avro only. Confluent publishes these two artifacts nowhere else, which is
    // why the plugin declares them `provided` and a consumer adds them. Skip this repository entirely
    // if you do not use Schema Registry.
    maven("https://packages.confluent.io/maven/")
}

kotlin {
    jvmToolchain(17)
}

dependencies {
    // Published by `sbt 'set ThisBuild / version := "0.0.0-EXAMPLES-SNAPSHOT"' publishM2` from the repo
    // root. Point this at a released version to run the examples against a release instead.
    gatling("org.galaxio:gatling-kafka-plugin_2.13:0.0.0-EXAMPLES-SNAPSHOT")

    gatling("io.confluent:kafka-avro-serializer:7.9.9") {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
    gatling("org.apache.kafka:kafka-clients:3.9.2")
    gatling("org.apache.avro:avro:1.12.1")
}
