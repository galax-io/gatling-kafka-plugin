package org.galaxio.gatling.kafka.build

import java.nio.file.{Files, Paths}

/** T-007 of `specs/015-sbt-2-upgrade/`: sbt-avro is enabled nowhere in this build, compiles no Avro sources, and is published
  * only for sbt 2.0.0 milestones, so it — and the avro-compiler meta-build dependency it alone needed — must not sit in
  * `project/plugins.sbt` as a false sbt 2 blocker.
  */
final class PluginsSbtSpec extends munit.FunSuite {

  private val pluginsSbt      = Files.readString(Paths.get("project/plugins.sbt"))
  private val buildProperties = Files.readString(Paths.get("project/build.properties"))

  test("project/plugins.sbt does not declare sbt-avro") {
    assert(!pluginsSbt.contains("sbt-avro"), "sbt-avro is unused and must be removed from project/plugins.sbt")
  }

  test("project/plugins.sbt does not declare the avro-compiler meta-build dependency") {
    assert(
      !pluginsSbt.contains("avro-compiler"),
      "avro-compiler was only needed by sbt-avro; nothing else under project/ uses it",
    )
  }

  test("project/build.properties records why sbt-avro was removed") {
    assert(
      buildProperties.contains("sbt-avro") && buildProperties.toLowerCase.contains("unused"),
      "project/build.properties must comment that sbt-avro was removed as unused, not kept as an sbt 2 blocker",
    )
  }
}
