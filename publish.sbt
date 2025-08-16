ThisBuild / versionScheme        := Some("semver-spec")
ThisBuild / organization         := "org.galaxio"
ThisBuild / organizationName     := "Galaxio Team"
ThisBuild / organizationHomepage := Some(url("https://github.com/galax-io"))
ThisBuild / description          := "Plugin to support kafka performance testing in Gatling."
ThisBuild / homepage             := Some(url("https://github.com/galax-io/gatling-kafka-plugin"))
ThisBuild / scmInfo              := Some(
  ScmInfo(
    url("https://github.com/galax-io/gatling-kafka-plugin"),
    "scm:git:git@github.com:galax-io/gatling-kafka-plugin.git",
  ),
)

ThisBuild / scalaVersion := "2.13.16"

ThisBuild / developers := List(
  Developer(
    id = "jigarkhwar",
    name = "Ioann Akhaltsev",
    email = "jigarkhwar88@gmail.com",
    url = url("https://github.com/jigarkhwar"),
  ),
)

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))
