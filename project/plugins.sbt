resolvers ++= Seq(
  Resolver.bintrayIvyRepo("rallyhealth", "sbt-plugins"),
  // need for load sbt-schema-registry-plugin dependencies
  "Confluent" at "https://packages.confluent.io/maven/",
)
libraryDependencies += "org.apache.avro" % "avro-compiler" % "1.11.4"

addSbtPlugin("com.github.sbt" % "sbt-ci-release"             % "1.5.12")
addSbtPlugin("io.gatling"     % "gatling-sbt"                % "4.7.0")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"               % "2.5.2")
addSbtPlugin("com.github.sbt" % "sbt-avro"                   % "3.4.3")
addSbtPlugin("org.galaxio"    % "sbt-schema-registry-plugin" % "0.4.0")
addSbtPlugin("org.scoverage"  % "sbt-scoverage"              % "2.2.1")
