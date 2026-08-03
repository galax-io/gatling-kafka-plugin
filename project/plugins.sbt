resolvers ++= Seq(
  // need for load sbt-schema-registry-plugin dependencies
  "Confluent" at "https://packages.confluent.io/maven/",
)
libraryDependencies += "org.apache.avro" % "avro-compiler" % "1.11.4"

addSbtPlugin("com.github.sbt" % "sbt-ci-release"             % "1.11.2")
addSbtPlugin("com.github.sbt" % "sbt-git"                    % "2.1.0")
addSbtPlugin("io.gatling"     % "gatling-sbt"                % "4.18.4")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"               % "2.6.1")
addSbtPlugin("com.github.sbt" % "sbt-avro"                   % "4.0.1")
addSbtPlugin("org.galaxio"    % "sbt-schema-registry-plugin" % "1.0.2")
addSbtPlugin("org.scoverage"  % "sbt-scoverage"              % "2.4.4")
