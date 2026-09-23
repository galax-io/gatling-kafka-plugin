resolvers ++= Seq(
  // need for load sbt-schema-registry-plugin dependencies
  "Confluent" at "https://packages.confluent.io/maven/",
)
libraryDependencies += "org.apache.avro" % "avro-compiler" % "1.12.2"

addSbtPlugin("com.github.sbt" % "sbt-ci-release"             % "1.12.1")
addSbtPlugin("com.github.sbt" % "sbt-git"                    % "2.2.0")
addSbtPlugin("io.gatling"     % "gatling-sbt"                % "4.19.2")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"               % "2.6.2")
addSbtPlugin("com.github.sbt" % "sbt-avro"                   % "4.0.2")
addSbtPlugin("org.galaxio"    % "sbt-schema-registry-plugin" % "1.8.0")
addSbtPlugin("org.scoverage"  % "sbt-scoverage"              % "2.4.4")
addSbtPlugin("com.typesafe"   % "sbt-mima-plugin"            % "1.2.1")
