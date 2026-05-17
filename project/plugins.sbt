resolvers ++= Seq(
  // need for load sbt-schema-registry-plugin dependencies
  "Confluent" at "https://packages.confluent.io/maven/",
)
libraryDependencies += "org.apache.avro" % "avro-compiler" % "1.12.1"

addSbtPlugin("com.github.sbt" % "sbt-ci-release"             % "1.11.2")
addSbtPlugin("io.gatling"     % "gatling-sbt"                % "4.18.0")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"               % "2.5.6")
addSbtPlugin("com.github.sbt" % "sbt-avro"                   % "4.0.1")
addSbtPlugin("org.galaxio"    % "sbt-schema-registry-plugin" % "0.7.0")
addSbtPlugin("org.scoverage"  % "sbt-scoverage"              % "2.4.4")
addSbtPlugin("com.thesamet"   % "sbt-protoc"                 % "1.0.7")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"
