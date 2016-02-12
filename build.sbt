name := "relasticsearch"

version := "0.1.0"

scalaVersion := "2.11.7"

scalaVersion in ThisBuild := "2.11.7"

scalaSource in Compile := baseDirectory.value / "src/main"
scalaSource in Test := baseDirectory.value / "src/test"
javaSource in Compile := baseDirectory.value / "src/main"
javaSource in Test := baseDirectory.value / "src/test"

val logLibraries = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0" ,
  "ch.qos.logback" % "logback-classic" % "1.1.3"
)

mainClass in Compile := Some("com.masda70.relasticsearch.Indexer")

// Akka testing fails otherwise
sbt.Keys.fork in Test := true

val akkaStreamLibrary = "com.typesafe.akka" %% "akka-stream-experimental" % "2.0.1"

val json4sLibrary = "org.json4s" %% "json4s-jackson" % "3.2.10"

val elasticSearchLibrary = "org.elasticsearch" % "elasticsearch" % "2.2.0"

val apacheCommonsIOLibrary = "commons-io" % "commons-io" % "2.4"

val scalaTestLibrary = "org.scalatest" %% "scalatest" % "2.2.4" % "test"

val commandLineParsingLibrary = "com.github.scopt" %% "scopt" % "3.3.0"

libraryDependencies ++= logLibraries ++ Seq(akkaStreamLibrary, json4sLibrary, elasticSearchLibrary, apacheCommonsIOLibrary, scalaTestLibrary, commandLineParsingLibrary)