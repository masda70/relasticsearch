name := "relasticsearch"

// Akka testing fails otherwise
sbt.Keys.fork in Test := true


val logLibraries = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3"
)
val akkaStreamLibrary = "com.typesafe.akka" %% "akka-stream-experimental" % "2.0.1"
val json4sLibrary = "org.json4s" %% "json4s-jackson" % "3.2.10"
val apacheCommonsIOLibrary = "commons-io" % "commons-io" % "2.4"
val scalaTestLibrary = "org.scalatest" %% "scalatest" % "2.2.4" % "test"
val commandLineParsingLibrary = "com.github.scopt" %% "scopt" % "3.3.0"
val sprayLibrary = "io.spray" %% "spray-client" % "1.3.3"
val mapDBLibrary = "org.mapdb" % "mapdb" % "3.0.0-M2"

val commonSettings = Seq(
  organization := "com.masda70",
  version := "0.2.0",
  scalaVersion := "2.11.7",
  scalaVersion in ThisBuild := "2.11.7",
  scalaSource in Compile := baseDirectory.value / "src/main",
  scalaSource in Test := baseDirectory.value / "src/test",
  javaSource in Compile := baseDirectory.value / "src/main",
  javaSource in Test := baseDirectory.value / "src/test",
  libraryDependencies ++= logLibraries ++ Seq(akkaStreamLibrary, json4sLibrary, apacheCommonsIOLibrary, scalaTestLibrary, commandLineParsingLibrary, sprayLibrary, mapDBLibrary)
)
val core = (project in file("core")).settings(commonSettings: _*)
val cliSettings = commonSettings ++ Seq(
  mainClass in Compile := Some("com.masda70.relasticsearch.CommandLine")
)
val es1_7 = (project in file("cli-es1.7")).settings(cliSettings: _*).settings(elasticsearchDependency("1.7.4")).dependsOn(core)
val es2_0 = (project in file("cli-es2.0")).settings(cliSettings: _*).settings(elasticsearchDependency("2.0.2")).dependsOn(core)
val es2_1 = (project in file("cli-es2.1")).settings(cliSettings: _*).settings(elasticsearchDependency("2.1.2")).dependsOn(core)
val es2_2 = (project in file("cli-es2.2")).settings(cliSettings: _*).settings(elasticsearchDependency("2.2.0")).dependsOn(core)

def elasticsearchDependency(version: String) = {
  libraryDependencies += "org.elasticsearch" % "elasticsearch" % version
}



