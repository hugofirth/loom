name := "loom"

version := "0.1"

organization := "org.gdget"

scalaVersion := "2.11.8"

resolvers ++= Seq(Resolver.sonatypeRepo("releases"))

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "org.typelevel" %% "cats" % "0.7.0",
  "org.gdget" %% "core" % "0.1",
  "org.gdget" %% "partitioned" % "0.1",
  "org.apache.commons" % "commons-math3" % "3.6.1"
)

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.8.0")
addCompilerPlugin("com.milessabin" % "si2712fix-plugin" % "1.2.0" cross CrossVersion.full)

mainClass in (Compile, run) := Some("Sandbox")

