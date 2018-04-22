name := "ScalaMLRecApp"

version := "1.0"

scalaVersion := "2.11.8"

dependencyOverrides ++= Seq(
  "io.netty" % "netty" % "3.9.9.Final",
  "commons-net" % "commons-net" % "2.2",
  "com.google.guava" % "guava" % "11.0.2"
)

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-mllib_2.11" % "2.2.0" % "provided",
  "com.github.nscala-time" %% "nscala-time" % "2.18.0"
)

parallelExecution in Test := false