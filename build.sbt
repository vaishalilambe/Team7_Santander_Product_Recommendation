import sbt._
import Keys._

name := "Santander-Product-Recommendation"

version := "1.0"

scalaVersion := "2.11.8"

lazy val dataclean = (project in file("data-cleaning-app"))

lazy val scalaMlRecApp = (project in file("scala-ml-rec-app"))

lazy val root = (project in file(".")).aggregate(dataclean, scalaMlRecApp)

val meta = """META.INF(.)*""".r

assemblyMergeStrategy in assembly :=
  {
    case "log4j.properties" => MergeStrategy.last
    case meta(_) => MergeStrategy.discard
    case x => MergeStrategy.last
  }


parallelExecution in Test := false
