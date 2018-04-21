import sbt._
import Keys._
//import AssemblyKeys._

name := "Santander-Product-Recommendation"

version := "1.0"

scalaVersion := "2.11.8"

lazy val dataclean = (project in file("data-cleaning-app"))

lazy val scalaMlRecApp = (project in file("scala-ml-rec-app"))

lazy val root = (project in file(".")).aggregate(dataclean, scalaMlRecApp)

////val meta = """META.INF(.)*""".r
//
//assemblyMergeStrategy in assembly := {
//    case "log4j.properties" => MergeStrategy.last
//    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
////    case meta(_) => MergeStrategy.discard
//    case x => MergeStrategy.deduplicate
//  }
//
//mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
//{
//  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
//  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
//  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
//  case "about.html" => MergeStrategy.rename
//  case x => old(x)
//}
//}
assemblyMergeStrategy in assembly := {
  case x => MergeStrategy.discard
}
parallelExecution in Test := false
