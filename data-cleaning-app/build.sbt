name := "DataCleaningApp"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"
val scalatestVersion = "3.0.1"
val configVersion = "1.3.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % configVersion,
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % "test"
)

parallelExecution in Test := false      

