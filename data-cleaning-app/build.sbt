name := "DataCleaningApp"

version := "1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.apache.spark" % "spark-core_2.11" % "2.2.0"
)

parallelExecution in Test := false