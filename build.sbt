name := "Santander-Product-Recommendation"

version := "1.0"

scalaVersion := "2.11.8"

lazy val dataclean = (project in file("data-cleaning-app"))

lazy val scalaMlRecApp = (project in file("scala-ml-rec-app"))

lazy val root = (project in file(".")).aggregate(dataclean, scalaMlRecApp)

parallelExecution in Test := false