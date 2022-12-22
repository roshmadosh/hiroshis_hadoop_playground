ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "scala_example"
  )

libraryDependencies += "org.apache.spark"%%"spark-sql"%"3.3.1"