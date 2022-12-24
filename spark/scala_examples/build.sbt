ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "scala_example"
  )
val sparkVersion = "3.3.1"
val scalatestVersion = "3.2.14"

libraryDependencies ++= Seq(
  // Apache Spark
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // testing
  "com.novocode" % "junit-interface" % "0.11" % "test"
)