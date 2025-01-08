import scala.collection.Seq

name := "FlightAnalyzer"

ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "finalProject"
  )

lazy val sparkVersion = "3.2.1"
val circeVersion = "0.14.9"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

idePackagePrefix := Some("com.example")
