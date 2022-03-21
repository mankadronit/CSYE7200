scalaVersion := "2.12.15"

name := "AnalyzeMovieRating"
version := "1.0"

val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.11" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
