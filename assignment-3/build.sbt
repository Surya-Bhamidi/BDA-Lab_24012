ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "1.0.0"

name := "diabetes-ml"
Compile / unmanagedSourceDirectories := Seq(baseDirectory.value / "src")
Compile / mainClass := Some("DiabetesPipeline")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.5" % Provided,
  "org.apache.spark" %% "spark-mllib" % "3.5.5" % Provided,
  "com.univocity" % "univocity-parsers" % "2.9.1" % Provided
)
