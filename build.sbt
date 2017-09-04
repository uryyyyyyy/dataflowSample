name := "dataflowSample"

version := "0.1"

val commonSettings = Seq(
  version := "1.0",
  organization := "com.github.uryyyyyyy",
  scalaVersion := "2.11.8"
)

lazy val simple = (project in file("simple"))
  .settings(commonSettings: _*)
  .settings(
    name := "simple",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "1.9.1"
    )
  ).enablePlugins(JavaAppPackaging)

lazy val from_gcs = (project in file("from_gcs"))
  .settings(commonSettings: _*)
  .settings(
    name := "from_gcs",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "com.google.cloud" % "google-cloud-storage" % "1.2.1"
    )
  ).enablePlugins(JavaAppPackaging)
