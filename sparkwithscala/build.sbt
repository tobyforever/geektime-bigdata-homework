ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "sparkwithscala",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.12" % "3.3.2",
      "org.apache.spark" % "spark-sql_2.12" % "3.3.2",
      "org.apache.hadoop" % "hadoop-client" % "3.3.4",
    ),
    idePackagePrefix := Some("com.klqf.bigdata")
  )


