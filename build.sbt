// this file was written for spark 2.0.0 and scala 2.11.8

version := "1.0"

val sparkVersion = "3.0.1"

name := "SparkLogAnalysis"

logBuffered in Test := false

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "commons-io" % "commons-io" % "2.9.0"
libraryDependencies += "commons-cli" % "commons-cli" % "1.4"
libraryDependencies += "com.github.losizm" %% "little-cli" % "0.8.0"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false

