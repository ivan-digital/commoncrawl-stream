import sbt._
import sbt.Keys._
import sbtassembly.AssemblyPlugin
import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .enablePlugins(AssemblyPlugin)
  .settings(
    name := "CommonCrawlStream",
    version := "0.1",

    libraryDependencies ++= Seq(
      // Spark libraries in compile scope, excluding Log4j 1.x
      "org.apache.spark" %% "spark-core" % "3.5.5"
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("log4j", "log4j")
        exclude("org.apache.logging.log4j", "log4j-1.2-api"),
      "org.apache.spark" %% "spark-sql" % "3.5.5"
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("log4j", "log4j")
        exclude("org.apache.logging.log4j", "log4j-1.2-api"),
      "org.apache.spark" %% "spark-mllib" % "3.5.5"
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("log4j", "log4j")
        exclude("org.apache.logging.log4j", "log4j-1.2-api"),
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.5"
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("log4j", "log4j")
        exclude("org.apache.logging.log4j", "log4j-1.2-api"),

      // Bring in Log4j 2 explicitly
      "org.apache.logging.log4j" % "log4j-api" % "2.24.3",
      "org.apache.logging.log4j" % "log4j-core" % "2.24.3",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.24.3",

      // Tika, Spark NLP, etc.
      "org.apache.tika"  % "tika-core"                 % "3.1.0",
      "org.apache.tika"  % "tika-langdetect"           % "3.1.0",
      "org.apache.tika"  % "tika-langdetect-optimaize" % "3.1.0",
    ),

    assemblyMergeStrategy := {
      case "META-INF/MANIFEST.MF" => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.exists { x =>
        val lower = x.toLowerCase
        lower.endsWith(".rsa") || lower.endsWith(".sf") || lower.endsWith(".dsa")
      } => MergeStrategy.discard
      case x if x.endsWith("module-info.class") =>
        MergeStrategy.discard
      case PathList("META-INF", "services", _ @ _*) =>
        MergeStrategy.concat
      case "reference.conf" =>
        MergeStrategy.concat
      case "log4j.properties" =>
        MergeStrategy.discard
      case _ =>
        MergeStrategy.first
    }
  )