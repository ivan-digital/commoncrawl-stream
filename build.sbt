import sbt._
import sbt.Keys._

// Import AssemblyPlugin auto-import members (this brings in MergeStrategy, PathList, and ShadeRule)
import sbtassembly.AssemblyPlugin
import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .enablePlugins(AssemblyPlugin)
  .settings(
    name := "CommonCrawlStream",
    version := "0.1",

    libraryDependencies ++= Seq(
      // Spark libraries (provided by the cluster)
      "org.apache.spark" %% "spark-core"             % "3.5.5",
      "org.apache.spark" %% "spark-sql"              % "3.5.5",
      "org.apache.spark" %% "spark-mllib"            % "3.5.5",
      "org.apache.spark" %% "spark-sql-kafka-0-10"     % "3.5.5",

      // Apache Tika for language detection (compile scope so it is packaged)
      "org.apache.tika"  % "tika-core"                % "3.1.0",
      "org.apache.tika"  % "tika-langdetect"          % "3.1.0",
      "org.apache.tika"  % "tika-langdetect-optimaize" % "3.1.0",

      // Spark NLP
      "com.johnsnowlabs.nlp" %% "spark-nlp"          % "5.5.3"
    ),

    // Merge strategy: handle duplicate files and service files.
    assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") =>
        MergeStrategy.discard

      // Discard signature files
      case PathList("META-INF", xs @ _*) if xs.exists { x =>
        val lower = x.toLowerCase; lower.endsWith(".rsa") || lower.endsWith(".sf") || lower.endsWith(".dsa")
      } =>
        MergeStrategy.discard

      // Discard Java 9+ module descriptors
      case x if x.endsWith("module-info.class") =>
        MergeStrategy.discard

      // Concatenate service files so that Tika can load its detectors
      case PathList("META-INF", "services", _ @ _*) =>
        MergeStrategy.concat

      // Concatenate reference.conf files if present
      case "reference.conf" =>
        MergeStrategy.concat

      // For duplicate annotation classes, pick the last one
      case x if x.contains("org/intellij/lang/annotations") ||
        x.contains("org/jetbrains/annotations") =>
        MergeStrategy.last

      // For NativeLibrary collisions, pick the first
      case x if x.endsWith("org/tensorflow/NativeLibrary.class") =>
        MergeStrategy.first

      case _ =>
        MergeStrategy.first
    }
  )
