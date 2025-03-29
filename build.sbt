lazy val root = (project in file("."))
  .settings(
    name := "commoncrawl-stream"
  )

name := "CommonCrawlStream"
version := "0.1"
scalaVersion := "2.12.17"
val sparkVer = "3.5.3"
val tikaVer = "2.9.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVer,
  "org.apache.spark" %% "spark-sql"  % sparkVer,
  "org.apache.spark" %% "spark-mllib" % sparkVer,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVer,
  "org.apache.tika" % "tika-core" % tikaVer,
  "org.apache.tika" % "tika-langdetect-optimaize" % tikaVer,
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.4.1",
)

javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)