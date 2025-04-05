package digital.ivan.commoncrawl

import digital.ivan.commoncrawl.config.{AppConfig, SparkSessionManager}
import digital.ivan.commoncrawl.processor.{CommonCrawlRawProcessor, LanguageDedupProcessor}

import scala.concurrent.ExecutionContext

object CCApp {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: CCProcessorApp <raw|dedup>")
      System.exit(1)
    }

    val mode = args(0).toLowerCase
    val spark = SparkSessionManager.spark
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

    try {
      mode match {
        case "raw" =>
          val crawlId      = "CC-MAIN-2024-51"
          val outputDir    = "output"
          val stagingDir   = AppConfig.localStagingDir
          val processedFile= "output/processed_chunks.txt"

          println(s"[CCProcessorApp] Starting RAW pipeline for crawlId=$crawlId")

          CommonCrawlRawProcessor.process(
            crawlId      = crawlId,
            outputDir    = outputDir,
            stagingDir   = stagingDir,
            processedFile= processedFile
          )(spark, ec)

        case "dedup" =>
          val inputParquetPath  = "output/languages_parquet"
          val outputDedupedPath = "output/languages_parquet_deduped"

          println("[CCProcessorApp] Starting DEDUP pipeline...")

          LanguageDedupProcessor.process(
            spark              = spark,
            inputParquetPath   = inputParquetPath,
            outputDedupedPath  = outputDedupedPath
          )

        case other =>
          println(s"[CCProcessorApp] Unknown mode: $other. Must be 'raw' or 'dedup'.")
          System.exit(1)
      }

    } finally {
      spark.stop()
    }
  }
}