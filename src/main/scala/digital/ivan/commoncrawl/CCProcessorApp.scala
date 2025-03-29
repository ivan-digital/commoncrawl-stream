package digital.ivan.commoncrawl

import digital.ivan.commoncrawl.config.{AppConfig, SparkSessionManager}
import digital.ivan.commoncrawl.io.{FileDownloadManager, FileReader, WetMetadataFetcher}
import digital.ivan.commoncrawl.pipeline.LanguageUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object CCProcessorApp {
  def main(args: Array[String]): Unit = {
    implicit val ec: ExecutionContext = ExecutionContext.global

    // 1) Download listing
    val crawlId      = "CC-MAIN-2024-51"
    val wetPathsFile = "output/wet.paths"
    WetMetadataFetcher.fetchWetPaths(crawlId, "output")

    // 2) Kick off parallel downloads
    val processedFile  = "output/processed_chunks.txt"
    val downloadFuture = FileDownloadManager.downloadAllChunksAsync(
      warcPathsFile       = wetPathsFile,
      stagingDir          = AppConfig.localStagingDir,
      processedChunksFile = processedFile,
      parallelism         = 5
    )

    // 3) Create SparkSession
    val spark = SparkSessionManager.spark
    import spark.implicits._

    try {
      // 4) Streaming read of local WET files
      val warcRawDf = FileReader.readFiles(
          spark,
          AppConfig.localStagingDir,
          AppConfig.maxFilesPerTrigger
        )
        .withColumn("input_filename", input_file_name())
        .withColumn("processed_at", current_timestamp())

      // 5) Language detection
      val processedDf = warcRawDf
        .withColumn("language", LanguageUtils.detectLanguageUdf($"value"))

      // ========== STREAM 1: Language Aggregator -> Console ==========
      val langAggDf = processedDf.groupBy("language").count()

      val aggregatorConsoleQuery = langAggDf.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .trigger(Trigger.ProcessingTime("1 hour")) // every hour
        .option("checkpointLocation", AppConfig.localCheckpointPath + "/lang_agg_console")
        .start()

      // ========== STREAM 2: Cleanup Files (foreachBatch) ==========
      val cleanupQuery = processedDf.writeStream
        .outputMode("append")
        .foreachBatch { (batchDf: Dataset[Row], batchId: Long) =>
          val filesInBatch = batchDf
            .select("input_filename")
            .distinct()
            .as[String]
            .collect()

          filesInBatch.foreach { path =>
            val file = new java.io.File(path)
            if (file.exists()) {
              println(s"Deleting file: $path")
              file.delete()
            }
          }
        }
        .trigger(Trigger.ProcessingTime("1 hour")) // every hour
        .option("checkpointLocation", AppConfig.localCheckpointPath + "/cleanup_checkpoint")
        .start()

      // 6) Wait for downloads
      Await.result(downloadFuture, Duration.Inf)
      println("All WET files have been downloaded.")

      // 7) Wait for streaming queries
      aggregatorConsoleQuery.awaitTermination()
      cleanupQuery.awaitTermination()

    } finally {
      spark.stop()
    }
  }
}