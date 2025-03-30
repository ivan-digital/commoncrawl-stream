package digital.ivan.commoncrawl

import digital.ivan.commoncrawl.config.{AppConfig, SparkSessionManager}
import digital.ivan.commoncrawl.io.{FileDownloadManager, FileReader, WetMetadataFetcher}
import digital.ivan.commoncrawl.utils.FormatUtils.htmlToMarkdownUdf
import digital.ivan.commoncrawl.utils.LanguageUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row}
import org.apache.tika.language.detect.LanguageDetector

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object CCProcessorApp {
  def main(args: Array[String]): Unit = {
    Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
    val detector = LanguageDetector.getDefaultLanguageDetector.loadModels()
    println(s"Detected: ${detector.detect("Hello world!").getLanguage}")
    implicit val ec: ExecutionContext = ExecutionContext.global

    val crawlId      = "CC-MAIN-2024-51"
    val wetPathsFile = "output/wet.paths"
    WetMetadataFetcher.fetchWetPaths(crawlId, "output")

    val processedFile  = "output/processed_chunks.txt"
    val downloadFuture = FileDownloadManager.downloadAllChunksAsync(
      warcPathsFile       = wetPathsFile,
      stagingDir          = AppConfig.localStagingDir,
      processedChunksFile = processedFile,
      parallelism         = 5
    )

    val spark = SparkSessionManager.spark
    import spark.implicits._

    try {
      val warcRawDf = FileReader.readFiles(
          spark,
          AppConfig.localStagingDir,
          AppConfig.maxFilesPerTrigger
        )
        .withColumn("input_filename", input_file_name())
        .withColumn("processed_at", current_timestamp())

      val processedDf = warcRawDf
        .withColumn("markdown_text", htmlToMarkdownUdf($"value"))
        .withColumn("language", LanguageUtils.detectLanguageUdf($"markdown_text"))

      val langAggDf = processedDf.groupBy("language").count()

      val aggQuery = langAggDf.writeStream
        .outputMode("complete")
        .foreachBatch { (batchDf: Dataset[Row], batchId: Long) =>
          batchDf
            .coalesce(1)
            .write
            .mode("overwrite")
            .json("output/lang_agg_json")
        }
        .trigger(Trigger.ProcessingTime("1 hour"))
        .option("checkpointLocation", "path/to/checkpoint/lang_agg/")
        .start()

      val filteredLanguagesDf = processedDf.filter($"language".isin("ru", "it", "de"))

      val storeFilteredLanguagesQuery = filteredLanguagesDf.writeStream
        .format("parquet")
        .option("path", "output/languages_parquet")
        .option("checkpointLocation", AppConfig.localCheckpointPath + "/languages_parquet_checkpoint")
        .partitionBy("language")
        .outputMode("append")
        .start()

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
        .trigger(Trigger.ProcessingTime("1 hour"))
        .option("checkpointLocation", AppConfig.localCheckpointPath + "/cleanup_checkpoint")
        .start()

      Await.result(downloadFuture, Duration.Inf)
      println("All WET files have been downloaded.")

      storeFilteredLanguagesQuery.awaitTermination()
      aggQuery.awaitTermination()
      cleanupQuery.awaitTermination()

    } finally {
      spark.stop()
    }
  }
}