package digital.ivan.commoncrawl

import digital.ivan.commoncrawl.config.{AppConfig, SparkSessionManager}
import digital.ivan.commoncrawl.io.{FileDownloadManager, FileReader, WetMetadataFetcher}
import digital.ivan.commoncrawl.utils.FormatUtils.htmlToMarkdownUdf
import digital.ivan.commoncrawl.utils.LanguageUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row}

import java.net.URI
import java.nio.file.Paths
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object CCProcessorApp {

  def main(args: Array[String]): Unit = {
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

      val unifiedQuery = warcRawDf.writeStream
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("1 minute"))
        .foreachBatch { (batchDf: Dataset[Row], batchId: Long) =>
          val processedBatchDf = batchDf
            .withColumn("markdown_text", htmlToMarkdownUdf($"value"))
            .withColumn("language", LanguageUtils.detectLanguageUdf($"markdown_text"))

          val langAggBatchDf = processedBatchDf.groupBy("language").count()

          langAggBatchDf
            .coalesce(1)
            .write
            .mode("overwrite")
            .json("output/lang_agg_json")

          val filteredBatchDf = processedBatchDf.filter($"language".isin("ru", "it", "de"))
          filteredBatchDf
            .write
            .format("parquet")
            .option("path", "output/languages_parquet")
            .option("checkpointLocation", AppConfig.localCheckpointPath + "/languages_parquet_checkpoint")
            .partitionBy("language")
            .mode("append")
            .save()

          val filesInBatch = batchDf
            .select("input_filename")
            .distinct()
            .as[String]
            .collect()

          filesInBatch.foreach { originalPath =>
            val uri = new URI(originalPath)
            val file = Paths.get(uri).toFile
            val fileName = file.getName
            if (file.exists()) {
              println(s"Deleting file: $fileName")
              val deleted = file.delete()
              if (!deleted) {
                println(s"Unable to delete file: $fileName")
              }
            } else {
              println(s"File does not exist, skipping: $fileName")
            }
          }

          println(s"Batch $batchId processed. # of files in batch: ${filesInBatch.length}")
        }
        .option("checkpointLocation", AppConfig.localCheckpointPath + "/unified_checkpoint")
        .start()

      Await.result(downloadFuture, Duration.Inf)
      println("All WET files have been downloaded.")

      unifiedQuery.awaitTermination()

    } finally {
      spark.stop()
    }
  }
}
