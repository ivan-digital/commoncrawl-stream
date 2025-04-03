package digital.ivan.commoncrawl

import digital.ivan.commoncrawl.config.{AppConfig, SparkSessionManager}
import digital.ivan.commoncrawl.io.{FileDownloadManager, WetMetadataFetcher}
import digital.ivan.commoncrawl.utils.{LanguageUtils, WarcProcessor}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import java.io.File
import java.net.URI

object CCProcessorApp {

  def main(args: Array[String]): Unit = {
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

    val crawlId      = "CC-MAIN-2024-51"
    val wetPathsFile = "output/wet.paths"
    WetMetadataFetcher.fetchWetPaths(crawlId, "output")

    val processedFile = "output/processed_chunks.txt"
    val downloadFuture: Future[Unit] =
      FileDownloadManager.downloadInBackground(
        wetPathsFile,
        AppConfig.localStagingDir,
        processedChunksFile = processedFile,
        maxParallel = 10,
        maxWindowSize = 100
      )

    val stagingDir = new File(AppConfig.localStagingDir)
    if (!stagingDir.exists()) {
      println(s"Staging directory ${stagingDir.getAbsolutePath} does not exist. Creating it...")
      stagingDir.mkdirs()
    }

    val spark = SparkSessionManager.spark
    import spark.implicits._

    val binaryFileSchema = new StructType()
      .add("path", StringType)
      .add("modificationTime", TimestampType)
      .add("length", LongType)
      .add("content", BinaryType)

    try {

      val warcBinaryDf = spark.readStream
        .schema(binaryFileSchema)
        .format("binaryFile")
        .option("pathGlobFilter", "*.wet.gz")
        .load(AppConfig.localStagingDir)

      val parsedWarcDf = warcBinaryDf.flatMap { row =>
          val content = row.getAs[Array[Byte]]("content")
          val filePath = row.getAs[String]("path")

          WarcProcessor.parseWarcRecords(content).map { case (url, rawText) =>
            (url, rawText, filePath)
          }
        }(Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.STRING))
        .toDF("url", "raw_text", "file_path")

      val transformedDf = parsedWarcDf
        .withColumn("processed_at", current_timestamp())
        .withColumn("language", LanguageUtils.detectLanguageUdf($"raw_text"))

      val query = transformedDf.writeStream
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .foreachBatch { (batchDf: Dataset[Row], batchId: Long) =>

          val langAggBatchDf = batchDf.groupBy("language").count()
          langAggBatchDf
            .coalesce(1)
            .write
            .mode(SaveMode.Append)
            .json("output/lang_agg_json")

          val filteredBatchDf = batchDf.filter($"language".isin("ru", "it", "de"))
          filteredBatchDf
            .write
            .format("parquet")
            .option("path", "output/languages_parquet")
            .option("compression", "gzip")
            .option("checkpointLocation", AppConfig.localCheckpointPath + "/languages_parquet_checkpoint")
            .partitionBy("language")
            .mode(SaveMode.Append)
            .save()

          println(s"[Spark] Done batch $batchId with ${batchDf.count()} rows")

          val filePaths = batchDf
            .select("file_path")
            .distinct()
            .as[String]
            .collect()

          filePaths.foreach { pathStr =>
            try {
              val uri = new URI(pathStr)
              val localFile = new File(uri)
              if (localFile.exists()) {
                println(s"[Spark] Deleting already processed file: $localFile")
                localFile.delete()
              }
            } catch {
              case e: Exception =>
                println(s"[Spark] Failed deleting file $pathStr: ${e.getMessage}")
            }
          }
        }
        .option("checkpointLocation", AppConfig.localCheckpointPath + "/unified_checkpoint")
        .start()

      downloadFuture.onComplete {
        case Success(_) => println("[Downloader] All files have been queued for download.")
        case Failure(e) => println(s"[Downloader] Download failed: $e")
      }

      query.awaitTermination()

    } finally {
      spark.stop()
    }
  }
}