package digital.ivan.commoncrawl.processor

import digital.ivan.commoncrawl.config.AppConfig
import digital.ivan.commoncrawl.io.{FileDownloadManager, WetMetadataFetcher}
import digital.ivan.commoncrawl.utils.{LanguageUtils, WarcProcessor}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.io.File
import java.net.URI
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object CommonCrawlRawProcessor {

  /**
   * Process WET files from a given crawlId and write out language-filtered parquet.
   *
   * @param crawlId        e.g. "CC-MAIN-2024-51"
   * @param outputDir      local directory where wet.paths is fetched and downloaded
   * @param stagingDir     directory for local staging
   * @param processedFile  file path to track processed chunks
   * @param spark          SparkSession to use
   * @param ec             ExecutionContext for futures
   */
  def process(
               crawlId: String,
               outputDir: String,
               stagingDir: String,
               processedFile: String
             )(
               implicit spark: SparkSession,
               ec: ExecutionContext
             ): Unit = {

    import spark.implicits._

    val wetPathsFile = s"$outputDir/wet.paths"
    WetMetadataFetcher.fetchWetPaths(crawlId, outputDir)

    val downloadFuture: Future[Unit] =
      FileDownloadManager.downloadInBackground(
        wetPathsFile,
        stagingDir,
        processedChunksFile = processedFile,
        maxParallel = 10,
        maxWindowSize = 1000
      )

    val stagingFile = new File(stagingDir)
    if (!stagingFile.exists()) {
      println(s"Staging directory ${stagingFile.getAbsolutePath} does not exist. Creating it...")
      stagingFile.mkdirs()
    }

    val binaryFileSchema = new StructType()
      .add("path", StringType)
      .add("modificationTime", TimestampType)
      .add("length", LongType)
      .add("content", BinaryType)

    val warcBinaryDf = spark.readStream
      .schema(binaryFileSchema)
      .format("binaryFile")
      .option("pathGlobFilter", "*.wet.gz")
      .load(stagingDir)

    val parsedWarcDf = warcBinaryDf.flatMap { row =>
        val content = row.getAs[Array[Byte]]("content")
        val filePath = row.getAs[String]("path")

        WarcProcessor.parseWarcRecords(content).map {
          case (url, rawText) => (url, rawText, filePath)
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
          .json(s"$outputDir/lang_agg_json")

        val filteredBatchDf = batchDf.filter($"language".isin("ru", "ua", "it", "de", "gr"))
        filteredBatchDf
          .write
          .format("parquet")
          .option("path", s"$outputDir/languages_parquet")
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
      case Success(_) =>
        println("[Downloader] All files have been queued for download.")
      case Failure(e) =>
        println(s"[Downloader] Download failed: $e")
    }

    query.awaitTermination()
  }
}