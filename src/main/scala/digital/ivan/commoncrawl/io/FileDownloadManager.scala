package digital.ivan.commoncrawl.io

import java.io.File
import java.net.URL
import digital.ivan.commoncrawl.util.ProcessedChunksTracker
import org.apache.commons.io.FileUtils

import scala.io.Source
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object FileDownloadManager {

  /**
   * Download all WET/WARC chunks in parallel (batched),
   * marking them as processed once downloaded.
   *
   * @param warcPathsFile   A file listing all chunk paths
   * @param stagingDir      Local directory to store the chunk
   * @param processedChunksFile  File tracking downloaded chunks
   * @param parallelism     How many downloads to run in parallel
   * @return A Future that completes when all downloads are finished
   */
  def downloadAllChunksAsync(warcPathsFile: String,
                             stagingDir: String,
                             processedChunksFile: String,
                             parallelism: Int = 5
                            )(implicit ec: ExecutionContext): Future[Unit] = {

    // 1) Load existing processed-chunks
    ProcessedChunksTracker.loadProcessedChunks(processedChunksFile)

    // 2) Read lines from warcPathsFile
    val lines = Source.fromFile(warcPathsFile).getLines().toList

    // 3) Filter out already processed
    val unprocessedLines = lines.filter { line =>
      val chunkFileName = line.split("/").last
      !ProcessedChunksTracker.isChunkProcessed(chunkFileName)
    }

    // 4) Group into batches of size = parallelism
    val groupedBatches = unprocessedLines.grouped(parallelism).toList

    // 5) Traverse each batch sequentially, but each batch in parallel
    val batchFutures = groupedBatches.map { batch =>
      // For each line in the batch, download in parallel
      val chunkFutures = batch.map { line =>
        Future {
          val chunkFileName = line.split("/").last
          val fullUrl       = s"https://data.commoncrawl.org/$line"
          val dest          = new File(stagingDir, chunkFileName)

          // Download if needed
          if (!dest.exists()) {
            println(s"Downloading $fullUrl to ${dest.getAbsolutePath}")
            FileUtils.copyURLToFile(new URL(fullUrl), dest, 30000, 30000)
          } else {
            println(s"${dest.getName} already exists, skipping.")
          }

          // Mark processed
          ProcessedChunksTracker.markChunkProcessed(chunkFileName, processedChunksFile)
        }
      }
      // Wait for all in this batch to finish
      Future.sequence(chunkFutures).map(_ => ())
    }

    // 6) Sequence the batched futures. All must complete for the final Future[Unit].
    val allBatchesFuture = batchFutures.foldLeft(Future.successful(())) { (acc, batchFut) =>
      acc.flatMap(_ => batchFut)
    }

    // Return the final future
    allBatchesFuture.map(_ => println("All chunks are downloaded and marked processed."))
  }
}