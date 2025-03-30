package digital.ivan.commoncrawl.io

import java.io.{File, FileInputStream}
import java.net.URL
import java.util.zip.GZIPInputStream

import digital.ivan.commoncrawl.util.ProcessedChunksTracker
import org.apache.commons.io.FileUtils

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

object FileDownloadManager {

  /**
   * Download all WET/WARC chunks in parallel (batched),
   * marking them as processed once downloaded.
   *
   * @param warcPathsFile       A file listing all chunk paths (can be a .gz file)
   * @param stagingDir          Local directory to store the chunk
   * @param processedChunksFile File tracking downloaded chunks
   * @param parallelism         How many downloads to run in parallel
   * @return A Future that completes when all downloads are finished
   */
  def downloadAllChunksAsync(warcPathsFile: String,
                             stagingDir: String,
                             processedChunksFile: String,
                             parallelism: Int = 5
                            )(implicit ec: ExecutionContext): Future[Unit] = {

    ProcessedChunksTracker.loadProcessedChunks(processedChunksFile)

    val source = if (warcPathsFile.toLowerCase.endsWith(".gz")) {
      val fis = new FileInputStream(warcPathsFile)
      val gzis = new GZIPInputStream(fis)
      Source.fromInputStream(gzis)
    } else {
      Source.fromFile(warcPathsFile)
    }

    val lines = source.getLines().toList
    source.close()

    val unprocessedLines = lines.filter { line =>
      val chunkFileName = line.split("/").last
      !ProcessedChunksTracker.isChunkProcessed(chunkFileName)
    }

    // 4) Group into batches of size = parallelism
    val groupedBatches = unprocessedLines.grouped(parallelism).toList

    // 5) Process each batch sequentially, with each batch downloading in parallel
    val batchFutures = groupedBatches.map { batch =>
      val chunkFutures = batch.map { line =>
        Future {
          val chunkFileName = line.split("/").last
          val fullUrl       = s"https://data.commoncrawl.org/$line"
          val dest          = new File(stagingDir, chunkFileName)

          if (!dest.exists()) {
            println(s"Downloading $fullUrl to ${dest.getAbsolutePath}")
            FileUtils.copyURLToFile(new URL(fullUrl), dest, 30000, 30000)
          } else {
            println(s"${dest.getName} already exists, skipping.")
          }

          ProcessedChunksTracker.markChunkProcessed(chunkFileName, processedChunksFile)
        }
      }
      Future.sequence(chunkFutures).map(_ => ())
    }

    val allBatchesFuture = batchFutures.foldLeft(Future.successful(())) { (acc, batchFut) =>
      acc.flatMap(_ => batchFut)
    }

    allBatchesFuture.map(_ => println("All chunks are downloaded and marked processed."))
  }
}
