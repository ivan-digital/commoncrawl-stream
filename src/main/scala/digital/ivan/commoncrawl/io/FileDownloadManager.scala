package digital.ivan.commoncrawl.io

import org.apache.commons.io.FileUtils
import digital.ivan.commoncrawl.utils.ProcessedChunksTracker

import java.io.{File, FileInputStream}
import java.net.URL
import java.util.concurrent.Semaphore
import java.util.zip.GZIPInputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

object FileDownloadManager {

  val connectTimeoutMs = 2 * 60 * 1000  // 2 minutes
  val readTimeoutMs    = 10 * 60 * 1000 // 10 minutes for large Common Crawl WETs

  /**
   * Continuously downloads files from `wetPathsFile` into `stagingDir`.
   * Limits concurrency to `maxParallel`.
   * Also ensures the staging directory never holds more than `maxWindowSize` files at once.
   *
   * Once it finishes all lines, it completes the returned Future.
   */
  def downloadInBackground(
                            wetPathsFile: String,
                            stagingDir: String,
                            processedChunksFile: String,
                            maxParallel: Int,
                            maxWindowSize: Int
                          )(implicit ec: ExecutionContext): Future[Unit] = {

    ProcessedChunksTracker.loadProcessedChunks(processedChunksFile)

    val source = {
      if (wetPathsFile.toLowerCase.endsWith(".gz")) {
        val fis = new FileInputStream(wetPathsFile)
        val gzis = new GZIPInputStream(fis)
        Source.fromInputStream(gzis)
      } else {
        Source.fromFile(wetPathsFile)
      }
    }

    val allLines = source.getLines().toList
    source.close()

    val unprocessed = allLines.filter { line =>
      val chunkFile = line.split("/").last
      !ProcessedChunksTracker.isChunkProcessed(chunkFile)
    }

    println(s"Will download ${unprocessed.size} new files, ignoring already processed.")

    val downloadSem = new Semaphore(maxParallel)

    Future {
      unprocessed.foreach { line =>
        blockIfTooManyInStaging(stagingDir, maxWindowSize)

        downloadSem.acquire()
        try {
          val chunkFileName = line.split("/").last
          val url           = s"https://data.commoncrawl.org/$line"

          val tmpDest   = new File(stagingDir, chunkFileName + ".tmp")
          val finalDest = new File(stagingDir, chunkFileName)

          if (!finalDest.exists()) {
            println(s"[Downloader] Downloading $url -> ${tmpDest.getName}")
            FileUtils.copyURLToFile(new URL(url), tmpDest, connectTimeoutMs, readTimeoutMs)
            if (isGzipValid(tmpDest)) {
              println(s"[Downloader] Renaming ${tmpDest.getName} -> ${finalDest.getName}")
              val renamedOk = tmpDest.renameTo(finalDest)
              if (!renamedOk) {
                println(s"[Downloader] WARNING: Could not rename ${tmpDest.getName} to ${finalDest.getName}")
              }
            } else {
              println(s"[Downloader] Detected invalid/corrupt GZIP for ${tmpDest.getName}. Deleting or re-trying.")
              tmpDest.delete()
            }
          } else {
            println(s"[Downloader] Already exists: ${finalDest.getName}")
          }

          ProcessedChunksTracker.markChunkProcessed(chunkFileName, processedChunksFile)

        } finally {
          downloadSem.release()
        }
      }

      println("[Downloader] Finished all lines. No more files to download.")
    }
  }

  /**
   * Blocks if we already have >= maxWindowSize files in staging, waiting until
   * Spark presumably processes (and deletes) some.
   */
  private def blockIfTooManyInStaging(stagingDir: String, maxWindowSize: Int): Unit = {
    def currentCount: Int = new File(stagingDir).listFiles().length

    while (currentCount >= maxWindowSize) {
      println(s"[Downloader] Staging is full ($currentCount >= $maxWindowSize). Waiting...")
      Thread.sleep(20000)
    }
  }

  /**
   * Returns true if we can fully read the file as valid GZIP.
   * If the file is truncated or corrupt, we catch the exception and return false.
   */
  def isGzipValid(file: File): Boolean = {
    var in: GZIPInputStream = null
    try {
      in = new GZIPInputStream(new FileInputStream(file))
      val buf = new Array[Byte](8192)
      while (in.read(buf) != -1) {
      }
      true
    } catch {
      case e: Exception =>
        println(s"[Downloader] isGzipValid failed for ${file.getName}: ${e.getMessage}")
        false
    } finally {
      if (in != null) in.close()
    }
  }

}