package digital.ivan.commoncrawl.io

import digital.ivan.commoncrawl.utils.ProcessedChunksTracker
import org.apache.commons.io.FileUtils

import java.io.{File, FileInputStream}
import java.net.URL
import java.util.zip.GZIPInputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import java.util.concurrent.Semaphore

object FileDownloadManager {

  val connectTimeoutMs = 1 * 60 * 1000
  val readTimeoutMs    = 5 * 60 * 1000

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
            val success = downloadWithRetries(url, tmpDest, maxRetries = 3)
            if (success && isGzipValid(tmpDest)) {
              println(s"[Downloader] Renaming ${tmpDest.getName} -> ${finalDest.getName}")
              val renamedOk = tmpDest.renameTo(finalDest)
              if (!renamedOk) {
                println(s"[Downloader] WARNING: Could not rename ${tmpDest.getName} to ${finalDest.getName}")
              }
              ProcessedChunksTracker.markChunkProcessed(chunkFileName, processedChunksFile)
            } else {
              println(s"[Downloader] Failed to download or invalid GZIP for ${tmpDest.getName}. Deleting.")
              tmpDest.delete()
            }
          } else {
            println(s"[Downloader] Already exists: ${finalDest.getName}")
            ProcessedChunksTracker.markChunkProcessed(chunkFileName, processedChunksFile)
          }

        } finally {
          downloadSem.release()
        }
      }
      println("[Downloader] Finished all lines. No more files to download.")
    }
  }

  /**
   * A helper function to download a file with up to `maxRetries` attempts.
   */
  private def downloadWithRetries(url: String, dest: File, maxRetries: Int): Boolean = {
    var attempt = 0
    var success = false
    while (!success && attempt < maxRetries) {
      attempt += 1
      println(s"[Downloader] Attempt #$attempt to download $url")
      try {
        FileUtils.copyURLToFile(new URL(url), dest, connectTimeoutMs, readTimeoutMs)
        success = true
      } catch {
        case e: Exception =>
          println(s"[Downloader] Error downloading $url (attempt $attempt/$maxRetries). " +
            s"Error: ${e.getMessage}")
          if (attempt < maxRetries) {
            Thread.sleep(5000)
            println("[Downloader] Retrying...")
          } else {
            println("[Downloader] Reached max retries; giving up on this file.")
          }
      }
    }
    success
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
      while (in.read(buf) != -1) {}
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