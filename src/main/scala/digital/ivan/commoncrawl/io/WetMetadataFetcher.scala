package digital.ivan.commoncrawl.io

import sys.process._
import java.io.File

object WetMetadataFetcher {
  /**
   * Downloads the warc.paths.gz file for a specific crawl, then gunzips it.
   * For example: "CC-MAIN-2023-30".
   * Outputs a text file "warc.paths" with one chunk path per line.
   */
  def fetchWetPaths(crawlId: String, outputDir: String): String = {
    // Ensure `outputDir` exists
    val outDir = new File(outputDir)
    if (!outDir.exists()) {
      // Create directory (including parent dirs)
      val created = outDir.mkdirs()
      if (!created) {
        println(s"WARNING: Could not create directory ${outDir.getAbsolutePath}")
      }
    }

    // Example: "CC-MAIN-2023-30"
    val baseUrl = "https://data.commoncrawl.org/crawl-data"
    val warcPathsGzUrl = s"$baseUrl/$crawlId/wet.paths.gz"  // or wat.paths.gz, wet.paths.gz, etc.

    // The local file we’ll store to:
    val warcPathsGzFile = s"$outputDir/wet.paths.gz"
    val warcPathsFile   = s"$outputDir/wet.paths"

    // Download:
    println(s"Downloading $warcPathsGzUrl to $warcPathsGzFile ...")
    val cmdDownload = s"wget -O $warcPathsGzFile $warcPathsGzUrl"
    cmdDownload.!

    // Gunzip:
    println(s"Unzipping $warcPathsGzFile ...")
    val cmdGunzip = s"gunzip -f $warcPathsGzFile"
    cmdGunzip.!

    // Return the path to the final text file
    warcPathsFile
  }
}