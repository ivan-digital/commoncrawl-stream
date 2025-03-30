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
    val outDir = new File(outputDir)
    if (!outDir.exists()) {
      val created = outDir.mkdirs()
      if (!created) {
        println(s"WARNING: Could not create directory ${outDir.getAbsolutePath}")
      }
    }

    val baseUrl = "https://data.commoncrawl.org/crawl-data"
    val warcPathsGzUrl = s"$baseUrl/$crawlId/wet.paths.gz"

    val warcPathsGzFile = s"$outputDir/wet.paths.gz"
    val warcPathsFile   = s"$outputDir/wet.paths"

    println(s"Downloading $warcPathsGzUrl to $warcPathsGzFile ...")
    val cmdDownload = s"wget -O $warcPathsGzFile $warcPathsGzUrl"
    cmdDownload.!

    println(s"Unzipping $warcPathsGzFile ...")
    val cmdGunzip = s"gzip -d -f $warcPathsGzFile"
    cmdGunzip.!

    warcPathsFile
  }
}