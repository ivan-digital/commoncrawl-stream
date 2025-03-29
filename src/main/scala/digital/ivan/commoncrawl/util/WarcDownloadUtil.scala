package digital.ivan.commoncrawl.util

import java.net.URL
import java.io.File
import org.apache.commons.io.FileUtils

object WarcDownloadUtil {

  /**
   * Downloads a chunk from a given URL to `stagingDir`, if not already present.
   * Returns the local file name if downloaded (or if it already exists).
   */
  def downloadChunkIfNeeded(fileUrl: String, stagingDir: String): Option[String] = {
    val fileName = fileUrl.split("/").last
    val destDir = new File(stagingDir)
    if (!destDir.exists()) {
      destDir.mkdirs()
    }
    val destFile = new File(destDir, fileName)

    // Check if file already exists (not just processed, but physically present)
    if (!destFile.exists()) {
      println(s"Downloading chunk from $fileUrl to ${destFile.getAbsolutePath}")
      FileUtils.copyURLToFile(new URL(fileUrl), destFile, 10000, 10000)
      Some(fileName)
    } else {
      // Already exists
      None
    }
  }
}
