package digital.ivan.commoncrawl.utils

import java.io.{BufferedReader, File, FileReader, FileWriter}
import scala.collection.mutable

object ProcessedChunksTracker {
  @volatile private var loaded = false
  private val processed = mutable.Set[String]()

  private val lock = new Object

  def loadProcessedChunks(filePath: String): Unit = lock.synchronized {
    if (!loaded) {
      val f = new File(filePath)
      if (f.exists()) {
        val br = new BufferedReader(new FileReader(f))
        try {
          var line = br.readLine()
          while (line != null) {
            processed += line.trim
            line = br.readLine()
          }
        } finally {
          br.close()
        }
      }
      loaded = true
    }
  }

  def isChunkProcessed(chunkName: String): Boolean = lock.synchronized {
    processed.contains(chunkName)
  }

  def markChunkProcessed(chunkName: String, filePath: String): Unit = lock.synchronized {
    if (!processed.contains(chunkName)) {
      processed += chunkName
      val fw = new FileWriter(filePath, true)
      try {
        fw.write(chunkName + "\n")
      } finally {
        fw.close()
      }
    }
  }
}