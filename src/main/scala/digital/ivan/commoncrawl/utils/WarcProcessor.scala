package digital.ivan.commoncrawl.utils

import java.io._
import java.util.zip.GZIPInputStream

import scala.util.Try

object WarcProcessor {

  /**
   * Parses .wet.gz content into (url, text) for each 'conversion' record.
   *
   * Steps:
   *  1) Read lines until "WARC/1.0" => beginning of a record.
   *  2) Collect header lines until empty line => end of WARC headers.
   *  3) If WARC-Type == "conversion", parse out WARC-Target-URI. Then
   *     read the next `Content-Length` bytes as the record text
   *     until the next "WARC/1.0" or EOF.
   *  4) Return (url, text) if URL is found. Otherwise skip.
   */
  def parseWarcRecords(content: Array[Byte]): Iterator[(String, String)] = {
    val gis    = new GZIPInputStream(new ByteArrayInputStream(content))
    val reader = new BufferedReader(new InputStreamReader(gis, "UTF-8"))

    new Iterator[(String, String)] {
      private var nextRecord: Option[(String, String)] = None
      private var linesFinished = false

      advance()

      override def hasNext: Boolean = nextRecord.isDefined
      override def next(): (String, String) = {
        val out = nextRecord.get
        advance()
        out
      }

      private def advance(): Unit = {
        nextRecord = None
        if (linesFinished) return

        var line: String = skipUntilRecordBoundary()
        if (line == null) {
          linesFinished = true
          return
        }

        val headers = scala.collection.mutable.Map.empty[String, String]
        while ({ line = reader.readLine(); line != null && line.trim.nonEmpty }) {
          val idx = line.indexOf(':')
          if (idx > 0) {
            val key   = line.substring(0, idx).trim.toLowerCase
            val value = line.substring(idx + 1).trim
            headers(key) = value
          }
        }

        val warcType       = headers.getOrElse("warc-type", "")
        val url            = headers.getOrElse("warc-target-uri", "")
        val contentLenStr  = headers.getOrElse("content-length", "0")

        // Skip warcinfo payload if present
        if (warcType.equalsIgnoreCase("warcinfo")) {
          skipPayload(contentLenStr)
          advance()
          return
        }

        // Read the WET payload
        val textPayload = readPayloadLines(contentLenStr.toInt)

        if (url.nonEmpty) {
          nextRecord = Some(url -> textPayload)
        }
      }

      /**
       * Reads lines until we see "WARC/1.0" or EOF, returning the line containing "WARC/1.0" or null.
       */
      private def skipUntilRecordBoundary(): String = {
        var line: String = null
        while ({ line = reader.readLine(); line != null }) {
          if (line.startsWith("WARC/1.0")) {
            return line
          }
        }
        null
      }

      /**
       * Skip the payload for the next record by reading `contentLength` bytes.
       */
      private def skipPayload(contentLength: String): Unit = {
        val length = Try(contentLength.toInt).getOrElse(0)
        var remaining = length
        while (remaining > 0) {
          val toSkip = reader.read()
          if (toSkip < 0) {
            remaining = 0
          } else {
            remaining -= 1
          }
        }
      }

      /**
       * Read payload lines up to `contentLength`. Returns them as a string.
       */
      private def readPayloadLines(contentLength: Int): String = {
        if (contentLength <= 0) return ""
        val sb = new StringBuilder(contentLength)
        var readCount = 0
        while (readCount < contentLength) {
          val c = reader.read()
          if (c < 0) {
            return sb.toString()
          }
          sb.append(c.toChar)
          readCount += 1
        }
        sb.toString()
      }
    }
  }
}