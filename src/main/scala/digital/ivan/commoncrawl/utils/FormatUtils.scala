package digital.ivan.commoncrawl.utils

import com.vladsch.flexmark.html2md.converter.FlexmarkHtmlConverter
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object FormatUtils {
  private lazy val converter: FlexmarkHtmlConverter = FlexmarkHtmlConverter.builder().build()

  /**
   * Converts the provided HTML string to Markdown.
   * Returns an empty string in case of any exception.
   *
   * @param html the HTML content to convert.
   * @return the Markdown representation.
   */
  def htmlToMarkdown(html: String): String = {
    try {
      converter.convert(html)
    } catch {
      case _: Exception => ""
    }
  }

  val htmlToMarkdownUdf: UserDefinedFunction = udf(FormatUtils.htmlToMarkdown _)
}
