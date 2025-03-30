package digital.ivan.commoncrawl.pipeline

import org.apache.tika.language.detect.{LanguageDetector, LanguageResult}
import org.apache.spark.sql.functions.udf

object LanguageUtils {

  val detectLanguageUdf = udf { text: String =>
    if (text == null || text.trim.isEmpty) {
      null
    } else {
      val detector = LanguageDetector.getDefaultLanguageDetector().loadModels()
      val result: LanguageResult = detector.detect(text)
      result.getLanguage
    }
  }

}
