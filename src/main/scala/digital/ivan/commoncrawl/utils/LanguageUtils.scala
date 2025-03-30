package digital.ivan.commoncrawl.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.tika.language.detect.{LanguageDetector, LanguageResult}

object LanguageUtils {

  val detectLanguageUdf: UserDefinedFunction = udf { text: String =>
    if (text == null || text.trim.isEmpty) {
      null
    } else {
      val detector = LanguageDetector.getDefaultLanguageDetector.loadModels()
      val result: LanguageResult = detector.detect(text)
      result.getLanguage
    }
  }

}
