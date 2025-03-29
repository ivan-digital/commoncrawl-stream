package digital.ivan.commoncrawl.pipeline

import com.johnsnowlabs.nlp.LightPipeline
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.udf
import org.apache.tika.language.detect.{LanguageDetector, LanguageResult}

/**
 * Utility object for:
 *  - Tika-based language detection
 *  - Tokenization with Spark NLP (LightPipeline)
 */
object LanguageUtils {

  // --------------------------------------------------------------------------
  // 1) Simple Tika-based language detector (no Spark DataFrame creation needed)
  // --------------------------------------------------------------------------
  private val languageDetector =
    LanguageDetector.getDefaultLanguageDetector
      .loadModels()

  def detectLanguage(text: String): String = {
    if (text == null || text.trim.isEmpty) {
      "unknown"
    } else {
      val detection: LanguageResult = languageDetector.detect(text)
      if (detection.isReasonablyCertain) detection.getLanguage
      else "unknown"
    }
  }

  /** Simple UDF for Tika-based detection. */
  val detectLanguageUdf = udf { text: String =>
    detectLanguage(text)
  }

  // --------------------------------------------------------------------------
  // 2) Broadcast pipeline model + local inference with LightPipeline
  // --------------------------------------------------------------------------
  @volatile private var broadcastModel: Broadcast[PipelineModel] = _

  /**
   * Called once on the driver to store the broadcasted model reference.
   * The executors can see this broadcast variable, but cannot create new DFs.
   */
  def setBroadcastModel(model: Broadcast[PipelineModel]): Unit = {
    broadcastModel = model
  }

  /**
   * Tokenize UDF. Creates a LightPipeline locally and calls .annotate(...) on text.
   */
  val tokenizeUdf = udf { text: String =>
    if (text == null || text.trim.isEmpty || broadcastModel == null) {
      Seq.empty[String]
    } else {
      // Create a LightPipeline from the broadcasted PipelineModel (local inference)
      val pipelineModel = broadcastModel.value
      val lp            = new LightPipeline(pipelineModel)
      val annotations   = lp.annotate(text)
      annotations.getOrElse("normalizedToken", Seq.empty[String])
    }
  }

}