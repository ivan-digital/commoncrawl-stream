package digital.ivan.commoncrawl.config

import org.apache.spark.sql.SparkSession

object SparkSessionManager {
  lazy val spark: SparkSession = {
    SparkSession.builder()
      .appName("CommonCrawlLocalLangDetector")
      // Hardcoded 8 kernels usage
      .master("local[8]")
      .config("fs.http.impl", "org.apache.hadoop.fs.http.HttpFileSystem")
      .getOrCreate()
  }

  def stopSpark(): Unit = {
    spark.stop()
  }
}