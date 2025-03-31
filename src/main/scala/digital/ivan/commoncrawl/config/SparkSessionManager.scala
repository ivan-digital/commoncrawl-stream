package digital.ivan.commoncrawl.config

import org.apache.spark.sql.SparkSession

object SparkSessionManager {
  lazy val spark: SparkSession = {
    SparkSession.builder()
      .appName("CommonCrawlLocalLangDetector")
      .master("local[*]")
      .config("spark.driver.memory", "24g")
      .config("spark.executor.memory", "24g")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.shuffle.partitions", 32)
      .config("spark.default.parallelism", 64)
      .config("fs.http.impl", "org.apache.hadoop.fs.http.HttpFileSystem")
      .getOrCreate()
  }

  def stopSpark(): Unit = {
    spark.stop()
  }
}