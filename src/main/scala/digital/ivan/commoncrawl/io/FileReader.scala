package digital.ivan.commoncrawl.io

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReader {
  def readFiles(spark: SparkSession,
                inputPath: String,
                maxFilesPerTrigger: Int): DataFrame = {
    spark.readStream
      .format("text")
      .option("maxFilesPerTrigger", maxFilesPerTrigger)
      .load(inputPath)
  }
}