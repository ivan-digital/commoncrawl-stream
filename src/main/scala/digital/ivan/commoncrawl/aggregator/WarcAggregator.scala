package digital.ivan.commoncrawl.aggregator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object WarcAggregator {

  /**
   * Aggregates data by language to get document count.
   */
  def aggregateLanguages(processedDf: DataFrame): DataFrame = {
    processedDf
      .groupBy(col("language"))
      .agg(count("*").as("doc_count"))
      .orderBy(desc("doc_count"))
  }

}