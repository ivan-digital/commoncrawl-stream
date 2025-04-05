package digital.ivan.commoncrawl.processor

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, MinHashLSH, Tokenizer}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.Map

object LanguageDedupProcessor {

  private case class DocInfo(
                      url: String,
                      raw_text: String,
                      file_path: String,
                      processed_at: java.sql.Timestamp,
                      language: String
                    )

  private case class DedupedRecord(
                            url: String,
                            raw_text: String,
                            file_path: String,
                            processed_at: java.sql.Timestamp,
                            language: String,
                            componentId: Long
                          )

  /**
   * Run near-duplicate detection on each language partition in the dataset.
   *
   * @param spark             Spark session
   * @param inputParquetPath  Path to the partitioned parquet (including a 'language' column)
   * @param outputDedupedPath Path to write the deduplicated output
   * @param distanceThreshold Max Jaccard distance for near-duplicates
   * @param numFeatures       Number of features in HashingTF
   * @param numHashTables     Number of hash tables in MinHashLSH
   */
  def process(
               spark: SparkSession,
               inputParquetPath: String,
               outputDedupedPath: String,
               distanceThreshold: Double = 0.1,
               numFeatures: Int = 1 << 18,
               numHashTables: Int = 5
             ): Unit = {

    import spark.implicits._

    val df = spark.read.parquet(inputParquetPath)

    val distinctLanguages = df
      .select("language")
      .distinct()
      .as[String]
      .collect()

    distinctLanguages.foreach { lang =>
      println(s"[LanguageDedupProcessor] Processing language=$lang")

      val langDf = df.filter($"language" === lang)

      val columnsOfInterest = Seq("url", "raw_text", "file_path", "processed_at", "language")
      val baseDf = langDf.select(columnsOfInterest.map(col): _*)
      val docDs = baseDf.as[DocInfo]

      val tokenizer = new Tokenizer()
        .setInputCol("raw_text")
        .setOutputCol("words")

      val tokenizedDf = tokenizer.transform(docDs.toDF).na.fill("")

      val hashingTF = new HashingTF()
        .setInputCol("words")
        .setOutputCol("features")
        .setNumFeatures(numFeatures)

      val featurizedDf = hashingTF.transform(tokenizedDf)

      val minHash = new MinHashLSH()
        .setInputCol("features")
        .setOutputCol("hashes")
        .setNumHashTables(numHashTables)

      val lshModel = minHash.fit(featurizedDf)
      val transformedDf = lshModel.transform(featurizedDf)

      val joined = lshModel.approxSimilarityJoin(
        transformedDf,
        transformedDf,
        distanceThreshold,
        "JaccardDistance"
      )

      val dedupPairs = joined.filter($"datasetA.url" =!= $"datasetB.url")

      val pairEdges = dedupPairs.select(
        $"datasetA.url".as("urlA"),
        $"datasetB.url".as("urlB"),
        $"JaccardDistance"
      )

      val distinctUrls = pairEdges.select("urlA")
        .union(pairEdges.select("urlB"))
        .distinct()
        .as[String]
        .rdd
        .zipWithIndex()

      val urlToId: Map[String, Long] = distinctUrls.collectAsMap()
      val bcUrlToId = spark.sparkContext.broadcast(urlToId)

      val edges: RDD[Edge[Double]] = pairEdges.rdd.map { row =>
        val a = row.getAs[String]("urlA")
        val b = row.getAs[String]("urlB")
        val dist = row.getAs[Double]("JaccardDistance")

        Edge(
          bcUrlToId.value.getOrElse(a, -1L),
          bcUrlToId.value.getOrElse(b, -1L),
          dist
        )
      }

      val vertices: RDD[(VertexId, String)] = distinctUrls.map {
        case (url, idx) => (idx, url)
      }

      val graph = Graph(vertices, edges)
      val cc = graph.connectedComponents()

      val ccMap = cc.vertices.map { case (vId, compId) => (vId, compId) }.collectAsMap()
      val bcCcMap = spark.sparkContext.broadcast(ccMap)

      val dedupedDS = transformedDf.map { row =>
        val url = row.getAs[String]("url")
        val vId = bcUrlToId.value.getOrElse(url, -1L)
        val compId = bcCcMap.value.getOrElse(vId, vId)

        DedupedRecord(
          url,
          row.getAs[String]("raw_text"),
          row.getAs[String]("file_path"),
          row.getAs[java.sql.Timestamp]("processed_at"),
          row.getAs[String]("language"),
          compId
        )
      }(Encoders.product[DedupedRecord])

      val allUrlsDf = dedupedDS
        .groupBy("componentId")
        .agg(collect_list("url").alias("all_urls"))

      val dedupedWithAllUrlsDf = dedupedDS.join(allUrlsDf, "componentId")

      val w = Window.partitionBy("componentId").orderBy($"processed_at".asc)

      val dedupedDf = dedupedWithAllUrlsDf
        .withColumn("rank", row_number().over(w))
        .filter($"rank" === 1)
        .drop("rank")

      val finalPath = s"$outputDedupedPath/$lang"
      dedupedDf
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(finalPath)

      println(s"[LanguageDedupProcessor] Dedup completed for language=$lang -> $finalPath")
    }

    println(s"[LanguageDedupProcessor] All languages processed and deduplicated.")
  }
}