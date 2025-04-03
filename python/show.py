from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ReadParquetAndJson") \
        .getOrCreate()

    lang_agg_df = spark.read.json("../output/lang_agg_json")

    lang_parquet_df = spark.read.parquet("../output/languages_parquet")

    # 4. Print the top 5 rows
    print("=== Language Aggregation (JSON) ===")
    lang_agg_df.show(5)

    print("=== Language Parquet Data ===")
    lang_parquet_df.show(5)

    spark.stop()

if __name__ == "__main__":
    main()