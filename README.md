## CommonCrawl Processing with Spark NLP & Tika

This project demonstrates how to:
1.	**Fetch metadata** (paths to WET files) from a CommonCrawl dataset.
2.	**Download** the WET chunks in parallel to a local directory.
3.	**Use Spark Structured Streaming** to read the downloaded files (in streaming mode).
4.	**Detect languages** in text (via Apache Tika).
6.	**Aggregate** results (language frequencies and token frequencies).
7.	**Write** outputs to Parquet.

### Table of Contents
-	Project Structure(#project-structure)
-	Build & Run Instructions(#build-run-instructions)
-	Workflow Overview(#workflow-overview)
-	Configuration(#configuration)
-	Notes & Potential Improvements(#notes-potential-improvements)


### Project Structure

```text
.
├── src
│   └── main
│       └── scala
│           └── digital
│               └── ivan
│                   └── commoncrawl
│                       ├── CCProcessorApp.scala
│                       ├── config
│                       │   ├── AppConfig.scala
│                       │   └── SparkSessionManager.scala
│                       ├── io
│                       │   ├── FileDownloadManager.scala
│                       │   └── WetMetadataFetcher.scala
│                       └── utils
│                           ├── ProcessedChunksTracker.scala
│                           ├── LanguageUtils.scala
│                           └── WarcProcessor.scala
├── build.sbt
├── README.md
└── output
       └── ...
```

### Build & Run Instructions

1.	Prerequisites
   - Java 8 or 11
   - Scala 2.12 (matching the Spark version in build.sbt)
   - Apache Spark 3.x
   - wget and gunzip installed (required by WetMetadataFetcher).
   - Sufficient disk space for the WET files to be downloaded (each chunk can be tens or hundreds of MB).
2. Clone the repository (or copy the code into your project directory).
3. Configure any desired settings in AppConfig.scala.
4. Build with SBT (sample command):
```bash
sbt clean assembly
```
Or from IntelliJ / another IDE with Scala & SBT support.
5.	Run the main app:
```bash
java --add-opens java.base/sun.nio.ch=ALL-UNNAMED -jar "target\scala-2.12\CommonCrawlStream-assembly-0.1.jar"
```
This will:
-	Download wet.paths.gz from CommonCrawl.
-	Decompress it into output/wet.paths.
-	Start downloading WET files in parallel to output/staging.
-	Simultaneously, start a Spark Structured Streaming job reading from output/staging/.
-	Write result Parquet files to output/commoncrawl_results.

Intermediate result:
```text
+--------+-----+
|language|count|
+--------+-----+
|en      |23118|
|vi      |1620 |
|ro      |1147 |
|sl      |345  |
|ur      |85   |
|lv      |327  |
|pl      |1719 |
|sk      |449  |
|pt      |1242 |
|NULL    |2203 |
|oc      |775  |
|gl      |717  |
|tl      |955  |
|sw      |290  |
|ms      |1045 |
|ko      |456  |
|uk      |528  |
|be      |684  |
|cs      |472  |
|sr      |490  |
+--------+-----+

```

## Workflow Overview
1.	Fetch WET Paths
-	Use WetMetadataFetcher to pull the wet.paths.gz file for the specified Common Crawl index (e.g., CC-MAIN-2024-51).
-	This produces a local file, for example: output/wet.paths.
2.	Parallel Download of WET Chunks
-	FileDownloadManager reads output/wet.paths, checks which .wet.gz chunks have already been processed (via processed_chunks.txt), and downloads only new ones to a local staging directory (e.g., output/staging).
-	Downloads occur in parallel (default concurrency = 5 or a user-defined value).
3.	Spark Initialization
-	A SparkSession is initialized (typically local mode with a configured number of cores, e.g., local[*]).
-	(Optional) A Spark NLP pipeline model is loaded or built (e.g., DocumentAssembler, Tokenizer, Normalizer) if text processing beyond simple language detection is required.
4.	Structured Streaming from Staging
-	We use spark.readStream.format("binaryFile") to pick up newly downloaded .wet.gz files from the staging directory.
-	This approach handles files in micro-batches, respecting any maxFilesPerTrigger or similar config.
5.	Parsing & Processing Each Micro-Batch
-	WARC Parsing: Each micro-batch contains binary chunks of WET data. We call WarcProcessor.parseWarcRecords(content) on each chunk to extract (url, raw_text) records.
-	Language Detection: Tika-based LanguageUtils.detectLanguageUdf(...) identifies the document language.
-	Token Counting: We may compute size(tokens) for each record to get a token count.
6.	Aggregation
-	We can group data by language or other fields for summary statistics (e.g., total documents per language).
7.	Outputs
-	Parquet Sink: Write processed records (e.g., (url, raw_text, language, token_count)) to a Parquet table under output/commoncrawl_results/.
-	Language Aggregates: Write JSON or Parquet language counts to a separate subdirectory (e.g., lang_agg_complete/).
8.	Completion
-	Optionally, we wait for all WET downloads to complete (i.e., once FileDownloadManager finishes).
-	Spark’s streaming query continues to process newly downloaded files until manually terminated (e.g., query.awaitTermination()).
9.	Configuration
-	Various paths (wetPathsFile, stagingDir, output paths, checkpoints) can be customized in AppConfig or runtime args.
-	Parallelism, memory, and batch intervals (Triggers) are also configurable based on the Spark environment and performance needs.

---
**Happy crawling and analyzing CommonCrawl!** If you have questions or suggestions, feel free to modify the code and adapt it to your workflow.