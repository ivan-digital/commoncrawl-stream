## CommonCrawl Processing with Spark NLP & Tika

This project demonstrates how to:
1.	**Fetch metadata** (paths to WET files) from a CommonCrawl dataset.
2.	**Download** the WET chunks in parallel to a local directory.
3.	**Use Spark Structured Streaming** to read the downloaded files (in streaming mode).
4.	**Detect languages** in text (via Apache Tika).
5.	**Tokenize** text (via Spark NLP).
6.	**Aggregate** results (language frequencies and token frequencies).
7.	**Write** outputs to both console and Parquet.

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
│                       ├── aggregator
│                       │   └── WarcAggregator.scala
│                       ├── config
│                       │   ├── AppConfig.scala
│                       │   └── SparkSessionManager.scala
│                       ├── io
│                       │   ├── FileDownloadManager.scala
│                       │   ├── FileReader.scala
│                       │   └── WetMetadataFetcher.scala
│                       └── util
│                           ├── ProcessedChunksTracker.scala
│                           ├── LanguageUtils.scala
│                           └── WarcDownloadUtil.scala
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
-	Print aggregator outputs to console.
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
1.	Fetch WET paths:
WetMetadataFetcher pulls wet.paths.gz for CC-MAIN-2024-51 (or any specified crawl).
You’ll see a local file output/wet.paths.
2.	Parallel Download of WET Chunks:
FileDownloadManager reads output/wet.paths, checks which chunks are already processed (via processed_chunks.txt), and downloads new ones to output/staging.
Downloads happen in parallel (default concurrency = 5).
3.	Spark Initialization:
A SparkSession (local mode, 8 cores) is created. We also build a Spark NLP pipeline model with:
-	DocumentAssembler
-	Tokenizer
-	Normalizer
4.	Structured Streaming:
We use spark.readStream.format("text").load("output/staging") to pick up newly downloaded files (limited by maxFilesPerTrigger for each micro-batch).
5.	Processing Each Micro-Batch:
-	Language Detection: Tika-based LanguageUtils.detectLanguageUdf.
-	Filtering if required on language.
-	Tokenization: Spark NLP LightPipeline via LanguageUtils.tokenizeUdf.
-	Count Tokens: Compute size(tokens) as token_count.
6.	Aggregation:
-	Language Statistics:
WarcAggregator.aggregateLanguages => group by language => doc_count, avg_tokens.
7.	Outputs:
-	Console Sink: Print top results to the console every 30 seconds.
-	Parquet Sink: Overwrite Parquet tables in output/commoncrawl_results/:
-	lang_agg_complete/
8.	Completion:
-	We await the entire set of downloads to finish (optional step).
-	Spark streaming queries continue until terminated.
     Configuration

## You can customize a few settings in AppConfig.scala:
-	localStagingDir (where to store downloaded WET files)
-	maxFilesPerTrigger (how many files to process per micro-batch)
-	localParquetOutputPath (where Parquet results are saved)
-	localCheckpointPath (where streaming checkpoints are stored)
-	parallelism in FileDownloadManager.downloadAllChunksAsync can be tweaked for faster/slower concurrent downloads.

---
**Happy crawling and analyzing CommonCrawl!** If you have questions or suggestions, feel free to modify the code and adapt it to your workflow.