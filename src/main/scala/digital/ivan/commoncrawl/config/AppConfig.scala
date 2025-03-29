package digital.ivan.commoncrawl.config

object AppConfig {
  val localStagingDir: String = "output/staging"
  val maxFilesPerTrigger: Int = 1
  val localParquetOutputPath: String = "output/commoncrawl_results"
  val localCheckpointPath:  String = "output/commoncrawl_checkpoints"

  // This is no longer used for direct reading; we instead do an initial step to get warc.paths
  val inputWarcPath: String = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-30/..."
  val processedChunksFile = "output/processed_chunks.txt"
  val warcPathsFile       = "output/warc.paths"
  val crawlId            = "CC-MAIN-2023-30"  // whichever monthly crawl you want
}