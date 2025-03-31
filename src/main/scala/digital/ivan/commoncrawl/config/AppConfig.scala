package digital.ivan.commoncrawl.config

object AppConfig {
  val localStagingDir: String = "output/staging"
  val localCheckpointPath:  String = "output/commoncrawl_checkpoints"

  val processedChunksFile = "output/processed_chunks.txt"
  val warcPathsFile       = "output/warc.paths"
  val crawlId            = "CC-MAIN-2023-30"
}