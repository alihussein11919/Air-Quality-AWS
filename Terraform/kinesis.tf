resource "aws_kinesis_stream" "openaq_stream" {
  name             = "${var.project_prefix}-openaq-stream"
  shard_count      = 1
  retention_period = 24

  tags = {
    Project = var.project_prefix
    Layer   = "streaming-ingestion"
  }
}
