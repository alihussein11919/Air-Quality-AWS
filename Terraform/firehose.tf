resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${var.project_prefix}-openaq-firehose"
  retention_in_days = 14
}

resource "aws_kinesis_firehose_delivery_stream" "openaq_to_s3" {
  name        = "${var.project_prefix}-openaq-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.openaq_stream.arn
    role_arn           = data.aws_iam_role.lab_role.arn
  }

  extended_s3_configuration {
    role_arn            = data.aws_iam_role.lab_role.arn
    bucket_arn          = aws_s3_bucket.lakehouse.arn
    prefix              = "raw/openaq/!{timestamp:yyyy/MM/dd}/"
    error_output_prefix = "errors/openaq/"

    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = "S3Delivery"
    }
  }

  tags = {
    Project = var.project_prefix
    Layer   = "streaming-ingestion"
  }
}
