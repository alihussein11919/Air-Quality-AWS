resource "aws_cloudwatch_log_group" "openaq_poller" {
  name              = "/aws/lambda/${var.project_prefix}-openaq-poller"
  retention_in_days = 14
}

resource "aws_lambda_function" "openaq_poller" {
  function_name = "${var.project_prefix}-openaq-poller"
  runtime       = "python3.11"
  handler       = "openaq_poller.lambda_handler"
  memory_size   = 256
  timeout       = 300
  role          = data.aws_iam_role.lab_role.arn

  s3_bucket = aws_s3_bucket.lakehouse.id
  s3_key    = "scripts/openaq_poller.zip"

  environment {
    variables = {
      KINESIS_STREAM_NAME = aws_kinesis_stream.openaq_stream.name
      OPENAQ_API_KEY      = var.openaq_api_key
    }
  }

  tags = {
    Project = var.project_prefix
    Layer   = "streaming-ingestion"
  }
}
