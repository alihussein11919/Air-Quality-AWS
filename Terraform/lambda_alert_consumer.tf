resource "aws_cloudwatch_log_group" "alert_consumer" {
  name              = "/aws/lambda/${var.project_prefix}-alert-consumer"
  retention_in_days = 14
}

resource "aws_lambda_function" "alert_consumer" {
  function_name = "${var.project_prefix}-alert-consumer"
  runtime       = "python3.11"
  handler       = "alert_consumer.lambda_handler"
  memory_size   = 256
  timeout       = 60
  role          = data.aws_iam_role.lab_role.arn

  s3_bucket = aws_s3_bucket.lakehouse.id
  s3_key    = "scripts/alert_consumer.zip"

  environment {
    variables = {
      SNS_TOPIC_ARN = aws_sns_topic.pipeline_alerts.arn
    }
  }

  tags = {
    Project = var.project_prefix
    Layer   = "streaming-ingestion"
  }
}

resource "aws_lambda_event_source_mapping" "kinesis_to_alert_consumer" {
  event_source_arn  = aws_kinesis_stream.openaq_stream.arn
  function_name     = aws_lambda_function.alert_consumer.arn
  starting_position = "LATEST"
  batch_size        = 100
  enabled           = true
}
