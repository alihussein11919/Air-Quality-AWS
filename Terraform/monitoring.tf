# Step Functions execution failure alarm
resource "aws_cloudwatch_metric_alarm" "sfn_failure" {
  alarm_name          = "${var.project_prefix}-sfn-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pipeline_orchestrator.arn
  }

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}

# Fargate task failure alarm
resource "aws_cloudwatch_metric_alarm" "ecs_failure" {
  alarm_name          = "${var.project_prefix}-ecs-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ServiceStatus"
  namespace           = "AWS/ECS"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    ClusterName = aws_ecs_cluster.pipeline.name
  }

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}

# Kinesis iterator age alarm (real-time consumer falling behind)
resource "aws_cloudwatch_metric_alarm" "kinesis_iterator_age" {
  alarm_name          = "${var.project_prefix}-kinesis-iterator-age"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 300
  statistic           = "Maximum"
  threshold           = 300000
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    StreamName = aws_kinesis_stream.openaq_stream.name
  }

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}

# Athena bytes scanned alarm (cost guardrail)
resource "aws_cloudwatch_metric_alarm" "athena_bytes_scanned" {
  alarm_name          = "${var.project_prefix}-athena-bytes-scanned"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ProcessedBytes"
  namespace           = "AWS/Athena"
  period              = 86400
  statistic           = "Sum"
  threshold           = 1073741824
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    WorkGroup = aws_athena_workgroup.lakehouse_workgroup.name
  }

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}

# Glue job failure alarms (one per job)
resource "aws_cloudwatch_metric_alarm" "glue_refined_noaa_failure" {
  alarm_name          = "${var.project_prefix}-glue-refined-noaa-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "JobStatus"
  namespace           = "AWS/Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    JobName = aws_glue_job.refined_noaa.name
  }

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}

resource "aws_cloudwatch_metric_alarm" "glue_refined_openaq_failure" {
  alarm_name          = "${var.project_prefix}-glue-refined-openaq-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "JobStatus"
  namespace           = "AWS/Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    JobName = aws_glue_job.refined_openaq.name
  }

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}

resource "aws_cloudwatch_metric_alarm" "glue_spatial_join_failure" {
  alarm_name          = "${var.project_prefix}-glue-spatial-join-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "JobStatus"
  namespace           = "AWS/Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    JobName = aws_glue_job.spatial_temporal_join.name
  }

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}

# Lambda error alarms (producer + alert consumer)
resource "aws_cloudwatch_metric_alarm" "lambda_producer_errors" {
  alarm_name          = "${var.project_prefix}-lambda-producer-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.openaq_poller.function_name
  }

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_consumer_errors" {
  alarm_name          = "${var.project_prefix}-lambda-consumer-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.alert_consumer.function_name
  }

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_producer_throttles" {
  alarm_name          = "${var.project_prefix}-lambda-producer-throttles"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Throttles"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.openaq_poller.function_name
  }

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_consumer_throttles" {
  alarm_name          = "${var.project_prefix}-lambda-consumer-throttles"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Throttles"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.alert_consumer.function_name
  }

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}
