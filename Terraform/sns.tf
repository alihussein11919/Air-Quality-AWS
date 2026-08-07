resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project_prefix}-pipeline-alerts"

  tags = {
    Project = var.project_prefix
    Layer   = "monitoring"
  }
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
