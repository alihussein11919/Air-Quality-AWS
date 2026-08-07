resource "aws_scheduler_schedule" "noaa_batch" {
  name       = "${var.project_prefix}-noaa-trigger"
  group_name = "default"

  schedule_expression = "cron(0 1,7,13,19 * * ? *)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.pipeline_orchestrator.arn
    role_arn = data.aws_iam_role.lab_role.arn
  }

}

resource "aws_scheduler_schedule" "openaq_poller" {
  name       = "${var.project_prefix}-openaq-poller"
  group_name = "default"

  schedule_expression = "rate(1 hour)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.openaq_poller.arn
    role_arn = data.aws_iam_role.lab_role.arn
  }

}
