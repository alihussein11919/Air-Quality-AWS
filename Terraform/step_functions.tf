resource "aws_sfn_state_machine" "pipeline_orchestrator" {
  name     = "${var.project_prefix}-pipeline"
  role_arn = data.aws_iam_role.lab_role.arn

  definition = jsonencode({
    Comment = "9-stage pipeline: Fargate NOAA decode, refined transforms, DQ check, curated IDW join, SNS notifications"
    StartAt = "RunNOAADecode"
    States = {
      RunNOAADecode = {
        Type     = "Task"
        Resource = "arn:aws:states:::ecs:runTask.sync"
        Parameters = {
          Cluster        = aws_ecs_cluster.pipeline.arn
          TaskDefinition = aws_ecs_task_definition.noaa_decode.arn
          LaunchType     = "FARGATE"
          NetworkConfiguration = {
            AwsvpcConfiguration = {
              Subnets        = ["subnet-0d4b6f45f848674e9", "subnet-03b7a3bd77ab6e8b5", "subnet-0e4ed4f4cb88c6fc1", "subnet-0166c45873dac04b3", "subnet-008f134153498b4bb", "subnet-080470626ff81333f"]
              AssignPublicIp = "ENABLED"
            }
          }
          Overrides = {
            ContainerOverrides = [{
              Name = "noaa-decode"
              Environment = [
                { Name = "S3_OUTPUT_BUCKET", Value = aws_s3_bucket.lakehouse.id },
                { Name = "S3_OUTPUT_PREFIX", Value = "raw/noaa_gfs/" }
              ]
            }]
          }
        }
        Next = "RunRefinedNOAA"
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed", "ECS.RuntimeError"]
            IntervalSeconds = 60
            MaxAttempts     = 3
            BackoffRate     = 2
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "NotifyFailure"
          }
        ]
      }

      RunRefinedNOAA = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.refined_noaa.name
        }
        Next = "RunRefinedOpenAQ"
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 60
            MaxAttempts     = 3
            BackoffRate     = 2
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "NotifyFailure"
          }
        ]
      }

      RunRefinedOpenAQ = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.refined_openaq.name
        }
        Next = "RunDataQuality"
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 60
            MaxAttempts     = 3
            BackoffRate     = 2
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "NotifyFailure"
          }
        ]
      }

      RunDataQuality = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.data_quality.name
        }
        Next = "RunCuratedJoin"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "NotifyDQWarning"
          }
        ]
      }

      RunCuratedJoin = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.spatial_temporal_join.name
        }
        Next = "NotifySuccess"
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 60
            MaxAttempts     = 3
            BackoffRate     = 2
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "NotifyFailure"
          }
        ]
      }

      NotifySuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "Pipeline Success"
          Message  = "Pipeline completed successfully."
        }
        End = true
      }

      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "Pipeline Failure"
          Message  = "Pipeline failed. Check Step Functions execution logs."
        }
        End = true
      }

      NotifyDQWarning = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "Pipeline Warning: Data Quality Check Failed"
          Message  = "Data quality check failed but pipeline continues. Review DQ job logs."
        }
        Next = "RunCuratedJoin"
      }
    }
  })

  logging_configuration {
    include_execution_data = true
    level                  = "ALL"

    log_destination = "${aws_cloudwatch_log_group.step_functions.arn}:*"
  }

  tags = {
    Project = var.project_prefix
    Layer   = "orchestration"
  }
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/stepfunctions/${var.project_prefix}-pipeline"
  retention_in_days = 14
}
