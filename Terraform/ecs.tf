# ECR Repository for NOAA decode container
resource "aws_ecr_repository" "noaa_decode" {
  name                 = "${var.project_prefix}-noaa-decode"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }

  tags = {
    Project = var.project_prefix
    Layer   = "batch-ingestion"
  }
}

# ECS Cluster
resource "aws_ecs_cluster" "pipeline" {
  name = "${var.project_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "disabled"
  }

  tags = {
    Project = var.project_prefix
    Layer   = "batch-ingestion"
  }
}

# CloudWatch Log Group for ECS task
resource "aws_cloudwatch_log_group" "ecs_noaa" {
  name              = "/ecs/${var.project_prefix}-noaa-decode"
  retention_in_days = 14
}

# ECS Task Definition (Fargate)
resource "aws_ecs_task_definition" "noaa_decode" {
  family                   = "${var.project_prefix}-noaa-decode"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "2048"
  memory                   = "4096"
  execution_role_arn       = data.aws_iam_role.lab_role.arn
  task_role_arn            = data.aws_iam_role.lab_role.arn

  ephemeral_storage {
    size_in_gib = 30
  }

  container_definitions = jsonencode([{
    name      = "noaa-decode"
    image     = "${aws_ecr_repository.noaa_decode.repository_url}:latest"
    essential = true

    environment = [
      { name = "S3_OUTPUT_BUCKET", value = aws_s3_bucket.lakehouse.id },
      { name = "S3_OUTPUT_PREFIX", value = "raw/noaa_gfs/" },
      { name = "AWS_DEFAULT_REGION", value = var.aws_region }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.ecs_noaa.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "noaa-decode"
      }
    }
  }])

  tags = {
    Project = var.project_prefix
    Layer   = "batch-ingestion"
  }
}
