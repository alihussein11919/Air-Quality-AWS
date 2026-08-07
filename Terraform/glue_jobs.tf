resource "aws_glue_job" "refined_noaa" {
  name     = "${var.project_prefix}-refined-noaa"
  role_arn = data.aws_iam_role.lab_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/scripts/refined_noaa.py"
  }

  worker_type       = "Standard"
  number_of_workers = 2
  glue_version      = "4.0"

  default_arguments = {
    "--S3_INPUT_PATH"  = "s3://${aws_s3_bucket.lakehouse.id}/raw/noaa_gfs/"
    "--S3_OUTPUT_PATH" = "s3://${aws_s3_bucket.lakehouse.id}/refined/weather_forecast/"
  }

  tags = {
    Project = var.project_prefix
    Layer   = "transform"
  }
}

resource "aws_glue_job" "refined_openaq" {
  name     = "${var.project_prefix}-refined-openaq"
  role_arn = data.aws_iam_role.lab_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/scripts/refined_openaq.py"
  }

  worker_type       = "Standard"
  number_of_workers = 2
  glue_version      = "4.0"

  default_arguments = {
    "--S3_INPUT_PATH"  = "s3://${aws_s3_bucket.lakehouse.id}/raw/openaq/"
    "--S3_OUTPUT_PATH" = "s3://${aws_s3_bucket.lakehouse.id}/refined/air_quality/"
  }

  tags = {
    Project = var.project_prefix
    Layer   = "transform"
  }
}

resource "aws_glue_job" "data_quality" {
  name     = "${var.project_prefix}-data-quality"
  role_arn = data.aws_iam_role.lab_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/scripts/data_quality.py"
  }

  worker_type       = "Standard"
  number_of_workers = 2
  glue_version      = "4.0"

  default_arguments = {
    "--S3_AQ_PATH"      = "s3://${aws_s3_bucket.lakehouse.id}/refined/air_quality/"
    "--S3_WEATHER_PATH" = "s3://${aws_s3_bucket.lakehouse.id}/refined/weather_forecast/"
  }

  tags = {
    Project = var.project_prefix
    Layer   = "transform"
  }
}

resource "aws_glue_job" "spatial_temporal_join" {
  name     = "${var.project_prefix}-spatial-temporal-join"
  role_arn = data.aws_iam_role.lab_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/scripts/spatial_temporal_join.py"
  }

  worker_type       = "Standard"
  number_of_workers = 2
  glue_version      = "4.0"

  default_arguments = {
    "--S3_AQ_PATH"      = "s3://${aws_s3_bucket.lakehouse.id}/refined/air_quality/"
    "--S3_WEATHER_PATH" = "s3://${aws_s3_bucket.lakehouse.id}/refined/weather_forecast/"
    "--S3_OUTPUT_PATH"  = "s3://${aws_s3_bucket.lakehouse.id}/curated/fact_air_quality_weather/"
  }

  tags = {
    Project = var.project_prefix
    Layer   = "transform"
  }
}
