# 1. Primary S3 Data Lakehouse Bucket
resource "aws_s3_bucket" "lakehouse" {
  bucket        = "${var.project_prefix}-lakehouse-${data.aws_caller_identity.current.account_id}"
  force_destroy = true # Helpful in a lab environment for easy cleanup
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# 2. S3 Folders / Prefixes
resource "aws_s3_object" "folders" {
  for_each = toset([
    "raw/openaq/",
    "raw/noaa_gfs/",
    "refined/air_quality/",
    "refined/weather_forecast/",
    "curated/fact_air_quality_weather/",
    "curated/dim_station/",
    "curated/dim_date/",
    "curated/dim_pollutant/",
    "athena-query-results/",
    "scripts/",
    "errors/openaq/",
    "errors/noaa_gfs/"
  ])

  bucket = aws_s3_bucket.lakehouse.id
  key    = each.value
}

# 3. AWS Glue Catalog Database
resource "aws_glue_catalog_database" "lakehouse_db" {
  name        = "air_quality_weather_db"
  description = "Database for Global Air Quality and Weather Intelligence Lakehouse"
}

# 4. Athena Workgroup configured for Iceberg / S3 results
resource "aws_athena_workgroup" "lakehouse_workgroup" {
  name = "${var.project_prefix}-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 1073741824 # 1 GB hard limit — cost guardrail

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}