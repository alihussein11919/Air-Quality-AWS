locals {
  catalog_id = data.aws_caller_identity.current.account_id
  db_name    = aws_glue_catalog_database.lakehouse_db.name
}

# =============================================================================
# RAW ZONE
# =============================================================================

resource "aws_glue_catalog_table" "raw_openaq_measurements" {
  catalog_id    = local.catalog_id
  database_name = local.db_name
  name          = "raw_openaq_measurements"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
    "format"         = "json"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lakehouse.id}/raw/openaq/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "location"
      type = "string"
    }
    columns {
      name = "parameter"
      type = "string"
    }
    columns {
      name = "value"
      type = "double"
    }
    columns {
      name = "unit"
      type = "string"
    }
    columns {
      name = "timestamp"
      type = "string"
    }
    columns {
      name = "latitude"
      type = "double"
    }
    columns {
      name = "longitude"
      type = "double"
    }
    columns {
      name = "country"
      type = "string"
    }
    columns {
      name = "city"
      type = "string"
    }
    columns {
      name = "source_name"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "raw_noaa_gfs_forecast" {
  catalog_id    = local.catalog_id
  database_name = local.db_name
  name          = "raw_noaa_gfs_forecast"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "json"
    "format"         = "json"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lakehouse.id}/raw/noaa_gfs/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "variable"
      type = "string"
    }
    columns {
      name = "value"
      type = "double"
    }
    columns {
      name = "latitude"
      type = "double"
    }
    columns {
      name = "longitude"
      type = "double"
    }
    columns {
      name = "forecast_time"
      type = "string"
    }
  }

  partition_keys {
    name = "cycle_date"
    type = "string"
  }
  partition_keys {
    name = "cycle_hour"
    type = "string"
  }
}

# =============================================================================
# REFINED ZONE
# =============================================================================

resource "aws_glue_catalog_table" "refined_air_quality" {
  catalog_id    = local.catalog_id
  database_name = local.db_name
  name          = "refined_air_quality"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "format"         = "parquet"
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lakehouse.id}/refined/air_quality/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "station_id"
      type = "string"
    }
    columns {
      name = "parameter"
      type = "string"
    }
    columns {
      name = "value"
      type = "double"
    }
    columns {
      name = "unit"
      type = "string"
    }
    columns {
      name = "timestamp"
      type = "timestamp"
    }
    columns {
      name = "latitude"
      type = "double"
    }
    columns {
      name = "longitude"
      type = "double"
    }
    columns {
      name = "country"
      type = "string"
    }
    columns {
      name = "city"
      type = "string"
    }
    columns {
      name = "sensor_type"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "refined_weather_forecast" {
  catalog_id    = local.catalog_id
  database_name = local.db_name
  name          = "refined_weather_forecast"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "format"         = "parquet"
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lakehouse.id}/refined/weather_forecast/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "grid_lat"
      type = "double"
    }
    columns {
      name = "grid_lon"
      type = "double"
    }
    columns {
      name = "variable"
      type = "string"
    }
    columns {
      name = "value"
      type = "double"
    }
    columns {
      name = "forecast_time"
      type = "string"
    }
    columns {
      name = "cycle_date"
      type = "string"
    }
    columns {
      name = "cycle_hour"
      type = "string"
    }
  }
}

# =============================================================================
# CURATED ZONE
# =============================================================================

resource "aws_glue_catalog_table" "fact_air_quality_weather" {
  catalog_id    = local.catalog_id
  database_name = local.db_name
  name          = "fact_air_quality_weather"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "format"         = "parquet"
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lakehouse.id}/curated/fact_air_quality_weather/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "station_id"
      type = "string"
    }
    columns {
      name = "parameter"
      type = "string"
    }
    columns {
      name = "actual_value"
      type = "double"
    }
    columns {
      name = "unit"
      type = "string"
    }
    columns {
      name = "reading_timestamp"
      type = "timestamp"
    }
    columns {
      name = "forecast_value"
      type = "double"
    }
    columns {
      name = "forecast_timestamp"
      type = "string"
    }
    columns {
      name = "nearest_grid_lat"
      type = "double"
    }
    columns {
      name = "nearest_grid_lon"
      type = "double"
    }
    columns {
      name = "distance_km"
      type = "double"
    }
    columns {
      name = "country"
      type = "string"
    }
    columns {
      name = "city"
      type = "string"
    }
    columns {
      name = "region"
      type = "string"
    }
  }

  partition_keys {
    name = "event_date"
    type = "string"
  }
  partition_keys {
    name = "region"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "dim_station" {
  catalog_id    = local.catalog_id
  database_name = local.db_name
  name          = "dim_station"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "format"         = "parquet"
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lakehouse.id}/curated/dim_station/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "station_id"
      type = "string"
    }
    columns {
      name = "name"
      type = "string"
    }
    columns {
      name = "country"
      type = "string"
    }
    columns {
      name = "city"
      type = "string"
    }
    columns {
      name = "latitude"
      type = "double"
    }
    columns {
      name = "longitude"
      type = "double"
    }
    columns {
      name = "sensor_type"
      type = "string"
    }
    columns {
      name = "region"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "dim_date" {
  catalog_id    = local.catalog_id
  database_name = local.db_name
  name          = "dim_date"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "format"         = "parquet"
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lakehouse.id}/curated/dim_date/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "date_key"
      type = "string"
    }
    columns {
      name = "full_date"
      type = "date"
    }
    columns {
      name = "year"
      type = "int"
    }
    columns {
      name = "month"
      type = "int"
    }
    columns {
      name = "day"
      type = "int"
    }
    columns {
      name = "day_of_week"
      type = "string"
    }
    columns {
      name = "month_name"
      type = "string"
    }
    columns {
      name = "quarter"
      type = "int"
    }
  }
}

resource "aws_glue_catalog_table" "dim_pollutant" {
  catalog_id    = local.catalog_id
  database_name = local.db_name
  name          = "dim_pollutant"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "format"         = "parquet"
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lakehouse.id}/curated/dim_pollutant/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "pollutant_code"
      type = "string"
    }
    columns {
      name = "name"
      type = "string"
    }
    columns {
      name = "unit"
      type = "string"
    }
    columns {
      name = "safe_threshold"
      type = "double"
    }
    columns {
      name = "health_category"
      type = "string"
    }
  }
}
