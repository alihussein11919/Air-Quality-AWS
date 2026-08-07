import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_INPUT_PATH", "S3_OUTPUT_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

input_path = args["S3_INPUT_PATH"]
output_path = args["S3_OUTPUT_PATH"]

import boto3
import gzip
import json

s3 = boto3.client("s3")
bucket = "air-quality-weather-lakehouse-785248360353"
prefix = input_path.split(bucket + "/")[1].rstrip("/") + "/"

paginator = s3.get_paginator("list_objects_v2")
all_records = []
decoder = json.JSONDecoder()

for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".gz"):
            continue
        print(f"Reading: {key}")
        raw = s3.get_object(Bucket=bucket, Key=key)
        text = gzip.decompress(raw["Body"].read()).decode("utf-8")
        text = text.strip()
        pos = 0
        while pos < len(text):
            obj_val, end_pos = decoder.raw_decode(text, pos)
            all_records.append(obj_val)
            pos = end_pos
            while pos < len(text) and text[pos] in " \n\t":
                pos += 1

print(f"Parsed {len(all_records)} records")
df = spark.createDataFrame(all_records)

df = df.dropDuplicates(["location", "parameter", "timestamp"])

df = df.withColumn("value", F.col("value").cast(DoubleType()))
df = df.withColumn("latitude", F.col("latitude").cast(DoubleType()))
df = df.withColumn("longitude", F.col("longitude").cast(DoubleType()))
df = df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))

df = df.withColumn("station_id", F.concat_ws("_", F.col("location"), F.col("country")))

df = df.withColumn(
    "value_standardized",
    F.when(
        (F.col("parameter") == "pm25") & (F.col("unit") == "ppm"),
        F.col("value") * 1000.0
    ).otherwise(F.col("value"))
)

df = df.withColumn("unit", F.when(
    (F.col("parameter") == "pm25") & (F.col("unit") == "ppm"), F.lit("ug/m3")
).otherwise(F.col("unit")))

df = df.select(
    "station_id", "location", "parameter", "value_standardized", "unit",
    "timestamp", "latitude", "longitude", "country", "city", "source_name"
)

df = df.withColumnRenamed("value_standardized", "value")

df = df.filter(F.col("value").isNotNull())
df = df.filter(F.col("latitude").isNotNull())
df = df.filter(F.col("longitude").isNotNull())

print(f"Writing {df.count()} rows")
df.write.mode("overwrite").parquet(output_path)

job.commit()
