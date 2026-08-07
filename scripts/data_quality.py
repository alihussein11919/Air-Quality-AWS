import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_AQ_PATH", "S3_WEATHER_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

aq_path = args["S3_AQ_PATH"]
weather_path = args["S3_WEATHER_PATH"]

issues = []

try:
    aq_df = spark.read.parquet(aq_path)
    total_aq = aq_df.count()
    null_values = aq_df.filter(F.col("value").isNull()).count()
    null_timestamps = aq_df.filter(F.col("timestamp").isNull()).count()
    null_coords = aq_df.filter(F.col("latitude").isNull() | F.col("longitude").isNull()).count()

    if null_values > 0:
        issues.append(f"AQ: {null_values} rows with null values out of {total_aq}")
    if null_timestamps > 0:
        issues.append(f"AQ: {null_timestamps} rows with null timestamps")
    if null_coords > 0:
        issues.append(f"AQ: {null_coords} rows with null coordinates")

    pm25_df = aq_df.filter(F.col("parameter") == "pm25")
    if pm25_df.count() > 0:
        out_of_range = pm25_df.filter((F.col("value") < 0) | (F.col("value") > 1000)).count()
        if out_of_range > 0:
            issues.append(f"AQ: {out_of_range} PM2.5 readings outside 0-1000 ug/m3 range")

except Exception as e:
    issues.append(f"AQ table read failed: {str(e)}")

try:
    weather_df = spark.read.parquet(weather_path)
    total_weather = weather_df.count()
    null_values_w = weather_df.filter(F.col("value").isNull()).count()

    if null_values_w > 0:
        issues.append(f"Weather: {null_values_w} rows with null values out of {total_weather}")

except Exception as e:
    issues.append(f"Weather table read failed: {str(e)}")

if issues:
    for issue in issues:
        logger.warning(f"DATA QUALITY ISSUE: {issue}")
    logger.warning(f"Total issues found: {len(issues)}. Pipeline will continue.")
else:
    logger.info("Data quality checks passed. No issues found.")

job.commit()
