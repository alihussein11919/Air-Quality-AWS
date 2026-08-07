import sys
from awsglue.transforms import *
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

df = spark.read.json(input_path)

columns_to_keep = ["latitude", "longitude", "variable", "value", "forecast_time", "cycle_date", "cycle_hour"]
existing_cols = [c for c in columns_to_keep if c in df.columns]
df = df.select(existing_cols)

for col_name in ["latitude", "longitude", "value"]:
    if col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

df = df.filter(F.col("variable") != "grib_chunk")

df = df.dropDuplicates(["latitude", "longitude", "variable", "cycle_date", "cycle_hour"])

df = df.withColumn("grid_lat", F.round(F.col("latitude"), 2))
df = df.withColumn("grid_lon_raw", F.round(F.col("longitude"), 2))
df = df.withColumn("grid_lon", F.when(F.col("grid_lon_raw") > 180, F.col("grid_lon_raw") - 360).otherwise(F.col("grid_lon_raw")))

df = df.drop("latitude", "longitude")

df.write.mode("overwrite").partitionBy("cycle_date", "cycle_hour").parquet(output_path)

job.commit()
