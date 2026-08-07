import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime, timedelta

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_AQ_PATH", "S3_OUTPUT_BASE"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

aq_path = args["S3_AQ_PATH"]
output_base = args["S3_OUTPUT_BASE"]

# --- dim_station ---
print("Building dim_station...")
aq_df = spark.read.parquet(aq_path)

def assign_region(lat, lon):
    if lat is None or lon is None:
        return "unknown"
    if 25 < lat < 72 and -130 < lon < -60:
        return "north_america"
    elif 35 < lat < 72 and -10 < lon < 40:
        return "europe"
    elif -10 < lat < 55 and 60 < lon < 150:
        return "asia"
    elif -40 < lat < 15 and -20 < lon < 55:
        return "africa"
    elif -55 < lat < 10 and -80 < lon < -35:
        return "south_america"
    elif -50 < lat < -10 and 110 < lon < 180:
        return "oceania"
    else:
        return "other"

region_udf = F.udf(assign_region, StringType())

dim_station = aq_df.groupBy("station_id").agg(
    F.first("location").alias("name"),
    F.first("country").alias("country"),
    F.first("city").alias("city"),
    F.first("latitude").alias("latitude"),
    F.first("longitude").alias("longitude"),
    F.first("parameter").alias("sensor_type")
).withColumn(
    "region", region_udf(F.col("latitude"), F.col("longitude"))
)

dim_station_count = dim_station.count()
print(f"dim_station: {dim_station_count} rows")
dim_station.write.mode("overwrite").parquet(f"{output_base}/curated/dim_station/")

# --- dim_date ---
print("Building dim_date...")
min_date = aq_df.agg(F.min("timestamp")).collect()[0][0]
max_date = aq_df.agg(F.max("timestamp")).collect()[0][0]

if min_date is None:
    min_date = datetime(2020, 1, 1)
if max_date is None:
    max_date = datetime.now()

if isinstance(min_date, str):
    min_date = datetime.fromisoformat(min_date.replace("Z", "+00:00"))
if isinstance(max_date, str):
    max_date = datetime.fromisoformat(max_date.replace("Z", "+00:00"))

start = min_date.replace(hour=0, minute=0, second=0, microsecond=0)
end = max_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

dates = []
current = start
while current <= end:
    dates.append(current)
    current += timedelta(days=1)

dim_date_data = []
for d in dates:
    dim_date_data.append({
        "date_key": d.strftime("%Y-%m-%d"),
        "full_date": d.date(),
        "year": d.year,
        "month": d.month,
        "day": d.day,
        "day_of_week": d.strftime("%A"),
        "month_name": d.strftime("%B"),
        "quarter": (d.month - 1) // 3 + 1
    })

dim_date = spark.createDataFrame(dim_date_data)
dim_date_count = dim_date.count()
print(f"dim_date: {dim_date_count} rows")
dim_date.write.mode("overwrite").parquet(f"{output_base}/curated/dim_date/")

# --- dim_pollutant ---
print("Building dim_pollutant...")
pollutants = [
    {"pollutant_code": "pm25", "name": "PM2.5", "unit": "ug/m3", "safe_threshold": 15.0, "health_category": "Fine Particulates"},
    {"pollutant_code": "pm10", "name": "PM10", "unit": "ug/m3", "safe_threshold": 45.0, "health_category": "Coarse Particulates"},
    {"pollutant_code": "o3", "name": "Ozone", "unit": "ug/m3", "safe_threshold": 100.0, "health_category": "Ground-Level Ozone"},
    {"pollutant_code": "no2", "name": "Nitrogen Dioxide", "unit": "ug/m3", "safe_threshold": 40.0, "health_category": "Nitrogen Oxides"},
    {"pollutant_code": "so2", "name": "Sulfur Dioxide", "unit": "ug/m3", "safe_threshold": 20.0, "health_category": "Sulfur Oxides"},
    {"pollutant_code": "co", "name": "Carbon Monoxide", "unit": "ug/m3", "safe_threshold": 4000.0, "health_category": "Carbon Monoxide"},
]

dim_pollutant = spark.createDataFrame(pollutants)
dim_pollutant_count = dim_pollutant.count()
print(f"dim_pollutant: {dim_pollutant_count} rows")
dim_pollutant.write.mode("overwrite").parquet(f"{output_base}/curated/dim_pollutant/")

print(f"Dimension tables populated: dim_station={dim_station_count}, dim_date={dim_date_count}, dim_pollutant={dim_pollutant_count}")
job.commit()
