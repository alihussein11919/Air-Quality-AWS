import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
import math

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_AQ_PATH", "S3_WEATHER_PATH", "S3_OUTPUT_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

aq_path = args["S3_AQ_PATH"]
weather_path = args["S3_WEATHER_PATH"]
output_path = args["S3_OUTPUT_PATH"]

aq_df = spark.read.parquet(aq_path)
weather_df = spark.read.parquet(weather_path)

weather_pivot = weather_df.groupBy(
    "grid_lat", "grid_lon", "forecast_time", "cycle_date", "cycle_hour"
).pivot("variable").agg(F.first("value"))

wx_var_cols = [c for c in weather_pivot.columns if c not in ["grid_lat", "grid_lon", "forecast_time", "cycle_date", "cycle_hour"]]
weather_flat = weather_pivot.select(
    "grid_lat", "grid_lon", "forecast_time", "cycle_date", "cycle_hour",
    *[F.col(c) for c in wx_var_cols]
)
print(f"Weather variables: {wx_var_cols}")

aq_with_grid = aq_df.withColumn("grid_lat", F.round(F.col("latitude"), 2))
aq_with_grid = aq_with_grid.withColumn("grid_lon", F.round(F.col("longitude"), 2))

nearby_grids = weather_flat.select("grid_lat", "grid_lon").distinct()
aq_nearby = aq_with_grid.crossJoin(
    nearby_grids.withColumnRenamed("grid_lat", "ng_lat").withColumnRenamed("grid_lon", "ng_lon")
).withColumn(
    "dist_deg", F.sqrt(
        (F.col("latitude") - F.col("ng_lat"))**2 + (F.col("longitude") - F.col("ng_lon"))**2
    )
).filter(F.col("dist_deg") <= 2.0)

from pyspark.sql.window import Window
window_spec = Window.partitionBy("station_id", "timestamp").orderBy("dist_deg")
aq_nearest = aq_nearby.withColumn(
    "rank", F.row_number().over(window_spec)
).filter(F.col("rank") <= 4)

aq_nearest = aq_nearest.join(
    weather_flat,
    (aq_nearest.ng_lat == weather_flat.grid_lat) & (aq_nearest.ng_lon == weather_flat.grid_lon),
    how="left"
).drop(weather_flat.grid_lat).drop(weather_flat.grid_lon)

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

haversine_udf = F.udf(haversine, DoubleType())

aq_nearest = aq_nearest.withColumn(
    "distance_km", haversine_udf(F.col("latitude"), F.col("longitude"), F.col("ng_lat"), F.col("ng_lon"))
)
aq_nearest = aq_nearest.withColumn(
    "weight", F.when(F.col("distance_km") > 0, 1.0 / (F.col("distance_km") ** 2)).otherwise(1.0)
)

idw_agg = []
for var in wx_var_cols:
    idw_agg.append(F.sum(F.col(var) * F.col("weight")).alias(f"wsum_{var}"))
idw_agg.append(F.sum("weight").alias("total_weight"))

result = aq_nearest.groupBy(
    "station_id", "location", "parameter", "value", "unit", "timestamp",
    "latitude", "longitude", "country", "city"
).agg(*idw_agg)

for var in wx_var_cols:
    result = result.withColumn(f"forecast_{var}", F.col(f"wsum_{var}") / F.col("total_weight"))

result = result.drop(*[f"wsum_{v}" for v in wx_var_cols] + ["total_weight"])

result = result.withColumnRenamed("value", "actual_value")
result = result.withColumnRenamed("timestamp", "reading_timestamp")

if "forecast_2t" in result.columns:
    result = result.withColumn("forecast_value", F.col("forecast_2t"))
else:
    first_fc = [c for c in result.columns if c.startswith("forecast_")]
    if first_fc:
        result = result.withColumn("forecast_value", F.col(first_fc[0]))
    else:
        result = result.withColumn("forecast_value", F.lit(None).cast(DoubleType()))

result = result.withColumn("forecast_timestamp", F.col("reading_timestamp"))
result = result.withColumn("nearest_grid_lat", F.lit(None).cast(DoubleType()))
result = result.withColumn("nearest_grid_lon", F.lit(None).cast(DoubleType()))
result = result.withColumn("distance_km", F.lit(None).cast(DoubleType()))

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
result = result.withColumn("region", region_udf(F.col("latitude"), F.col("longitude")))
result = result.withColumn("event_date", F.date_format(F.col("reading_timestamp"), "yyyy-MM-dd"))

result = result.select(
    "station_id", "parameter", "actual_value", "unit", "reading_timestamp",
    "forecast_value", "forecast_timestamp", "nearest_grid_lat", "nearest_grid_lon",
    "distance_km", "country", "city", "region", "event_date"
)

print(f"Writing {result.count()} rows")
result.write.mode("overwrite").partitionBy("event_date", "region").parquet(output_path)

job.commit()
