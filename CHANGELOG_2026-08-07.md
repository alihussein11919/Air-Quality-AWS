# Changelog - August 7, 2026

## Summary
Fixed multiple bugs in the air quality pipeline, populated dimension tables, ran full pipeline successfully, resolved Glue table schema issues, and backfilled 3M rows of historical OpenAQ data as Parquet.

---

## 1. Alert Consumer Lambda Fix
**File:** `scripts/alert_consumer.py`

### Bugs Fixed
- **4 unterminated string literals** in CloudWatch Logs subscription filter patterns:
  - `"Records,` -> `"Records"`
  - `eventSource,` -> `eventSource"`
  - `kinesis,` -> `kinesis"`
  - etc.
- **Base64 decoding missing:** Kinesis data arrives base64-encoded. Added `base64.b64decode()` before JSON parsing.

### Deployment
- Repackaged `alert_consumer.zip` twice (syntax fix, then base64 fix)
- Redeployed to Lambda function `air-quality-weather-alert-consumer`
- Verified with test invocations: ESM status `LastProcessingResult: OK`

---

## 2. S3 Data Verification
- **Raw OpenAQ:** 17 files in `raw/openaq/`
- **Raw NOAA GFS:** 863 MB in `raw/noaa_gfs/`
- **Refined:** 27 files, 27.5 MB in `refined/`
- **Curated:** 84 files, 0.28 MB in `curated/`

---

## 3. Glue Jobs Verified
All Glue jobs confirmed working:
- `air-quality-weather-refined-noaa` - NOAA data refinement
- `air-quality-weather-refined-openaq` - OpenAQ data refinement
- `air-quality-weather-data-quality` - Data quality checks
- `air-quality-weather-spatial-temporal-join` - Spatial join producing fact table

---

## 4. Step Functions
- 4/5 recent runs: SUCCEEDED
- Full pipeline run: All 6 steps SUCCEEDED in 14.2 minutes

---

## 5. Dimension Tables - Populate Dimensions Glue Job
**File:** `scripts/populate_dimensions.py` (NEW)

Created Glue job `air-quality-weather-populate-dimensions` to build:
- **dim_station** - Station metadata (location_id, lat, lon, location name)
- **dim_date** - Date dimension (year, month, day, quarter, etc.)
- **dim_pollutant** - Pollutant reference (parameter, unit)

### Configuration
- Glue Version: 4.0
- Worker type: Standard
- Number of workers: 2
- No MaxCapacity (caused previous errors)

### Spark Fix
- Fixed aggregation error: `station_id` was missing from GROUP BY clause
- Re-uploaded `populate_dimensions.py` to S3 `scripts/` folder
- Re-ran successfully

---

## 6. Step Functions Update
Updated Step Functions definition to include `RunPopulateDimensions` step after `RunCuratedJoin`:

```
IngestData -> RunRefinedNOAA -> RunRefinedOpenAQ -> RunDataQuality
  -> RunCuratedJoin -> RunPopulateDimensions
```

---

## 7. Full Pipeline Run
- Triggered Step Functions execution
- All 6 steps SUCCEEDED in 14.2 minutes
- 1,950 rows in fact table (from live poller data only)

---

## 8. Glue Table Schema Fix - fact_air_quality_weather
**Problem:** `region` column appeared in both `Columns` and `PartitionKeys` of the Glue table definition, causing `Duplicate column name` error in Athena.

**Fix:**
- Dropped existing table: `DROP TABLE fact_air_quality_weather`
- Recreated with correct schema (12 columns, no `region` duplicate)
- Ran `MSCK REPAIR TABLE` to recover partition discovery

### Final Schema
| Column | Type |
|--------|------|
| station_id | int |
| datetime | string |
| lat | double |
| lon | double |
| location | string |
| parameter | string |
| value | double |
| unit | string |
| forecast_value | double |
| forecast_parameter | string |
| forecast_datetime | string |
| diff_hours | double |
| **Partition:** region | string |

---

## 9. Athena Queries Verified
- 1,950 rows in fact table
- 7 regions covered
- Data spanning 2016-2026

---

## 10. Historical OpenAQ Data Backfill (3M Rows)

### Problem
The live poller only fetches 300 records per hour, resulting in only 1,950 rows in the fact table. To enable meaningful analysis, we backfilled 3 million rows from the OpenAQ bulk archive.

### Data Source
- **Bucket:** `s3://openaq-data-archive/records/csv.gz/`
- **Format:** Gzip-compressed CSV, partitioned by `locationid/year/month`
- **Schema:** 9 columns (location_id, sensors_id, location, datetime, lat, lon, parameter, units, value)

### Process

#### Step 1: Create External Table
Created `openaq_raw_csv` pointing to the OpenAQ archive:
```sql
CREATE EXTERNAL TABLE openaq_raw_csv (
  location_id STRING, sensors_id STRING, location STRING,
  datetime STRING, lat STRING, lon STRING, parameter STRING,
  units STRING, value STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://openaq-data-archive/records/csv.gz/'
TBLPROPERTIES ('skip.header.line.count'='1');
```

**Key Finding:** OpenCSVSerde (not LazySimpleSerDe) was required because the CSV files contain Unicode characters (e.g., `ug/m3` with special encoding).

#### Step 2: Create Parquet Table (CTAS)
```sql
CREATE TABLE openaq_historical
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://air-quality-weather-lakehouse-785248360353/refined/openaq_historical/'
)
AS
SELECT * FROM openaq_raw_csv
LIMIT 3000000;
```

**Note:** Year/month partitioning was attempted but Athena CTAS has a 100-partition limit, and the data spans ~192 year/month combinations (2006-2022). Used flat Parquet instead.

### Results
| Metric | Value |
|--------|-------|
| Total rows | 3,000,000 |
| Parquet files | 30 |
| Compressed size | 11.8 MB |
| Format | Snappy Parquet |
| Location | `s3://.../refined/openaq_historical/` |
| Glue table | `air_quality_weather_db.openaq_historical` |

### Data Distribution by Parameter
| Parameter | Rows |
|-----------|------|
| pm25 | 737,035 |
| pm10 | 506,740 |
| no2 | 443,912 |
| o3 | 440,880 |
| co | 438,271 |
| so2 | 385,193 |
| temperature | 5,082 |
| no | 4,772 |
| pm1 | 4,638 |
| um025 | 4,141 |
| humidity | 3,379 |
| pressure | 3,379 |
| voc | 1,277 |

### Coverage
- **Unique stations:** 49
- **Date range:** 2006-11-13 to 2026-08-04
- **Data years:** 2006-2026 (20 years)

### Issues Encountered & Resolved
1. **Column name mismatch:** CSV header has `units` not `unit` - fixed
2. **LazySimpleSerDe returned all NULLs:** Switched to OpenCSVSerde for proper CSV parsing
3. **TIMESTAMP cast failed:** Timezone offsets in datetime strings (e.g., `+02:00`) caused `INVALID_CAST_ARGUMENT` - kept as STRING
4. **DOUBLE cast failed:** OpenCSVSerde reads all columns as STRING, and Parquet column reordering caused `Cannot cast 'pm10' to DOUBLE` - used flat table without type casting
5. **Partition limit (100):** Year/month creates ~192 partitions exceeding Athena CTAS limit - used flat Parquet without partitioning

### Known Limitations
- All columns stored as STRING (not typed) due to OpenCSVSerde behavior
- No partitioning (flat Parquet) due to Athena CTAS 100-partition limit
- Some rows contain `-999.0` values (missing/sentinel values from source)
- A few misaligned values appear as parameters (e.g., coordinates like `-68.016195`)

---

## EC2 Grafana Update
- **Old IP:** 52.23.185.90
- **New IP:** 54.87.218.58
- Port: 3000
- Default login: admin/admin
- Key pair: `vockey.pem` (not `labuser.pem`)

---

## Environment Notes
- **AWS Account:** 785248360353 (AWS Academy Learner Lab)
- **Region:** us-east-1
- **S3 Bucket:** air-quality-weather-lakehouse-785248360353
- **AWS CLI:** Not installed; use `boto3` (Python 3.11) for all AWS operations
- **Session:** Temporary STS credentials (will expire)

---

## Tables in air_quality_weather_db
| Table | Description |
|-------|-------------|
| dim_date | Date dimension |
| dim_pollutant | Pollutant reference |
| dim_station | Station metadata |
| fact_air_quality_weather | Main fact table (1,950 rows, live data) |
| **openaq_historical** | **NEW: 3M rows historical OpenAQ (Parquet)** |
| openaq_raw_csv | External table pointing to OpenAQ archive |
| raw_noaa_gfs_forecast | Raw NOAA GFS forecast data |
| raw_openaq_measurements | Raw OpenAQ measurements from Kinesis |
| refined_air_quality | Refined AQ data (from Glue ETL) |
| refined_weather_forecast | Refined weather forecast data |
