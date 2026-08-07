# Pipeline Architecture & Data Flow

## Overview

This is a serverless, event-driven data pipeline on AWS that ingests global air quality measurements from [OpenAQ](https://openaq.org/) and weather forecast data from [NOAA GFS](https://nomads.ncep.noaa.gov/), transforms them through a medallion architecture (raw → refined → curated), and visualizes the results on Grafana dashboards.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          INGESTION LAYER                                    │
│                                                                             │
│  OpenAQ API ──(hourly)──► Lambda Poller ──► Kinesis Stream ──► Firehose     │
│                                                              ──► S3 raw/    │
│  NOAA GFS ──(every 6h)──► Fargate Container (GRIB decode) ──► S3 raw/      │
│                                                                             │
│  Real-time: Kinesis ──► Alert Consumer Lambda ──► SNS ──► Hazard Alerts     │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    AWS STEP FUNCTIONS WORKFLOW                               │
│                                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐             │
│  │  NOAA    │───►│ Refine   │───►│ Refine   │───►│  Data    │             │
│  │  Decode  │    │  NOAA    │    │ OpenAQ   │    │ Quality  │             │
│  │ (Fargate)│    │  (Glue)  │    │  (Glue)  │    │  (Glue)  │             │
│  └──────────┘    └──────────┘    └──────────┘    └────┬─────┘             │
│                                                       │                    │
│                                                       ▼                    │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────────────┐             │
│  │ SNS      │◄───│ Spatial  │◄───│   Curated (Iceberg)      │             │
│  │ Success  │    │ Temporal │    │   fact_air_quality_weather │             │
│  │ Notify   │    │ Join     │    │   + dim_* tables          │             │
│  └──────────┘    │ (Glue)   │    └──────────────────────────┘             │
│                  └──────────┘                                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      ANALYSIS & VISUALIZATION                               │
│                                                                             │
│  Glue Data Catalog ──► Amazon Athena ──► Grafana (EC2)                     │
│                                           ├── Historical AQ Dashboard      │
│                                           └── Live Monitoring Dashboard    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Stage 1: Data Ingestion

### OpenAQ (Streaming — Hourly)

```
OpenAQ REST API
      │
      ▼
Lambda: openaq_poller.py          (EventBridge: rate(1 hour))
      │  Fetches latest measurements from OpenAQ v2 API
      │  Puts JSON records onto Kinesis Data Stream
      ▼
Kinesis Data Stream               (openaq_stream)
      │
      ▼
Kinesis Data Firehose             (openaq_to_s3)
      │  Buffers, compresses (GZIP), and delivers to S3
      │  S3 prefix: raw/openaq/{yyyy}/{MM}/{dd}/
      │  Error prefix: errors/openaq/
      ▼
S3: raw/openaq/                   (JSON, gzipped, date-partitioned)
```

**Configuration:**
- Firehose buffer: 60 seconds or 1 MB
- Compression: GZIP
- CloudWatch logging: enabled, 14-day retention

### NOAA GFS (Batch — Every 6 Hours)

```
NOAA Public S3 Bucket (NCEP GFS GRIB2)
      │
      ▼
ECS Fargate Task: noaa_decode.py  (EventBridge: cron(0 1,7,13,19 * * ? *))
      │  Downloads GRIB2 files from NOAA
      │  Decodes using eccodes Python library
      │  Extracts: temperature, wind_u, wind_v, humidity, precipitation
      │  Writes decoded JSON to S3
      ▼
S3: raw/noaa_gfs/                 (JSON, partitioned by cycle_date/cycle_hour)
```

**Configuration:**
- ECS Task Definition: 1 vCPU, 2 GB memory
- Docker image built from `Dockerfile` (Python 3.9 + eccodes)
- Schedule: 01:00, 07:00, 13:00, 19:00 UTC

### Real-Time Alerting

```
Kinesis Stream (raw records)
      │
      ▼
Lambda: alert_consumer.py         (Kinesis trigger)
      │  Decodes base64, parses JSON
      │  Checks: parameter IN ('pm25', 'pm10') AND value > 150
      │  If triggered → publishes to SNS
      ▼
SNS: pipeline_alerts              "Hazardous AQI Alert: {location}"
      │
      ▼
Email / HTTPS subscription
```

---

## Stage 2: Data Refinement (Glue ETL)

### Glue Job: `refined_openaq`

**Input:** `s3://.../raw/openaq/` (JSON gzipped)
**Output:** `s3://.../refined/air_quality/` (Parquet)

**Transformations:**
1. Decompress and parse concatenated JSON objects
2. Deduplicate on `(location, parameter, timestamp)`
3. Cast `value`, `latitude`, `longitude` to DoubleType
4. Cast `timestamp` to Spark TimestampType
5. Create `station_id` as `concat_ws("_", location, country)`
6. PM2.5 unit standardization: if unit is `ppm`, multiply value by 1000 and convert to `ug/m3`
7. Filter out null values, latitudes, or longitudes

**Output Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `station_id` | string | `{location}_{country}` |
| `location` | string | Station name |
| `parameter` | string | Pollutant code (pm25, pm10, o3, no2, etc.) |
| `value` | double | Measured value |
| `unit` | string | Measurement unit |
| `timestamp` | timestamp | Reading time |
| `latitude` | double | Station latitude |
| `longitude` | double | Station longitude |
| `country` | string | Country code |
| `city` | string | City name |
| `source_name` | string | Data source |

### Glue Job: `refined_noaa`

**Input:** `s3://.../raw/noaa_gfs/` (JSON)
**Output:** `s3://.../refined/weather_forecast/` (Parquet)

**Transformations:**
1. Keep only: `latitude, longitude, variable, value, forecast_time, cycle_date, cycle_hour`
2. Cast to proper types
3. Filter out `grib_chunk` rows
4. Deduplicate on `(latitude, longitude, variable, cycle_date, cycle_hour)`
5. Round `latitude` to 2 decimal places as `grid_lat`
6. Round `longitude` to 2 decimal places, adjust if > 180 (subtract 360)

**Output Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `grid_lat` | double | Rounded latitude (2dp) |
| `grid_lon` | double | Rounded longitude (2dp) |
| `variable` | string | Weather variable (2t, 10v, 10u, etc.) |
| `value` | double | Forecast value |
| `forecast_time` | string | Forecast timestamp |

**Partitioned by:** `cycle_date`, `cycle_hour`

---

## Stage 3: Data Quality (Non-Blocking)

### Glue Job: `data_quality`

**Input:** Refined AQ + Weather tables
**Output:** CloudWatch logs (warnings only — pipeline always continues)

**Checks Performed:**

| Check | Table | Condition |
|-------|-------|-----------|
| Null values | AQ | `value IS NULL` |
| Null timestamps | AQ | `timestamp IS NULL` |
| Null coordinates | AQ | `latitude IS NULL OR longitude IS NULL` |
| PM2.5 range | AQ | `parameter = 'pm25' AND (value < 0 OR value > 1000)` |
| Null values | Weather | `value IS NULL` |
| Total row counts | Both | Informational |

---

## Stage 4: Curated Layer (Star Schema)

### Glue Job: `spatial_temporal_join`

**Input:** Refined AQ + Refined Weather
**Output:** `s3://.../curated/fact_air_quality_weather/` (Parquet, Iceberg)

**Processing Logic:**

1. **Weather Pivot:** Group weather by `(grid_lat, grid_lon, forecast_time, cycle_date, cycle_hour)`, pivot `variable` column to get one row per grid-cell/time with weather variables as columns.

2. **AQ Grid Mapping:** Round AQ station coordinates to 2 decimal places to match weather grid cells.

3. **Nearest Grid Discovery:** Cross-join AQ stations with weather grid cells, compute Euclidean distance, filter to grids within 2.0 degrees, rank by distance, keep top 4 nearest grids per station/timestamp.

4. **Haversine + IDW Weighting:** Compute actual distance in km via Haversine formula, assign inverse-distance-squared weights (`1 / distance_km²`; weight=1.0 if distance=0), perform Inverse Distance Weighted aggregation over top-4 grids.

5. **Region Assignment:** Classify stations into regions based on lat/lon bounding boxes: `north_america`, `europe`, `asia`, `africa`, `south_america`, `oceania`, `other`.

**Fact Table Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `station_id` | string | `{location}_{country}` |
| `parameter` | string | Pollutant code |
| `actual_value` | double | Measured AQ value |
| `unit` | string | Measurement unit |
| `reading_timestamp` | timestamp | When reading was taken |
| `forecast_value` | double | IDW-weighted weather forecast |
| `forecast_timestamp` | string | Forecast reference time |
| `nearest_grid_lat` | double | Nearest weather grid latitude |
| `nearest_grid_lon` | double | Nearest weather grid longitude |
| `distance_km` | double | Distance to nearest grid (km) |
| `country` | string | Country code |
| `city` | string | City name |
| `region` | string | Continent region |

**Partitioned by:** `event_date` (yyyy-MM-dd), `region`

### Glue Job: `populate_dimensions`

**Input:** Refined AQ data
**Output:** Three dimension tables in `s3://.../curated/`

#### dim_station

| Column | Type | Description |
|--------|------|-------------|
| `station_id` | string | Unique station identifier |
| `name` | string | Station name |
| `country` | string | Country code |
| `city` | string | City name |
| `latitude` | double | Station latitude |
| `longitude` | double | Station longitude |
| `sensor_type` | string | Primary pollutant measured |
| `region` | string | Continent region |

#### dim_date

| Column | Type | Description |
|--------|------|-------------|
| `date_key` | string | `yyyy-MM-dd` |
| `full_date` | date | Full date |
| `year` | int | Year |
| `month` | int | Month (1-12) |
| `day` | int | Day of month |
| `day_of_week` | string | e.g., "Monday" |
| `month_name` | string | e.g., "January" |
| `quarter` | int | Quarter (1-4) |

#### dim_pollutant

| Column | Type | Description |
|--------|------|-------------|
| `pollutant_code` | string | e.g., `pm25` |
| `name` | string | e.g., "PM2.5" |
| `unit` | string | e.g., "ug/m3" |
| `safe_threshold` | double | WHO guideline value |
| `health_category` | string | e.g., "Fine Particulates" |

**Reference Values:**

| Code | Name | Unit | Safe Threshold | Category |
|------|------|------|---------------|----------|
| pm25 | PM2.5 | ug/m3 | 15.0 | Fine Particulates |
| pm10 | PM10 | ug/m3 | 45.0 | Coarse Particulates |
| o3 | Ozone | ug/m3 | 100.0 | Ground-Level Ozone |
| no2 | Nitrogen Dioxide | ug/m3 | 40.0 | Nitrogen Oxides |
| so2 | Sulfur Dioxide | ug/m3 | 20.0 | Sulfur Oxides |
| co | Carbon Monoxide | ug/m3 | 4000.0 | Carbon Monoxide |

---

## Stage 5: Orchestration (Step Functions)

```
┌─────────────────────────────────────────────────────────────────┐
│                Pipeline Orchestrator (Step Functions)            │
│                                                                 │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐      │
│  │ RunNOAADecode│────►│RunRefined   │────►│RunRefined   │      │
│  │  (Fargate)  │     │   NOAA      │     │  OpenAQ     │      │
│  │  retry: 3x  │     │  (Glue)     │     │  (Glue)     │      │
│  └─────────────┘     │  retry: 3x  │     │  retry: 3x  │      │
│                      └─────────────┘     └──────┬──────┘      │
│                                                  │              │
│                                                  ▼              │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐      │
│  │NotifySuccess│◄────│ RunCurated  │◄────│RunData      │      │
│  │  (SNS)      │     │   Join      │     │  Quality    │      │
│  └─────────────┘     │  (Glue)     │     │  (Glue)     │      │
│                      │  retry: 3x  │     │ non-blocking │      │
│                      └─────────────┘     └──────┬──────┘      │
│                                                  │              │
│                               ┌──────────────────┘              │
│                               ▼                                 │
│                        ┌─────────────┐                          │
│                        │NotifyDQ     │                          │
│                        │  Warning    │──(continues to join)     │
│                        │  (SNS)      │                          │
│                        └─────────────┘                          │
│                                                                 │
│  On any failure: ──► NotifyFailure (SNS) ──► END                │
└─────────────────────────────────────────────────────────────────┘

Schedule:
  • Pipeline: EventBridge cron(0 1,7,13,19 * * ? *) — every 6 hours
  • OpenAQ poller: EventBridge rate(1 hour)
```

**Error Handling:**
- NOAA decode, refined, and join steps: retry 3 times with 60s interval and exponential backoff (factor 2)
- Data quality: non-blocking — logs warnings and continues to the next step
- All failures: publish SNS notification and halt pipeline

---

## Stage 6: Storage (S3 Data Lakehouse)

```
{project_prefix}-lakehouse-{account_id}/
│
├── scripts/                          # Glue ETL scripts
│
├── raw/
│   ├── openaq/                       # JSON (gzipped), date-partitioned by Firehose
│   │   └── {yyyy}/{MM}/{dd}/
│   └── noaa_gfs/                     # JSON, partitioned by cycle_date/cycle_hour
│       └── cycle_date={}/cycle_hour={}/
│
├── errors/
│   └── openaq/                       # Firehose error records
│
├── refined/
│   ├── air_quality/                  # Parquet (from refined_openaq.py)
│   └── weather_forecast/             # Parquet (from refined_noaa.py)
│       └── cycle_date={}/cycle_hour={}/
│
└── curated/
    ├── fact_air_quality_weather/     # Parquet/Iceberg, partitioned by event_date/region
    │   └── event_date={}/region={}/
    ├── dim_station/                  # Parquet/Iceberg
    ├── dim_date/                     # Parquet/Iceberg
    └── dim_pollutant/                # Parquet/Iceberg
```

---

## Stage 7: Analytics & Visualization

### Amazon Athena

- Queries curated Iceberg tables via Glue Data Catalog
- Database: `air_quality_weather_db`
- On-demand, pay-per-query

### Grafana (Self-Hosted on EC2)

**EC2 Instance:**
- Ubuntu 22.04 LTS, t3.micro
- Grafana 13.1.1 with Athena datasource plugin
- Port 3000 (HTTP)

**Dashboards:**

| Dashboard | Description |
|-----------|-------------|
| Historical Air Quality Data | 3M+ rows from `openaq_historical`, monthly trends, pollutant distribution, station geomap, data coverage table |
| Live Air Quality Monitoring | Fact table analytics, region breakdown, parameter distribution, recent measurements table |

**Geomap Panels:**
- 50 monitoring stations worldwide
- Bubble size = number of readings
- Bubble color = average PM2.5 value
- Color thresholds: green (<12), yellow (12-35), orange (35-55), red (>55)

---

## Key Metrics

| Metric | Value |
|--------|-------|
| Historical data points | 3,000,000+ |
| Monitoring stations | 50 |
| Regions covered | 7 (north_america, europe, asia, africa, south_america, oceania, other) |
| Date range | 2006-11-13 to 2026-08-04 |
| Pollutant parameters | 20 (pm25, pm10, no2, o3, co, so2, temperature, etc.) |
| Fact table rows | ~2,000 |
| Pipeline frequency | Every 6 hours (batch) + hourly (streaming) |
| Full pipeline duration | ~14 minutes |
| Glue version | 4.0 (PySpark, Python 3) |
| Worker type | Standard, 2 workers |

---

## Alerting

| Alert Type | Trigger | Channel |
|------------|---------|---------|
| Hazardous AQI | PM2.5 or PM10 > 150 µg/m³ | SNS → Email (real-time streaming) |
| Pipeline failure | Any Step Functions step fails | SNS → Email |
| DQ warning | Null values, invalid ranges | SNS → Email (non-blocking) |
| Pipeline success | All steps completed | SNS → Email |

---

## Infrastructure

All infrastructure is managed via Terraform. Key resources:

| Resource | Count | Purpose |
|----------|-------|---------|
| S3 Bucket | 1 | Data lakehouse |
| Kinesis Data Stream | 1 | OpenAQ buffering |
| Kinesis Data Firehose | 1 | S3 delivery |
| Glue Jobs | 5 | ETL processing |
| Glue Catalog Database | 1 | Table metadata |
| Lambda Functions | 2 | Poller + Alert Consumer |
| ECS Task Definition | 1 | NOAA GRIB decoder |
| Step Functions | 1 | Pipeline orchestration |
| EventBridge Schedules | 2 | Trigger pipeline + poller |
| EC2 Instance | 1 | Grafana server |
| SNS Topic | 1 | Alert notifications |
| CloudWatch Log Groups | 5 | Logging |
