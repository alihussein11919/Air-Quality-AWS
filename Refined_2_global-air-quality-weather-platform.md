# Global Air Quality & Weather Intelligence Platform

A serverless, AWS-native data engineering project that joins real-time global air-quality readings with NOAA weather forecast data in an Iceberg lakehouse, queried through Athena to keep costs minimal — designed to run entirely within an AWS Academy Learner Lab account.

## 1. Project Goal

Build an end-to-end pipeline that:
1. Ingests real-time air quality measurements (PM2.5, PM10, NO2, O3, CO, SO2) from OpenAQ, with a real-time alerting path for hazardous readings.
2. Ingests NOAA GFS weather forecast data (temperature, wind, precipitation, humidity) directly from its public S3 bucket.
3. Joins the two — using distance-weighted spatial interpolation, not a naive nearest-neighbor match — in a Medallion-architecture lakehouse.
4. Serves curated, query-ready tables via Athena on Apache Iceberg (no warehouse cluster to pay for).
5. Visualizes trends, forecast-vs-actual patterns, and live alerts on a self-hosted Grafana dashboard.

## 2. Real-World Applications & Primary Use Case

Joining live air-quality readings with a weather *forecast* (not just historical weather) unlocks a "predict tomorrow, not just report today" capability. That's the differentiator worth building the project around.

**Primary use case (flagship framing): Wildfire smoke & public-health early warning**
- Correlate live AQ sensor spikes with wind-direction/speed forecasts to project smoke plume movement and downwind AQI impact hours before it arrives — the same category of capability behind tools like EPA AirNow.
- The real-time Kinesis alerting path (Stage 1) *is* the early-warning system: sub-minute detection of a hazardous reading, not something you'd only see after the next batch job runs.

**Other applications this pipeline supports:**
- **Urban planning / government** — informing low-emission-zone triggers, and identifying neighborhoods that chronically get bad air under specific wind patterns.
- **Insurance & real estate** — chronic air-quality exposure as an underwriting/valuation input.
- **Logistics & outdoor operations** — pausing construction or outdoor events ahead of forecasted particulate/dust events.
- **Corporate ESG / compliance** — separating a facility's own emissions from wind-blown pollution using the weather join.
- **Consumer/lifestyle** — commute route AQI comparison, air-purifier auto-scheduling (the shallowest use case on this list, but the most familiar one).

## 3. Data Sources

### OpenAQ — Air Quality
- **Historical/bulk**: Public S3 archive on the AWS Open Data Registry (`registry.opendata.aws/openaq`) — good for backfilling and initial lake population.
- **Real-time**: REST API (`explore.openaq.org`, API key required) — polled on a schedule for live measurements from ~4,000+ global stations.
- **Format**: JSON.
- **Key fields**: location, coordinates, parameter (PM2.5, NO2, etc.), value, unit, timestamp.

### NOAA Global Forecast System (GFS) — Weather
- **Source**: Public S3 bucket on the AWS Open Data Registry (`registry.opendata.aws/noaa-gfs-bdp-pds`), hosted in `us-east-1`.
- **Update cadence**: 4x/day (00Z, 06Z, 12Z, 18Z cycles).
- **Format**: GRIB2 (decoded via `cfgrib`/eccodes inside a custom Fargate container — see Stage 2).
- **Key fields**: temperature, wind speed/direction, precipitation, humidity, soil moisture — global grid, ~0.25° resolution.

## 4. Architecture, Stage by Stage

### Stage 1 — Streaming Ingestion (OpenAQ)

```
EventBridge Scheduler (every N min)
        │
        ▼
Lambda Producer (polls OpenAQ API, put_records to Kinesis)
        │
        ▼
   Kinesis Data Streams  ──────────────┬──────────────────
        │                              │
        ▼                              ▼
   Firehose                    Lambda Consumer (event source
   (buffer + batch)             mapping on the stream)
        │                              │
        ▼                              ▼
   S3 Raw Zone                  Evaluate each reading against
   (archival, feeds              WHO/EPA thresholds in real time
   the batch pipeline)                 │
                                        ▼
                                 SNS Topic → email/SMS alert
                                 ("hazardous AQI detected in
                                  <region>, PM2.5 = X")
```

Kinesis fans out to two independent consumers rather than being a pass-through: Firehose handles archival, a Lambda consumer handles real-time hazard detection. This is what makes the real-time alerting/early-warning use case (Section 2) actually real rather than aspirational.

### Stage 2 — Batch Ingestion (NOAA GFS)

```
EventBridge Scheduler (cron: 00Z, 06Z, 12Z, 18Z — matches GFS cycles)
        │
        ▼
ECS Fargate Task (custom Docker image, cfgrib/eccodes bundled in)
        │
        ├─ Pulls GRIB2 files from NOAA GFS S3 bucket (in-place read)
        ├─ Decodes, extracts needed variables
        └─ Writes structured Parquet output
        │
        ▼
S3 Raw Zone (raw_noaa_gfs_forecast, partitioned by cycle_date/cycle_hour)
```

Fargate (not Glue Python Shell or Lambda) because GRIB2 decoding needs the `eccodes` C library — a system-level dependency Glue Python Shell can't install and Lambda's /tmp and memory ceilings make fragile for files this size. A custom container gives full control over the environment.

### Stage 3 — Storage / Lake Format

- **Table format**: Apache Iceberg across all three zones (raw/refined/curated) — schema evolution, time travel, and ACID guarantees matter as the dataset's variables grow over time.
- **Catalog**: AWS Glue Data Catalog + your own S3 bucket (not Amazon S3 Tables — too uncertain a fit for a Learner Lab). Glue's automatic Iceberg table optimization (compaction + snapshot/orphan-file cleanup) enabled on the curated zone specifically, where the streaming ingestion would otherwise cause small-file buildup.
- **Partitioning**:

| Table | Partition by |
|---|---|
| `raw_openaq_measurements` | `ingest_date` |
| `raw_noaa_gfs_forecast` | `cycle_date`, `cycle_hour` |
| `fact_air_quality_weather` (curated) | `event_date`, `region` |

`region` is a derived attribute (not present in either source), computed during the refined→curated transform from station lat/lon and added to `dim_station` for consistency.

### Stage 4 — Catalog & Governance

No AWS Lake Formation — fine-grained permissions require IAM admin-level setup the Learner Lab's single `LabRole` doesn't allow. Governance is instead approximated through:
- Glue Data Catalog table registration
- S3 bucket-prefix isolation (`/raw/`, `/refined/`, `/curated/`)
- SSE-S3 encryption at rest
- An **optional, non-blocking** data quality check between raw and refined (basic range/completeness checks — not a hard gate for now, can be tightened later)

Worth being explicit about this gap in interviews: in a production account, Lake Formation would handle column-level masking and role-based access; here it's approximated through prefix isolation given the Learner Lab's constraints.

### Stage 5 — Transformation

| Transform | Tool |
|---|---|
| Raw → Refined: OpenAQ cleaning (dedupe, unit standardization) | Glue Studio visual job, DataBrew recipe node |
| Raw → Refined: NOAA flattening (grid cell → row) | Glue Studio visual job, custom PySpark node |
| **Refined → Curated: spatial-temporal join** | Glue Studio visual job, custom PySpark node — **inverse-distance-weighted (IDW) interpolation** across the 4 nearest GFS grid points per station (weight = 1/distance², normalized), not naive nearest-neighbor, to avoid artificial step-changes in weather values near grid boundaries |
| Curated: dimension table builds (`dim_station`, `dim_date`, `dim_pollutant`) | Glue Studio visual job, mostly built-in nodes |

DataBrew has no native Iceberg connector, so it's used as a recipe node embedded inside a Glue Studio job (Iceberg source → DataBrew recipe → Iceberg target), not standalone.

### Stage 6 — Orchestration

Single **Step Functions Standard workflow**, triggered by EventBridge Scheduler on the GFS cycle (NOAA-cadence-driven, not a separate schedule per source):

```
1. Start Fargate task (NOAA decode)         — .sync, Retry + Catch → SNS on failure
2. Glue Studio job: refined-NOAA transform  — .sync
3. Glue Studio job: refined-OpenAQ transform — .sync (processes whatever's
                                                accumulated in raw since last run)
4. (optional) Data quality check            — non-blocking; SNS warning on fail, continues
5. Glue Studio job: curated join (IDW)      — .sync
6. Success → SNS notification
```

Each `.sync` step has its own retry block (exponential backoff, 2–3 attempts) before falling through to the SNS failure alert — this is deliberately visible in the Step Functions console as a fault-tolerant design, not just a happy-path chain.

**Known tradeoff**: the curated table refreshes only 4x/day (NOAA cadence), while the Stage 1 real-time alert path is sub-minute. Two freshness tiers in one system — alerting is "hot path," the curated warehouse is not — which mirrors how real systems separate the two, but is worth being able to explain if asked why the dashboard doesn't instantly reflect an alert that just fired.

### Stage 7 — Query / Serving Layer

- Athena engine v3 (required for native Iceberg support).
- Dedicated workgroup (not `primary`), own query result S3 location.
- Rely on **Iceberg's native manifest-based partition pruning** — no partition projection config needed (a real technical distinction from Hive tables, worth citing if asked "why Iceberg here").
- Query result reuse enabled (~60 min TTL) — free cost saver for repeated dashboard queries.
- **Per-query scan cutoff: 1 GB hard limit** on the workgroup — the actual budget guardrail against a runaway unpartitioned query.
- CloudWatch alarm on the workgroup's `ProcessedBytes` metric → same SNS topic used elsewhere.

### Stage 8 — BI / Visualization

- **Primary, live dashboard: self-hosted Grafana on a small EC2 instance**, running under `LabInstanceProfile`, using the official `grafana-athena-datasource` plugin with **AWS SDK Default auth** — this picks up the instance's IAM role credentials automatically via the instance metadata service, so there's no manual credential refresh even though Learner Lab STS credentials expire every session. (Amazon Managed Grafana and QuickSight were both ruled out — same category of risk as being unavailable/uncertain in a Learner Lab, plus added cost.)
- Dashboard content: AQI trend by station/region, forecast-vs-actual overlay (the piece that makes the "predict tomorrow" pitch visible), a station map colored by current AQI, and an active-alerts panel tied to the Stage 1 real-time consumer.
- **Secondary artifact: Power BI Desktop**, built periodically (accepting a one-time manual STS credential paste each session) specifically as a portfolio artifact showcasing the Power BI certification — not the live system.

### Stage 9 — Monitoring & Observability

Default/native CloudWatch monitoring for now — no dedicated Grafana "Pipeline Health" dashboard yet (can be added later on the same EC2 Grafana instance if desired). All of the following are emitted natively by CloudWatch with no custom instrumentation:

| Signal | Why it matters |
|---|---|
| Step Functions execution status/duration | Did today's run actually complete |
| Fargate task failures/duration | NOAA decode step silently failing or running long |
| Glue job success/duration | Same, for the three transform stages |
| Athena bytes scanned | Cost guardrail (already wired to SNS in Stage 7) |
| Kinesis `GetRecords.IteratorAge` | Catches the real-time alert consumer falling behind — easy to forget, and without watching it "real-time" alerting can silently stop being real-time |
| Lambda errors/throttles | Standard health signal for both the OpenAQ producer and the alert consumer |

CloudWatch alarms on the critical ones (Step Functions failure, Fargate failure, Kinesis IteratorAge threshold) route to the same SNS topic used throughout the pipeline.

## 5. AWS Academy Learner Lab Considerations

This project is designed to run entirely inside a Learner Lab account, which imposes constraints beyond a normal AWS account. The points below are confirmed against the account's official service-restriction documentation, not assumed:

- **No custom IAM roles/policies.** IAM access is "extremely limited" — you cannot create users, groups, or roles (service-linked roles are the one exception). Only the pre-created `LabRole` (and `LabInstanceProfile` for EC2) are available. If a service-to-service call fails with an authorization error, it's a missing permission on `LabRole` you can't add yourself — **test each service-to-service permission early**, in isolation, before wiring the full pipeline together.
- **ECR push access — important correction:** `LabRole` itself has **read-only** access to ECR. Pulling the Fargate container image at task runtime (under `LabRole`) works fine, but **pushing** the built image to ECR must be done using your own logged-in console/CLI credentials (the Learner Lab's provided access key), not `LabRole`. Build and push locally with those credentials; only the pull happens under `LabRole` at runtime.
- **Fargate task definition setup**: explicitly set `LabRole` as **both** the task role *and* the task execution role when creating the task definition — the Learner Lab docs flag this specifically as a common source of permission errors if skipped.
- **Glue ETL job limits**: only `G.1X` and `Standard` worker types are allowed, capped at a **maximum of 10 workers**, and **maximum concurrency of 1** (a given job definition can't have multiple runs executing at once). This doesn't conflict with the design here since the Step Functions workflow already runs the Glue Studio jobs sequentially, not in parallel — but it's worth knowing if you're ever tempted to parallelize the refined-zone transforms.
- **Lambda concurrency cap**: a maximum of **10 concurrent execution environments** account-wide. Not a real constraint at this project's scale (producer + alert consumer), but worth keeping in mind if the design grows.
- **Kinesis Firehose setup**: when creating the delivery stream, use "Advanced settings" to select the existing `LabRole` — it's not the default path in the console wizard.
- **Lake Formation, QuickSight, and Amazon Managed Grafana are all avoided** — each requires either IAM admin-level setup or is a separate managed service outside what's listed as supported. Governance is approximated via S3 prefix isolation; BI runs on self-hosted Grafana on EC2 instead.
- **Redshift confirmed as a poor fit even setting cost aside**: the account only supports provisioned Redshift (`ra3.large`, max 2-instance cluster) — no mention of Redshift Serverless as a supported configuration — which reinforces the Athena-on-Iceberg choice from Stage 7 independent of the original cost reasoning.
- **EMR avoided in favor of Fargate/Glue Studio.** EMR clusters don't survive a Learner Lab session ending (stopping an EMR cluster isn't supported by AWS — the session end just stops the underlying EC2 instances), on top of the shared 32-vCPU / 9-instance cap across all EC2-backed services.
- **EC2 instance sizing (for the Grafana host in Stage 8)**: only `nano`, `micro`, `small`, `medium`, and `large` instance types are supported — anything larger gets terminated automatically. On-demand only, no Spot. Stick to `t3.micro` or `t3.small` for Grafana; it's a dashboard host, not a compute-heavy workload.
- **Region-locked to `us-east-1` / `us-west-2`.** Both data sources are already hosted in `us-east-1`.
- **Session and budget limits.** Sessions run for a fixed window and auto-stop; running EC2/RDS/EMR/ECS instances get stopped (not terminated) at session end and will restart automatically — and keep consuming budget — the next time you start a session unless you stop them yourself first. Push code continuously to GitHub, and explicitly stop the Grafana EC2 instance and any lingering resources before ending each session.
- **Bedrock availability varies by course** — any future GenAI data-quality step is a stretch goal, not a dependency.

## 6. Infrastructure as Code (Terraform)

Infrastructure is codified in Terraform rather than clicked together in the console — especially valuable in a Learner Lab, since sessions expire and get torn down, so `terraform apply`/`destroy` replaces manually rebuilding all 9 stages every session.

**Learner Lab-specific adjustments:**
- **Credential refresh**: Learner Lab issues temporary STS credentials that expire with the session (~4 hrs). A small script exports fresh `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` at the start of every session before running `plan`/`apply` — not configured once and forgotten.
- **`LabRole` / `LabInstanceProfile` are referenced, not created**: use `data "aws_iam_role"` / `data "aws_iam_instance_profile"` data sources, not `aws_iam_role` resources, since custom IAM roles aren't available anyway.
- **State backend stays local** (`.gitignore`'d), not S3-remote — a remote backend adds a bootstrapping problem (the state bucket needs to exist before Terraform can use it) that isn't worth solving for a solo project in an ephemeral lab account. Tradeoff: state won't survive a machine change, which is fine to state explicitly if asked.

**Module layout**, mirroring the 9 architecture stages so the repo structure explains the architecture on its own:

```
terraform/
  streaming/       (Lambda producer+consumer, Kinesis, Firehose, SNS)
  batch/           (Fargate task def, ECR repo, EventBridge rule)
  storage/         (S3 buckets, Glue Data Catalog, Iceberg table DDL via Glue)
  transform/       (Glue Studio jobs)
  orchestration/   (Step Functions state machine, EventBridge trigger)
  serving/         (Athena workgroup)
  bi/              (EC2 instance for Grafana)
  monitoring/      (CloudWatch alarms)
```

## 7. Why This Project Stands Out

- Two genuinely different ingestion patterns (streaming JSON API with a real dual-consumer fan-out, plus in-place processing of scientific binary formats via a custom container) — beyond the typical CSV-to-Parquet portfolio project.
- A real spatial-interpolation join (IDW across 4 grid points), not a naive nearest-neighbor match — defensible technical depth if asked why.
- Fully serverless and cost-conscious throughout: Athena-on-Iceberg instead of a standing warehouse, Fargate instead of EMR, self-hosted Grafana instead of a managed BI service, explicit per-query cost guardrails.
- A coordinated, fault-tolerant orchestration layer (Step Functions with retry/catch) rather than independent cron jobs.
- Infrastructure fully codified in Terraform, module-per-stage — reproducible across ephemeral Learner Lab sessions, not console click-ops.
- Directly maps to AWS Certified Data Engineer – Associate exam domains (ingestion, storage/catalog, transform, orchestration, governance, monitoring).
- Built and delivered within real-world platform constraints (Learner Lab's IAM and resource limits) — a good interview story about working within guardrails, not just greenfield access.
- A concrete, defensible business use case (wildfire smoke / public-health early warning) backed by an actual real-time alerting path, not just a generic dashboard pitch.
