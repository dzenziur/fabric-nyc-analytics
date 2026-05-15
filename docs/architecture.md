# Architecture Overview

## High-Level Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          EXTERNAL DATA SOURCES                              │
├───────────────────┬──────────────────────┬─────────────────┬────────────────┤
│  NYC Taxi (TLC)   │  OpenAQ API + S3     │  World Bank API │  ECB FX API    │
│  Parquet / month  │  JSON / CSV.gz       │  JSON           │  CSV           │
└────────┬──────────┴──────────┬───────────┴────────┬────────┴───────┬────────┘
         │                     │                    │                │
  pl_ingest_nyc_taxi   bronze_ingest_openaq_ df_worldbank_gdp   df_ecb_fx
  Data Factory          locations +           Dataflow Gen2      Dataflow Gen2
  Pipeline              bronze_ingest_openaq_
                        measurements Notebooks
         │                     │                    │                │
         └─────────────────────┴────────────────────┴────────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │  pl_master_orchestrator │
                           │  year_start / year_end  │
                           └────────────┬────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │      BRONZE LAKEHOUSE   │
                           │  raw · immutable        │
                           │  OneLake · Delta Lake   │
                           └────────────┬────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │   silver_etl Notebook   │
                           │   PySpark               │
                           └────────────┬────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │      SILVER LAKEHOUSE   │
                           │  clean · normalized     │
                           │  Delta Lake             │
                           └────────────┬────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │   gold_etl Notebook     │
                           │   PySpark               │
                           └────────────┬────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │      FABRIC WAREHOUSE   │
                           │  star schema · T-SQL    │
                           │  FactTaxiDaily          │
                           │  FactAirQualityDaily    │
                           │  DimDate · DimZone      │
                           │  DimFX · DimGDP         │
                           └────────────┬────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │   nyc_analytics_model   │
                           │   Direct Lake on SQL    │
                           └────────────┬────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │   NYC ANALYTICS REPORT  │
                           │   Mobility              │
                           │   Air Quality           │
                           │   Correlation           │
                           │   Economic Impact       │
                           └─────────────────────────┘

┌─── Phase 7 ────────────────────────────────────────────────────────────────────────┐
│  Open-Meteo API ──► Python Job ──► Bronze Lakehouse ──► silver_etl                 │
│                          │                                  └──► FactWeatherDaily  │
│                          └──► InfluxDB Cloud ──► Grafana Dashboard                 │
│                                                                                    │
│  Great Expectations ──► Silver validation ──► Telegram / Discord Bot ──► DQ report │
└────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Components

### Microsoft Fabric Workspace
- **Name:** `Fabric NYC Analytics`
- **Capacity:** Fabric Trial (F64-equivalent, 60 days)
- **Region:** Poland Central
- **Provisioned via:** Terraform (`terraform/workspace.tf`)

### Terraform Infrastructure
- **Provider:** `microsoft/fabric` (~> 1.0)
- **Manages:** workspace, lakehouses (bronze, silver), warehouse (gold)
- **Does NOT manage:** Dataflow Gen2 definitions, Pipeline definitions, Notebook content (provider does not support these — synced via Fabric Git integration instead)
- **Auth mode (default):** Service Principal (`tenant_id`/`client_id`/`client_secret` in `terraform.tfvars`) — works on Fabric Trial and paid F-SKU
- **Auth mode (fallback):** Azure CLI (`use_cli = true`) — for quick local tests without an SP
- **Run:** `make -C terraform plan|apply|output`
- **Note:** `fabric/bronze_lakehouse.Lakehouse/`, `fabric/silver_lakehouse.Lakehouse/`, `fabric/gold_warehouse.Warehouse/` are auto-exported by Fabric Git for all workspace items — the actual resources are managed by Terraform, not these files.

### Lakehouse: Bronze
- **Purpose:** Raw landing zone — data is never modified after ingestion
- **Format:** Delta Lake (auto-created by Dataflow Gen2 and Pipeline)
- **Tables:**
  - `bronze_taxi_trips` — raw Parquet loaded from TLC (via Pipeline)
  - `bronze_openaq_locations` — OpenAQ location metadata (24k+ global stations) via `bronze_ingest_openaq_locations` Notebook (replaced Dataflow Gen2 for API key security)
  - `bronze_openaq_measurements` — OpenAQ pollutant readings for 22 NYC stations, last 5 years, via PySpark Notebook reading public S3 archive
  - `bronze_gdp` — World Bank yearly GDP per country via Dataflow Gen2
  - `bronze_fx_rates` — ECB daily USD/EUR rates via Dataflow Gen2

### Lakehouse: Silver
- **Purpose:** Cleaned, deduplicated, schema-standardized data
- **Transformations applied:** see [fabric/silver_etl.Notebook/notebook-content.py](../fabric/silver_etl.Notebook/notebook-content.py)
- **Tables:**
  - `silver_taxi_trips` — renamed columns to snake_case, dropped nulls, deduped by (pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, fare_amount), partitioned by year/month
  - `silver_openaq_locations` — location metadata, deduped by location_id, rows with null location_id or country_id dropped
  - `silver_openaq_measurements` — pollutant readings for NYC stations, value > 0, deduped by (location_id, parameter, datetime), partitioned by year/month; gas parameters (no2, o3, co, no, nox, so2) normalized from ppm to µg/m³ using EPA conversion factors at 25°C
  - `silver_gdp` — yearly GDP per country, nulls dropped, cast to correct types
  - `silver_fx_rates` — daily USD/EUR, deduped by date, nulls dropped

### Fabric Warehouse (Gold)
- **Purpose:** Analytical star schema optimized for reporting
- **Schema:** see [data_dictionary.md](data_dictionary.md)
- **Access:** SQL endpoint (T-SQL compatible)

### Data Factory
- **Pipeline:** `pl_ingest_nyc_taxi` — copies monthly Parquet files to Bronze; parameters: `year` (int), `month` (int); source URL and destination filename are dynamically built from parameters
- **Pipeline:** `pl_master_orchestrator` — single-entry-point orchestrator; parameters: `year_start` (int), `year_end` (int); runs all ingestion in parallel (ForEach taxi months + notebooks + dataflows), then triggers silver_etl and gold_etl sequentially
- **Dataflow Gen2:** `df_worldbank_gdp` — World Bank API → `bronze_gdp`; end year is dynamic (`DateTime.LocalNow() - 1`)
- **Dataflow Gen2:** `df_ecb_fx` — ECB CSV → `bronze_fx_rates` (full history, no date filter needed)

### Power BI Semantic Model
- **Item:** `fabric/nyc_analytics_model.SemanticModel/`
- **Storage mode:** Direct Lake on SQL (delegated identity mode, DirectQuery fallback enabled)
- **Source:** `gold_warehouse` SQL analytics endpoint
- **Tables:** DimDate, DimFX, DimGDP, DimZone, FactTaxiDaily, FactAirQualityDaily
- **Relationships:**
  - `FactTaxiDaily[date_key]` → `DimDate[date_key]` (Many:1, active)
  - `FactTaxiDaily[fx_key]` → `DimFX[fx_key]` (Many:1, active)
  - `FactTaxiDaily[zone_key]` → `DimZone[zone_key]` (Many:1, active)
  - `FactAirQualityDaily[date_key]` → `DimDate[date_key]` (Many:1, active)
  - `DimGDP` — no relationship (used as standalone context table)
- **DAX measures in FactTaxiDaily:** Total Trips, Total Revenue USD, Total Revenue EUR, Avg Fare USD, Avg Trip Distance (mi), Avg Trip Duration (min)
- **DAX measures in FactAirQualityDaily:** Avg PM2.5, Avg NO2, Avg O3
- **DAX measures in DimGDP:** USA GDP (USD) — `CALCULATE(MAX(DimGDP[gdp_usd]), DimGDP[country_code] = "US")`

### Power BI Report: NYC Analytics
- **Item:** `fabric/NYC Analytics.Report/`
- **Semantic model:** `nyc_analytics_model`
- **Pages:**
  - **Mobility** — KPI cards (Total Trips, Total Revenue USD, Avg Fare USD, Avg Trip Distance (mi)), year tile slicer, trips/day trend, top 10 pickup zones by trip count
  - **Air Quality** — KPI cards (Avg NO2, Avg O3, Avg PM2.5), year tile slicer, station dropdown slicer, combined PM2.5+NO2+O3 daily trend (responds to slicer), top 10 stations by Avg PM2.5
  - **Correlation** — KPI cards (Total Trips, Avg PM2.5, Avg NO2), bar+line combo chart (Total Trips bars + Avg PM2.5 + Avg NO2 lines, monthly aggregation), year tile slicer
  - **Economic Impact** — KPI cards (Total Revenue USD, Total Revenue EUR, USA GDP), clustered column chart (revenue USD vs EUR by year), line chart (USA GDP 2000–present), line chart (USD/EUR exchange rate full history)

### Notebooks
All notebooks live in `fabric/` as Fabric Notebook items synced via Git integration. There is no separate `notebooks/` directory.
- `fabric/bronze_ingest_openaq_locations.Notebook/` — fetches all OpenAQ station metadata via API v3 (paginated) → `bronze_openaq_locations`; parameter: `openaq_api_key`
- `fabric/bronze_ingest_openaq_measurements.Notebook/` — reads OpenAQ public S3 archive for NYC stations (filtered by bounding box) → `bronze_openaq_measurements`; parameters: `year_start`, `year_end`
- `fabric/silver_etl.Notebook/` — Bronze → Silver transformations (PySpark): all 5 data sources; parameters: `year_start`, `year_end`; handles TLC Parquet schema drift (INT32/INT64) via file-by-file read with explicit casts; normalizes OpenAQ gas measurements from ppm to µg/m³
- `fabric/gold_etl.Notebook/` — Silver → Gold / Warehouse load (PySpark + synapsesql); parameters: `year_start`, `year_end`

### Weather External Job — Phase 7 (not yet implemented)
- **Source:** Open-Meteo API (free, no key) — hourly weather for NYC (lat 40.71, lon -74.01)
- **Script:** `jobs/weather_ingest.py` — runs on schedule (cron / Azure Function / Railway.app)
- **Sink:** InfluxDB Cloud + Bronze Lakehouse for Silver processing

### InfluxDB Cloud — Phase 7 (not yet implemented)
- **Purpose:** Time-series store for weather data
- **Bucket:** `nyc_analytics`
- **Access:** InfluxDB Cloud free tier (us-east-1)

### Grafana — Phase 7 (not yet implemented)
- **Purpose:** Weather + mobility dashboard outside of Fabric
- **Data source:** InfluxDB Cloud via Flux query language
- **Hosting:** Grafana Cloud free tier or local Docker

### Great Expectations — Phase 7 (not yet implemented)
- **Purpose:** Data quality validation on Silver tables
- **Trigger:** Telegram / Discord Bot command `/report`

### Telegram / Discord Bot — Phase 7 (not yet implemented)
- **Purpose:** User-friendly DQ trigger and report delivery
- **Command:** `/report [table_name]` → runs GE checkpoint → replies with pass/fail summary
- **Implementation:** `bot/dq_bot.py` using python-telegram-bot

---

## Architectural Decisions

### Why boto3 for OpenAQ S3 ingestion (not Spark S3A)

Fabric Spark runs on Azure infrastructure with a hardcoded AWS credential provider chain
(`TemporaryAWSCredentialsProvider → SimpleAWSCredentialsProvider → EnvironmentVariableCredentialsProvider → IAMInstanceCredentialsProvider`).
`AnonymousAWSCredentialsProvider` cannot be injected — neither `spark.conf.set`,
`sc._jsc.hadoopConfiguration().set()`, nor `%%configure` override the chain after Fabric
initializes it. Therefore, native `spark.read.csv("s3a://...")` fails on public S3 buckets.
**Solution:** `boto3` with `Config(signature_version=UNSIGNED)` runs in the Python layer,
bypasses Spark's S3A entirely, and achieves anonymous access. Downloads are parallelized via
`ThreadPoolExecutor`, then converted to Spark DataFrames for Delta writes.

### Why Terraform for infrastructure
- **Reproducibility:** workspace + all 3 storage layers can be destroyed and recreated in <2 minutes
- **Audit trail:** all infra changes go through Git, not click-ops
- **Auth:** Service Principal is the default (`tenant_id`/`client_id`/`client_secret` in `terraform.tfvars`). Works on Fabric Trial and paid F-SKU alike. Azure CLI auth (`use_cli = true`) is available as a fallback for quick local tests without an SP.
- **Limitation accepted:** the provider does not yet cover Dataflow Gen2, Pipelines, or Notebook content — these are managed via Fabric Git integration (workspace ↔ repo sync of JSON definitions)
- **Alternative considered:** Pulumi — has a Fabric provider too, but smaller community and ecosystem. Terraform is the industry default for IaC.

### Why Open-Meteo for Weather?
- Completely free, no API key, no rate limits for historical + forecast
- Returns hourly JSON with temperature, precipitation, windspeed, weather_code
- Alternative: OpenWeatherMap (requires API key, limited free tier)

### Why InfluxDB for Weather Data?
- Native time-series storage: data is indexed by timestamp — queries like "avg temp per hour" are 10–100× faster than on a relational DB
- First-class Grafana integration (official data source plugin)
- Free cloud tier sufficient for this project's data volume
- Alternative considered: TimescaleDB (PostgreSQL extension) — more setup, less Grafana-native

### Why Grafana (not Power BI) for Weather?
- Power BI cannot connect to InfluxDB natively
- Grafana is the industry standard for time-series monitoring dashboards
- Demonstrates understanding of polyglot persistence (different DB for different use cases)

### Why Great Expectations?
- Industry-standard Python library for data quality validation
- Generates human-readable HTML reports and machine-readable JSON
- Supports both pandas and Spark backends
- Alternative: dbt tests — but dbt is harder to integrate with Fabric Notebooks

### Why Telegram / Discord Bot (not email)?
- Demonstrates event-driven / interactive data quality monitoring
- Low-latency: report arrives within seconds of command
- More engaging for a defense demo than "it sends an email"

### Zone-level air quality correlation — known limitation

OpenAQ sensor IDs and TLC taxi zone IDs are different geographic systems with no shared key. OpenAQ stations are identified by `location_id` (a global integer) with lat/lon coordinates; TLC zones are polygons referenced by `LocationID` (1–265). Joining them requires external geocoding (reverse geocode sensor lat/lon to a TLC zone polygon), which is not implemented.

As a result, `FactAirQualityDaily` cannot be directly joined to `FactTaxiDaily` at the zone level. The Correlation page shows city-wide aggregates (all stations averaged, all zones summed) — not zone-specific correlation. This is a known architectural constraint, not a bug.

### Why USA national GDP (not NYC GDP)

The World Bank API provides GDP data at country level only. There is no city-level GDP available from this source. USA national GDP (`NY.GDP.MKTP.CD` for country code `USA`) is the closest available macro-economic context for NYC taxi demand. The limitation is intentional and acceptable for academic scope.

### Why Gold uses read-filter-union-overwrite (not append)

Fabric Warehouse is written via `synapsesql`, which only supports `mode("overwrite")` (full truncate+insert) or `mode("append")`. Delta Lake `replaceWhere` is not available.

- **Append** is non-idempotent: re-running the orchestrator for the same year range duplicates every row.
- **Full overwrite** deletes all years except the current run range — running for 2024 would erase 2021–2023.
- **Read-filter-union-overwrite:** read the existing table, filter out rows in the target year range (the rows we're about to replace), union the fresh data in, write all rows back. This is idempotent (same result regardless of how many times run) and preserves all years outside the current range.

`DimDate` uses the same principle extended in the other direction: it reads the existing min/max year from the warehouse and extends the date spine to cover both the existing range and the new range, so no dates are lost on partial re-runs.

### Why Medallion (Bronze/Silver/Gold)?
- **Bronze** preserves raw data forever — allows re-processing if Silver logic changes
- **Silver** decouples cleaning from modeling — reusable by multiple Gold layers
- **Gold** is optimized for query performance, not storage efficiency
- Alternative considered: flat Lambda architecture — rejected due to higher operational complexity

### Why Dataflow Gen2 for APIs, Pipeline for Files?
- Dataflow Gen2 has native Power Query M for JSON/CSV transformations and pagination
- Data Factory Pipeline Copy Activity is optimized for large binary file transfers
- Mixing them would add unnecessary complexity

### Why Star Schema in Warehouse (not flat table)?
- Separates measures (facts) from descriptive attributes (dimensions)
- Enables additive aggregations across any dimension
- Power BI DAX measures work most efficiently against star schema
- Easier to extend: new fact table can reuse existing DimDate/DimZone

### Why Delta Lake for Bronze and Silver?
- ACID transactions prevent partial writes on pipeline failure
- Time Travel enables re-processing historical data without re-ingestion
- Schema evolution support for when source APIs change fields

### Partitioning Strategy
- `silver_taxi_trips` — partitioned by `year`, `month` (aligns with source file cadence)
- `silver_openaq_locations` — not partitioned (static locations table, no date dimension)
- `silver_gdp` — not partitioned (small table, ~6k rows)
- `silver_fx_rates` — not partitioned (small table, ~7k rows)
- Gold/Warehouse — no partitioning (managed by Fabric Warehouse engine)

---

## Data Flow — End-to-End

```
pl_master_orchestrator(year_start, year_end)
  [Parallel]
    df_ecb_fx                              (Dataflow Gen2)
    df_worldbank_gdp                       (Dataflow Gen2, end year dynamic)
    bronze_ingest_openaq_locations         (Notebook, openaq_api_key parameter)
    bronze_ingest_openaq_measurements      (Notebook, year_start/year_end)
    ForEach(year × month) → pl_ingest_nyc_taxi  (Pipeline, year/month dynamic URL)
  [Sequential]
    silver_etl(year_start, year_end)       (Notebook, partition-level overwrite)
  [Sequential]
    gold_etl(year_start, year_end)         (Notebook, synapsesql write)
  [Always-on]
    Power BI reads Warehouse via Direct Lake semantic model
```

---

## Key Numbers (fill in after first run)

| Table | Row count | Refresh cadence |
|-------|-----------|-----------------|
| bronze_taxi_trips (Files) | 2,964,624 | Monthly |
| silver_taxi_trips | 2,869,710 | Monthly |
| bronze_openaq_locations | 5,000 | Daily |
| silver_openaq_locations | 5,000 | Daily |
| bronze_gdp | 6,384 | Yearly |
| silver_gdp | 6,193 | Yearly |
| bronze_fx_rates | 7,058 | Daily |
| silver_fx_rates | 6,996 | Daily |
| FactTaxiDaily | 6,856 | Monthly |
| FactAirQualityDaily | 49,287 | Daily |

---

## Security & Governance

- **Row-Level Security:** not configured (Phase 6 — optional)
- **Purview Lineage:** not enabled (Phase 6 — optional)
- **Access control:** Workspace-level roles (Admin / Member / Contributor / Viewer)
