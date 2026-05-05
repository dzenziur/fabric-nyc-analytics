# Architecture Overview

## High-Level Diagram

```
External Sources
    │
    ├── NYC Taxi (Parquet, CloudFront)   ──► Data Factory Pipeline ─────────────┐
    ├── OpenAQ API (JSON, paginated)     ──► Dataflow Gen2 ────────────────────►│
    ├── World Bank API (JSON)            ──► Dataflow Gen2 ────────────────────►│
    ├── ECB FX API (CSV)                ──► Dataflow Gen2 ────────────────────►│
    └── Open-Meteo Weather API (JSON)   ──► Python External Job ───────────────►│
                                                                                 │
                                                                    ┌────────────▼────────────┐
                                                                    │   BRONZE LAKEHOUSE       │
                                                                    │   (raw, immutable)       │
                                                                    │   OneLake / Delta        │
                                                                    └────────────┬────────────┘
                                                                                 │
                                                               PySpark Notebook (Silver ETL)
                                                                                 │
                                                                    ┌────────────▼────────────┐
                                                                    │   SILVER LAKEHOUSE       │
                                                                    │   (clean, normalized)    │
                                                                    │   Delta tables           │
                                                                    └────────────┬────────────┘
                                                                                 │
                                                               PySpark Notebook (Gold ETL)
                                                                                 │
                                                                    ┌────────────▼────────────┐
                                                                    │   FABRIC WAREHOUSE       │
                                                                    │   (star schema / Gold)   │
                                                                    │   SQL endpoint           │
                                                                    └────────────┬────────────┘
                                                                                 │
                                                                    ┌────────────┴────────────┐
                                                                    │                         │
                                                               Power BI                  Notebooks
                                                              Reports               (matplotlib/plotly)

Open-Meteo ──► Python Job (enriched data) ──► InfluxDB Cloud ──► Grafana Dashboard

Great Expectations ──► Checkpoint run ──► Telegram / Discord Bot ──► DQ Report to user
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
  - `bronze_air_quality` — OpenAQ location metadata (station list, ~5k rows) via Dataflow Gen2
  - `bronze_air_quality_measurements` — OpenAQ pollutant readings for 22 NYC stations, last 5 years, via PySpark Notebook reading public S3 archive
  - `bronze_gdp` — World Bank yearly GDP per country via Dataflow Gen2
  - `bronze_fx_rates` — ECB daily USD/EUR rates via Dataflow Gen2

### Lakehouse: Silver
- **Purpose:** Cleaned, deduplicated, schema-standardized data
- **Transformations applied:** see [fabric/silver_etl.Notebook/notebook-content.py](../fabric/silver_etl.Notebook/notebook-content.py)
- **Tables:**
  - `silver_taxi_trips` — renamed columns to snake_case, dropped nulls, deduped by (pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, fare_amount), partitioned by year/month
  - `silver_air_quality` — location metadata, deduped by location_id, rows with null location_id or country_id dropped
  - `silver_air_quality_measurements` — pollutant readings for NYC stations, value > 0, deduped by (location_id, parameter, datetime), partitioned by year/month
  - `silver_gdp` — yearly GDP per country, nulls dropped, cast to correct types
  - `silver_fx_rates` — daily USD/EUR, deduped by date, nulls dropped

### Fabric Warehouse (Gold)
- **Purpose:** Analytical star schema optimized for reporting
- **Schema:** see [data_dictionary.md](data_dictionary.md)
- **Access:** SQL endpoint (T-SQL compatible)

### Data Factory
- **Pipeline:** `pl_ingest_nyc_taxi` — copies monthly Parquet files to Bronze
- **Dataflow Gen2:** `df_openaq` — OpenAQ API v3 `/locations` → `bronze_air_quality` (station metadata); API key stored in Fabric Connections
- **Dataflow Gen2:** `df_worldbank_gdp` — World Bank API → `bronze_gdp`
- **Dataflow Gen2:** `df_ecb_fx` — ECB CSV → `bronze_fx_rates`
- **Orchestration Pipeline:** `pl_master_orchestrator` — runs all ingestion + triggers notebooks

### Notebooks
All notebooks live in `fabric/` as Fabric Notebook items synced via Git integration. There is no separate `notebooks/` directory.
- `fabric/bronze_ingest_openaq_measurements.Notebook/` — reads OpenAQ public S3 archive for 22 NYC stations × last 5 years → `bronze_air_quality_measurements`
- `fabric/silver_etl.Notebook/` — Bronze → Silver transformations (PySpark): all 4 data sources + air quality measurements
- `fabric/gold_etl.Notebook/` — Silver → Gold / Warehouse load (PySpark + SQL) *(Phase 3)*
- `fabric/analytics.Notebook/` — Correlation analysis and visualizations *(Phase 4)*

### Weather External Job (Python)
- **Source:** Open-Meteo API (free, no key) — hourly weather for NYC (lat 40.71, lon -74.01)
- **Script:** `jobs/weather_ingest.py` — added in Phase 5; runs on schedule (cron / Azure Function / Railway.app)
- **Enrichment:** joins weather readings with taxi trip counts by hour and zone
- **Sink:** InfluxDB Cloud (measurement: `nyc_weather_enriched`)
- Also writes raw JSON → Bronze Lakehouse for Silver processing

### InfluxDB Cloud
- **Purpose:** Time-series store for weather + enriched taxi data
- **Bucket:** `nyc_analytics`
- **Measurements:** `weather_hourly`, `taxi_daily`, `nyc_weather_enriched`
- **Access:** InfluxDB Cloud free tier (us-east-1)

### Grafana
- **Purpose:** Weather + mobility dashboard outside of Fabric
- **Data source:** InfluxDB Cloud via Flux query language
- **Dashboards:** `Weather NYC`, `Weather vs Taxi Demand`
- **Hosting:** Grafana Cloud free tier or local Docker

### Great Expectations (Phase 5)
- **Purpose:** Data quality validation on Silver tables
- **Suite files:** `ge/expectations/silver_taxi_trips.json`, etc. (added in Phase 5)
- **Trigger:** Telegram/Discord bot command `/report`
- **Output:** HTML report + JSON result summary

### Telegram / Discord Bot (Phase 5)
- **Purpose:** User-friendly DQ trigger and report delivery
- **Command:** `/report [table_name]` → runs GE checkpoint → replies with pass/fail summary
- **Implementation:** `bot/dq_bot.py` using python-telegram-bot (added in Phase 5)
- **Hosting:** local during defense; Railway.app or Azure Container Instance in production

---

## Architectural Decisions

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

### Why Telegram/Discord Bot (not email)?
- Demonstrates event-driven / interactive data quality monitoring
- Low-latency: report arrives within seconds of command
- More engaging for a defense demo than "it sends an email"

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
- `silver_air_quality` — not partitioned (static locations table, no date dimension)
- `silver_gdp` — not partitioned (small table, ~6k rows)
- `silver_fx_rates` — not partitioned (small table, ~7k rows)
- Gold/Warehouse — no partitioning (managed by Fabric Warehouse engine)

---

## Data Flow — End-to-End

```
1. [Trigger] Master Pipeline runs (scheduled or manual)
2. [Parallel] Dataflow Gen2 × 3 + Pipeline ingest to Bronze Lakehouse
3. [Sequential] silver_etl Notebook reads Bronze → writes Silver
4. [Sequential] gold_etl Notebook reads Silver → loads Fabric Warehouse
5. [Always-on] Power BI Semantic Model / Notebook reads Warehouse
```

---

## Key Numbers (fill in after first run)

| Table | Row count | Refresh cadence |
|-------|-----------|-----------------|
| bronze_taxi_trips (Files) | 2,964,624 | Monthly |
| silver_taxi_trips | 2,869,710 | Monthly |
| bronze_air_quality | 5,000 | Daily |
| silver_air_quality | 5,000 | Daily |
| bronze_gdp | 6,384 | Yearly |
| silver_gdp | 6,193 | Yearly |
| bronze_fx_rates | 7,058 | Daily |
| silver_fx_rates | 6,996 | Daily |
| FactTaxiDaily | ___ | Monthly |
| FactAirQualityDaily | ___ | Daily |

---

## Security & Governance

- **Row-Level Security:** [configured / not configured] in Power BI Semantic Model
- **Purview Lineage:** [enabled / not enabled]
- **Access control:** Workspace-level roles (Admin / Member / Contributor / Viewer)
