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
- **Auth mode (local dev):** Azure CLI (`az login` as workspace admin user)
- **Auth mode (production):** Service Principal — code is in place but commented; see "Why two auth modes" below
- **Run:** `make -C terraform plan|apply|output`
- **Note:** `fabric/bronze_lakehouse.Lakehouse/`, `fabric/silver_lakehouse.Lakehouse/`, `fabric/gold_warehouse.Warehouse/` are auto-exported by Fabric Git for all workspace items — the actual resources are managed by Terraform, not these files.

### Lakehouse: Bronze
- **Purpose:** Raw landing zone — data is never modified after ingestion
- **Format:** Delta Lake (auto-created by Dataflow Gen2 and Pipeline)
- **Tables:**
  - `bronze_taxi_trips` — raw Parquet loaded from TLC
  - `bronze_air_quality` — raw JSON from OpenAQ API
  - `bronze_gdp` — raw JSON from World Bank
  - `bronze_fx_rates` — raw CSV from ECB

### Lakehouse: Silver
- **Purpose:** Cleaned, deduplicated, schema-standardized data
- **Transformations applied:** see [notebooks/silver_etl.ipynb](../notebooks/silver_etl.ipynb)
- **Tables:**
  - `silver_taxi_trips` — parsed timestamps, dropped nulls, deduped by trip_id
  - `silver_air_quality` — flattened measurements, standardized location fields
  - `silver_gdp` — yearly GDP per country, normalized
  - `silver_fx_rates` — daily USD/EUR, clean date index

### Fabric Warehouse (Gold)
- **Purpose:** Analytical star schema optimized for reporting
- **Schema:** see [data_dictionary.md](data_dictionary.md)
- **Access:** SQL endpoint (T-SQL compatible)

### Data Factory
- **Pipeline:** `pl_ingest_nyc_taxi` — copies monthly Parquet files to Bronze
- **Dataflow Gen2:** `df_openaq` — OpenAQ API → Bronze Delta
- **Dataflow Gen2:** `df_worldbank_gdp` — World Bank API → Bronze Delta
- **Dataflow Gen2:** `df_ecb_fx` — ECB CSV → Bronze Delta
- **Orchestration Pipeline:** `pl_master_orchestrator` — runs all ingestion + triggers notebooks

### Notebooks
- `silver_etl.ipynb` — Bronze → Silver transformations (PySpark)
- `gold_etl.ipynb` — Silver → Gold / Warehouse load (PySpark + SQL)
- `analytics.ipynb` — Correlation analysis and visualizations

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
- `silver_air_quality` — partitioned by `year`, `month`
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

| Table | Row count | Size | Refresh cadence |
|-------|-----------|------|-----------------|
| bronze_taxi_trips | ___ | ___ | Monthly |
| silver_taxi_trips | ___ | ___ | Monthly |
| bronze_air_quality | ___ | ___ | Daily |
| silver_air_quality | ___ | ___ | Daily |
| FactTaxiDaily | ___ | ___ | Monthly |
| FactAirQualityDaily | ___ | ___ | Daily |

---

## Security & Governance

- **Row-Level Security:** [configured / not configured] in Power BI Semantic Model
- **Purview Lineage:** [enabled / not enabled]
- **Access control:** Workspace-level roles (Admin / Member / Contributor / Viewer)
