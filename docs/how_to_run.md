# How to Run the Project

## Prerequisites

1. **Microsoft Fabric trial or paid capacity** (F64 minimum for full features)
   - Activate at: https://app.fabric.microsoft.com → Start trial
2. **Microsoft account** with Fabric access (M365 or Azure AD)
3. **This git repository** — for storing Fabric workspace items, IaC, and documentation

---

## Step 0 — Provision Infrastructure (Terraform)

All Fabric resources (workspace, lakehouses, warehouse) are managed via Terraform. Never create them manually through the UI.

1. Install prerequisites: **Terraform >= 1.5**, **Azure CLI**
2. Create a Service Principal in Azure Entra ID and add it as **Admin** on the Fabric workspace
   (Manage Access → Add people or groups → paste SP application ID → Admin)
3. Fill in variables:
   ```bash
   cd terraform/
   cp terraform.tfvars.example terraform.tfvars
   # Edit terraform.tfvars: tenant_id, client_id, client_secret, capacity_id
   ```
4. Authenticate and apply:
   ```bash
   make login   # verify SP credentials
   make init    # download provider
   make apply   # create workspace + bronze_lakehouse + silver_lakehouse + gold_warehouse
   ```
5. Confirm outputs:
   ```bash
   make output  # shows workspace_id, lakehouse IDs, warehouse ID
   ```

Resources created:
- Workspace `Fabric NYC Analytics`
- `bronze_lakehouse`, `silver_lakehouse`
- `gold_warehouse`

---

## Step 1 — Connect Git (optional but recommended)

1. Workspace Settings → Git integration
2. Connect to this GitHub/Azure DevOps repo
3. Fabric will sync notebooks and pipeline definitions automatically

---

## Step 2 — Configure Data Ingestion (Bronze)

### 2a. NYC Taxi Pipeline

1. In workspace → New → **Data Pipeline** → name: `pl_ingest_nyc_taxi`
2. Add **Copy Data** activity:
   - Source: HTTP connector → URL pattern:
     `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
   - Sink: `bronze_lakehouse` → Files → `raw/taxi/`
3. Parameterize year/month for reuse
4. Test with one file (e.g., `2024-01`) before scheduling

### 2b. OpenAQ Locations Notebook

> Requires `OPENAQ_API_KEY` — register at https://openaq.org (free)

1. Sync repo via Fabric Git integration — `bronze_ingest_openaq_locations` notebook appears in workspace
2. Ensure `bronze_lakehouse` is the default attached lakehouse
3. Run with parameter `openaq_api_key` = your API key
4. Expected: ~24k+ global station records → `bronze_lakehouse.bronze_openaq_locations`

### 2e. OpenAQ Measurements Notebook

1. Sync repo via Fabric Git integration — `bronze_ingest_openaq_measurements` notebook appears in workspace
2. Ensure `bronze_lakehouse` is the default attached lakehouse
3. Run all cells — reads OpenAQ public S3 archive for all NYC stations (last 5 years) → `bronze_openaq_measurements`
4. Expected: ~1.1M rows across ~22 NYC stations

### 2c. World Bank GDP Dataflow Gen2

1. New → **Dataflow Gen2** → name: `df_worldbank_gdp`
2. Source: Web API → `https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?format=json&per_page=20000&date=2000:YYYY` where end year is dynamic (`DateTime.LocalNow() - 1` in M-code)
3. Navigate to second element of JSON array (index 1 contains data), convert to table
4. Expand records → keep: `country` (id, name), `date`, `value`
5. Rename: `country_code`, `country_name`, `year`, `gdp_usd`
6. Destination: `bronze_lakehouse` → Table: `bronze_gdp`

### 2d. ECB FX Dataflow Gen2

1. New → **Dataflow Gen2** → name: `df_ecb_fx`
2. Source: Web → `https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata`
3. Parse CSV, rename columns
4. Destination: `bronze_lakehouse` → Table: `bronze_fx_rates`

---

## Step 3 — Run Silver ETL Notebook

1. Sync `feature/data-orchestration` branch via Fabric Git integration — `silver_etl` notebook appears in workspace automatically
2. Open `silver_etl` notebook → attach `bronze_lakehouse` as additional data item (read source)
3. Default attached lakehouse must be **silver_lakehouse** (write target)
4. Run all cells top to bottom
5. Verify tables exist: `spark.sql("SHOW TABLES IN silver_lakehouse").show()`

```
Expected output tables:
  silver_lakehouse/Tables/silver_taxi_trips            (~2.87M rows, partitioned by year/month)
  silver_lakehouse/Tables/silver_openaq_locations      (~5k rows)
  silver_lakehouse/Tables/silver_openaq_measurements   (~1.1M rows, partitioned by year/month)
  silver_lakehouse/Tables/silver_gdp                   (~6.2k rows)
  silver_lakehouse/Tables/silver_fx_rates              (~7k rows)
```

---

## Step 4 — Run Gold ETL Notebook

1. Sync branch → `gold_etl` notebook appears in workspace automatically
2. Attach **silver_lakehouse** as data item (read source) and **gold_warehouse** as default (write target)
3. Run all cells — creates Fact and Dim tables in Warehouse

```
Expected tables in gold_warehouse:
  dbo.FactTaxiDaily
  dbo.FactAirQualityDaily
  dbo.DimDate
  dbo.DimZone
  dbo.DimFX
  dbo.DimGDP
```

---

## Step 5 — Build Visualizations

### 5a. Semantic Model (required before Power BI reports)

1. Open **gold_warehouse** in workspace → click **New semantic model**
2. Name: `nyc_analytics_model`, storage mode: **Direct Lake on SQL** → select all 6 tables → **Confirm**
3. Open the model → **Model** tab → add relationships:
   - `FactTaxiDaily[date_key]` → `DimDate[date_key]` (Many:1)
   - `FactTaxiDaily[fx_key]` → `DimFX[fx_key]` (Many:1)
   - `FactTaxiDaily[zone_key]` → `DimZone[zone_key]` (Many:1)
   - `FactAirQualityDaily[date_key]` → `DimDate[date_key]` (Many:1)
4. Add DAX measures to **FactTaxiDaily**: `Total Trips`, `Total Revenue USD`, `Total Revenue EUR`, `Avg Fare USD`, `Avg Trip Distance (mi)`, `Avg Trip Duration (min)`
5. Add DAX measures to **FactAirQualityDaily**: `Avg PM2.5`, `Avg NO2`, `Avg O3`, `Max PM2.5`
6. Sync back to Git: workspace → Source control → Commit

### 5b. Power BI Reports

1. In workspace → New → **Report** → pick `nyc_analytics_model` → **Create blank report** → save as `NYC Analytics`
2. Build **Mobility** page: KPI cards (Total Trips, Total Revenue USD, Avg Fare USD, Avg Trip Distance (mi)), trips/day line chart, top 10 pickup zones bar chart
3. Build **Air Quality** page: KPI cards (Avg NO2, Avg O3, Avg PM2.5), year tile slicer, station dropdown slicer (`FactAirQualityDaily.city`), combined PM2.5+NO2+O3 line chart (responds to slicer), top 10 stations by Avg PM2.5 bar chart (does not respond to slicer)
4. Build **Correlation** page: KPI cards (Total Trips, Avg PM2.5, Avg NO2), bar+line combo chart (Total Trips bars + Avg PM2.5 + Avg NO2 lines by month), year tile slicer
5. Build **Economic Impact** page: KPI cards (Total Revenue USD, Total Revenue EUR, USA GDP), clustered column chart (revenue USD vs EUR by year), line chart (USA GDP by year from DimGDP), line chart (USD/EUR exchange rate from DimFX)

---

## Step 6 — Master Orchestrator

1. Sync `feature/data-orchestration` branch — `pl_master_orchestrator` pipeline appears in workspace
2. Pipeline parameters: `year_start` (Int), `year_end` (Int)
3. Activity structure:
   ```
   [Parallel]
     df_ecb_fx                              (Dataflow Gen2)
     df_worldbank_gdp                       (Dataflow Gen2)
     bronze_ingest_openaq_locations         (Notebook, pass openaq_api_key)
     bronze_ingest_openaq_measurements      (Notebook, pass year_start/year_end)
     ForEach year/month → pl_ingest_nyc_taxi (Pipeline, dynamic URL/filename)
   [Then]
     silver_etl                             (Notebook, pass year_start/year_end)
   [Then]
     gold_etl                               (Notebook, pass year_start/year_end)
   ```
4. Run with parameters `year_start=2023`, `year_end=2023` for single-year demo; `year_start=2022`, `year_end=2024` for full backfill

### Typical activity durations (measured 2026-05-12)

| Activity | 1 year | 2 years |
|----------|--------|---------|
| ForEach_taxi_months (12 months/yr, parallel) | ~2m 54s | ~3m 30s–4m 3s |
| Each ingest_taxi_month (Copy Data) | ~2m 3–4s | ~2m 3–4s |
| df_worldbank_gdp | ~1m 1s | ~1m 4–17s |
| df_ecb_fx | ~1m 2s | ~1m 17–18s |
| bronze_ingest_openaq_locations | ~2m 18s | ~1m 49s–2m 5s |
| bronze_ingest_openaq_measurements | ~4m 38s | ~4m 53s–8m 38s |
| silver_etl | ~6m 9s | ~7m 8s–12m 14s |
| gold_etl | ~2m 22s | ~2m 37s–3m 22s |

Note: `bronze_ingest_openaq_measurements` and `silver_etl` scale with cumulative data volume — later years (2024–2025) have more station coverage than earlier years.

Full run estimate: ~5–7 min (bronze parallel) + ~6–12 min silver + ~2–3 min gold ≈ **~15–22 min per 2-year range**.

---

## Step 7 — Weather External Job + InfluxDB (Phase 7 — not yet implemented)

### InfluxDB Cloud setup
1. Register at https://cloud2.influxdata.com (free tier)
2. Create bucket: `nyc_analytics`
3. Generate API token (All Access)
4. Save: `INFLUXDB_URL`, `INFLUXDB_TOKEN`, `INFLUXDB_ORG`, `INFLUXDB_BUCKET`

### Run weather ingest job
```bash
pip install influxdb-client requests

# Set env vars
export INFLUXDB_URL="https://us-east-1-1.aws.cloud2.influxdata.com"
export INFLUXDB_TOKEN="your_token_here"
export INFLUXDB_ORG="your_org"
export INFLUXDB_BUCKET="nyc_analytics"

# Run once manually (script added in Phase 5)
# python jobs/weather_ingest.py

# Schedule (Linux cron, every hour):
# 0 * * * * /usr/bin/python /path/to/jobs/weather_ingest.py
```

### Grafana setup
1. Register at https://grafana.com (free cloud) or run locally:
   ```bash
   docker run -d -p 3000:3000 grafana/grafana
   ```
2. Add data source: InfluxDB → Flux mode → paste token + org + bucket
3. Import dashboard from `grafana/dashboards/weather_nyc.json`

---

## Step 7b — Great Expectations + Telegram Bot (Phase 7 — not yet implemented)

### Great Expectations setup
```bash
pip install great-expectations

# Initialize GE project (run once)
great_expectations init

# Run a checkpoint manually
great_expectations checkpoint run silver_taxi_checkpoint
# → generates HTML report in great_expectations/uncommitted/data_docs/
```

### Telegram Bot setup
1. Create bot via @BotFather in Telegram → get `BOT_TOKEN`
2. Get your chat ID: message @userinfobot
3. Set env vars:
   ```bash
   export TELEGRAM_BOT_TOKEN="your_bot_token"
   export TELEGRAM_CHAT_ID="your_chat_id"
   ```
4. Run bot:
   ```bash
   pip install python-telegram-bot
   python bot/dq_bot.py
   ```
5. In Telegram: send `/report` → bot runs GE → replies with pass/fail summary

### Available bot commands
- `/report` — run all checkpoints, send summary
- `/report silver_taxi_trips` — run specific table
- `/status` — show last run results without re-running

---

## Step 7c — Schedule Automation (Phase 6)

1. `pl_master_orchestrator` already exists — sync `feature/data-orchestration` branch
2. Activity structure (same as Step 5 above)
3. Schedule:
   - Daily at 06:00 UTC for FX + Air Quality + OpenAQ
   - Monthly trigger for Taxi (manual or first-of-month cron)

---

## Full Run Order (Manual)

```
1. Run df_ecb_fx                                  → bronze_fx_rates
2. Run df_worldbank_gdp                           → bronze_gdp
3. Run bronze_ingest_openaq_locations             → bronze_openaq_locations
4. Run bronze_ingest_openaq_measurements          → bronze_openaq_measurements
5. Run pl_ingest_nyc_taxi (per year/month)        → Files/raw/taxi/
6. Run silver_etl notebook                        → silver_* tables
7. Run gold_etl notebook                          → Fact/Dim tables in Warehouse
8. Refresh Power BI                               → Reports update
--- Phase 7 additions ---
8. python jobs/weather_ingest.py → bronze_weather + InfluxDB
9. Run silver_etl notebook (weather) → silver_weather (+ GE validation)
10. Open Grafana             → Weather dashboard live
11. Send /report to bot      → DQ report in Telegram
```

---

## Verification Checks

After each phase, run these sanity checks:

```sql
-- Bronze: check row counts
SELECT COUNT(*) FROM bronze_lakehouse.bronze_taxi_trips;
SELECT COUNT(*) FROM bronze_lakehouse.bronze_openaq_locations;

-- Silver: check no nulls in key columns
SELECT COUNT(*) FROM silver_lakehouse.silver_taxi_trips WHERE pickup_datetime IS NULL;

-- Gold: check fact table has data for expected dates
SELECT MIN(date_key), MAX(date_key), COUNT(*) FROM gold_warehouse.dbo.FactTaxiDaily;

-- Cross-check: trips in silver vs gold should match (aggregated)
SELECT SUM(trip_count) FROM gold_warehouse.dbo.FactTaxiDaily;
```

---

## Delta Time Travel (demo / debugging)

```python
# In any Fabric Notebook — read Bronze at version 0 (original ingest)
df = spark.read.format("delta").option("versionAsOf", 0).load("abfss://bronze@onelake.../Tables/bronze_taxi_trips")

# Or by timestamp
df = spark.read.format("delta").option("timestampAsOf", "2025-05-01").load("...")

# View history
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "abfss://...")
dt.history().show()
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Dataflow Gen2 fails on OpenAQ pagination | Reduce `limit` to 500; check API key if using authenticated endpoint |
| Taxi Parquet file not found | URL uses 2-month lag — use files from 2+ months ago |
| Notebook can't write to silver_lakehouse | Ensure silver_lakehouse is the default attached lakehouse; use `notebookutils.lakehouse.get("bronze_lakehouse")` to build correct ABFS paths for cross-lakehouse reads |
| Warehouse table not visible in Power BI | Wait ~2 min after creation; refresh dataset connection |
| Delta Time Travel fails | Delta log may be expired (default 30 days retention); increase with `delta.logRetentionDuration` |
