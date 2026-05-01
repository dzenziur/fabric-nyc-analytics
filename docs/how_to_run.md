# How to Run the Project

## Prerequisites

1. **Microsoft Fabric trial or paid capacity** (F64 minimum for full features)
   - Activate at: https://app.fabric.microsoft.com → Start trial
2. **Microsoft account** with Fabric access (M365 or Azure AD)
3. **This git repository** — for storing notebooks, SQL scripts, and documentation

---

## Step 1 — Set Up Fabric Workspace

1. Go to https://app.fabric.microsoft.com
2. Create a new **Workspace** → name it `fabric-analytics-project`
3. Assign to Fabric capacity (Trial or F64)
4. Inside the workspace, create these items:
   - **Lakehouse** → name: `bronze_lakehouse`
   - **Lakehouse** → name: `silver_lakehouse`
   - **Warehouse** → name: `gold_warehouse`

---

## Step 2 — Connect Git (optional but recommended)

1. Workspace Settings → Git integration
2. Connect to this GitHub/Azure DevOps repo
3. Fabric will sync notebooks and pipeline definitions automatically

---

## Step 3 — Configure Data Ingestion (Bronze)

### 3a. NYC Taxi Pipeline

1. In workspace → New → **Data Pipeline** → name: `pl_ingest_nyc_taxi`
2. Add **Copy Data** activity:
   - Source: HTTP connector → URL pattern:
     `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
   - Sink: `bronze_lakehouse` → Files → `raw/taxi/`
3. Parameterize year/month for reuse
4. Test with one file (e.g., `2024-01`) before scheduling

### 3b. OpenAQ Dataflow Gen2

1. New → **Dataflow Gen2** → name: `df_openaq`
2. New source → REST API → URL: `https://api.openaq.org/v3/measurements`
3. Add pagination: Query param `page`, increment until empty response
4. Flatten JSON → select columns → Destination: `bronze_lakehouse` → Table: `bronze_air_quality`

### 3c. World Bank GDP Dataflow Gen2

1. New → **Dataflow Gen2** → name: `df_worldbank_gdp`
2. Source: Web API → `https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&per_page=100`
3. Navigate to second element of JSON array (index 1 contains data)
4. Destination: `bronze_lakehouse` → Table: `bronze_gdp`

### 3d. ECB FX Dataflow Gen2

1. New → **Dataflow Gen2** → name: `df_ecb_fx`
2. Source: Web → `https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata`
3. Parse CSV, rename columns
4. Destination: `bronze_lakehouse` → Table: `bronze_fx_rates`

---

## Step 4 — Run Silver ETL Notebook

1. Upload `notebooks/silver_etl.ipynb` to workspace (or create via Git sync)
2. Attach to **silver_lakehouse**
3. Run all cells top to bottom
4. Verify tables exist in silver_lakehouse → Tables section

```
Expected output tables:
  silver_lakehouse/Tables/silver_taxi_trips
  silver_lakehouse/Tables/silver_air_quality
  silver_lakehouse/Tables/silver_gdp
  silver_lakehouse/Tables/silver_fx_rates
```

---

## Step 5 — Run Gold ETL Notebook

1. Upload/open `notebooks/gold_etl.ipynb`
2. Attach to **gold_warehouse** (or silver_lakehouse with cross-workspace write)
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

## Step 6 — Build Visualizations

**Option A — Notebooks (faster):**
- Open `notebooks/analytics.ipynb`
- Run cells to generate matplotlib/plotly charts

**Option B — Power BI:**
1. In workspace → New → **Report**
2. Connect to `gold_warehouse` SQL endpoint
3. Import tables, build relationships, create visuals

---

## Step 6b — Weather External Job + InfluxDB

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

## Step 6c — Great Expectations + Telegram Bot

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

## Step 7 — Set Up Automation (Master Orchestrator)

1. New → **Data Pipeline** → name: `pl_master_orchestrator`
2. Add activities in order:
   ```
   [Parallel]
     df_ecb_fx (Dataflow Gen2 activity)
     df_openaq (Dataflow Gen2 activity)
     df_worldbank_gdp (Dataflow Gen2 activity)
     pl_ingest_nyc_taxi (Pipeline activity, only on new month)
   [Then]
     silver_etl notebook (Notebook activity)
   [Then]
     gold_etl notebook (Notebook activity)
   ```
3. Schedule:
   - Daily at 06:00 UTC for FX + Air Quality
   - Monthly trigger for Taxi (manual or first-of-month cron)

---

## Full Run Order (Manual)

```
1. Run df_ecb_fx             → bronze_fx_rates
2. Run df_worldbank_gdp      → bronze_gdp
3. Run df_openaq             → bronze_air_quality
4. Run pl_ingest_nyc_taxi    → Files/raw/taxi/
5. Run silver_etl.ipynb      → silver_* tables
6. Run gold_etl.ipynb        → Fact/Dim tables in Warehouse
7. Refresh Power BI          → Reports update
--- Phase 5 additions ---
8. python jobs/weather_ingest.py → bronze_weather + InfluxDB
9. Run silver_etl.ipynb (weather) → silver_weather (+ GE validation)
10. Open Grafana             → Weather dashboard live
11. Send /report to bot      → DQ report in Telegram
```

---

## Verification Checks

After each phase, run these sanity checks:

```sql
-- Bronze: check row counts
SELECT COUNT(*) FROM bronze_lakehouse.bronze_taxi_trips;
SELECT COUNT(*) FROM bronze_lakehouse.bronze_air_quality;

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
| Notebook can't write to silver_lakehouse | Check notebook is attached to correct lakehouse; use `spark.conf.set` for cross-lakehouse writes |
| Warehouse table not visible in Power BI | Wait ~2 min after creation; refresh dataset connection |
| Delta Time Travel fails | Delta log may be expired (default 30 days retention); increase with `delta.logRetentionDuration` |
