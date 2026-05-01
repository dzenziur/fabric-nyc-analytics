# Project Plan

## Summary

Unified analytics platform on Microsoft Fabric integrating:
- **NYC Taxi** (Parquet, monthly) — mobility data
- **OpenAQ** (JSON API, paginated) — air quality (PM2.5, NO2, O3)
- **World Bank** (JSON API) — GDP per country
- **ECB** (CSV API) — USD/EUR FX rates
- **Open-Meteo Weather** (JSON API, free) — hourly NYC weather

Architecture: **Bronze → Silver → Gold** (Medallion) via Lakehouse + Warehouse + Power BI
External stack (Phase 5): **InfluxDB** + **Grafana** + **Great Expectations** + **Telegram Bot**

---

## Phase 1 — Data Ingestion / Bronze

### NYC Taxi (Data Factory Pipeline)
- Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Format: Parquet (CloudFront URLs, monthly, ~2-month lag)
- Task: Copy Activity in Pipeline → OneLake Bronze Lakehouse (Files section)
- Note: test on 1–2 months first — files are large

### OpenAQ (Dataflow Gen2)
- Source: https://docs.openaq.org/about/about
- Format: JSON API, pagination via `page` + `limit=1000`
- Task: Dataflow Gen2 → flatten JSON → Delta table in Bronze Lakehouse
- Note: free-tier rate limits apply

### World Bank GDP (Dataflow Gen2)
- Source: `https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json`
- Format: JSON (nested array)
- Task: Dataflow Gen2 → extract yearly GDP → Delta table

### ECB FX (Dataflow Gen2)
- Source: `https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata`
- Format: CSV
- Task: Dataflow Gen2 → Delta table

---

## Phase 2 — Transformation / Silver

PySpark Notebooks in Fabric:
- Standardize schemas across all datasets
- Deduplicate records
- Parse and normalize date/time fields
- Add derived columns (hour, day_of_week, month, year)
- Write cleaned Delta tables → Silver Lakehouse

Key tables:
- `silver_taxi_trips` — cleaned trips with parsed timestamps
- `silver_air_quality` — flattened measurements with location/time
- `silver_gdp` — yearly GDP per country
- `silver_fx_rates` — daily USD/EUR rates

---

## Phase 3 — Data Modeling / Gold

Fabric Warehouse — Star Schema:

**Fact tables:**
- `FactTaxiDaily` — daily trips, fares, distances per zone
- `FactAirQualityDaily` — daily avg/max pollutants per location

**Dimension tables:**
- `DimDate` — full date spine with calendar attributes
- `DimZone` — NYC taxi zones (join with TLC zone lookup CSV)
- `DimFX` — daily USD/EUR rates
- `DimGDP` — yearly GDP per country

---

## Phase 4 — Analytics & Visualization

Power BI or Notebook visualizations:
1. **Mobility Dashboard** — trips/day, avg fare, busiest zones
2. **Air Quality Dashboard** — PM2.5/NO2 trends by day and location
3. **Mobility vs Air Quality Correlation** — overlay taxi trips with pollution spikes
4. **Economic Impact Dashboard** — revenue in USD vs EUR, GDP context

---

## Phase 5 — Governance & External Integrations

### Weather ingestion
- Source: Open-Meteo API (free, no key) — hourly NYC weather
- Script: `jobs/weather_ingest.py` → writes to Bronze Lakehouse + InfluxDB Cloud
- Silver table: `silver_weather` (temp, precipitation, windspeed)
- Warehouse: `FactWeatherDaily`

### Grafana dashboard
- Data source: InfluxDB Cloud
- Panels: temperature/precipitation over time, weather vs taxi demand

### Great Expectations
- Validate Silver tables: null checks, value ranges, allowed categories
- Suites stored in `ge/expectations/`

### Telegram Bot
- Command `/report` → runs GE checkpoint → replies with pass/fail summary
- Script: `bot/dq_bot.py`

### Governance
- Automated refresh schedules (daily for FX/OpenAQ/Weather, monthly for Taxi/GDP)
- Row-Level Security in Power BI (optional)
- Microsoft Purview lineage (optional)

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Cloud platform | Microsoft Fabric (Lakehouse, Warehouse, Data Factory, Notebooks) |
| Storage format | Delta Lake (Bronze + Silver), T-SQL tables (Gold) |
| ETL | Data Factory Pipelines + Dataflow Gen2 + PySpark |
| Time-series DB | InfluxDB Cloud (Phase 5) |
| External dashboards | Grafana (Phase 5) |
| Data quality | Great Expectations (Phase 5) |
| DQ notifications | Telegram Bot (Phase 5) |
| Reporting | Power BI / Fabric Notebooks (matplotlib/plotly) |
| Version control | Git |

---

## Learning Resources

| Topic | Resource |
|-------|----------|
| Fabric overview | Microsoft Learn: "Get started with Microsoft Fabric" |
| Lakehouse + Delta | Microsoft Learn: "Work with Delta Lake tables in Fabric" |
| Dataflow Gen2 | Microsoft Learn: "Ingest data with Dataflows Gen2" |
| PySpark Notebooks | Microsoft Learn: "Use Apache Spark in Fabric" |
| Fabric Warehouse | Microsoft Learn: "Get started with data warehousing in Fabric" |
| Medallion architecture | Databricks blog: "What is the Medallion Lakehouse Architecture" |
| InfluxDB + Python | InfluxDB docs: influxdb-client-python |
| Grafana + InfluxDB | Grafana docs: "InfluxDB data source" |
| Great Expectations | docs.greatexpectations.io — "Quickstart" |
| Telegram Bot | python-telegram-bot docs |
