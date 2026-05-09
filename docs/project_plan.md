# Project Plan

## Summary

Unified analytics platform on Microsoft Fabric integrating:
- **NYC Taxi** (Parquet, monthly) — mobility data
- **OpenAQ** (JSON API, paginated) — air quality (PM2.5, NO2, O3)
- **World Bank** (JSON API) — GDP per country
- **ECB** (CSV API) — USD/EUR FX rates
- **Open-Meteo Weather** (JSON API, free) — hourly NYC weather

Architecture: **Bronze → Silver → Gold** (Medallion) via Lakehouse + Warehouse + Power BI
External stack (Phase 6): **InfluxDB** + **Grafana** + **Great Expectations** + **Telegram / Discord Bot**

---

## Phase 1 — Data Ingestion / Bronze

### NYC Taxi (Data Factory Pipeline)
- Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Format: Parquet (CloudFront URLs, monthly, ~2-month lag)
- Task: Copy Activity in Pipeline → OneLake Bronze Lakehouse (Files section)
- Note: test on 1–2 months first — files are large

### OpenAQ — Location Metadata (Dataflow Gen2)
- Source: OpenAQ API v3 `/v3/locations` — https://docs.openaq.org
- Format: JSON API, pagination via `page` + `limit=1000`
- Task: Dataflow Gen2 `df_openaq_locations` → flatten location records → `bronze_openaq_locations` (station metadata only)
- Columns: location_id, location_name, timezone, country_id, country_name, latitude, longitude
- Note: API key stored in Fabric Connections (not hardcoded in Dataflow)

### OpenAQ — Measurements (PySpark Notebook)
- Source: OpenAQ public S3 archive — `s3://openaq-data-archive/records/csv.gz/`
- Format: CSV.gz, Hive-partitioned by `locationid=` / `year=` / `month=`
- Task: Notebook `bronze_ingest_openaq_measurements` → read all NYC stations (filtered by bounding box: lat 40.4–40.9, lon −74.3 to −73.7) → `bronze_openaq_measurements`
- No credentials required (public AWS Open Data Registry bucket)
- Year range configurable via notebook parameters (default: up to current year)

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
- `silver_taxi_trips` — snake_case columns, year/month partition, invalid trips filtered
- `silver_openaq_locations` — location metadata, deduped by location_id
- `silver_openaq_measurements` — pollutant readings for NYC stations, year/month partition
- `silver_gdp` — yearly GDP per country, nulls dropped
- `silver_fx_rates` — daily USD/EUR rates, deduped by date

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

Power BI dashboards (all in single `NYC Analytics` report):
1. **Mobility** — trips/day trend, avg fare, busiest pickup zones, revenue USD vs EUR by year
2. **Air Quality** — PM2.5/NO2/O3 daily trends by location, top stations by Avg PM2.5
3. **Correlation** — dual-axis overlay of Total Trips + Avg PM2.5 by date, year slicer
4. **Economic Impact** — revenue USD/EUR by year, USA GDP trend from DimGDP

Semantic model: `nyc_analytics_model` — Direct Lake on SQL, 4 relationships, DAX measures in FactTaxiDaily, FactAirQualityDaily, DimGDP.

---

## Phase 5 — Master Orchestrator

Single-entry-point pipeline that runs the entire data platform end-to-end with configurable year range.

### Parameterization
- `pl_master_orchestrator` pipeline — parameters: `year_start` (int), `year_end` (int)
- `silver_etl` notebook — parameters: `year_start`, `year_end` (partition-level overwrite)
- `gold_etl` notebook — parameters: `year_start`, `year_end` (partition-level overwrite)
- `bronze_ingest_openaq_measurements` — already has `year_start`, `year_end`
- `pl_ingest_nyc_taxi` — already has `year`, `month`; wrapped in ForEach loop

### Pipeline structure
```
pl_master_orchestrator(year_start, year_end)
  [Parallel]
    df_ecb_fx
    df_openaq_locations
    df_worldbank_gdp
    bronze_ingest_openaq_measurements(year_start, year_end)
    ForEach(year, month) → pl_ingest_nyc_taxi(year, month)
  [Then] silver_etl(year_start, year_end)
  [Then] gold_etl(year_start, year_end)
```

### Data backfill
- Run orchestrator for 2022–2024 to populate multi-year trends in Power BI dashboards
- Fix city names in FactAirQualityDaily (join `silver_openaq_locations` on `location_id`)

---

## Phase 6 — Governance & External Integrations

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

### Telegram / Discord Bot
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
| Time-series DB | InfluxDB Cloud (Phase 6) |
| External dashboards | Grafana (Phase 6) |
| Data quality | Great Expectations (Phase 6) |
| DQ notifications | Telegram / Discord Bot (Phase 6) |
| Reporting | Power BI
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
| Telegram / Discord Bot | python-telegram-bot / discord.py docs |
