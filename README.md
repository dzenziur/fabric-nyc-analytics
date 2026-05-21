# NYC Analytics

> Unified analytics platform on **Microsoft Fabric** integrating NYC Taxi mobility, OpenAQ air quality, World Bank GDP, ECB FX rates, and Open-Meteo weather data — built on a medallion architecture (Bronze → Silver → Gold) with Power BI dashboards on top.

![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-Lakehouse%20%2B%20Warehouse-blue)
![Medallion](https://img.shields.io/badge/Architecture-Medallion-orange)
![Power BI](https://img.shields.io/badge/Power%20BI-Direct%20Lake-yellow)
![PySpark](https://img.shields.io/badge/PySpark-Notebooks-red)
![Status](https://img.shields.io/badge/Status-Feature%20Complete-success)

---

## Overview

The platform answers **four cross-domain analytical questions**:

1. How does taxi traffic intensity relate to NYC air quality (PM2.5 / NO₂ / O₃)?
2. Which zones and times of day show the strongest link between taxi demand and pollution peaks?
3. What is average revenue per trip in USD vs EUR, and how does FX fluctuation affect it?
4. Over multiple years, do we see mobility / economic growth at the expense of environmental quality?

All five data sources land in a single Fabric workspace, are cleaned through PySpark notebooks, and surface in a 4-page Power BI report. Two external integrations sit on top: a Grafana weather dashboard backed by InfluxDB, and a Telegram Great Expectations report bot.

---

## Dashboard previews

| Mobility | Air Quality |
|---|---|
| ![Mobility](docs/img/powerbi_mobility.png) | ![Air Quality](docs/img/powerbi_air_quality.png) |
| **Correlation** | **Economic Impact** |
| ![Correlation](docs/img/powerbi_correlation.png) | ![Economic Impact](docs/img/powerbi_economic_impact.png) |

Full visual breakdown: see [`docs/architecture.md`](docs/architecture.md#power-bi-report-nyc-analytics).

---

## Architecture

```
+--------------------------- Microsoft Fabric Workspace ---------------------------+
|                                                                                  |
|  Sources         Bronze Lakehouse     Silver Lakehouse     Gold Warehouse        |
|  -------         ----------------     ----------------     --------------        |
|  NYC TLC      -> bronze_taxi_trips -> silver_taxi_trips -> FactTaxiDaily         |
|  OpenAQ API   -> bronze_openaq_*   -> silver_openaq_*   -> FactAirQualityDaily   |
|  World Bank   -> bronze_gdp        -> silver_gdp        -> DimGDP, DimDate       |
|  ECB CSV      -> bronze_fx_rates   -> silver_fx_rates   -> DimFX, DimZone        |
|  Open-Meteo   -> bronze_weather    -> silver_weather    --+                      |
|                                                           |                      |
|  Orchestration: pl_master_orchestrator (Data Factory)     |                      |
|  Star schema -> Power BI semantic model (Direct Lake)     |                      |
+-----------------------------------------------------------+----------------------+
                                                            v
                  +---------------- External Stack (Docker) -----------------+
                  |                                                          |
                  |  silver_weather -> InfluxDB -> Grafana dashboard         |
                  |  Silver + Gold  -> Great Expectations -> Telegram bot    |
                  |                                                          |
                  +----------------------------------------------------------+
```

Architectural decisions (Why X over Y) documented in [`docs/architecture.md`](docs/architecture.md).

---

## Tech stack

| Layer | Tooling |
|---|---|
| Lakehouse / Warehouse | Microsoft Fabric (OneLake, Delta Lake, T-SQL) |
| ETL | PySpark notebooks, Data Factory pipelines, Dataflows Gen2 |
| Modeling | Star schema in Fabric Warehouse, Direct Lake semantic model |
| BI | Power BI (4 pages, DAX measures, RLS, Azure Maps) |
| IaC | Terraform (workspace, lakehouses, warehouse) |
| External stack | Docker Compose (InfluxDB, Grafana, Python app) |
| Data quality | Great Expectations + Telegram bot |
| CI | Fabric Git integration (notebook + report sync) |

---

## Data sources

| Source | Format | Ingestion tool | Frequency |
|---|---|---|---|
| NYC Taxi (TLC) | Parquet, monthly | Data Factory Pipeline (`pl_ingest_nyc_taxi`) | Monthly (~2-month lag) |
| OpenAQ Air Quality | JSON API + S3 archive | PySpark Notebook (`bronze_ingest_openaq_*`) | Daily |
| World Bank GDP | JSON API | Dataflow Gen2 (`df_worldbank_gdp`) | Yearly |
| ECB FX rates | CSV API | Dataflow Gen2 (`df_ecb_fx`) | Daily |
| Open-Meteo Weather | JSON API | Python job + Notebook (`bronze_ingest_weather`) | Hourly |

Full data dictionary: [`docs/data_dictionary.md`](docs/data_dictionary.md).

---

## Quick start

### Fabric

```bash
# Clone & push
git clone <repo>
cd nyc-analytics

# In Fabric UI: Workspace → Source control → Update all
# Then trigger the platform:
pl_master_orchestrator → Run
```

### Local stack (Docker)

```bash
make build           # build app image
make up              # start influxdb + grafana + weather-sync + telegram bot
make weather-sync-once   # one-shot Fabric → InfluxDB sync
make ge-report           # run Great Expectations, print report
```

- Grafana: <http://localhost:3000>
- InfluxDB: <http://localhost:8086>
- Telegram bot: send `/report` to your configured bot

Full setup (Service Principal, BotFather, `.env`): [`docs/how_to_run.md`](docs/how_to_run.md).

---

## Project structure

```
fabric/       Fabric workspace items — pipelines, dataflows, notebooks, warehouse, semantic model, Power BI report
              Synced via Fabric Git integration
app/          External Python CLI dispatcher (weather-sync, ge-report, Telegram bot)
              Single Docker image, three docker-compose services
terraform/    IaC: workspace, lakehouses, warehouse
grafana/      Provisioned datasource + dashboards
docs/         Architecture, data dictionary, how-to-run, screenshots
spec/         Original project specification (PDF)
Makefile      Compose + IaC shortcuts (`make help`)
```

---

## Documentation

| Question | Document |
|---|---|
| What's the architecture? Why this design? | [`docs/architecture.md`](docs/architecture.md) |
| What columns are in each table? | [`docs/data_dictionary.md`](docs/data_dictionary.md) |
| How do I run it end-to-end? | [`docs/how_to_run.md`](docs/how_to_run.md) |
| What Fabric items exist? | [`fabric/README.md`](fabric/README.md) |

---

## Key principles

- **Bronze is immutable** — raw data is never modified after landing
- **Silver owns cleaning** — all deduplication, normalization, and type casting happens here
- **Fabric is the source of truth** — external Docker stack reads from it; nothing flows back
- **Fail loudly** — pipelines raise on bad data instead of silent skips
- **Document decisions** — every non-obvious architectural choice has a "Why" entry in `docs/architecture.md`
