# Microsoft Fabric Data Engineering Project

Unified analytics platform on Microsoft Fabric that integrates NYC Taxi mobility data, OpenAQ air quality, World Bank GDP, ECB FX rates, and Open-Meteo weather into a single data lakehouse and warehouse.

**Architecture:** Medallion (Bronze → Silver → Gold) — raw landing in Lakehouse, cleaning via PySpark Notebooks, analytical star schema in Fabric Warehouse.

**Core technology:** Microsoft Fabric (Lakehouse, Data Factory Pipelines, Dataflow Gen2, PySpark Notebooks, Warehouse), Delta Lake, Python, T-SQL.

**External stack** (introduced later phases): InfluxDB Cloud (weather time-series), Grafana (weather dashboard), Great Expectations (data quality), Telegram Bot (DQ alerts).

## Project structure

```
notebooks/    PySpark notebooks: silver_etl, gold_etl, analytics
pipelines/    Data Factory pipeline definitions (JSON exports from Fabric)
warehouse/    SQL scripts: star schema DDL, stored procedures
jobs/         External Python jobs (added in Phase 5)
docs/         Architecture, data dictionary, how-to-run
spec/         Original project specification (PDF)
```

## Development workflow

| Branch | Phase |
|--------|-------|
| `phase/1-bronze-ingestion` | Data ingestion into Bronze Lakehouse |
| `phase/2-silver-transformation` | PySpark ETL into Silver Lakehouse |
| `phase/3-gold-modeling` | Star schema in Fabric Warehouse |
| `phase/4-visualization` | Power BI / Notebook dashboards |
| `phase/5-governance` | Scheduling, data quality, monitoring |

## Data sources

| Source | Format | Ingestion tool |
|--------|--------|----------------|
| NYC Taxi (TLC) | Parquet, monthly | Data Factory Pipeline |
| OpenAQ Air Quality | JSON API, paginated | Dataflow Gen2 |
| World Bank GDP | JSON API | Dataflow Gen2 |
| ECB FX rates | CSV API | Dataflow Gen2 |
| Open-Meteo Weather | JSON API | Python job (Phase 5) |

## Quick start

```bash
pip install -r requirements.txt
```

Full setup: see [docs/how_to_run.md](docs/how_to_run.md)
Architecture decisions: see [docs/architecture.md](docs/architecture.md)

## Environment

Required env vars are documented in `.env.example`.
Phase 1–3 require no env vars. InfluxDB + Telegram tokens are needed from Phase 5 only.

## Key principles

- **Bronze is immutable** — raw data is never modified after landing; re-run ingestion if you need to fix it.
- **Silver owns the cleaning logic** — all deduplication, null handling, and schema normalization happens in `silver_etl`; Gold only aggregates.
- **Fail loudly** — pipelines should raise errors, not silently skip bad records; data quality failures are surfaced to the user.
- **Parameterize everything** — pipelines use parameters for dates/sources so backfills are trivial.
- **Document decisions** — every non-obvious architectural choice has a "Why" entry in `docs/architecture.md`.
