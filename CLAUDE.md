# Microsoft Fabric Data Engineering Project

Unified analytics platform on Microsoft Fabric that integrates NYC Taxi mobility data, OpenAQ air quality, World Bank GDP, ECB FX rates, and Open-Meteo weather into a single data lakehouse and warehouse.

**Architecture:** Medallion (Bronze → Silver → Gold) — raw landing in Lakehouse, cleaning via PySpark Notebooks, analytical star schema in Fabric Warehouse.

**Core technology:** Microsoft Fabric (Lakehouse, Data Factory Pipelines, Dataflow Gen2, PySpark Notebooks, Warehouse), Delta Lake, Python, T-SQL.

**Infrastructure as Code:** Terraform with `microsoft/fabric` provider — manages workspace + lakehouses + warehouse declaratively. See `terraform/`.

**External stack** (introduced later phases): InfluxDB Cloud (weather time-series), Grafana (weather dashboard), Great Expectations (data quality), Telegram Bot (DQ alerts).

## Project structure

```
fabric/       All Fabric workspace items: dataflows, pipelines, notebooks, warehouse SQL
              Synced automatically via Fabric Git integration
jobs/         External Python jobs — run outside Fabric (added in Phase 5)
terraform/    IaC: workspace, lakehouses, warehouse (run `make help`)
docs/         Architecture, data dictionary, how-to-run
spec/         Original project specification (PDF)
```

## Development workflow

| Branch | Phase |
|--------|-------|
| `feature/terraform-iac` | Phase 0 — Infrastructure as Code (workspace, lakehouses, warehouse) |
| `feature/data-ingestion` | Phase 1 — Data ingestion into Bronze Lakehouse |
| `feature/data-transformation` | Phase 2 — PySpark ETL into Silver Lakehouse |
| `feature/data-modeling` | Phase 3 — Star schema in Fabric Warehouse |
| `feature/data-visualization` | Phase 4 — Power BI / Notebook dashboards |
| `feature/data-governance` | Phase 5 — Scheduling, data quality, monitoring |

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

| Variable | Phase | Purpose |
|----------|-------|---------|
| `OPENAQ_API_KEY` | Phase 1 | OpenAQ v3 API — required for `df_openaq` Dataflow |
| `INFLUXDB_URL/TOKEN/ORG/BUCKET` | Phase 5 | InfluxDB Cloud — weather time-series (add when starting Phase 5) |
| `TELEGRAM_BOT_TOKEN/CHAT_ID` | Phase 5 | Telegram bot — DQ alerts (add when starting Phase 5) |

## Key principles

- **Bronze is immutable** — raw data is never modified after landing; re-run ingestion if you need to fix it.
- **Silver owns the cleaning logic** — all deduplication, null handling, and schema normalization happens in `silver_etl`; Gold only aggregates.
- **Fail loudly** — pipelines should raise errors, not silently skip bad records; data quality failures are surfaced to the user.
- **Parameterize everything** — pipelines use parameters for dates/sources so backfills are trivial.
- **Document decisions** — every non-obvious architectural choice has a "Why" entry in `docs/architecture.md`.
- **Infrastructure is code** — if a Fabric resource can be managed via Terraform, it must be. Never create workspace, lakehouses, or warehouse through the UI. Run `make -C terraform apply`.
