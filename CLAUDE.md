# Microsoft Fabric Data Engineering Project

Unified analytics platform on Microsoft Fabric that integrates NYC Taxi mobility data, OpenAQ air quality, World Bank GDP, ECB FX rates, and Open-Meteo weather into a single data lakehouse and warehouse.

**Architecture:** Medallion (Bronze → Silver → Gold) — raw landing in Lakehouse, cleaning via PySpark Notebooks, analytical star schema in Fabric Warehouse.

**Core technology:** Microsoft Fabric (Lakehouse, Data Factory Pipelines, Dataflow Gen2, PySpark Notebooks, Warehouse), Delta Lake, Python, T-SQL.

**Infrastructure as Code:** Terraform with `microsoft/fabric` provider — manages workspace + lakehouses + warehouse declaratively. See `terraform/`.

**External stack** (introduced later phases): InfluxDB Cloud (weather time-series), Grafana (weather dashboard), Great Expectations (data quality), Telegram Bot (DQ alerts).

## Current Status

**Active branch:** `feature/data-orchestration`
**Deadline:** May 15, 2026

### Phase completion

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0 — Terraform IaC | ✅ Done | workspace + bronze_lakehouse + silver_lakehouse + gold_warehouse |
| Phase 1 — Bronze ingestion | ✅ Done | Taxi, GDP, FX, OpenAQ locations, OpenAQ measurements (S3 archive, boto3) |
| Phase 2 — Silver ETL | ✅ Done | silver_taxi_trips, silver_gdp, silver_fx_rates, silver_openaq_locations, silver_openaq_measurements |
| Phase 3 — Gold / star schema | ✅ Done | DimDate, DimZone, DimFX, DimGDP, FactTaxiDaily, FactAirQualityDaily in gold_warehouse |
| Phase 4 — Visualizations | ✅ Done | Semantic model, Mobility, Air Quality, Correlation, Economic Impact pages in Power BI |
| Phase 5 — Master Orchestrator | 🔄 In progress | pl_master_orchestrator + parameterized notebooks + data backfill |
| Phase 6 — Governance / monitoring | ❌ Not started | Weather, InfluxDB, Grafana, GE, Telegram bot |

### Current branch goal (`feature/data-orchestration`)

Phase 5 — Master orchestrator — single-entry-point pipeline with `year_start`/`year_end` parameters

- [ ] Add `year_start` / `year_end` notebook parameters to `silver_etl`
- [ ] Add `year_start` / `year_end` notebook parameters to `gold_etl`
- [ ] Create `pl_master_orchestrator` pipeline in Fabric with `year_start`/`year_end` parameters
- [ ] Add ForEach loop for `pl_ingest_nyc_taxi` (iterate year/month combinations)
- [ ] Wire all activities: parallel ingestion → silver_etl → gold_etl
- [ ] Backfill data: run orchestrator for 2022–2024 to populate multi-year trends
- [ ] Fix city names in FactAirQualityDaily (gold_etl: join silver_openaq_locations on location_id)

### Future improvements (post-Phase 5)

- **Multi-pollutant station coverage** — not all OpenAQ stations measure all pollutants; some stations have gaps in NO2/O3 data. Known data limitation from OpenAQ source.

### Key table row counts

| Table | Rows | Layer |
|-------|------|-------|
| silver_taxi_trips | ~2.87M | Silver |
| silver_openaq_locations | ~5k | Silver |
| silver_openaq_measurements | ~1.1M | Silver |
| silver_gdp | ~6.2k | Silver |
| silver_fx_rates | ~7k | Silver |
| DimDate | 2,557 | Gold |
| DimZone | 265 | Gold |
| DimFX | 6,996 | Gold |
| DimGDP | 6,193 | Gold |
| FactTaxiDaily | 6,856 | Gold |
| FactAirQualityDaily | 49,287 | Gold |

### Key reference docs

| Question | Read |
|----------|------|
| Table schemas / columns | `docs/data_dictionary.md` |
| Components, decisions, data flow | `docs/architecture.md` |
| Run order, setup steps | `docs/how_to_run.md` |
| Phase breakdown, tech stack | `docs/project_plan.md` |

---

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
| `feature/data-orchestration` | Phase 5 — Master orchestrator + data backfill |
| `feature/data-governance` | Phase 6 — Scheduling, data quality, monitoring |

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

```bash
# Provision / update infrastructure
make -C terraform plan
make -C terraform apply

# Fabric Git sync — push branch, then in Fabric UI:
# Workspace → Source control → Update all
```

Full setup: see [docs/how_to_run.md](docs/how_to_run.md)
Architecture decisions: see [docs/architecture.md](docs/architecture.md)

## Environment

Required env vars are documented in `.env.example`.

| Variable | Phase | Purpose |
|----------|-------|---------|
| `OPENAQ_API_KEY` | Phase 1 | OpenAQ v3 API — required for `df_openaq_locations` Dataflow |
| `INFLUXDB_URL/TOKEN/ORG/BUCKET` | Phase 5 | InfluxDB Cloud — weather time-series (add when starting Phase 5) |
| `TELEGRAM_BOT_TOKEN/CHAT_ID` | Phase 5 | Telegram bot — DQ alerts (add when starting Phase 5) |

## Key principles

- **Bronze is immutable** — raw data is never modified after landing; re-run ingestion if you need to fix it.
- **Silver owns the cleaning logic** — all deduplication, null handling, and schema normalization happens in `silver_etl`; Gold only aggregates.
- **Fail loudly** — pipelines should raise errors, not silently skip bad records; data quality failures are surfaced to the user.
- **Parameterize everything** — pipelines use parameters for dates/sources so backfills are trivial.
- **Document decisions** — every non-obvious architectural choice has a "Why" entry in `docs/architecture.md`.
- **Infrastructure is code** — if a Fabric resource can be managed via Terraform, it must be. Never create workspace, lakehouses, or warehouse through the UI. Run `make -C terraform apply`.
