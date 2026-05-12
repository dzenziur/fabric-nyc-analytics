# Microsoft Fabric Data Engineering Project

Unified analytics platform on Microsoft Fabric that integrates NYC Taxi mobility data, OpenAQ air quality, World Bank GDP, ECB FX rates, and Open-Meteo weather into a single data lakehouse and warehouse.

**Architecture:** Medallion (Bronze → Silver → Gold) — raw landing in Lakehouse, cleaning via PySpark Notebooks, analytical star schema in Fabric Warehouse.

**Core technology:** Microsoft Fabric (Lakehouse, Data Factory Pipelines, Dataflow Gen2, PySpark Notebooks, Warehouse), Delta Lake, Python, T-SQL.

**Infrastructure as Code:** Terraform with `microsoft/fabric` provider — manages workspace + lakehouses + warehouse declaratively. See `terraform/`.

**External stack** (Phase 7): InfluxDB Cloud (weather time-series), Grafana (weather dashboard), Great Expectations (data quality), Telegram / Discord Bot (DQ alerts).

## Current Status

**Active branch:** `feature/data-quality-viz`
**Deadline:** May 15, 2026

### Phase completion

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0 — Terraform IaC | ✅ Done | workspace + bronze_lakehouse + silver_lakehouse + gold_warehouse |
| Phase 1 — Bronze ingestion | ✅ Done | Taxi, GDP, FX, OpenAQ locations, OpenAQ measurements (S3 archive, boto3) |
| Phase 2 — Silver ETL | ✅ Done | silver_taxi_trips, silver_gdp, silver_fx_rates, silver_openaq_locations, silver_openaq_measurements |
| Phase 3 — Gold / star schema | ✅ Done | DimDate, DimZone, DimFX, DimGDP, FactTaxiDaily, FactAirQualityDaily in gold_warehouse |
| Phase 4 — Visualizations | ✅ Done | Semantic model, Mobility, Air Quality, Correlation, Economic Impact pages in Power BI |
| Phase 5 — Master Orchestrator | ✅ Done | pl_master_orchestrator + parameterized silver/gold notebooks + dynamic taxi loop |
| Phase 6 — Governance & Monitoring | 🔄 In progress | Schedules, RLS, Purview lineage |
| Phase 7 — External Integrations | ❌ Not started | Weather + InfluxDB + Grafana, Great Expectations + Telegram bot |

### Current branch goal (`feature/data-quality-viz`)

- [x] Backfill data: ran orchestrator for 2021–2025 (5 years); Power BI now has full multi-year trends
- [x] Fix city names in FactAirQualityDaily (gold_etl: city now from `silver_openaq_locations.location_name`)
- [x] Remove `DROP TABLE IF EXISTS` from `silver_etl` taxi section — enables replaceWhere to accumulate partitions across runs
- [ ] Gold incremental writes — replace full `overwrite` with `replaceWhere` for `FactTaxiDaily` and `FactAirQualityDaily`
- [ ] Bronze OpenAQ Measurements — skip already-ingested years (check existing years before downloading from S3)

### Upcoming: `feature/data-governance` (Phase 6)

#### Phase 6 — Governance & Monitoring

- [ ] Schedule automation — daily refresh for FX/OpenAQ, monthly for Taxi/GDP
- [ ] Row-Level Security in Power BI (optional)
- [ ] Purview lineage in Fabric (optional)

#### Phase 7 — External Integrations (Phase 7)

- [ ] Weather ingestion — `jobs/weather_ingest.py` → Bronze Lakehouse + InfluxDB Cloud
- [ ] Silver table `silver_weather` (temp, precipitation, windspeed) in `silver_etl`
- [ ] Gold table `FactWeatherDaily` in `gold_etl`
- [ ] Grafana dashboard — InfluxDB data source, weather vs taxi demand panels (`grafana/dashboards/weather_nyc.json`)
- [ ] Great Expectations — validate Silver tables: null checks, value ranges, allowed categories (`ge/expectations/`)
- [ ] Telegram/Discord bot `bot/dq_bot.py` — `/report` triggers GE checkpoint, replies with pass/fail summary

## Backlog

Items confirmed as needed but not yet scheduled. Claude reads this at the start of every session (see compaction instructions in `CLAUDE.local.md`). When a new improvement or fix is identified, add it here — do not leave it only in the conversation.

### Known data limitations
- **Multi-pollutant station coverage** — not all OpenAQ stations measure all pollutants; some stations have gaps in NO2/O3 data. Known data limitation from OpenAQ source.
- **TLC parquet schema drift** — files pre/post mid-2023 have INT32 vs INT64 for location columns; handled in `silver_etl` via file-by-file read + explicit cast.

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
jobs/         External Python jobs — run outside Fabric (added in Phase 7)
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
| `feature/data-governance` | Phase 6 + 7 — Governance, scheduling, external integrations |

## Data sources

| Source | Format | Ingestion tool |
|--------|--------|----------------|
| NYC Taxi (TLC) | Parquet, monthly | Data Factory Pipeline |
| OpenAQ Air Quality | JSON API, paginated | Dataflow Gen2 |
| World Bank GDP | JSON API | Dataflow Gen2 |
| ECB FX rates | CSV API | Dataflow Gen2 |
| Open-Meteo Weather | JSON API | Python job (Phase 7) |

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
| `OPENAQ_API_KEY` | Phase 1 | OpenAQ v3 API — passed as parameter to `bronze_ingest_openaq_locations` notebook |
| `INFLUXDB_URL/TOKEN/ORG/BUCKET` | Phase 7 | InfluxDB Cloud — weather time-series (add when starting Phase 7) |
| `TELEGRAM_BOT_TOKEN/CHAT_ID` | Phase 7 | Telegram / Discord Bot — DQ alerts (add when starting Phase 7) |

## Key principles

- **Bronze is immutable** — raw data is never modified after landing; re-run ingestion if you need to fix it.
- **Silver owns the cleaning logic** — all deduplication, null handling, and schema normalization happens in `silver_etl`; Gold only aggregates.
- **Fail loudly** — pipelines should raise errors, not silently skip bad records; data quality failures are surfaced to the user.
- **Parameterize everything** — pipelines use parameters for dates/sources so backfills are trivial.
- **Document decisions** — every non-obvious architectural choice has a "Why" entry in `docs/architecture.md`.
- **Infrastructure is code** — if a Fabric resource can be managed via Terraform, it must be. Never create workspace, lakehouses, or warehouse through the UI. Run `make -C terraform apply`.
