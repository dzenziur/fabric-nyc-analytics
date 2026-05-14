# Microsoft Fabric Data Engineering Project

Unified analytics platform on Microsoft Fabric that integrates NYC Taxi mobility data, OpenAQ air quality, World Bank GDP, ECB FX rates, and Open-Meteo weather into a single data lakehouse and warehouse.

**Architecture:** Medallion (Bronze → Silver → Gold) — raw landing in Lakehouse, cleaning via PySpark Notebooks, analytical star schema in Fabric Warehouse.

**Core technology:** Microsoft Fabric (Lakehouse, Data Factory Pipelines, Dataflow Gen2, PySpark Notebooks, Warehouse), Delta Lake, Python, T-SQL.

**Infrastructure as Code:** Terraform with `microsoft/fabric` provider — manages workspace + lakehouses + warehouse declaratively. See `terraform/`.

**External stack** (Phase 7): InfluxDB Cloud (weather time-series), Grafana (weather dashboard), Great Expectations (data quality), Telegram / Discord Bot (DQ alerts).

## Current Status

**Active branch:** `feature/data-governance`
**Deadline:** May 26, 2026 (defense) — target May 15 for main features

### Phase completion

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0 — Terraform IaC | ✅ Done | workspace + bronze_lakehouse + silver_lakehouse + gold_warehouse |
| Phase 1 — Bronze ingestion | ✅ Done | Taxi, GDP, FX, OpenAQ locations, OpenAQ measurements (S3 archive, boto3) |
| Phase 2 — Silver ETL | ✅ Done | silver_taxi_trips, silver_gdp, silver_fx_rates, silver_openaq_locations, silver_openaq_measurements |
| Phase 3 — Gold / star schema | ✅ Done | DimDate, DimZone, DimFX, DimGDP, FactTaxiDaily, FactAirQualityDaily in gold_warehouse |
| Phase 4 — Visualizations | ✅ Done | All 4 dashboards complete; ppm normalization, location slicer, Correlation enhanced, docs updated |
| Phase 5 — Master Orchestrator | ✅ Done | pl_master_orchestrator + parameterized silver/gold notebooks + dynamic taxi loop |
| Phase 6 — Governance & Monitoring | ❌ Not started | Schedules, RLS, Purview lineage |
| Phase 7 — External Integrations | ❌ Not started | Weather + InfluxDB + Grafana, Great Expectations + Telegram bot |

### Current branch goal (`feature/data-governance`)

Phase 6 — Governance & Monitoring + notebook robustness cleanup:

#### Schedule automation
- [ ] Set daily schedule on `pl_master_orchestrator` for FX rates + OpenAQ refresh
- [ ] Set monthly schedule trigger for Taxi + GDP refresh
- [ ] Verify schedule runs correctly in Fabric workspace

#### Notebook robustness (silver_etl)
- [ ] Remove redundant `df.count()` at the start of each ETL section — `write_silver` already logs row count; double scan wastes resources
- [ ] Remove `.orderBy()` before write on `silver_openaq_measurements` and `silver_openaq_locations` — Delta Lake uses partition pruning; full sort adds cost with no benefit

#### Notebook robustness (gold_etl)
- [ ] Narrow `except Exception` in `write_gold` to `AnalysisException` (table not found) so network/config errors are not silently swallowed

#### Notebook robustness (bronze)
- [ ] `bronze_ingest_openaq_locations` — raise hard page cap from 100 to a higher value (e.g. 500); after the last page check if it was full (`len(page) == limit`) and log a WARNING if so — prevents silent truncation when station count exceeds cap

#### Optional
- [ ] Row-Level Security in Power BI (optional)
- [ ] `gold_etl` DimZone — download `taxi_zone_lookup.csv` once to `bronze_lakehouse/Files/raw/taxi_zones/` instead of re-downloading from CloudFront on every run

## Backlog

Items confirmed as needed but not yet scheduled. Claude reads this at the start of every session (see compaction instructions in `CLAUDE.local.md`). When a new improvement or fix is identified, add it here — do not leave it only in the conversation.

### Power BI — dashboard insights
- [ ] Add key insight text box to each dashboard page (2–3 sentences per page, specific numbers):
  - Mobility: post-COVID growth, busiest zones, avg fare trend
  - Air Quality: PM2.5 seasonality, NO2 rush-hour pattern, data coverage note (2023+)
  - Correlation: trips vs PM2.5 overlay observation, caveat about 2023+ data
  - Economic Impact: revenue growth 2021→2025, EUR/USD gap explanation, GDP scale context

### Notebook performance & robustness
- [ ] `bronze_ingest_openaq_locations` — add retry logic on transient API failures (separate from page cap fix which is in current branch)

### Docs accuracy
- [ ] Audit all column types in `docs/data_dictionary.md` against actual Spark schemas — run `printSchema()` for each Bronze and Silver table and compare. Found first discrepancy: `bronze_openaq_measurements.datetime` is `string` in practice, `timestamp` in docs (already fixed).

### Power BI — semantic model field naming
- [ ] Audit and rename misnamed fields in `nyc_analytics_model` — `FactAirQualityDaily.city` actually contains station names (OpenAQ `location_name`), not city names; rename to `station_name` in the semantic model and update all visuals that reference it. Do a full pass of all column display names across all tables to ensure they match what the data actually contains.

### Known data limitations
- **OpenAQ historical coverage** — NYC stations in the S3 archive have data for 2021–2025, but with coverage gaps: mid-2022 (May–Sep) shows near-zero measurements for many stations. Not a pipeline issue — reflects actual S3 archive availability.
- **Multi-pollutant station coverage** — not all OpenAQ stations measure all pollutants; some stations have gaps in NO2/O3 data. Known data limitation from OpenAQ source.
- **TLC parquet schema drift** — files pre/post mid-2023 have INT32 vs INT64 for location columns; handled in `silver_etl` via file-by-file read + explicit cast.
- **OpenAQ gas units** — NO2, O3, CO, NO, NOx, SO2 are stored in ppm in the S3 archive; PM2.5/PM1/PM10 in µg/m³. `silver_etl` normalizes all gas parameters to µg/m³ using EPA conversion factors at 25°C.

### Key reference docs

| Question | Read |
|----------|------|
| Table schemas / columns | `docs/data_dictionary.md` |
| Components, decisions, data flow | `docs/architecture.md` |
| Run order, setup steps | `docs/how_to_run.md` |
| Phase breakdown, tech stack | `docs/project_plan.md` |
| Fabric workspace items, pipelines, notebooks | `fabric/README.md` |

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
| `feature/data-governance` | Phase 6 — Governance, scheduling, notebook robustness (current) |

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
