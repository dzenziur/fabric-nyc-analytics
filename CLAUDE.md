# Microsoft Fabric Data Engineering Project

Unified analytics platform on Microsoft Fabric that integrates NYC Taxi mobility data, OpenAQ air quality, World Bank GDP, ECB FX rates, and Open-Meteo weather into a single data lakehouse and warehouse.

**Architecture:** Medallion (Bronze → Silver → Gold) — raw landing in Lakehouse, cleaning via PySpark Notebooks, analytical star schema in Fabric Warehouse.

**Core technology:** Microsoft Fabric (Lakehouse, Data Factory Pipelines, Dataflow Gen2, PySpark Notebooks, Warehouse), Delta Lake, Python, T-SQL.

**Infrastructure as Code:** Terraform with `microsoft/fabric` provider — manages workspace + lakehouses + warehouse declaratively. See `terraform/`.

**External stack** (Phase 7): InfluxDB Cloud (weather time-series), Grafana (weather dashboard), Great Expectations (data quality), Telegram / Discord Bot (DQ alerts).

## Current Status

**Active branch:** `feature/governance-monitoring`
**Deadline:** May 26, 2026 (defense) — target May 15 for main features

### Phase completion

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0 — Terraform IaC | ✅ Done | workspace + bronze_lakehouse + silver_lakehouse + gold_warehouse |
| Phase 1 — Bronze ingestion | ✅ Done | Taxi, GDP, FX, OpenAQ locations, OpenAQ measurements (S3 archive, boto3), TLC taxi zones |
| Phase 2 — Silver ETL | ✅ Done | silver_taxi_trips, silver_gdp, silver_fx_rates, silver_openaq_locations, silver_openaq_measurements |
| Phase 3 — Gold / star schema | ✅ Done | DimDate, DimZone, DimFX, DimGDP, FactTaxiDaily, FactAirQualityDaily in gold_warehouse |
| Phase 4 — Visualizations | ✅ Done | 4 dashboards; Air Quality map (Azure Maps + WHO thresholds + conditional KPI fill); semantic model fixes |
| Phase 5 — Master Orchestrator | ✅ Done | pl_master_orchestrator + parameterized silver/gold notebooks + prepare_taxi_ingestion (pre-flight + incremental) |
| Phase 6 — Governance & Monitoring | 🔄 In progress | Schedules, RLS, Purview lineage |
| Phase 7 — External Integrations | ❌ Not started | Weather + InfluxDB + Grafana, Great Expectations + Telegram bot |

### Current branch goal (`feature/governance-monitoring`)

Phase 6 — Governance and Monitoring per spec (`spec/Microsoft Fabric Data Engineering Project.pdf`):
- Automate refresh schedules (daily/hourly) — **required**
- Row-Level Security in Power BI — optional
- Purview lineage — optional

#### Schedule automation

**Re-reading the spec:** the requirement is simply "Automate refresh schedules (daily/hourly)". No spec requirement for separate daily/monthly orchestrators — our previous interpretation was over-engineering. The existing `pl_master_orchestrator` is already well-suited for high-frequency scheduling because `prepare_taxi_ingestion` returns only missing files (idempotent — empty list on days TLC hasn't published).

**Plan:** schedule existing `pl_master_orchestrator` to run **twice daily at 06:00 and 18:00 UTC**. No pipeline duplication, no conditional logic.

Why 06:00 + 18:00 UTC:
- 06:00 UTC catches overnight ECB FX (~16:00 CET prev day) and OpenAQ measurements
- 18:00 UTC catches midday updates and provides afternoon refresh for dashboard users
- Twice-daily strikes balance between freshness and compute cost

Implementation tasks:
- [X] Confirmed `microsoft/fabric` Terraform provider supports schedule resource — `fabric_item_job_scheduler` (type=Daily, multiple times allowed). Will use Terraform per project principle "Infrastructure is code"
- [ ] Declare two `fabric_item_job_scheduler` resources in `terraform/` for `pl_master_orchestrator` (Daily type, times=["06:00", "18:00"])
- [ ] Verify schedules trigger correctly in Fabric workspace (let it run a few days, check monitoring)
- [ ] Update `docs/how_to_run.md` and `docs/architecture.md` with the schedule configuration

#### Incremental processing (force_refresh parameter)

Decided: implement incremental processing for 5 high-value tables to keep daily compute cost low. Unified under single `force_refresh` parameter (bool, default False) that cascades from orchestrator → notebooks. Default behavior (False) = incremental for those tables, True = full rebuild for manual backfill.

**Tables scoped for incremental:**
1. `silver_openaq_measurements` — watermark `MAX(datetime)` + Delta `MERGE INTO` on `(location_id, parameter, datetime)` ✅ **Done (Etap 1)**
2. `silver_taxi_trips` — process only missing `(year, month)` partitions
3. `FactAirQualityDaily` — re-aggregate only changed dates (last N days for late-arriving handling)
4. `FactTaxiDaily` — re-aggregate only changed `(year, month)`
5. `bronze_openaq_measurements` (optional) — fetch only missing S3 partitions

Other tables (`silver_fx_rates`, `silver_gdp`, `silver_openaq_locations`, all gold dims) remain full overwrite — small size makes incremental overhead exceed savings.

**Implementation order (Etaps):**
- [X] **Etap 1** — `silver_openaq_measurements` incremental via watermark (`MAX(datetime)`) + Delta `MERGE INTO`. Parameter `force_refresh` added to silver_etl and wired through orchestrator
- [X] **Etap 2** — `silver_taxi_trips` incremental via partition diff (skip files whose `(year, month)` already in silver; append new partitions)
- [X] **Etap 3** — `FactAirQualityDaily` incremental via `MAX(gold.date_key) - 7 days` lookback (handles late-arriving data + short missed-run gaps); `force_refresh` parameter added to gold_etl; constant `LATE_ARRIVING_LOOKBACK_DAYS = 7`
- [X] **Etap 4** — `FactTaxiDaily` incremental — same 7-day lookback pattern as Etap 3
- [X] **Etap 5** — `bronze_openaq_measurements` incremental S3 fetch. Default mode fetches current + previous month only (instead of full year range) via month-level S3 prefix; uses Delta `MERGE INTO` on `(location_id, sensors_id, datetime, parameter)`. `force_refresh=True` falls back to year-range download. First-run (table missing) auto-falls-back to full mode

#### Row-Level Security (optional per spec)
- [ ] Define role taxonomy (e.g., Admin / Manhattan-only / Outer-boroughs-only) and what each role sees
- [ ] Configure RLS in `nyc_analytics_model` — DAX filter expressions on DimZone
- [ ] Document role assignments; test with "View as role" in Power BI

#### Microsoft Purview lineage (optional per spec)
- [ ] Connect Fabric workspace to Purview tenant (requires Purview account)
- [ ] Verify Bronze → Silver → Gold lineage captured automatically
- [ ] Add screenshot/note in `docs/architecture.md`

#### Row-Level Security
- [ ] Configure RLS in `nyc_analytics_model` — restrict data visibility by role (e.g. borough-level access). Decide role taxonomy first (Admin / Manhattan-only / Outer-boroughs-only / etc.)
- [ ] Document role assignments and test with "View as role" in Power BI

#### Microsoft Purview lineage (optional)
- [ ] Connect Fabric workspace to Microsoft Purview for automated lineage tracking
- [ ] Verify Bronze → Silver → Gold lineage is captured automatically
- [ ] Document setup in `docs/architecture.md`

### Next branch — `feature/external-integrations` (Phase 7)

External monitoring and data-quality stack — runs outside Fabric.

#### Weather ingestion + InfluxDB + Grafana
- [ ] `jobs/weather_ingest.py` — Open-Meteo API → Bronze Lakehouse + InfluxDB Cloud (hourly NYC weather)
- [ ] Schedule weather ingest (Linux cron, Azure Function, or Railway.app — TBD)
- [ ] Silver/Gold processing for weather: `silver_weather` table, `FactWeatherDaily` star schema entry
- [ ] Grafana dashboard for weather time-series (InfluxDB data source) — temperature, precipitation, weather vs taxi demand
- [ ] Document setup in `docs/how_to_run.md` (already partially scaffolded)

#### Great Expectations + Telegram Bot
- [ ] Great Expectations expectation suites for Silver tables (specs already drafted in `docs/data_dictionary.md`)
- [ ] `bot/dq_bot.py` — Telegram bot with `/report` command → runs GE checkpoints → replies with pass/fail summary
- [ ] Document bot setup + secrets in `.env.example` and `docs/how_to_run.md`

## Backlog

Items confirmed as needed but not yet scheduled. Claude reads this at the start of every session (see compaction instructions in `CLAUDE.local.md`). When a new improvement or fix is identified, add it here — do not leave it only in the conversation.

### Power BI — dashboard insights
- [ ] Add key insight text box to each dashboard page (2–3 sentences per page, specific numbers):
  - Mobility: post-COVID growth, busiest zones, avg fare trend
  - Air Quality: PM2.5 seasonality, NO2 rush-hour pattern, data coverage note (2023+)
  - Correlation: trips vs PM2.5 overlay observation, caveat about 2023+ data
  - Economic Impact: revenue growth 2021→2025, EUR/USD gap explanation, GDP scale context

### Power BI — signature feature per dashboard
One "wow" feature per page, beyond the standard charts. Air Quality already has the Azure Maps station bubble visual (done). Remaining pages need their own signature:
- [ ] **Mobility — Sankey diagram for taxi flows (pickup zone → dropoff zone)**. Requires gold_etl change: drop FactTaxiDaily and rebuild with DO_zone_key in addition to current zone_key (PU), OR create new FactTaxiFlows table aggregated by (PU, DO, year). Marketplace visual: "Sankey" by Microsoft. Maps the actual movement patterns through NYC.
- [ ] **Correlation — Scatter plot with Play Axis animation**. Replace current bar+line monthly aggregate with daily scatter (one point per day): X=Total Trips, Y=Avg PM2.5, Play axis=year. Shows the actual correlation shape, not just monthly trend. Built-in scatter visual supports Play axis natively.
- [ ] **Economic Impact — Forecasting on USD/EUR line chart**. Built-in Power BI feature (Analytics pane → Forecast). Forecast length 90 days with 95% confidence interval. Demonstrates predictive analytics capability without external tools. Alternative: waterfall chart for YoY revenue change.

### Incremental ETL — future candidates (low priority, defer until cost matters)
All current high-value tables now have incremental mode via `force_refresh` (Etaps 1-5 done). Remaining tables are kept as full overwrite because savings are minimal vs added complexity. Listed here for reference if compute cost ever becomes a concern:
- [ ] `bronze_ingest_openaq_locations` — currently fetches all ~24k station records via paginated API. Could compare with current bronze and skip download if no changes (e.g., hash check or count diff). Marginal benefit — runs in ~2 min.
- [ ] `bronze_ingest_openaq_measurements` — pre-filter `nyc_ids` by station activity window before iterating S3. Would require enriching `bronze_openaq_locations` with `datetime_first` and `datetime_last` fields from OpenAQ API. Saves S3 LIST API calls (~5-10s) for stations that didn't report in the target time window. Low priority — current "no data, skipping" handles it gracefully.
- [ ] `silver_fx_rates` — small table (~7k rows). Could read only `date > MAX(silver.date)` from bronze and append. Saves seconds.
- [ ] `silver_gdp` — yearly data (~6k rows), almost never changes. Could skip processing if bronze unchanged. Trivial savings.
- [ ] `silver_openaq_locations` — small (~5k rows), rare changes. Could MERGE only updated stations. Trivial savings.
- [ ] `DimDate`/`DimZone`/`DimFX`/`DimGDP` (gold dims) — currently full rebuild each run. Could be incremental but compute cost is already <30s combined. Not worth complexity.
- [ ] `bronze_taxi_zones` — static (~265 rows), trivial cost. Could check ETag/Last-Modified to skip download.

Pattern reference: most candidates would use either watermark + MERGE (like Etap 1) or skip-if-unchanged (idempotent no-op check before fetch).

### Power BI — dashboard polish (smaller wins)
- [ ] Export current Power BI theme as JSON and check into repo for consistency across reports (deferred — Fabric Direct Lake report theme handling not yet evaluated)
- [ ] Smart Narrative AI visual on at least one page (e.g., Mobility) — auto-generated text insights from KPIs
- [ ] YoY change indicators on monetary/count KPI cards (Mobility, Economic Impact) — up/down arrow + color based on previous-year comparison (where WHO-style threshold doesn't apply)


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

| Branch | Phase | Status |
|--------|-------|--------|
| `feature/dashboards-and-robustness` | Dashboard polish + notebook/pipeline robustness + data quality | Merged |
| `feature/governance-monitoring` | Phase 6 — Governance & monitoring (schedules, RLS, Purview) | Active |
| `feature/external-integrations` | Phase 7 — Weather + InfluxDB + Grafana + GE + Telegram bot | Planned |

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
