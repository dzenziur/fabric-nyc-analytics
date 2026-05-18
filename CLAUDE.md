# Microsoft Fabric Data Engineering Project

Unified analytics platform on Microsoft Fabric that integrates NYC Taxi mobility data, OpenAQ air quality, World Bank GDP, ECB FX rates, and Open-Meteo weather into a single data lakehouse and warehouse.

**Architecture:** Medallion (Bronze → Silver → Gold) — raw landing in Lakehouse, cleaning via PySpark Notebooks, analytical star schema in Fabric Warehouse.

**Core technology:** Microsoft Fabric (Lakehouse, Data Factory Pipelines, Dataflow Gen2, PySpark Notebooks, Warehouse), Delta Lake, Python, T-SQL.

**Infrastructure as Code:** Terraform with `microsoft/fabric` provider — manages workspace + lakehouses + warehouse declaratively. See `terraform/`.

**External stack** (Phase 7): InfluxDB Cloud (weather time-series), Grafana (weather dashboard), Great Expectations (data quality), Telegram / Discord Bot (DQ alerts).

## Current Status

**Active branch:** `feature/governance-monitoring`
**Deadline:** May 26, 2026 (defense) — target May 15 for main features
**Last session:** 2026-05-18 — Phase 6 closed: RLS configured (5 roles on DimZone.service_zone), lineage documented via Fabric built-in workspace lineage view (Purview Data Map evaluated and skipped — free tier lacks lineage graph), docs synced

### Phase completion

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0 — Terraform IaC | ✅ Done | workspace + bronze_lakehouse + silver_lakehouse + gold_warehouse |
| Phase 1 — Bronze ingestion | ✅ Done | Taxi, GDP, FX, OpenAQ locations, OpenAQ measurements (S3 archive, boto3), TLC taxi zones |
| Phase 2 — Silver ETL | ✅ Done | silver_taxi_trips (~201M rows, incl. 2026 data), silver_gdp, silver_fx_rates, silver_openaq_locations, silver_openaq_measurements |
| Phase 3 — Gold / star schema | ✅ Done | DimDate, DimZone, DimFX, DimGDP, FactTaxiDaily, FactAirQualityDaily in gold_warehouse |
| Phase 4 — Visualizations | ✅ Done | 4 dashboards; Air Quality map (Azure Maps + WHO thresholds + conditional KPI fill); semantic model fixes |
| Phase 5 — Master Orchestrator | ✅ Done | pl_master_orchestrator + parameterized silver/gold notebooks + prepare_taxi_ingestion (pre-flight + incremental) |
| Phase 6 — Governance & Monitoring | ✅ Done | Twice-daily schedule + RLS (5 roles on DimZone.service_zone) + lineage via Fabric built-in workspace view |
| Phase 7 — External Integrations | ❌ Not started | Weather + InfluxDB + Grafana, Great Expectations + Telegram bot |

### Current branch goal (`feature/governance-monitoring`)

Phase 6 — Governance and Monitoring per spec (`spec/Microsoft Fabric Data Engineering Project.pdf`): Schedule ✅ (required) · RLS ✅ (optional) · Lineage ✅ (optional, via Fabric built-in lineage view — Purview Data Map skipped). Details below and in `docs/architecture.md`.

#### Schedule automation

- [x] `pl_master_orchestrator` scheduled twice daily — 06:00 and 18:00 UTC, `force_refresh=false`, `year_start=2021`, `year_end=2026`. See `docs/how_to_run.md` for details.

#### Incremental processing (force_refresh parameter)

Single `force_refresh` (bool, default False) cascades from orchestrator → notebooks. `False` = incremental (schedule-friendly), `True` = full rebuild for manual backfill.

- [x] **Step 1** — `silver_openaq_measurements`: watermark `MAX(datetime)` + Delta `MERGE INTO`
- [x] **Step 2** — `silver_taxi_trips`: partition diff — append only `(year, month)` not yet in silver
- [x] **Step 3** — `FactAirQualityDaily`: `MAX(gold.date_key) - 7 days` lookback (`LATE_ARRIVING_LOOKBACK_DAYS = 7`)
- [x] **Step 4** — `FactTaxiDaily`: same 7-day lookback pattern as Step 3
- [x] **Step 5** — `bronze_openaq_measurements`: current + previous month from S3; Delta `MERGE INTO`; `force_refresh=True` falls back to year-range

Remaining tables (`silver_fx_rates`, `silver_gdp`, `silver_openaq_locations`, all gold dims) — full overwrite, small size makes incremental overhead exceed savings. Further candidates in Backlog → Incremental ETL.

#### Row-Level Security (optional per spec)
- [x] RLS configured in `nyc_analytics_model` — 5 roles on `DimZone[service_zone]` (Admin, Yellow Cab Dispatcher, Green Cab Dispatcher, Airports Operator, EWR Operator) mapping to real NYC TLC licensing zones; filter propagates to FactTaxiDaily via zone_key relationship. Role-to-user assignment done post-deployment in Power BI Service. See `docs/architecture.md`.

#### Lineage (optional per spec)
- [x] End-to-end data lineage documented via Fabric built-in workspace lineage view — covers external sources → Bronze (Dataflows + Notebooks + Pipelines) → Silver → Gold → Semantic Model → Report. Screenshot at `docs/img/workspace-lineage.png`. Microsoft Purview Data Map evaluated and skipped (paid Azure resource, not needed for single-workspace deployment — free Purview Data Catalog tier does not include lineage graph). See `docs/architecture.md` → Security & Governance.

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

### Incremental ETL — future candidates
Steps 1-5 done. Remaining tables are full overwrite — savings minimal vs added complexity. Implement if compute cost becomes a concern:
- [ ] `bronze_ingest_openaq_locations` — ~24k records via paginated API; skip if no changes (hash/count diff). ~2 min run.
- [ ] `bronze_ingest_openaq_measurements` — pre-filter `nyc_ids` by station activity window (`datetime_first`/`datetime_last` from OpenAQ API). Saves S3 LIST calls for inactive stations.
- [ ] `silver_fx_rates` — ~7k rows; read only `date > MAX(silver.date)` from bronze, append.
- [ ] `silver_gdp` — ~6k rows, yearly data, rarely changes; skip if bronze unchanged.
- [ ] `silver_openaq_locations` — ~5k rows, rare changes; MERGE only updated stations.
- [ ] `DimDate`/`DimZone`/`DimFX`/`DimGDP` — full rebuild <30s combined; not worth complexity.
- [ ] `bronze_taxi_zones` — 265 rows, static; check ETag/Last-Modified to skip download.

### Power BI — dashboard polish (smaller wins)
- [ ] Export current Power BI theme as JSON and check into repo for consistency across reports (deferred — Fabric Direct Lake report theme handling not yet evaluated)
- [ ] Smart Narrative AI visual on at least one page (e.g., Mobility) — auto-generated text insights from KPIs
- [ ] YoY change indicators on monetary/count KPI cards (Mobility, Economic Impact) — up/down arrow + color based on previous-year comparison (where WHO-style threshold doesn't apply)


### Pipeline correctness
- [ ] Audit activity dependencies in `pl_master_orchestrator` — verify correct execution order (bronze → silver → gold), no missing or redundant dependency links between activities.

### Notebook markdown clarity — year_start/year_end parameter semantics
- [ ] Update the top-of-notebook header Markdown cell in each notebook to explicitly state when `year_start`/`year_end` are used vs. ignored:
  - `prepare_taxi_ingestion` — **always uses** year range (determines which TLC months to check/download); critical for schedule correctness
  - `bronze_ingest_openaq_measurements` — **ignores** year range when `force_refresh=False` (fetches current+prev month only); used only with `force_refresh=True`
  - `silver_etl` taxi section — **ignores** year range when `force_refresh=False` (partition diff); used only with `force_refresh=True`
  - `silver_etl` OpenAQ section — **ignores** year range when `force_refresh=False` (watermark); used only with `force_refresh=True`
  - `gold_etl` FactTaxiDaily/FactAirQualityDaily — **ignores** year range when `force_refresh=False` (7-day lookback); used only with `force_refresh=True`
  - `gold_etl` DimDate — **uses** year range as a floor/ceiling, expanded by actual data min/max

### Notebook logging improvements
- [ ] `silver_etl` — taxi incremental append: after append log total silver row count and delta (rows added). Pattern: `rows_before = spark.read.table(SILVER_TAXI_TRIPS).count()` before write → `rows_after = spark.read.table(SILVER_TAXI_TRIPS).count()` after → `print(f"appended {rows_after - rows_before:,} rows; silver total: {rows_after:,}")`. Currently only logs "rows before append" of the new batch, which is confusing.

### Governance — lineage completeness audit
- [ ] Verify Fabric built-in workspace lineage view (`Workspace → Lineage view`) captures all expected upstream/downstream edges. Suspected gaps: notebook → table edges may not be visible for all notebooks; external source nodes may be missing for some pipelines. Compare graph against `docs/architecture.md` → Data Flow section and document any missing edges (e.g. add manual annotations or screenshot caveats). If gaps are significant, re-evaluate Purview Data Map (free Azure tier).

### Docs accuracy
- [ ] Audit all column types in `docs/data_dictionary.md` against actual Spark schemas — run `printSchema()` for each Bronze and Silver table and compare. Found first discrepancy: `bronze_openaq_measurements.datetime` is `string` in practice, `timestamp` in docs (already fixed).

### Power BI — semantic model field naming
- [ ] Audit and rename misnamed fields in `nyc_analytics_model` — `FactAirQualityDaily.city` actually contains station names (OpenAQ `location_name`), not city names; rename to `station_name` in the semantic model and update all visuals that reference it. Do a full pass of all column display names across all tables to ensure they match what the data actually contains.

### Known data limitations
- **OpenAQ historical coverage** — NYC stations in the S3 archive have data for 2021–2025, but with coverage gaps: mid-2022 (May–Sep) shows near-zero measurements for many stations. Not a pipeline issue — reflects actual S3 archive availability.
- **Multi-pollutant station coverage** — not all OpenAQ stations measure all pollutants; some stations have gaps in NO2/O3 data. Known data limitation from OpenAQ source.
- **TLC parquet schema drift** — files pre/post mid-2023 have INT32 vs INT64 for location columns; 2026+ files additionally changed `passenger_count` (long vs double) and `RatecodeID` (long vs double) and renamed `airport_fee` → `Airport_fee` (capitalisation). Handled in `silver_etl` via per-file normalisation: rename `Airport_fee`→`airport_fee` if present, then cast `VendorID`/`PULocationID`/`DOLocationID`/`payment_type` → long, `passenger_count`/`RatecodeID` → double.
- **OpenAQ gas units** — NO2, O3, CO, NO, NOx, SO2 are stored in ppm in the S3 archive; PM2.5/PM1/PM10 in µg/m³. `silver_etl` normalizes all gas parameters to µg/m³ using EPA conversion factors at 25°C.

### Key reference docs

| Question | Read |
|----------|------|
| Table schemas / columns | `docs/data_dictionary.md` |
| Components, decisions, data flow | `docs/architecture.md` |
| Run order, setup steps | `docs/how_to_run.md` |
| Fabric workspace items, pipelines, notebooks | `fabric/README.md` |

---

## Project structure

```
fabric/       All Fabric workspace items: dataflows, pipelines, notebooks, warehouse SQL
              Synced automatically via Fabric Git integration
jobs/         External Python jobs — run outside Fabric (added in Phase 7)
terraform/    IaC: workspace, lakehouses, warehouse (run `make help`)
docs/         Architecture, data dictionary, how-to-run, governance screenshots (img/)
spec/         Original project specification (PDF)
```

## Development workflow

| Branch | Phase | Status |
|--------|-------|--------|
| `feature/governance-monitoring` | Phase 6 — Governance & monitoring (schedules, RLS, Purview) | Active |
| `feature/external-integrations` | Phase 7 — Weather + InfluxDB + Grafana + GE + Telegram bot | Planned |

## Data sources

| Source | Format | Ingestion tool |
|--------|--------|----------------|
| NYC Taxi (TLC) | Parquet, monthly | Data Factory Pipeline |
| OpenAQ Air Quality | JSON API + S3 archive | PySpark Notebook |
| World Bank GDP | JSON API | Dataflow Gen2 |
| ECB FX rates | CSV API | Dataflow Gen2 |
| Open-Meteo Weather | JSON API | Python job (Phase 7) |

## Quick start

```bash
# Fabric Git sync — push branch, then in Fabric UI:
# Workspace → Source control → Update all
```

Full setup: see [docs/how_to_run.md](docs/how_to_run.md)
Architecture decisions: see [docs/architecture.md](docs/architecture.md)

## Environment

Phase 7 env vars (not yet needed):

| Variable | Purpose |
|----------|---------|
| `INFLUXDB_URL/TOKEN/ORG/BUCKET` | InfluxDB Cloud — weather time-series |
| `TELEGRAM_BOT_TOKEN/CHAT_ID` | Telegram / Discord Bot — DQ alerts |

## Key principles

- **Bronze is immutable** — raw data is never modified after landing; re-run ingestion if you need to fix it.
- **Silver owns the cleaning logic** — all deduplication, null handling, and schema normalization happens in `silver_etl`; Gold only aggregates.
- **Fail loudly** — pipelines should raise errors, not silently skip bad records; data quality failures are surfaced to the user.
- **Document decisions** — every non-obvious architectural choice has a "Why" entry in `docs/architecture.md`.
