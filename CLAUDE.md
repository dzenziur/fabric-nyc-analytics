# Microsoft Fabric Data Engineering Project

Unified analytics platform on Microsoft Fabric that integrates NYC Taxi mobility data, OpenAQ air quality, World Bank GDP, ECB FX rates, and Open-Meteo weather into a single data lakehouse and warehouse.

**Architecture:** Medallion (Bronze → Silver → Gold) — raw landing in Lakehouse, cleaning via PySpark Notebooks, analytical star schema in Fabric Warehouse.

**Core technology:** Microsoft Fabric (Lakehouse, Data Factory Pipelines, Dataflow Gen2, PySpark Notebooks, Warehouse), Delta Lake, Python, T-SQL.

**Infrastructure as Code:** Terraform with `microsoft/fabric` provider — manages workspace + lakehouses + warehouse declaratively. See `terraform/`.

**External stack** (Phase 7): InfluxDB OSS (weather time-series, persistent volume), Grafana OSS (weather dashboard, auto-provisioned datasource), Great Expectations (data quality), Telegram bot with `/report` (DQ reports on demand, long-polling mode). All services run as Docker containers orchestrated by `docker-compose.yml` — local-first deployment, no cloud account dependencies beyond Fabric itself.

## Current Status

**Active branch:** `feature/external-integrations`
**Deadline:** May 21, 2026. Defense on May 26 but all artefacts must be ready by 21.
**Last session:** 2026-05-19 — Phase 7 started. Weather flow inside Fabric implemented end-to-end: `bronze_ingest_weather` notebook (Open-Meteo → bronze_weather, 47,112 rows 2021–2026), `silver_etl` extended with `## Weather` section (enriched renames + `is_rainy` derived flag + MERGE on watermark), `pl_master_orchestrator` integrated weather as parallel ingestion. Full pipeline run succeeded (10/10 activities). `app/` package scaffolded with CLI dispatcher (`python -m app {weather-sync,bot,ge-report}`), Dockerfile (python:3.11-slim + MS ODBC Driver 18), requirements.txt, `.env.example` for Phase 7 vars — bot and ge are stubs (NotImplementedError) to be filled in later tasks. Next: implement `weather_sync.py` against InfluxDB Cloud.

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
| Phase 7 — External Integrations | 🔄 In progress | Weather flow in Fabric done (bronze_weather + silver_weather + orchestrator); app/ scaffolded; weather→InfluxDB, GE, bot, Render deploy pending |

### Current branch goal (`feature/external-integrations`)

Phase 7 — External Integrations per spec section 7. Two integrations: (1) Weather → time-series DB → Grafana, (2) Great Expectations → Telegram bot `/report`. Fabric is the single source of truth; the external job reads enriched data **from Fabric** and pushes it to InfluxDB.

#### Weather flow inside Fabric ✅
- [x] `bronze_ingest_weather` notebook — Open-Meteo Archive + Forecast APIs → `bronze_lakehouse.bronze_weather`. Parameters: `year_start`, `year_end`, `force_refresh`. NYC single point (Manhattan); multi-point in backlog. Default `force_refresh=False` uses Forecast API `past_days=2` and MERGEs on `(lat, lon, datetime)`; `force_refresh=True` or first run uses Archive API for full year range + partition overwrite.
- [x] `silver_etl` — new `## Weather` section: cast datetime, enriched renames (`temperature_2m → temperature_c`, `apparent_temperature → feels_like_c`, `precipitation → precipitation_mm`, `wind_speed_10m → wind_speed_kmh`, `relative_humidity_2m → humidity_pct`), derived `is_rainy` flag, partition by year/month → `silver_lakehouse.silver_weather`. Incremental via `MAX(datetime)` watermark + MERGE (with `whenMatchedUpdateAll` because Open-Meteo retroactively refines recent data).
- [x] `pl_master_orchestrator` — `bronze_ingest_weather` added as parallel ingestion (depends on `prepare_taxi_ingestion` succeeded); `silver_etl` now depends on it succeeding (true fail-fast). Full pipeline run verified end-to-end (10/10 activities).
- [x] **No Gold / Power BI for weather** — Grafana on InfluxDB satisfies the visualisation requirement; a `FactWeatherDaily` with no downstream consumer would be dead code, so the medallion stops at Silver for weather.

#### External app scaffold 🔄 (in progress)
- [x] `app/` package scaffolded — CLI dispatcher (`python -m app {weather-sync,bot,ge-report}`), `config.py` (env vars via python-dotenv), `fabric_client.py` (pyodbc + Service Principal auth), `Dockerfile` (python:3.11-slim + MS ODBC Driver 18 + dependencies), `requirements.txt`, `.dockerignore`, `.env.example`. `bot.py` and `ge/__init__.py` are stubs raising `NotImplementedError` — to be filled in later tasks.
- [ ] Entra ID Service Principal — register app, grant Read access on `silver_lakehouse` SQL endpoint and `gold_warehouse`.

#### Weather export to InfluxDB + Grafana
- [ ] `app/weather_sync.py` — read new rows from `silver_weather` via Fabric SQL endpoint (watermark-based incremental), push to InfluxDB via `influxdb-client`.
- [ ] Grafana dashboard JSON — temperature, precipitation, wind, humidity panels. Provisioned automatically from `grafana/provisioning/`.

#### Great Expectations + Telegram Bot
- [ ] `app/ge/` — expectation suites for Silver + Gold layers (Bronze skipped — low ROI, in backlog). Per-table checks per `docs/data_dictionary.md`: nulls, ranges, FK integrity, row count ranges, distribution checks. Connection via Fabric SQL endpoints.
- [ ] `app/bot.py` — Telegram bot, `/report` command (user → bot direction). Long-polling mode (`run_polling()`) — no public URL required, no cold-start latency.

#### Local Docker Compose deployment
- [ ] `docker-compose.yml` at repo root — services: `influxdb` (OSS 2.x, persistent volume), `grafana` (OSS, datasource + dashboard auto-provisioned), `app-bot` (long-running, polling), `app-weather-sync` (sidecar with internal scheduler running hourly). Network: single bridge so app containers reach `influxdb:8086`.
- [ ] `grafana/provisioning/datasources/influxdb.yml` — InfluxDB datasource pointing at `http://influxdb:8086`, token from env.
- [ ] `grafana/provisioning/dashboards/weather.json` — pre-built weather dashboard.
- [ ] `docs/how_to_run.md` — quick start: `cp .env.example .env`, fill secrets, `docker compose up -d`, open Grafana on `localhost:3000`.

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
app/          External Python app (Phase 7) — single Docker image, multi-entry CLI
              (weather sync, Telegram bot, GE runner). Deployed on Render.com.
terraform/    IaC: workspace, lakehouses, warehouse (run `make help`)
docs/         Architecture, data dictionary, how-to-run, governance screenshots (img/)
spec/         Original project specification (PDF)
```

## Development workflow

| Branch | Phase | Status |
|--------|-------|--------|
| `feature/external-integrations` | Phase 7 — Weather + InfluxDB + Grafana + GE + Telegram bot | Active |

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

Phase 7 env vars (used by `app/`, full list in `.env.example`):

| Variable | Purpose |
|----------|---------|
| `INFLUXDB_URL` / `INFLUXDB_TOKEN` / `INFLUXDB_ORG` / `INFLUXDB_BUCKET` | InfluxDB connection — `http://influxdb:8086` in compose network, token + org seeded at first start |
| `FABRIC_SQL_SERVER` / `FABRIC_SP_CLIENT_ID` / `FABRIC_SP_CLIENT_SECRET` / `SILVER_LAKEHOUSE_DB` / `GOLD_WAREHOUSE_DB` | Fabric SQL endpoint via Entra ID Service Principal — read silver_weather + GE checks |
| `TELEGRAM_BOT_TOKEN` | Telegram bot auth (BotFather) — polling mode, no webhook required |

## Key principles

- **Bronze is immutable** — raw data is never modified after landing; re-run ingestion if you need to fix it.
- **Silver owns the cleaning logic** — all deduplication, null handling, and schema normalization happens in `silver_etl`; Gold only aggregates.
- **Fail loudly** — pipelines should raise errors, not silently skip bad records; data quality failures are surfaced to the user.
- **Document decisions** — every non-obvious architectural choice has a "Why" entry in `docs/architecture.md`.
