# Microsoft Fabric Data Engineering Project

Unified analytics platform on Microsoft Fabric that integrates NYC Taxi mobility data, OpenAQ air quality, World Bank GDP, ECB FX rates, and Open-Meteo weather into a single data lakehouse and warehouse.

**Architecture:** Medallion (Bronze → Silver → Gold) — raw landing in Lakehouse, cleaning via PySpark Notebooks, analytical star schema in Fabric Warehouse.

**Core technology:** Microsoft Fabric (Lakehouse, Data Factory Pipelines, Dataflow Gen2, PySpark Notebooks, Warehouse), Delta Lake, Python, T-SQL.

**Infrastructure as Code:** Terraform with `microsoft/fabric` provider — manages workspace + lakehouses + warehouse declaratively. See `terraform/`.

**External stack** (Phase 7): InfluxDB OSS (weather time-series, persistent volume), Grafana OSS (weather dashboard, auto-provisioned datasource), Great Expectations (data quality), Telegram bot with `/report` (DQ reports on demand, long-polling mode). All services run as Docker containers orchestrated by `docker-compose.yml` — local-first deployment, no cloud account dependencies beyond Fabric itself.

## Current Status

**Active branch:** `feature/external-integrations`
**Deadline:** May 21, 2026. Defense on May 26 but all artefacts must be ready by 21.
**Last session:** 2026-05-19 — Phase 7 complete end-to-end. On top of the weather pipeline (Fabric → app/weather_sync → InfluxDB → Grafana), added `app/ge/` Great Expectations runner (56 expectations across 12 Silver + Gold tables, hybrid: GE PandasDataset for small tables + SQL aggregates for large), `app/bot.py` Telegram long-polling bot (`/report` → `asyncio.to_thread(run_report)` → reply as HTML `<pre>` block), `app-bot` service in compose, `docs/how_to_run.md § Step 7` setup walkthrough. End-to-end smoke test: `/report` in Telegram returns 55/56 passing checks (one legitimate DQ finding: 13 rows with `fare_amount > $10k` from corrupted TLC upstream data). Phase 7 ready for PR + merge to main.

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
| Phase 7 — External Integrations | ✅ Done | Weather pipeline (Fabric→InfluxDB→Grafana) + Great Expectations runner (56 checks, Silver + Gold) + Telegram bot `/report` (long-polling) + docker-compose stack + Makefile + how_to_run docs |

### Current branch goal (`feature/external-integrations`)

Phase 7 — External Integrations per spec section 7. Two integrations: (1) Weather → time-series DB → Grafana, (2) Great Expectations → Telegram bot `/report`. Fabric is the single source of truth; the external job reads enriched data **from Fabric** and pushes it to InfluxDB. **Status: ready for PR + merge to main.**

#### Weather flow inside Fabric
- [x] `bronze_ingest_weather` notebook — Open-Meteo Archive + Forecast APIs → `bronze_lakehouse.bronze_weather`. Parameters: `year_start`, `year_end`, `force_refresh`. NYC single point (Manhattan); multi-point in backlog. Default `force_refresh=False` uses Forecast API `past_days=2` and MERGEs on `(lat, lon, datetime)`; `force_refresh=True` or first run uses Archive API for full year range + partition overwrite.
- [x] `silver_etl` — new `## Weather` section: cast datetime, enriched renames (`temperature_2m → temperature_c`, `apparent_temperature → feels_like_c`, `precipitation → precipitation_mm`, `wind_speed_10m → wind_speed_kmh`, `relative_humidity_2m → humidity_pct`), derived `is_rainy` flag, partition by year/month → `silver_lakehouse.silver_weather`. Incremental via `MAX(datetime)` watermark + MERGE (with `whenMatchedUpdateAll` because Open-Meteo retroactively refines recent data).
- [x] `pl_master_orchestrator` — `bronze_ingest_weather` added as parallel ingestion (depends on `prepare_taxi_ingestion` succeeded); `silver_etl` now depends on it succeeding (true fail-fast). Full pipeline run verified end-to-end (10/10 activities).
- [x] **No Gold / Power BI for weather** — Grafana on InfluxDB satisfies the visualisation requirement; a `FactWeatherDaily` with no downstream consumer would be dead code, so the medallion stops at Silver for weather.

#### Weather export to InfluxDB + Grafana
- [x] **Entra ID Service Principal** — registered app `nyc-analytics-app`, granted Viewer on workspace (covers SQL endpoint read for `silver_lakehouse` and `gold_warehouse`). Tenant setting "Service principals can call Fabric APIs" enabled.
- [x] **`app/` package** — `__main__.py` CLI dispatcher (commands: `weather-sync`, `ge-report`, `bot`) with `WEATHER_SYNC_INTERVAL_SECONDS` scheduler loop, `config.py`, `fabric_client.py` (pyodbc + SP auth), `influx_client.py`, `weather_sync.py`. `Dockerfile` (python:3.11-slim + MS ODBC Driver 18), `requirements.txt`, `.dockerignore`, `.env.example`.
- [x] **`app/weather_sync.py`** — watermark from InfluxDB last `_time` of `weather` measurement, T-SQL incremental `WHERE datetime > watermark` against `silver_weather`, batched write of Points. Verified end-to-end: 47,112 historical Points written.
- [x] **`docker-compose.yml`** — services `influxdb` (OSS 2.7, persistent volume, init bootstrap, healthcheck), `grafana` (OSS 11.2, waits on influxdb healthy, provisioning mount), `app-weather-sync` (builds local Dockerfile, hourly scheduler loop).
- [x] **`grafana/provisioning/`** — InfluxDB datasource + `weather.json` 4-panel dashboard (temperature, precipitation, wind, humidity).
- [x] **`Makefile`** — compose lifecycle, build/rebuild, ps + per-service logs, `weather-sync-once` and `ge-report` ad-hoc runs.

#### Great Expectations + Telegram Bot
- [x] **`app/ge/`** — 56 expectations across 12 Silver + Gold tables (Bronze skipped — low ROI, in backlog). Hybrid execution: GE PandasDataset for small tables (locations, gdp, fx, weather, all dims), SQL aggregates for large (taxi_trips, openaq_measurements, FactTaxiDaily, FactAirQualityDaily). `result.py` `CheckResult` dataclass + `format_report()` monospace text. Per-suite try/except so one failure doesn't break the report.
- [x] **`app/bot.py`** — Telegram bot via `python-telegram-bot` v21 in long-polling mode. `/start` welcome + `/report` placeholder-edit UX (`asyncio.to_thread(run_report)` so blocking pyodbc/pandas/GE doesn't freeze the event loop). HTML `<pre>` formatting. Optional `TELEGRAM_ALLOWED_CHAT_IDS` allowlist.
- [x] **`app-bot` service** in `docker-compose.yml` — reuses the image, `restart: unless-stopped`.

#### Docs
- [x] `docs/how_to_run.md § Step 7` — end-to-end Phase 7 walkthrough: SP registration, BotFather, `.env` fill, `make build` + `make up`, Grafana on `localhost:3000`, `/report` in Telegram.
- [x] `docs/architecture.md § Phase 7` — current Implemented list (no Pending items remain).

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

### Silver — taxi fare_amount outlier filter
- [ ] `silver_taxi_trips` contains ~13 rows from TLC source with `fare_amount` between $187k and $863k for trips of 1.2-21.3 miles — clearly corrupted upstream data (real NYC taxi fares cannot exceed ~$2k even in extreme cases). Currently surfaced as a real DQ finding by the GE suite. Better fix: add a sanity filter in `silver_etl` taxi section to drop rows with `fare_amount > 10_000` (alongside the existing `fare_amount > 0` filter). Then re-run silver and remove the recurring DQ failure.

### Silver — OpenAQ non-pollutant parameters
- [ ] `silver_openaq_measurements` currently contains non-pollutant parameters (`temperature`, `relativehumidity`, `um003`) that OpenAQ co-hosts with the pollutants. Surfaced by GE DQ check (parameter value_set originally listed only pollutants → ~612k rows flagged). For now value_set is broadened to include them; better fix is to either (a) filter them out in silver_etl since the project's analytical questions are about pollution only, or (b) split into a separate `silver_openaq_context` table.

### Silver — TIMESTAMP_NTZ unreadable via SQL endpoint
- [ ] `silver_taxi_trips.pickup_datetime` and `dropoff_datetime` are TIMESTAMP_NTZ in Delta and therefore invisible to the Lakehouse SQL endpoint (Fabric limitation — known issue with `timestamp_ntz` type, hidden from T-SQL surface). Same root cause as the Spark CBO bug we already work around at read time in `gold_etl` (cast `timestamp_ntz → timestamp` to avoid `FilterEstimation` MatchError when Delta column statistics are present). Current impact is narrower: GE DQ checks can't inspect those columns and any tool connecting via SQL only (e.g. Power BI in DirectQuery against the Lakehouse) won't see them. **Fix:** apply the same `timestamp_ntz → timestamp` cast at silver_etl write time, so the columns land typed as `timestamp` and become visible to the SQL endpoint. Then re-add `pickup_datetime not null` to the GE suite for `silver_taxi_trips`.

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
app/          External Python app (Phase 7) — CLI dispatcher + weather_sync.
              Single Docker image used by docker-compose service app-weather-sync.
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
| `INFLUXDB_INIT_USERNAME` / `INFLUXDB_INIT_PASSWORD` / `INFLUXDB_INIT_MODE` | InfluxDB bootstrap (used only on first container start) |
| `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` | Grafana admin login |
| `FABRIC_SQL_SERVER` / `FABRIC_SP_CLIENT_ID` / `FABRIC_SP_CLIENT_SECRET` / `SILVER_LAKEHOUSE_DB` / `GOLD_WAREHOUSE_DB` | Fabric SQL endpoint via Entra ID Service Principal — read silver_weather + (later) GE checks |

## Key principles

- **Bronze is immutable** — raw data is never modified after landing; re-run ingestion if you need to fix it.
- **Silver owns the cleaning logic** — all deduplication, null handling, and schema normalization happens in `silver_etl`; Gold only aggregates.
- **Fail loudly** — pipelines should raise errors, not silently skip bad records; data quality failures are surfaced to the user.
- **Document decisions** — every non-obvious architectural choice has a "Why" entry in `docs/architecture.md`.
