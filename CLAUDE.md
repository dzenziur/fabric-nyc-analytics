# NYC Analytics

> Unified analytics platform on **Microsoft Fabric** integrating NYC Taxi
> mobility, OpenAQ air quality, World Bank GDP, ECB FX, and Open-Meteo weather.
>
> - **Medallion architecture** — Bronze → Silver → Gold
> - **Power BI** dashboards on top of the Gold star schema
> - **External stack** — local Docker (InfluxDB + Grafana + Telegram DQ bot)
>   on top of the Fabric SQL endpoint

## Current Status

**Active branch:** `feature/polishing`
**Deadline:** May 21, 2026. Defense on May 26 but all artefacts must be ready by 21.
**Last session:** 2026-05-20 — Polishing pass mid-flight. Batch 0 (bronze download caching) and three of four Batch 1 items (silver DQ filters + taxi append logging) shipped and **verified end-to-end** — `/report` returns 56/56 PASS across 12 Silver + Gold tables (screenshot in `docs/how_to_run.md § Step 7f`). Full 2021–2026 master orchestrator backfill ran green in ~31 minutes wall-clock (73 activities, screenshot in `docs/how_to_run.md § Step 6`). Two TLC CloudFront 403 gotchas resolved along the way (missing-key vs anti-bot 403, `/misc/` path-specific anti-bot rules — see `Known data limitations`).

### Phase completion

| Phase | Status |
|-------|--------|
| Phase 0 — Terraform IaC | ✅ Done |
| Phase 1 — Bronze ingestion | ✅ Done |
| Phase 2 — Silver ETL | ✅ Done |
| Phase 3 — Gold / star schema | ✅ Done |
| Phase 4 — Visualizations | ✅ Done |
| Phase 5 — Master Orchestrator | ✅ Done |
| Phase 6 — Governance & Monitoring | ✅ Done |
| Phase 7 — External Integrations | ✅ Done |
| Phase 8 — Polish & Finalisation | 🔄 In progress |

### Current branch goal (`feature/polishing`)

No new features. The platform is feature-complete; this branch walks through `## Backlog` and lands the fixes that improve correctness, clarity, and defense-day polish. Each backlog item is its own focused commit. Priority order is decided per session — pick the next item from the backlog, implement, verify, commit, move on.

## Backlog

Items confirmed as needed but not yet scheduled. Claude reads this at the start of every session (see compaction instructions in `CLAUDE.local.md`). When a new improvement or fix is identified, add it here — do not leave it only in the conversation.

### Priority

Work is grouped into batches — one batch = one Power BI session / one notebook re-run / one doc pass — so each round of changes can be verified end-to-end before moving on. Order reflects defense-day visibility and correctness payoff.

| # | Priority | Status | Batch | What & why |
|---|----------|--------|-------|-----------|
| 0 | 🔥 P1 | ✅ Done (pending re-run) | **Bronze ingestion fixes & efficiency** | Pre-backfill bronze polish. Caching (`bronze_taxi_zones` skip-if-exists, `openaq_locations` count-diff skip, `openaq_measurements` station-activity pre-filter), TLC CloudFront 403 hotfixes (UA + reachability probe), parallelised `openaq_measurements` outer station loop to attack the 17m bottleneck. |
| 1 | 🔥 P1 | ✅ Done (pending re-run) | **Silver data-quality fixes** | One `silver_etl` re-run, then re-run GE. Closes real DQ findings visible in `/report` demo, restores `pickup_datetime` checks, fixes a corruption surface. High correctness payoff, low touch surface. |
| 2 | 🔥 P1 | ⬜ Not started | **Power BI signature features** | Most visible at defense — what the committee actually looks at. Sankey (Mobility), Scatter+Play (Correlation), Forecast (Economic Impact). |
| 3 | ⭐ P2 | ⬜ Not started | **Power BI readability polish** | Same Power BI Desktop session — insight text boxes per page, semantic-model rename (`city` → `station_name`), Smart Narrative, YoY indicators, theme JSON. |
| 4 | ⭐ P2 | 🔄 In progress | **Pipeline audits + defense screenshots** | Verification-only work + evidence captures for the defense slide deck — `pl_master_orchestrator` dependency audit, Fabric lineage view check, plus screenshots of the live external stack (`/report` in Telegram, a Power BI page, Grafana weather dashboard). |
| 5 | 💡 P3 | ⬜ Not started | **Notebook & docs clarity** | `year_start/year_end` semantics headers, `silver_etl` taxi append logging, `data_dictionary.md` type audit. Reviewer/handoff polish. |
| 6 | 🧊 P4 | 🧊 Deferred | **Incremental ETL — silver-side deferred** | Silver/dim full-rebuilds (FX, GDP, locations, dims). Already analysed as low-ROI; skip unless time allows. |

### Batch 0 — Bronze ingestion fixes & efficiency

Pre-backfill bronze polish — caching, TLC CloudFront 403 hotfixes, and one outstanding throughput optimisation. Caching items add a `force_refresh: bool = False` parameter so the orchestrator can still force a refresh on demand.

- [x] **`bronze_ingest_taxi_zones` — skip if table non-empty.** Added `force_refresh: bool = False` parameter; skips download via `notebookutils.notebook.exit` when `bronze_taxi_zones` has rows.
- [x] **`bronze_ingest_openaq_locations` — count-diff skip.** Added `force_refresh: bool = False` parameter + `limit=1` probe of `meta.found`; exits early when count matches bronze. Schema extended with `datetime_first`/`datetime_last` columns (data_dictionary.md updated).
- [x] **`bronze_ingest_openaq_measurements` — station activity pre-filter.** NYC bbox filter now followed by `[datetime_first, datetime_last]` overlap filter against `[year_start, year_end]`; graceful fallback with warning if activity columns absent.
- [x] **TLC CloudFront 403 hotfixes.** `bronze_ingest_taxi_zones` switched from `urllib` to `requests.get` with full browser-like headers (User-Agent + Accept + Accept-Language + Referer) because the `/misc/*` CloudFront path rejects bare UAs. `prepare_taxi_ingestion` keeps `urllib` + Chrome UA (the `/trip-data/*` path is permissive) and adds a reachability probe against a known-published month (2024-01) so missing-key 403s are disambiguated from anti-bot 403s. Full saga documented in the `Known data limitations` section below.
- [x] **`bronze_ingest_openaq_measurements` — parallelise outer station loop.** Outer loop wrapped in `ThreadPoolExecutor(max_workers=STATION_WORKERS=8)` with `as_completed` for result collection; inner per-station workers reduced from 50 → 16 to keep total concurrency at 128. boto3 client now configured with `max_pool_connections=128` to avoid pool exhaustion warnings. `spark.createDataFrame` stays serial on driver as futures complete (cheap in-memory step). Log order shifts from `nyc_ids` order to completion order. Expected 2–4× speedup on the 17m bottleneck — awaits next `pl_master_orchestrator` run to measure.

### Batch 1 — Silver data-quality fixes (`silver_etl` notebook)

Single notebook touch, re-run silver, re-run GE to verify the report goes 56/56.

- [x] **fare_amount outlier filter** — added `fare_amount <= 10_000` alongside `fare_amount > 0` in `silver_etl` taxi section. Drops ~13 corrupted TLC rows ($187k–$863k fares); awaits silver re-run + GE verification.
- [x] **OpenAQ non-pollutant filter** — `silver_etl` OpenAQ section now restricts `parameter` to the pollutant set (`pm25/pm10/pm1/no2/o3/co/so2/no/nox`); GE `parameter` value_set narrowed accordingly. Drops `temperature`, `relativehumidity`, `um003` rows.
- [x] **Taxi append logging** — incremental append now logs `appended N rows; silver total: M` instead of just "rows before append".
- [x] **TIMESTAMP_NTZ → timestamp cast** — `silver_etl` taxi section now casts `pickup_datetime`/`dropoff_datetime` from TIMESTAMP_NTZ to TIMESTAMP before write; GE suite re-adds `pickup_datetime not null`; data_dictionary updated. Awaits `silver_etl` re-run with `force_refresh=True` to rewrite the Delta files, then `/report` should show 57/57.

### Batch 2 — Power BI signature features (`nyc_analytics_report`)

One "wow" feature per page beyond the standard charts. Air Quality already has the Azure Maps station bubble visual (done).

- [ ] **Mobility — Sankey diagram** (pickup zone → dropoff zone). Requires gold_etl change: either rebuild `FactTaxiDaily` with `DO_zone_key` in addition to `zone_key` (PU), OR create a new `FactTaxiFlows` table aggregated by (PU, DO, year). Marketplace visual: "Sankey" by Microsoft. Maps actual NYC movement patterns.
- [ ] **Correlation — Scatter plot with Play Axis animation**. Replace current bar+line monthly aggregate with daily scatter (one point per day): X=Total Trips, Y=Avg PM2.5, Play axis=year. Shows the actual correlation shape. Built-in scatter visual supports Play axis natively.
- [ ] **Economic Impact — Forecast on USD/EUR line chart**. Built-in Analytics pane → Forecast. Length 90 days with 95% CI. Demonstrates predictive analytics without external tools. Alternative: waterfall chart for YoY revenue change.

### Batch 3 — Power BI readability polish (same Power BI Desktop session)

- [ ] **Key insight text boxes** — 2–3 sentences per page, specific numbers:
  - Mobility: post-COVID growth, busiest zones, avg fare trend
  - Air Quality: PM2.5 seasonality, NO2 rush-hour pattern, data coverage note (2023+)
  - Correlation: trips vs PM2.5 overlay observation, caveat about 2023+ data
  - Economic Impact: revenue growth 2021→2025, EUR/USD gap explanation, GDP scale context
- [ ] **Semantic model field rename** — `FactAirQualityDaily.city` actually contains station names (OpenAQ `location_name`), not city names; rename to `station_name` in `nyc_analytics_model` and update all visuals that reference it. Full pass of all column display names across all tables to ensure they match what the data actually contains.
- [ ] **Smart Narrative AI visual** on at least one page (e.g., Mobility) — auto-generated text insights from KPIs.
- [ ] **YoY change indicators** on monetary/count KPI cards (Mobility, Economic Impact) — up/down arrow + colour based on previous-year comparison (where WHO-style threshold doesn't apply).
- [ ] **Power BI theme JSON** — export current theme and check into repo for consistency across reports (deferred — Fabric Direct Lake report theme handling not yet evaluated).

### Batch 4 — Pipeline audits + defense screenshots

Verification work and evidence captures for the defense slide deck. Screenshots land in `docs/img/` and are referenced from `docs/architecture.md` / `docs/how_to_run.md` where relevant.

- [x] **`pl_master_orchestrator` full-run screenshot** — `docs/img/pl_master_orchestrator_full_run.png` (2026-05-20, 6-year backfill, 73/73 activities green). Referenced from `docs/how_to_run.md § Typical activity durations`.
- [ ] **`pl_master_orchestrator` dependency audit** — verify correct execution order (bronze → silver → gold), no missing or redundant dependency links between activities.
- [ ] **Fabric lineage view completeness** — verify built-in lineage (`Workspace → Lineage view`) captures all expected upstream/downstream edges. Suspected gaps: notebook → table edges may not be visible for all notebooks; external source nodes may be missing for some pipelines. Compare graph against `docs/architecture.md` → Data Flow section and document any missing edges (annotations or screenshot caveats). If gaps are significant, re-evaluate Purview Data Map (free Azure tier).
- [x] **Telegram `/report` screenshot** — `docs/img/telegram_report.png` captured 2026-05-20: 56/56 PASS across all 12 Silver + Gold tables. Embedded in `docs/how_to_run.md § Step 7f`.
- [ ] **Power BI dashboard screenshot** — one representative page (e.g. Mobility overview with KPIs + map). Save as `docs/img/powerbi_mobility.png`. Reference from `docs/architecture.md` (Visualizations section). Capture after Batch 2 signature features land so the screenshot reflects final state.
- [x] **Grafana weather dashboard screenshot** — `docs/img/grafana_weather.png` captured 2026-05-20: 4-panel NYC Weather dashboard (Temperature + feels_like, Precipitation, Wind speed, Humidity) over Last 30 days. Embedded in `docs/how_to_run.md § Step 7e`.

### Batch 5 — Notebook & docs clarity (markdown-only edits)

- [ ] **`year_start`/`year_end` semantics in notebook header cells** — explicitly state per notebook when the parameters are used vs. ignored:
  - `prepare_taxi_ingestion` — **always uses** year range (determines which TLC months to check/download); critical for schedule correctness
  - `bronze_ingest_openaq_measurements` — **ignores** year range when `force_refresh=False` (fetches current+prev month only); used only with `force_refresh=True`
  - `silver_etl` taxi section — **ignores** year range when `force_refresh=False` (partition diff); used only with `force_refresh=True`
  - `silver_etl` OpenAQ section — **ignores** year range when `force_refresh=False` (watermark); used only with `force_refresh=True`
  - `gold_etl` FactTaxiDaily/FactAirQualityDaily — **ignores** year range when `force_refresh=False` (7-day lookback); used only with `force_refresh=True`
  - `gold_etl` DimDate — **uses** year range as a floor/ceiling, expanded by actual data min/max
- [ ] **`docs/data_dictionary.md` type audit** — run `printSchema()` for each Bronze and Silver table and compare against the doc. First known discrepancy: `bronze_openaq_measurements.datetime` is `string` in practice, was `timestamp` in docs (already fixed).

### Batch 6 — Incremental ETL silver-side (deferred)

Bronze caching shipped in Batch 0. What remains here is silver/dim full-rebuilds — already analysed as low-ROI given the table sizes; skip for defense.

- [ ] `silver_fx_rates` — ~7k rows; read only `date > MAX(silver.date)` from bronze, append.
- [ ] `silver_gdp` — ~6k rows, yearly data, rarely changes; skip if bronze unchanged.
- [ ] `silver_openaq_locations` — ~5k rows, rare changes; MERGE only updated stations.
- [ ] `DimDate`/`DimZone`/`DimFX`/`DimGDP` — full rebuild <30s combined; not worth complexity.

## Known data limitations

Upstream quirks we accept and work around — not bugs to fix on our side.

- **OpenAQ historical coverage** — NYC stations in the S3 archive have data for 2021–2025, but with coverage gaps: mid-2022 (May–Sep) shows near-zero measurements for many stations. Not a pipeline issue — reflects actual S3 archive availability.
- **Multi-pollutant station coverage** — not all OpenAQ stations measure all pollutants; some stations have gaps in NO2/O3 data. Known data limitation from OpenAQ source.
- **TLC parquet schema drift** — TLC keeps changing the yellow_tripdata schema across years:
  - **pre/post mid-2023** — INT32 vs INT64 for location columns
  - **2023+** — added column `cbd_congestion_fee` (NYC Central Business District congestion fee)
  - **2026+** — `passenger_count` (long vs double), `RatecodeID` (long vs double), renamed `airport_fee` → `Airport_fee` (capitalisation)
  - Handled in `silver_etl` via (a) per-file normalisation: rename `Airport_fee` → `airport_fee` if present, cast `VendorID`/`PULocationID`/`DOLocationID`/`payment_type` → long, `passenger_count`/`RatecodeID` → double; and (b) `write_silver(merge_schema=True)` on the taxi write, so Delta auto-evolves the table when TLC adds new columns mid-range (older partitions get NULL for the new column).
- **OpenAQ gas units** — NO2, O3, CO, NO, NOx, SO2 are stored in ppm in the S3 archive; PM2.5/PM1/PM10 in µg/m³. `silver_etl` normalizes all gas parameters to µg/m³ using EPA conversion factors at 25°C.
- **TLC CloudFront 403 — two distinct problems**:
  1. **Missing-key 403 (not 404)** — origin policy denies on miss, so a HEAD on an unpublished month returns 403, indistinguishable on the wire from an anti-bot block. `prepare_taxi_ingestion` resolves this with a reachability probe against a known-published reference month (2024-01) before iterating; if the reference also 403s the notebook aborts loudly, otherwise per-month 403/404 is silently treated as "not yet published".
  2. **Path-specific anti-bot rules** — `/trip-data/*.parquet` is permissive (Chrome User-Agent header is enough). `/misc/taxi_zone_lookup.csv` is stricter and rejects even the full Chrome UA via `urllib`. `bronze_ingest_taxi_zones` therefore uses `requests.get` with a full browser-like header set (`User-Agent` + `Accept` + `Accept-Language` + `Referer: nyc.gov`). `prepare_taxi_ingestion` keeps `urllib` + Chrome UA because the parquet path is permissive.
  - The Copy activity used by `ingest_taxi_month` inside `pl_ingest_nyc_taxi` is unaffected — it has its own HTTP client.

## Key reference docs

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
app/          External Python app — CLI dispatcher (weather-sync, ge-report, bot).
              Single Docker image, three docker-compose services.
terraform/    IaC: workspace, lakehouses, warehouse (run `make help`)
grafana/      Provisioned datasource + dashboards (mounted into Grafana container)
docs/         Architecture, data dictionary, how-to-run, governance screenshots (img/)
spec/         Original project specification (PDF)
Makefile      Compose + IaC shortcuts (`make help` lists targets)
```

## Data sources

| Source | Format | Ingestion tool |
|--------|--------|----------------|
| NYC Taxi (TLC) | Parquet, monthly | Data Factory Pipeline |
| OpenAQ Air Quality | JSON API + S3 archive | PySpark Notebook |
| World Bank GDP | JSON API | Dataflow Gen2 |
| ECB FX rates | CSV API | Dataflow Gen2 |
| Open-Meteo Weather | JSON API | Python job (`app/weather_sync.py`) |

## Quick start

Two surfaces — Fabric workspace and the local Docker stack.

**Fabric** — push the branch, then in the Fabric UI: `Workspace → Source control → Update all`. Trigger the platform via `pl_master_orchestrator`.

**Local stack** — driven by `Makefile` (run from repo root, Docker Desktop required):

```bash
make build        # build the app image
make up           # start influxdb + grafana + app-weather-sync + app-bot
make ps           # status
make logs-sync    # tail weather-sync logs (also: logs-bot, logs-influx, logs-grafana)

make weather-sync-once   # one-shot Fabric → InfluxDB sync
make ge-report           # run Great Expectations, print report to stdout

make down         # stop containers (keep volumes)
make clean        # stop + DELETE volumes (wipes InfluxDB + Grafana data)
```

Grafana: <http://localhost:3000> · InfluxDB: <http://localhost:8086>

Full setup (Service Principal, BotFather, `.env`): see [docs/how_to_run.md](docs/how_to_run.md).
Architecture decisions: see [docs/architecture.md](docs/architecture.md).

## Environment

The `app/` container reads `.env` at the repo root (template in `.env.example`):

| Variable | Purpose |
|----------|---------|
| `INFLUXDB_URL` / `INFLUXDB_TOKEN` / `INFLUXDB_ORG` / `INFLUXDB_BUCKET` | InfluxDB connection — `http://influxdb:8086` inside the compose network; token + org seeded at first start |
| `INFLUXDB_INIT_USERNAME` / `INFLUXDB_INIT_PASSWORD` / `INFLUXDB_INIT_MODE` | InfluxDB bootstrap (only used on first container start) |
| `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` | Grafana admin login |
| `FABRIC_SQL_SERVER` / `FABRIC_SP_CLIENT_ID` / `FABRIC_SP_CLIENT_SECRET` / `SILVER_LAKEHOUSE_DB` / `GOLD_WAREHOUSE_DB` | Fabric SQL endpoint via Entra ID Service Principal — `weather_sync` reads `silver_weather`; GE runner reads Silver + Gold tables |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_ALLOWED_CHAT_IDS` | Telegram long-polling bot — token from BotFather; optional comma-separated chat-id allowlist |
| `WEATHER_SYNC_INTERVAL_SECONDS` | Scheduler tick for `app-weather-sync` (3600 in compose; `0` = one-shot) |

## Key principles

- **Bronze is immutable** — raw data is never modified after landing; re-run ingestion to fix it.
- **Silver owns cleaning** — all deduplication, null handling, and schema normalization happens in `silver_etl`; Gold only aggregates.
- **Fabric is the source of truth** — the external Docker stack is a read-only consumer of the Fabric SQL endpoint; nothing flows back into Fabric.
- **Fail loudly** — pipelines raise errors instead of silently skipping bad records; data quality findings surface to the user via `/report`.
- **Document decisions** — every non-obvious architectural choice has a "Why" entry in `docs/architecture.md`.
- **Backlog over conversations** — improvements found mid-task land in `## Backlog`, not only in chat.
