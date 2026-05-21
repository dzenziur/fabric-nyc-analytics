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
**Last session:** 2026-05-21 (Part 2) — Power BI + semantic model polish round. Shipped: (1) **Semantic model polish** — `FactAirQualityDaily.city` → `station_name`, all surrogate FK keys hidden (`date_key`, `zone_key`, `fx_key`, `location_id`, `gdp_key`), `country` (always "US") and raw `gdp_usd` hidden; (2) **DimGDP M2M relationship tried & dropped** — `DimDate[year] ↔ DimGDP[year]` caused cross-filter side effects (clicking taxi revenue bar blanked GDP chart). DAX `SELECTEDVALUE` approach used instead; (3) **Economic Impact redesign** — exchange rate chart removed and reworked, then restored after spec audit (required for question 3 "exchange rate fluctuation"); new measure `Revenue as % of US GDP` connects taxi revenue to GDP via DAX (no relationship); GDP card converted to `% of US GDP` card; `USA GDP (USD)` measure rewritten with `COALESCE(SELECTEDVALUE(DimGDP[year]), SELECTEDVALUE(DimDate[year]))` so card reacts to both DimGDP-axis and DimDate-axis cross-filters without breaking the GDP-by-year line chart; (4) **YoY KPI indicators** on Mobility + Economic Impact — Year slicer on each page, 2 DAX measures per KPI (`<X> YoY %` numeric + `<X> YoY Label` string with ▲/▼), Card (new) reference label with conditional font color (green ≥0, red <0). Applied to Total Trips, Total Revenue USD, Avg Fare USD, Total Revenue EUR; (5) **Correlation page enriched** — added 3 Pearson r measures (`Correlation Trips vs PM2.5/NO2/O3`) using SUMMARIZE+SUMX pattern; renamed visual fields to `r vs PM2.5/NO2/O3`; removed redundant Avg pollutant cards (already on Air Quality page); added Avg O3 line to combo chart; (6) **Semantic model spec-aware cleanup** — deleted empty `Measure` placeholder + `Avg Trip Distance (mi)` (not required by spec); kept `Avg Trip Duration (min)` (spec insight "trip duration trends"), `Avg FX Rate` + DimFX (spec Phase 3 requires DimFX in star schema; question 3 requires FX fluctuation visual); (7) **4 dashboard screenshots** captured: `docs/img/powerbi_{mobility,air_quality,correlation,economic_impact}.png`. Earlier session (Part 1) — Pipeline + docs polish round. Shipped: (1) `pl_master_orchestrator` dependency audit — removed 5 redundant `prepare_taxi_ingestion` links, only `ForEach_taxi_months` truly depends on prepare now (eliminates single-point-of-failure; verified — bronze layer now finishes in ~7 min total, `bronze_ingest_openaq_measurements` 17m → 5m 37s after parallelisation); (2) `bronze_ingest_openaq_measurements` union chain → single `pd.concat` (cleaner code, removes long Spark plan tree); (3) Fabric lineage view audit — captured 4 per-item screenshots (`bronze/silver/gold/orchestrator`), embedded in `how_to_run.md`, documented platform-level gap (notebook external sources invisible); (4) **Full `data_dictionary.md` type audit** — mass `float`→`double` and `int`→`long` fixes across Bronze+Silver+Gold, added missing `year`/`datetime_first`/`datetime_last` columns, deleted stale FactWeatherDaily/InfluxDB/GE sections (replaced with pointers to source files), clarified `bronze_taxi_trips` is source-schema only; (5) backlog restructure — dropped Sankey after honest ROI reassessment (2-stage flow not worth gold-schema cost), dropped Batch 6 (deferred silver-side incremental), reordered Power BI work — readability polish first (Batch 2), signature features second (Batch 3); (6) **silver_taxi_zones added for medallion strictness** — `gold_etl.DimZone` now reads from silver (not bronze); cleans up the gold↔bronze architectural shortcut, also resolves the schema-mismatch failure seen in the 2026-05-21 6-year backfill (gold_etl saw `StringType` from bronze vs `IntegerType` in Warehouse DimZone). Operational step: drop `gold_warehouse.dbo.DimZone` once, then re-run `pl_master_orchestrator` — silver_etl will create `silver_taxi_zones` with int location_id, gold_etl will recreate DimZone with matching schema.

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
| Phase 9 — Defense preparation | ⬜ Not started |

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
| 2 | 🔥 P1 | ✅ Done | **Power BI signature features** | Economic Impact `Revenue as % of US GDP` + FX chart restored; Correlation Pearson r cards (PM2.5/NO2/O3) shipped. |
| 3 | 🔥 P1 | ✅ Done | **Power BI readability polish** | Semantic model polish, YoY indicators, 4 dashboard screenshots shipped. |
| 4 | ⭐ P2 | ✅ Done | **Pipeline audits + defense screenshots** | Verification-only work + evidence captures for the defense slide deck — `pl_master_orchestrator` dependency audit, Fabric lineage view check, plus screenshots of the live external stack (`/report` in Telegram, a Power BI page, Grafana weather dashboard). |
| 5 | 💡 P3 | ⬜ Not started | **Notebook & docs clarity** | `year_start/year_end` semantics headers, `silver_etl` taxi append logging, `data_dictionary.md` type audit. Reviewer/handoff polish. |

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

**Start here.** Two substantive analytical visuals — each carries a real insight, not just aesthetic. Air Quality already has the Azure Maps station bubble visual (done). **Sankey dropped** after honest reassessment: pickup→dropoff is a 2-stage flow (Sankey shines on 3+ stages), top-N would just confirm well-known Manhattan↔Manhattan + airport hubs, and the gold-schema cost (new `FactTaxiFlows` table + semantic model relationships) wasn't justified by the marginal insight.

- [x] **Economic Impact — Revenue as % of US GDP** + FX chart restored. New measure `Revenue as % of US GDP = DIVIDE([Total Revenue USD], CALCULATE(MAX(DimGDP[gdp_usd]), DimGDP[country_code]="US", DimGDP[year]=SELECTEDVALUE(DimDate[year])))` connects taxi revenue to GDP via DAX (no model relationship — M2M tried earlier and dropped). GDP card converted to `% of US GDP` card. Chart switched from line to bars (4 data points read better). **FX chart restored** on bottom row (2021–2026 window) after spec audit — Phase 3 requires DimFX dimension, question 3 requires "exchange rate fluctuation". **Caveat:** World Bank GDP data ends 2024; 2025–2026 show BLANK.
- [x] **Correlation page — Pearson r cards** (signature analytical content). 3 DAX measures `Correlation Trips vs PM2.5/NO2/O3` using `SUMMARIZE` + `SUMX` over `DimDate[date_key]` to compute Pearson correlation coefficient. Visual fields renamed to `r vs PM2.5/NO2/O3` for clean display. Removed redundant Avg pollutant cards (duplicate of Air Quality page); added Avg O3 line to combo chart for visual consistency.

### Batch 3 — Power BI readability polish (same Power BI Desktop session)

One comprehensive polish pass after the new visuals from Batch 2 land — applies to the final state of the report. Highest defense-day ROI per hour of work: text + correct field names + YoY signals are what the committee actually reads and remembers.

- [x] **Semantic model field rename** — `FactAirQualityDaily.city` renamed to `station_name` in TMDL (`sourceColumn: city` preserved). All surrogate FK keys hidden (`date_key`, `zone_key`, `fx_key`, `location_id`, `gdp_key`) across all tables. `FactAirQualityDaily.country` (always "US") and `DimGDP.gdp_usd` (raw column, wrapped by measure) hidden. **Pending:** update any visuals that still reference the old `city` field name after publishing to Fabric.
- [x] **YoY change indicators** on monetary/count KPI cards (Mobility, Economic Impact). Pattern: Year slicer on each page; 2 DAX measures per KPI (`<X> YoY %` for color, `<X> YoY Label` returns string like `▲ +6.9% vs 2023`); Card (new) visual with reference label = YoY Label measure; conditional font color (green ≥0, red <0) bound to YoY % measure. Applied to: `Total Trips`, `Total Revenue USD`, `Avg Fare USD` (Mobility), `Total Revenue USD`, `Total Revenue EUR` (Economic Impact). Without slicer → label is BLANK. Avg Trip Distance skipped (low analytical value).
- [x] **Power BI dashboard screenshots** — all 4 pages captured: `docs/img/powerbi_{mobility,air_quality,correlation,economic_impact}.png`. Embed in `docs/architecture.md` (Visualizations section) — pending.

### Batch 4 — Pipeline audits + defense screenshots

Verification work and evidence captures for the defense slide deck. Screenshots land in `docs/img/` and are referenced from `docs/architecture.md` / `docs/how_to_run.md` where relevant.

- [x] **`pl_master_orchestrator` full-run screenshot** — `docs/img/pl_master_orchestrator_full_run.png` (2026-05-20, 6-year backfill, 73/73 activities green). Referenced from `docs/how_to_run.md § Typical activity durations`.
- [x] **`pl_master_orchestrator` dependency audit** — removed 5 redundant `dependsOn: prepare_taxi_ingestion` links from `df_ecb_fx`, `df_worldbank_gdp`, `bronze_ingest_openaq_locations`, `bronze_ingest_taxi_zones`, `bronze_ingest_weather`. Only `ForEach_taxi_months` truly depends on prepare (consumes its `exitValue`); the rest now start fully parallel. Eliminates single-point-of-failure where a TLC outage blocked unrelated OpenAQ/Weather/FX/GDP ingestion. Doc updates in `fabric/README.md`, `docs/how_to_run.md`.
- [x] **Fabric lineage view completeness** — verified 2026-05-20. Per-item lineage covers all internal item-to-item edges correctly (`bronze_lakehouse`, `silver_lakehouse`, `gold_warehouse`, `pl_master_orchestrator` screenshots in `docs/img/lineage_*.png`, embedded in `docs/how_to_run.md § Fabric Lineage View`). **Known gap (platform limitation):** Fabric can't introspect `requests`/`boto3`/`urllib` calls inside notebooks, so external HTTP/S3 sources are only visible for Dataflow Gen2 consumers (`Web → df_ecb_fx`). TLC CloudFront, OpenAQ REST + S3, Open-Meteo edges to notebooks not drawn; documented as caveat. Purview Data Map not needed.
- [x] **Telegram `/report` screenshot** — `docs/img/telegram_report.png` captured 2026-05-20: 56/56 PASS across all 12 Silver + Gold tables. Embedded in `docs/how_to_run.md § Step 7f`.
- [x] **Grafana weather dashboard screenshot** — `docs/img/grafana_weather.png` captured 2026-05-20: 4-panel NYC Weather dashboard (Temperature + feels_like, Precipitation, Wind speed, Humidity) over Last 30 days. Embedded in `docs/how_to_run.md § Step 7e`.

### Batch 5 — Notebook & docs clarity (markdown-only edits)

- [x] **`year_start`/`year_end` semantics in notebook header cells** — 2026-05-21: added per-source explicit "used / ignored" semantics under each notebook's header markdown cell in `prepare_taxi_ingestion`, `bronze_ingest_openaq_measurements`, `bronze_ingest_weather`, `silver_etl`, `gold_etl`. Also added `silver_taxi_zones` to silver_etl Input list and gold_etl Input list (was missed during medallion-strictness commit).
- [x] **`docs/data_dictionary.md` type audit** — 2026-05-20: full Bronze + Silver + Gold `printSchema()` diff. Fixed mass `float`→`double` and `int`→`long` drift across most numeric/ID columns (Spark long = SQL BIGINT in Warehouse). Added missing columns: `year` in `bronze_openaq_measurements`, `datetime_first`/`datetime_last` in `silver_openaq_locations`. Clarified `bronze_taxi_trips` is source-schema only (raw Parquet files, not a Delta table). Deleted obsolete `FactWeatherDaily` section (weather → InfluxDB, not gold). Replaced InfluxDB + GE inline schemas with pointers to authoritative source files (`app/weather_sync.py`, `app/ge/suites.py`). Fixed `cbd_congestion_fee` year (2023+ → 2025+) consistently. Fixed `silver_taxi_trips.payment_type` to list all 6 codes.

### Phase 9 — Defense preparation

Talking points and insights for the May 26 defense. Markdown-only, no code changes.

- [ ] **Answer the 4 key analytical questions** from `spec/Microsoft Fabric Data Engineering Project.pdf` with concrete numbers from the dashboards:
  1. How does traffic intensity (trips/day) relate to air quality (PM2.5/NO2)? → cite Pearson r values from Correlation page
  2. Which zones / times show the strongest link between taxi demand and pollution peaks? → cite Top Pickup Zones + combo chart
  3. What is average revenue per trip USD vs EUR, and how does FX fluctuation affect it? → cite Total Revenue cards + FX chart on Economic Impact
  4. Over multiple years, do we see mobility/economic growth at the expense of environmental quality? → cite YoY indicators + multi-year trends
- [ ] **Per-page insight notes** — 2–3 sentences per dashboard page with specific numbers (Mobility post-COVID growth, Air Quality PM2.5 seasonality + 2022 coverage gap caveat, Correlation r interpretation, Economic Impact revenue growth + % of GDP).
- [ ] **Defense slide structure** — outline of demo flow, architecture diagram references, screenshots already in `docs/img/`.

## Known data limitations

Upstream quirks we accept and work around — not bugs to fix on our side.

- **OpenAQ historical coverage** — NYC stations in the S3 archive have data for 2021–2025, but with coverage gaps: mid-2022 (May–Sep) shows near-zero measurements for many stations. Not a pipeline issue — reflects actual S3 archive availability.
- **Multi-pollutant station coverage** — not all OpenAQ stations measure all pollutants; some stations have gaps in NO2/O3 data. Known data limitation from OpenAQ source.
- **TLC parquet schema drift** — TLC keeps changing the yellow_tripdata schema across years:
  - **pre/post mid-2023** — INT32 vs INT64 for location columns
  - **2025+** — added column `cbd_congestion_fee` (NYC Central Business District congestion fee — congestion pricing went live 2025-01-05)
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
