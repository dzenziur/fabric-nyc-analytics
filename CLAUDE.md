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
**Last session:** 2026-05-19 — Phase 7 merged to `main`. All eight phases shipped end-to-end; switching to polishing pass: clearing items from `## Backlog`, tightening data quality, and improving documentation before defense.

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

| # | Priority | Batch | What & why |
|---|----------|-------|-----------|
| 1 | 🔥 P1 | **Silver data-quality fixes** | One `silver_etl` re-run, then re-run GE. Closes real DQ findings visible in `/report` demo, restores `pickup_datetime` checks, fixes a corruption surface. High correctness payoff, low touch surface. |
| 2 | 🔥 P1 | **Power BI signature features** | Most visible at defense — what the committee actually looks at. Sankey (Mobility), Scatter+Play (Correlation), Forecast (Economic Impact). |
| 3 | ⭐ P2 | **Power BI readability polish** | Same Power BI Desktop session — insight text boxes per page, semantic-model rename (`city` → `station_name`), Smart Narrative, YoY indicators, theme JSON. |
| 4 | ⭐ P2 | **Pipeline + governance audits** | Verification-only work — `pl_master_orchestrator` dependency audit + Fabric lineage view completeness. Confirms platform is correct; produces a screenshot/note for defense. |
| 5 | 💡 P3 | **Notebook & docs clarity** | `year_start/year_end` semantics headers, `silver_etl` taxi append logging, `data_dictionary.md` type audit. Reviewer/handoff polish. |
| 6 | 🧊 P4 | **Incremental ETL — deferred** | 7 candidates already analysed as low-ROI. Keep listed for completeness but skip unless time allows. |

### Batch 1 — Silver data-quality fixes (`silver_etl` notebook)

Single notebook touch, re-run silver, re-run GE to verify the report goes 56/56.

- [ ] **fare_amount outlier filter** — `silver_taxi_trips` contains ~13 rows from TLC source with `fare_amount` between $187k and $863k for trips of 1.2–21.3 miles. Add `fare_amount <= 10_000` alongside the existing `fare_amount > 0` filter. Then re-run silver and remove the recurring DQ failure.
- [ ] **TIMESTAMP_NTZ → timestamp cast** — `silver_taxi_trips.pickup_datetime` and `dropoff_datetime` are TIMESTAMP_NTZ in Delta and therefore invisible to the Lakehouse SQL endpoint (Fabric limitation). Apply a cast to `timestamp` at silver write time so the columns become visible to T-SQL / Power BI DirectQuery. Then re-add `pickup_datetime not null` to the GE suite for `silver_taxi_trips`. Same root cause as the Spark CBO `FilterEstimation` MatchError we already work around at read time in `gold_etl`.
- [ ] **OpenAQ non-pollutant filter** — `silver_openaq_measurements` carries non-pollutant parameters (`temperature`, `relativehumidity`, `um003`) that OpenAQ co-hosts with the pollutants. The project's analytical questions are about pollution only — filter these out in `silver_etl`. Then narrow the GE `parameter` value_set back to actual pollutants.
- [ ] **Taxi append logging** — after the incremental append, log total silver row count and delta (rows added). Pattern: `rows_before = spark.read.table(SILVER_TAXI_TRIPS).count()` before write → `rows_after = ...count()` after → `print(f"appended {rows_after - rows_before:,} rows; silver total: {rows_after:,}")`. Currently only logs "rows before append" of the new batch, which is confusing.

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

### Batch 4 — Pipeline + governance audits (verification-only)

- [ ] **`pl_master_orchestrator` dependency audit** — verify correct execution order (bronze → silver → gold), no missing or redundant dependency links between activities.
- [ ] **Fabric lineage view completeness** — verify built-in lineage (`Workspace → Lineage view`) captures all expected upstream/downstream edges. Suspected gaps: notebook → table edges may not be visible for all notebooks; external source nodes may be missing for some pipelines. Compare graph against `docs/architecture.md` → Data Flow section and document any missing edges (annotations or screenshot caveats). If gaps are significant, re-evaluate Purview Data Map (free Azure tier).

### Batch 5 — Notebook & docs clarity (markdown-only edits)

- [ ] **`year_start`/`year_end` semantics in notebook header cells** — explicitly state per notebook when the parameters are used vs. ignored:
  - `prepare_taxi_ingestion` — **always uses** year range (determines which TLC months to check/download); critical for schedule correctness
  - `bronze_ingest_openaq_measurements` — **ignores** year range when `force_refresh=False` (fetches current+prev month only); used only with `force_refresh=True`
  - `silver_etl` taxi section — **ignores** year range when `force_refresh=False` (partition diff); used only with `force_refresh=True`
  - `silver_etl` OpenAQ section — **ignores** year range when `force_refresh=False` (watermark); used only with `force_refresh=True`
  - `gold_etl` FactTaxiDaily/FactAirQualityDaily — **ignores** year range when `force_refresh=False` (7-day lookback); used only with `force_refresh=True`
  - `gold_etl` DimDate — **uses** year range as a floor/ceiling, expanded by actual data min/max
- [ ] **`docs/data_dictionary.md` type audit** — run `printSchema()` for each Bronze and Silver table and compare against the doc. First known discrepancy: `bronze_openaq_measurements.datetime` is `string` in practice, was `timestamp` in docs (already fixed).

### Batch 6 — Incremental ETL (deferred)

Steps 1–5 already done. Remaining tables are full overwrite — savings minimal vs added complexity. Implement only if compute cost becomes a concern; otherwise skip for defense.

- [ ] `bronze_ingest_openaq_locations` — ~24k records via paginated API; skip if no changes (hash/count diff). ~2 min run.
- [ ] `bronze_ingest_openaq_measurements` — pre-filter `nyc_ids` by station activity window (`datetime_first`/`datetime_last` from OpenAQ API). Saves S3 LIST calls for inactive stations.
- [ ] `silver_fx_rates` — ~7k rows; read only `date > MAX(silver.date)` from bronze, append.
- [ ] `silver_gdp` — ~6k rows, yearly data, rarely changes; skip if bronze unchanged.
- [ ] `silver_openaq_locations` — ~5k rows, rare changes; MERGE only updated stations.
- [ ] `DimDate`/`DimZone`/`DimFX`/`DimGDP` — full rebuild <30s combined; not worth complexity.
- [ ] `bronze_taxi_zones` — 265 rows, static; check ETag/Last-Modified to skip download.

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
