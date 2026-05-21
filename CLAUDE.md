# NYC Analytics

> Unified analytics platform on **Microsoft Fabric** integrating NYC Taxi
> mobility, OpenAQ air quality, World Bank GDP, ECB FX, and Open-Meteo weather.
>
> - **Medallion architecture** — Bronze → Silver → Gold
> - **Power BI** dashboards on top of the Gold star schema
> - **External stack** — local Docker (InfluxDB + Grafana + Telegram DQ bot)
>   on top of the Fabric SQL endpoint

## Current Status

**Deadline:** May 21, 2026. Defense on May 26 but all artefacts must be ready by 21.

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
| Phase 8 — Polish & Finalisation | ✅ Done |
| Phase 9 — Defense preparation | ⬜ Not started |

## Phase 9 — Defense preparation

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
| Project overview, dashboard previews, quick start | `README.md` |
| Table schemas / columns | `docs/data_dictionary.md` |
| Components, decisions, data flow | `docs/architecture.md` |
| Run order, setup steps | `docs/how_to_run.md` |
| Fabric workspace items, pipelines, notebooks | `fabric/README.md` |

---

## Project structure

```
fabric/       All Fabric workspace items: dataflows, pipelines, notebooks, warehouse, semantic model, Power BI report
              Synced automatically via Fabric Git integration
app/          External Python app — CLI dispatcher (weather-sync, ge-report, bot).
              Single Docker image, three docker-compose services.
terraform/    IaC: workspace, lakehouses, warehouse (own Makefile inside — `make -C terraform help`)
grafana/      Provisioned datasource + dashboards (mounted into Grafana container)
docs/         Architecture, data dictionary, how-to-run, screenshots (img/)
spec/         Original project specification (PDF)
README.md     Project overview, dashboard previews, architecture diagram, quick start
Makefile      Docker Compose shortcuts for the external stack (`make help`)
```

## Data sources

| Source | Format | Ingestion tool |
|--------|--------|----------------|
| NYC Taxi (TLC) | Parquet, monthly | Data Factory Pipeline (`pl_ingest_nyc_taxi`) |
| OpenAQ Air Quality | JSON API + S3 archive | PySpark Notebooks (`bronze_ingest_openaq_*`) |
| World Bank GDP | JSON API | Dataflow Gen2 (`df_worldbank_gdp`) |
| ECB FX rates | CSV API | Dataflow Gen2 (`df_ecb_fx`) |
| Open-Meteo Weather | JSON API | PySpark Notebook (`bronze_ingest_weather`) |

## Quick start

Two surfaces — Fabric workspace and the local Docker stack.

**Fabric** — push the branch, then in the Fabric UI: `Workspace → Source control → Update all`. Trigger the platform via `pl_master_orchestrator` (runs on schedule twice daily; manual trigger via Run button).

**Local stack** — driven by `Makefile` (run from repo root, Docker Desktop required):

```bash
make build        # build the app image
make up           # start influxdb + grafana + app-weather-sync + app-bot
make ps           # status
make logs         # tail logs from all services

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

- **Bronze is immutable** — raw data is never modified after landing
- **Silver owns cleaning** — all deduplication, normalization, and type casting happens here
- **Fabric is the source of truth** — external Docker stack reads from it; nothing flows back
- **Fail loudly** — pipelines raise on bad data instead of silent skips
- **Document decisions** — every non-obvious architectural choice has a "Why" entry in `docs/architecture.md`
