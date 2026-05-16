# Microsoft Fabric Data Engineering Project

Unified analytics platform on Microsoft Fabric that integrates NYC Taxi mobility data, OpenAQ air quality, World Bank GDP, ECB FX rates, and Open-Meteo weather into a single data lakehouse and warehouse.

**Architecture:** Medallion (Bronze → Silver → Gold) — raw landing in Lakehouse, cleaning via PySpark Notebooks, analytical star schema in Fabric Warehouse.

**Core technology:** Microsoft Fabric (Lakehouse, Data Factory Pipelines, Dataflow Gen2, PySpark Notebooks, Warehouse), Delta Lake, Python, T-SQL.

**Infrastructure as Code:** Terraform with `microsoft/fabric` provider — manages workspace + lakehouses + warehouse declaratively. See `terraform/`.

**External stack** (Phase 7): InfluxDB Cloud (weather time-series), Grafana (weather dashboard), Great Expectations (data quality), Telegram / Discord Bot (DQ alerts).

## Current Status

**Active branch:** `feature/dashboards-and-robustness`
**Deadline:** May 26, 2026 (defense) — target May 15 for main features

### Phase completion

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0 — Terraform IaC | ✅ Done | workspace + bronze_lakehouse + silver_lakehouse + gold_warehouse |
| Phase 1 — Bronze ingestion | ✅ Done | Taxi, GDP, FX, OpenAQ locations, OpenAQ measurements (S3 archive, boto3), TLC taxi zones |
| Phase 2 — Silver ETL | ✅ Done | silver_taxi_trips, silver_gdp, silver_fx_rates, silver_openaq_locations, silver_openaq_measurements |
| Phase 3 — Gold / star schema | ✅ Done | DimDate, DimZone, DimFX, DimGDP, FactTaxiDaily, FactAirQualityDaily in gold_warehouse |
| Phase 4 — Visualizations | ✅ Done | All 4 dashboards complete; ppm normalization, location slicer, Correlation enhanced, semantic model date formats + summarizeBy fixed, Avg O3 KPI added |
| Phase 5 — Master Orchestrator | ✅ Done | pl_master_orchestrator + parameterized silver/gold notebooks + dynamic taxi loop |
| Phase 6 — Governance & Monitoring | ❌ Not started | Schedules, RLS, Purview lineage — planned for `feature/governance-monitoring` |
| Phase 7 — External Integrations | ❌ Not started | Weather + InfluxDB + Grafana, Great Expectations + Telegram bot |

### Current branch goal (`feature/dashboards-and-robustness`)

Polish, robustness, and data-quality improvements that emerged after Phase 4–5 completion.
Originally scoped as Phase 6 governance work, but evolved into broader cleanup —
strict governance items deferred to next branch.

#### Power BI dashboards & semantic model
- [X] Investigate Max PM2.5 = 2.18K anomaly — confirmed source data issue from OpenAQ S3 (station "State Dept of Environmental Conservation", Nov 2024, 325 bad rows up to 2175 µg/m³); replaced Max PM2.5 KPI with Avg O3 on Air Quality page
- [X] Add year tile slicer to Air Quality page
- [X] Add year tile slicer to Mobility page
- [X] Investigate Avg Trip Distance = 24 mi anomaly — root cause: TLC source data has corrupt trip_distance values in low-volume zones (e.g. Great Kills: 53 trips, avg 1134 mi); fixed in silver_etl by adding `trip_distance <= 100` filter
- [X] Fix date format on `DimDate[date]` and `DimFX[date]` from `General Date` to `mmmm d, yyyy` — eliminates "12:00:00 AM" in tooltips and shows year on multi-year charts
- [X] `summarizeBy: none` on date components (year/quarter/month/week_of_year/day_of_month/day_of_week), avg metrics (avg_fare_usd, avg_trip_duration_min, avg_trip_distance_mi, avg_value, max_value, min_value), and `FactAirQualityDaily[location_id]` — prevents accidental aggregation when columns dragged to Values well
- [X] `DimDate[month_name]` `sortByColumn: month` — month names sort chronologically not alphabetically on charts
- [X] Remove `Max PM2.5` DAX measure (replaced with `Avg O3` on Air Quality page)
- [X] Air Quality map visual — added `latitude`/`longitude` to `FactAirQualityDaily` in gold_etl (join with `silver_openaq_locations`); Azure Maps bubble visual on Air Quality page with gradient color by Avg PM2.5 (min light blue → mid yellow → max red), Avg PM2.5 in Size, tooltips for all 3 pollutants, click on bubble filters trend chart and KPI cards (Edit interactions); replaced station dropdown slicer
- [X] Air Quality trend polish — WHO 24h threshold reference lines (PM2.5=15, NO2=25, O3=100) as Y-axis Constant Lines, dashed, color-matched to trend lines; series labels replace legend (per-line label at right edge); zoom slider added for time-range drill-down
- [X] Conditional fill color on KPI cards — Rules-based (no DAX) on Air Quality Avg PM2.5 and Avg NO2 cards (pastel green/yellow/red based on WHO thresholds); Correlation page Avg PM2.5 and Avg NO2 cards updated via Format Painter for consistency

#### Silver ETL robustness
- [X] Remove redundant `df.count()` at the start of each ETL section — `write_silver` already logs row count; double scan wastes resources (5 places)
- [X] Remove `.orderBy()` before write — applied to all 4 tables (fx_rates, gdp, openaq_locations, openaq_measurements) for consistency; Delta Lake doesn't use this sort
- [X] Add `trip_distance <= 100` filter on silver_taxi_trips — caps physically implausible TLC source data corruption (root cause of Avg Trip Distance anomaly)

#### Gold ETL robustness
- [X] Narrow exception handling in `write_gold` and DimDate range lookup — catch `Py4JJavaError` (synapsesql connector wraps Java exceptions; `AnalysisException` doesn't apply here) with message-based filter for "source is invalid" / "read access" patterns; network/config errors now propagate instead of being silently swallowed
- [X] Separate taxi zones ingestion into new `bronze_ingest_taxi_zones` notebook → writes to `bronze_taxi_zones` Delta table; `gold_etl` now reads from that table instead of downloading CSV from CloudFront every run

#### Bronze improvements
- [X] `bronze_ingest_openaq_locations` — page cap raised 100→500 with WARNING on cap hit; retries on transient errors (5xx/429/network) via urllib3 Retry; request timeout added
- [X] New `bronze_ingest_taxi_zones` notebook — ingests static TLC zone reference data (~265 rows) into `bronze_taxi_zones` Delta table
- [X] New `prepare_taxi_ingestion` notebook — per-month HEAD check on TLC for each `(year, month)` in range (treats HTTP 403/404 as "not yet published", proceeds with whatever is available) + lists existing taxi files in bronze; outputs JSON list of `{year, month}` to download (intersection of "available on TLC" and "not in bronze"); `force_refresh` parameter forces re-download; fails only if NO months in range are available at source

#### Pipeline improvements
- [X] Add `bronze_ingest_taxi_zones` to `pl_master_orchestrator` parallel block; `silver_etl` dependency updated to include it
- [X] Reduce activity timeouts from 12h to 1h on all `pl_master_orchestrator` activities for faster fail-fast on hung activities (retry already 0, dependency conditions already "Succeeded")
- [X] Add `prepare_taxi_ingestion` as first activity in `pl_master_orchestrator` — all other parallel activities now depend on it (true fail-fast); ForEach iterates over notebook output (missing files only) instead of hardcoded `range(0, N*12)`; new `force_refresh` pipeline parameter propagates to prepare notebook

### Next branch — `feature/governance-monitoring` (Phase 6)

Strict governance and monitoring work, deferred from current branch.

#### Schedule automation — design decision required

Per `docs/project_plan.md`, the spec requires **two distinct refresh cadences**:
- **Daily:** ECB FX rates, OpenAQ locations + measurements
- **Monthly:** NYC Taxi (TLC), World Bank GDP

Rationale: each data source has independent value. Air Quality dashboard refreshes from new OpenAQ measurements daily even when there's no new taxi data. Taxi files are published monthly by TLC with ~2-month lag.

**Option A — Two separate orchestrators (recommended)**
- New `pl_daily_orchestrator`: FX + OpenAQ → silver_etl (subset) → gold_etl (subset)
- New `pl_monthly_orchestrator`: Taxi + GDP + reuses daily activities → full silver/gold rebuild
- Each pipeline has its own schedule
- **Pros:** clean separation, matches spec, easier to debug, no internal conditional logic
- **Cons:** `silver_etl`/`gold_etl` may need new parameters to run partial layers (currently all-or-nothing); some duplication of activity definitions between pipelines

**Option B — Single orchestrator with conditional logic**
- Keep `pl_master_orchestrator`, schedule it daily, add `If` activities that skip taxi/GDP unless month boundary crossed (e.g. first of month, or based on `dayofmonth`)
- **Pros:** single source of truth, no pipeline duplication
- **Cons:** complex internal branching; daily runs still re-process current year silver/gold even when only FX/OpenAQ refreshed → wasted compute

**Schedule configuration approach**
- **Fabric UI** (Pipeline → Schedule) — quick, manual
- **Terraform** (`microsoft/fabric` provider) — preferred per project principle "Infrastructure is code". Check provider docs for whether schedule resources are supported; if yes, schedules become declarative and reproducible.

Implementation tasks (after design decision):
- [ ] Decide Option A vs Option B
- [ ] Implement chosen architecture (new pipeline(s) + silver/gold parameter adjustments if needed)
- [ ] Configure schedules: daily for FX + OpenAQ, monthly for Taxi + GDP
- [ ] Verify schedules trigger correctly in Fabric workspace
- [ ] If `microsoft/fabric` Terraform provider supports schedule resources — declare them in `terraform/`; otherwise document manual UI setup

#### Row-Level Security
- [ ] Configure RLS in `nyc_analytics_model` — restrict data visibility by role (details TBD when starting this branch)

#### Microsoft Purview lineage
- [ ] Optional — connect Fabric workspace to Purview for automated lineage tracking (details TBD)

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
| `feature/dashboards-and-robustness` | Dashboard polish + notebook/pipeline robustness + data quality | Active |
| `feature/governance-monitoring` | Phase 6 — Governance & monitoring (schedules, RLS, Purview) | Planned |

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
