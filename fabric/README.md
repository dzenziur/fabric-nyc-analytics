# fabric/

Microsoft Fabric workspace items synced via Fabric Git Integration.
All items are auto-exported by Fabric and versioned here — do not edit JSON/TMDL files manually.

---

## Workspace Overview

| Item | Type | Layer | Status |
|------|------|-------|--------|
| `bronze_lakehouse` | Lakehouse | Bronze | ✅ Active |
| `silver_lakehouse` | Lakehouse | Silver | ✅ Active |
| `gold_warehouse` | Warehouse | Gold | ✅ Active |
| `df_ecb_fx` | Dataflow Gen2 | Bronze | ✅ Active |
| `df_worldbank_gdp` | Dataflow Gen2 | Bronze | ✅ Active |
| `pl_ingest_nyc_taxi` | Pipeline | Bronze | ✅ Active |
| `pl_master_orchestrator` | Pipeline | Orchestration | ✅ Active |
| `bronze_ingest_openaq_locations` | Notebook | Bronze | ✅ Active |
| `bronze_ingest_openaq_measurements` | Notebook | Bronze | ✅ Active |
| `bronze_ingest_taxi_zones` | Notebook | Bronze | ✅ Active |
| `bronze_ingest_weather` | Notebook | Bronze | ✅ Active |
| `prepare_taxi_ingestion` | Notebook | Bronze | ✅ Active |
| `silver_etl` | Notebook | Silver | ✅ Active |
| `gold_etl` | Notebook | Gold | ✅ Active |
| `nyc_analytics_model` | Semantic Model | Reporting | ✅ Active |
| `NYC Analytics` | Report | Reporting | ✅ Active |

---

## Ingestion (Bronze)

### Dataflows Gen2
| Item | Source | Destination | Notes |
|------|--------|-------------|-------|
| `df_ecb_fx` | ECB FX API — daily USD/EUR CSV | `bronze_lakehouse.bronze_fx_rates` | Full history, no date filter |
| `df_worldbank_gdp` | World Bank API — yearly GDP JSON | `bronze_lakehouse.bronze_gdp` | End year dynamic (`DateTime.LocalNow() - 1`) |

### Pipelines
| Item | Source | Destination | Parameters |
|------|--------|-------------|------------|
| `pl_ingest_nyc_taxi` | TLC CloudFront — monthly Parquet | `bronze_lakehouse/Files/raw/taxi/` | `year` (int), `month` (int) — URL and filename built dynamically |
| `pl_master_orchestrator` | — | Triggers all ingestion + ETL | `year_start` (int), `year_end` (int), `force_refresh` (bool) |

`pl_master_orchestrator` runs: [Parallel] all bronze ingestion + dataflows + `prepare_taxi_ingestion` (per-month TLC availability + missing-file planning) — `ForEach_taxi_months` is the only activity that depends on `prepare_taxi_ingestion` (consumes its `exitValue`); others run independently so an OpenAQ/Weather/FX/GDP outage doesn't block TLC and vice-versa. `bronze_ingest_openaq_measurements` depends on `bronze_ingest_openaq_locations` (needs the station list). Then [Sequential] `silver_etl` → [Sequential] `gold_etl`. ForEach iterates over months returned by prepare (skips months not yet published on TLC and already-downloaded files). `force_refresh=true` ignores existing taxi files and re-downloads everything available.
**Schedule:** twice daily at 06:00 and 18:00 UTC with `force_refresh=false`, `year_start=2021`, `year_end=<current year>` (update annually each January). `openaq_api_key` stored as SecureString in trigger — not exported to Git.

### Notebooks
| Item | Source | Destination | Parameters |
|------|--------|-------------|------------|
| `bronze_ingest_openaq_locations` | OpenAQ API v3 `/locations` (paginated) | `bronze_lakehouse.bronze_openaq_locations` | `openaq_api_key` (string) |
| `bronze_ingest_openaq_measurements` | OpenAQ public S3 archive (`s3://openaq-data-archive/`) via boto3 | `bronze_lakehouse.bronze_openaq_measurements` | `year_start` (int), `year_end` (int), `force_refresh` (bool) — default mode fetches current + previous month only and MERGEs into target |
| `bronze_ingest_taxi_zones` | TLC CloudFront — `taxi_zone_lookup.csv` (~265 rows, static) | `bronze_lakehouse.bronze_taxi_zones` | — |
| `bronze_ingest_weather` | Open-Meteo Archive API (historical) + Forecast API (recent days) — NYC single point | `bronze_lakehouse.bronze_weather` | `year_start`, `year_end`, `force_refresh` (bool) — default mode fetches Forecast API `past_days=2` and MERGEs on `(latitude, longitude, datetime)`; `force_refresh=True` or first run uses Archive API for full year range + partition overwrite |
| `prepare_taxi_ingestion` | TLC CloudFront (per-month HEAD across range) + `Files/raw/taxi/` (list existing) | Notebook exit value — JSON list of `{year, month}` to download (months available on TLC AND not yet in bronze) | `year_start`, `year_end`, `force_refresh` (bool) |

---

## Transformation (Silver)

| Item | Input | Output | Parameters |
|------|-------|--------|------------|
| `silver_etl` | All Bronze tables + `Files/raw/taxi/` | `silver_taxi_trips`, `silver_taxi_zones`, `silver_openaq_locations`, `silver_openaq_measurements`, `silver_gdp`, `silver_fx_rates`, `silver_weather` | `year_start` (int), `year_end` (int), `force_refresh` (bool) — default mode is incremental for `silver_openaq_measurements` and `silver_weather` (both MERGE on `MAX(datetime)` watermark) and `silver_taxi_trips` (partition diff append) |

Transformations: snake_case rename, null filtering, deduplication, type casting, year/month partitioning. OpenAQ gas measurements (no2, o3, co, no, nox, so2) normalized from ppm to µg/m³ using EPA conversion factors at 25°C. Weather columns renamed with explicit unit suffixes (`temperature_c`, `feels_like_c`, `precipitation_mm`, `wind_speed_kmh`, `humidity_pct`) and enriched with derived `is_rainy` flag; MERGE uses `whenMatchedUpdateAll` because Open-Meteo retroactively refines recent observations.
Taxi files read file-by-file to handle TLC Parquet schema drift: `Airport_fee` renamed to `airport_fee` if present (capitalisation changed in 2026 files); explicit casts: `VendorID`/`PULocationID`/`DOLocationID`/`payment_type` → `long`, `passenger_count`/`RatecodeID` → `double` (types vary across TLC file generations).

---

## Modeling (Gold)

| Item | Input | Output | Parameters |
|------|-------|--------|------------|
| `gold_etl` | All Silver tables | `FactTaxiDaily`, `FactAirQualityDaily`, `DimDate`, `DimZone`, `DimFX`, `DimGDP` | `year_start` (int), `year_end` (int), `force_refresh` (bool) — default mode is incremental for FactTaxiDaily and FactAirQualityDaily (re-aggregate `MAX(gold.date_key) - 7 days` forward) |

Star schema in `gold_warehouse` (T-SQL / SQL analytics endpoint). Written via `synapsesql`.

---

## Reporting

### Semantic Model — `nyc_analytics_model`
- Storage mode: Direct Lake on SQL (`gold_warehouse`)
- Relationships: FactTaxiDaily → DimDate, DimZone, DimFX · FactAirQualityDaily → DimDate
- DAX measures: Total Trips, Total Revenue USD/EUR, Avg Fare USD, Avg Trip Distance, Avg Trip Duration, Avg PM2.5, Avg NO2, Avg O3, USA GDP (USD)
- RLS: 5 roles on `DimZone[service_zone]` — `Admin` (no filter), `Yellow Cab Dispatcher` (Yellow Zone), `Green Cab Dispatcher` (Boro Zone), `Airports Operator` (Airports), `EWR Operator` (EWR). Filter propagates to FactTaxiDaily via zone_key. See `docs/architecture.md`.

### Report — `NYC Analytics`
| Page | Key visuals |
|------|------------|
| Mobility | KPI cards (Total Trips, Revenue USD, Avg Fare, Avg Distance), year tile slicer, trips/day trend, top 10 pickup zones |
| Air Quality | KPI cards (Avg NO2/O3/PM2.5) with WHO-based conditional fill color, year tile slicer, Azure Maps bubble visual (Avg PM2.5 gradient), PM2.5+NO2+O3 daily trend with WHO threshold lines and zoom slider, top 10 stations by Avg PM2.5 |
| Correlation | KPI cards (Total Trips, Avg PM2.5, Avg NO2) — PM2.5/NO2 with WHO-based fill color, bar+line chart (Trips vs PM2.5+NO2 by month), year tile slicer (multi-select) |
| Economic Impact | KPI cards (Revenue USD, Revenue EUR, USA GDP), revenue by year, USA GDP trend, USD/EUR exchange rate |

---

## Infrastructure

`bronze_lakehouse.Lakehouse/`, `silver_lakehouse.Lakehouse/`, `gold_warehouse.Warehouse/` — auto-exported by Fabric Git for all workspace items. The actual resources are managed by Terraform (`terraform/`), not these files.
