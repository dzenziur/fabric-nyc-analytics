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

`pl_master_orchestrator` runs in order: `prepare_taxi_ingestion` (per-month source availability + missing-file planning) → [Parallel] all ingestion (depend on prepare succeeded — true fail-fast) → [Sequential] `silver_etl` → [Sequential] `gold_etl`. ForEach iterates over months returned by prepare (skips months not yet published on TLC and already-downloaded files). `force_refresh=true` ignores existing taxi files and re-downloads everything available.

### Notebooks
| Item | Source | Destination | Parameters |
|------|--------|-------------|------------|
| `bronze_ingest_openaq_locations` | OpenAQ API v3 `/locations` (paginated) | `bronze_lakehouse.bronze_openaq_locations` | `openaq_api_key` (string) |
| `bronze_ingest_openaq_measurements` | OpenAQ public S3 archive (`s3://openaq-data-archive/`) via boto3 | `bronze_lakehouse.bronze_openaq_measurements` | `year_start` (int), `year_end` (int) |
| `bronze_ingest_taxi_zones` | TLC CloudFront — `taxi_zone_lookup.csv` (~265 rows, static) | `bronze_lakehouse.bronze_taxi_zones` | — |
| `prepare_taxi_ingestion` | TLC CloudFront (per-month HEAD across range) + `Files/raw/taxi/` (list existing) | Notebook exit value — JSON list of `{year, month}` to download (months available on TLC AND not yet in bronze) | `year_start`, `year_end`, `force_refresh` (bool) |

---

## Transformation (Silver)

| Item | Input | Output | Parameters |
|------|-------|--------|------------|
| `silver_etl` | All Bronze tables + `Files/raw/taxi/` | `silver_taxi_trips`, `silver_openaq_locations`, `silver_openaq_measurements`, `silver_gdp`, `silver_fx_rates` | `year_start` (int), `year_end` (int) |

Transformations: snake_case rename, null filtering, deduplication, type casting, year/month partitioning. OpenAQ gas measurements (no2, o3, co, no, nox, so2) normalized from ppm to µg/m³ using EPA conversion factors at 25°C.
Taxi files read file-by-file to handle INT32/INT64 schema drift across TLC Parquet releases; explicit casts normalize `VendorID`, `PULocationID`, `DOLocationID`, `payment_type` to `long`.

---

## Modeling (Gold)

| Item | Input | Output | Parameters |
|------|-------|--------|------------|
| `gold_etl` | All Silver tables + `bronze_taxi_zones` (for DimZone) | `FactTaxiDaily`, `FactAirQualityDaily`, `DimDate`, `DimZone`, `DimFX`, `DimGDP` | `year_start` (int), `year_end` (int) |

Star schema in `gold_warehouse` (T-SQL / SQL analytics endpoint). Written via `synapsesql`.

---

## Reporting

### Semantic Model — `nyc_analytics_model`
- Storage mode: Direct Lake on SQL (`gold_warehouse`)
- Relationships: FactTaxiDaily → DimDate, DimZone, DimFX · FactAirQualityDaily → DimDate
- DAX measures: Total Trips, Total Revenue USD/EUR, Avg Fare USD, Avg Trip Distance, Avg Trip Duration, Avg PM2.5, Avg NO2, Avg O3, USA GDP (USD)

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
