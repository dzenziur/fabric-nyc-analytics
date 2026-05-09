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
| `df_openaq_locations` | Dataflow Gen2 | Bronze | ✅ Active |
| `pl_ingest_nyc_taxi` | Pipeline | Bronze | ✅ Active |
| `bronze_ingest_openaq_measurements` | Notebook | Bronze | ✅ Active |
| `silver_etl` | Notebook | Silver | ✅ Active |
| `gold_etl` | Notebook | Gold | ✅ Active |
| `nyc_analytics_model` | Semantic Model | Reporting | ✅ Active |
| `NYC Analytics` | Report | Reporting | ✅ Active |

---

## Ingestion (Bronze)

### Dataflows Gen2
| Item | Source | Destination |
|------|--------|-------------|
| `df_ecb_fx` | ECB FX API — daily USD/EUR CSV | `bronze_lakehouse.bronze_fx_rates` |
| `df_worldbank_gdp` | World Bank API — yearly GDP JSON | `bronze_lakehouse.bronze_gdp` |
| `df_openaq_locations` | OpenAQ API v3 `/locations` (paginated JSON) | `bronze_lakehouse.bronze_openaq_locations` |

### Pipelines
| Item | Source | Destination |
|------|--------|-------------|
| `pl_ingest_nyc_taxi` | TLC CloudFront — monthly Parquet | `bronze_lakehouse/Files/raw/taxi/` |

Parameters: `year` (int), `month` (int)

### Notebooks
| Item | Source | Destination |
|------|--------|-------------|
| `bronze_ingest_openaq_measurements` | OpenAQ public S3 archive (`s3://openaq-data-archive/`) via boto3 | `bronze_lakehouse.bronze_openaq_measurements` |

Parameters: `year_start` (int), `year_end` (int) — dynamic window, no hardcoded years.

---

## Transformation (Silver)

| Item | Input | Output |
|------|-------|--------|
| `silver_etl` | All Bronze tables | `silver_taxi_trips`, `silver_openaq_locations`, `silver_openaq_measurements`, `silver_gdp`, `silver_fx_rates` |

Transformations: snake_case rename, null filtering, deduplication, type casting, year/month partitioning.

---

## Modeling (Gold)

| Item | Input | Output |
|------|-------|--------|
| `gold_etl` | All Silver tables | `FactTaxiDaily`, `FactAirQualityDaily`, `DimDate`, `DimZone`, `DimFX`, `DimGDP` |

Star schema in `gold_warehouse` (T-SQL / SQL analytics endpoint).

---

## Reporting

### Semantic Model — `nyc_analytics_model`
- Storage mode: Direct Lake on SQL (`gold_warehouse`)
- Relationships: FactTaxiDaily → DimDate, DimZone, DimFX · FactAirQualityDaily → DimDate
- DAX measures: Total Trips, Total Revenue USD/EUR, Avg Fare USD, Avg Trip Distance, Avg Trip Duration, Avg PM2.5, Avg NO2, Avg O3, Max PM2.5, USA GDP (USD)

### Report — `NYC Analytics`
| Page | Key visuals |
|------|------------|
| Mobility | KPI cards, trips/day trend, top 10 pickup zones, revenue USD vs EUR by year |
| Air Quality | KPI cards, PM2.5 daily trend, NO2+O3 trends, top 10 stations by Avg PM2.5 |
| Correlation | Dual-axis line chart (Total Trips + Avg PM2.5 by date), year tile slicer |
| Economic Impact | Revenue USD/EUR by year, USA GDP trend (2000–2023) |

---

## Infrastructure

`bronze_lakehouse.Lakehouse/`, `silver_lakehouse.Lakehouse/`, `gold_warehouse.Warehouse/` — auto-exported by Fabric Git for all workspace items. The actual resources are managed by Terraform (`terraform/`), not these files.
