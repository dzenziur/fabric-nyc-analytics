# Data Dictionary

> Column types reflect the actual Spark/Delta schema (verified via `printSchema()` 2026-05-20).
> Warehouse types: Spark `long` = SQL `BIGINT`, `double` = `FLOAT(53)`, `integer` = `INT`.

## Bronze Layer — Raw Tables

### `bronze_taxi_trips` (source-schema only — not a Delta table)

Source: NYC TLC — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

**Storage:** raw Parquet files under `bronze_lakehouse/Files/raw/taxi/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet` — there is **no** `bronze_taxi_trips` Delta table. `silver_etl` reads each Parquet file individually (handles per-file schema drift) and writes `silver_taxi_trips`. The schema below describes the TLC source files.

| Column | Type | Description |
|--------|------|-------------|
| VendorID | int | Taxi vendor (1 = Creative Mobile, 2 = VeriFone) |
| tpep_pickup_datetime | timestamp_ntz | Trip start datetime |
| tpep_dropoff_datetime | timestamp_ntz | Trip end datetime |
| passenger_count | double/long | Number of passengers (type varies by TLC file generation: double in 2021–2025, long in 2026+) |
| trip_distance | double | Distance in miles |
| RatecodeID | double/long | Rate code (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride) — long in 2026+ files |
| store_and_fwd_flag | string | Y/N — trip record held in vehicle memory before send |
| PULocationID | int/long | Pickup TLC zone ID (INT32 pre-mid-2023, INT64 after) |
| DOLocationID | int/long | Dropoff TLC zone ID (INT32 pre-mid-2023, INT64 after) |
| payment_type | int/long | 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided |
| fare_amount | double | Metered fare (USD) |
| extra | double | Extras and surcharges |
| mta_tax | double | MTA tax |
| tip_amount | double | Tip (auto-populated for credit card only) |
| tolls_amount | double | Toll charges |
| improvement_surcharge | double | $0.30 improvement surcharge |
| total_amount | double | Total charged (USD) |
| congestion_surcharge | double | NYC congestion surcharge |
| airport_fee / Airport_fee | double | JFK/LaGuardia airport fee (capitalisation drift in 2026+ files) |
| cbd_congestion_fee | double | NYC Central Business District congestion fee (introduced in TLC files from 2025+; older files do not have this column) |

---

### `bronze_openaq_locations`
Source: OpenAQ API v3 `/v3/locations` — https://docs.openaq.org
Ingested by: Notebook `bronze_ingest_openaq_locations`
Note: contains **location metadata only** (sensor stations), not measurements.

| Column | Type | Description |
|--------|------|-------------|
| location_id | long | OpenAQ sensor location ID |
| location_name | string | Human-readable location name |
| timezone | string | IANA timezone string (e.g., America/New_York) |
| country_id | string | ISO-2 country code (e.g., US) |
| country_name | string | Full country name |
| latitude | double | Sensor latitude |
| longitude | double | Sensor longitude |
| datetime_first | string | UTC ISO-8601 timestamp of the station's earliest measurement (OpenAQ `datetimeFirst.utc`); used by measurements ingestion to pre-filter inactive stations |
| datetime_last | string | UTC ISO-8601 timestamp of the station's most recent measurement (OpenAQ `datetimeLast.utc`) |

---

### `bronze_openaq_measurements`
Source: OpenAQ public S3 archive — `s3://openaq-data-archive/records/csv.gz/`
Ingested by: PySpark Notebook `bronze_ingest_openaq_measurements`
Note: contains **actual pollutant measurements** for NYC stations. Partitioned by `year`.

| Column | Type | Description |
|--------|------|-------------|
| location_id | long | OpenAQ sensor location ID |
| sensors_id | long | Individual sensor ID within location |
| location | string | Location name |
| datetime | string | Measurement datetime (UTC) — stored as string; cast to timestamp in silver_etl |
| lat | double | Sensor latitude |
| lon | double | Sensor longitude |
| parameter | string | Pollutant — values in archive include pm25, pm10, pm1, no2, o3, co, so2, no, nox, plus non-pollutant rows (temperature, relativehumidity, um003) which are filtered out in `silver_etl` |
| units | string | Unit of measurement (µg/m³ for PM family, ppm for gases) |
| value | double | Measured pollutant value |
| year | integer | Calendar year — partition key, derived from `datetime` |

---

### `bronze_gdp`
Source: World Bank API
Ingested by: Dataflow Gen2 `df_worldbank_gdp`

| Column | Type | Description |
|--------|------|-------------|
| country_code | string | ISO-3 country code (e.g., USA) |
| country_name | string | Full country name |
| year | long | Calendar year |
| gdp_usd | double | GDP in current USD |

---

### `bronze_fx_rates`
Source: ECB Data Portal
Ingested by: Dataflow Gen2 `df_ecb_fx`

| Column | Type | Description |
|--------|------|-------------|
| date | date | Trading date |
| usd_eur_rate | double | Exchange rate (EUR per 1 USD) |

---

### `bronze_taxi_zones`
Source: TLC Zone Lookup CSV — https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
Ingested by: Notebook `bronze_ingest_taxi_zones`
Note: static reference data (~265 rows), rarely changes; downloaded once per notebook run.

| Column | Type | Description |
|--------|------|-------------|
| location_id | int | TLC zone ID (1–265) — renamed from `LocationID`; cast from CSV string to int at bronze write time (requires force_refresh re-run after notebook update to fully materialise) |
| borough | string | Manhattan / Brooklyn / Queens / Bronx / Staten Island / EWR — renamed from `Borough` |
| zone | string | Zone name (e.g., "JFK Airport") — renamed from `Zone` |
| service_zone | string | Boro Zone / Yellow Zone / Airports |

---

### `bronze_weather`
Source: Open-Meteo Archive API (historical) + Forecast API (recent days) — https://open-meteo.com
Ingested by: PySpark Notebook `bronze_ingest_weather`
Note: single NYC point (40.7128, -74.0060); Open-Meteo snaps to its ~11 km grid so observed lat/lon in data are the grid centroid. Partitioned by `year`.

| Column | Type | Description |
|--------|------|-------------|
| datetime | string | Hourly timestamp ISO 8601, GMT — cast to timestamp in silver_etl |
| temperature_2m | double | Air temperature at 2 m (°C) |
| apparent_temperature | double | Apparent ("feels like") temperature (°C) |
| precipitation | double | Hourly precipitation total (mm) |
| wind_speed_10m | double | Wind speed at 10 m (km/h) |
| relative_humidity_2m | long | Relative humidity at 2 m (%) |
| weather_code | long | WMO weather interpretation code |
| latitude | double | Grid centroid latitude (~40.738) |
| longitude | double | Grid centroid longitude (~-74.043) |
| year | integer | Calendar year — partition key |
| ingestion_timestamp | timestamp | When this row was written to Bronze |

---

## Silver Layer — Cleaned Tables

### `silver_taxi_trips`
Transformations: columns renamed to snake_case, `pickup_datetime`/`dropoff_datetime`
cast from TIMESTAMP_NTZ (TLC source type) to TIMESTAMP so they are visible to the
Lakehouse SQL endpoint, year/month added for partitioning, invalid trips filtered
(trip_distance > 0 and <= 100 mi, fare_amount between $0 and $10k), deduped by
(pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, fare_amount).
Partitioned by: `year`, `month`. `cbd_congestion_fee` added to the Delta schema via
`mergeSchema=True` when older partitions are appended alongside 2025+ files.

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| vendor_id | long | Taxi vendor (1=Creative Mobile, 2=VeriFone) | Renamed from VendorID; cast to long |
| pickup_datetime | timestamp | Trip start | Renamed from tpep_pickup_datetime; cast TIMESTAMP_NTZ → TIMESTAMP for SQL endpoint visibility |
| dropoff_datetime | timestamp | Trip end | Renamed from tpep_dropoff_datetime; cast TIMESTAMP_NTZ → TIMESTAMP for SQL endpoint visibility |
| passenger_count | double | Number of passengers | Cast to double (TLC files vary: double in 2021–2025, long/int in 2026+) |
| trip_distance | double | Distance in miles | Filtered: > 0 and <= 100 (trips above 100 mi are physically implausible for NYC) |
| ratecode_id | double | Rate code | Renamed from RatecodeID; cast to double (long in 2026+ files) |
| store_and_fwd_flag | string | Trip stored in vehicle memory before send | Unchanged |
| pu_location_id | long | Pickup TLC zone ID | Renamed from PULocationID; cast to long |
| do_location_id | long | Dropoff TLC zone ID | Renamed from DOLocationID; cast to long |
| payment_type | long | 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided | Cast to long |
| fare_amount | double | Metered fare (USD) | Filtered: > 0 and <= 10,000 |
| extra | double | Extras and surcharges | Unchanged |
| mta_tax | double | MTA tax | Unchanged |
| tip_amount | double | Tip amount | Unchanged |
| tolls_amount | double | Toll charges | Unchanged |
| improvement_surcharge | double | $0.30 improvement surcharge | Unchanged |
| total_amount | double | Total charged (USD) | Unchanged |
| congestion_surcharge | double | NYC congestion surcharge | Unchanged |
| airport_fee | double | JFK/LaGuardia airport fee | Renamed from Airport_fee in 2026+ files (capitalisation drift) |
| cbd_congestion_fee | double | NYC CBD congestion fee (introduced 2025+; NULL for older partitions via mergeSchema) | Unchanged |
| year | integer | Calendar year (partition key) | Derived: YEAR(pickup_datetime) |
| month | integer | Month 1–12 (partition key) | Derived: MONTH(pickup_datetime) |

---

### `silver_openaq_locations`
Transformations: deduped by location_id, rows with null location_id or country_id dropped,
ordered by country_id, location_id. Schema carries through `datetime_first`/`datetime_last`
from bronze (used by `bronze_ingest_openaq_measurements` for station-activity pre-filtering).

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| location_id | long | OpenAQ sensor location ID | Dedup key; nulls dropped |
| location_name | string | Human-readable location name | Unchanged |
| timezone | string | IANA timezone string | Unchanged |
| country_id | string | ISO-2 country code | Nulls dropped |
| country_name | string | Full country name | Unchanged |
| latitude | double | Sensor latitude | Unchanged |
| longitude | double | Sensor longitude | Unchanged |
| datetime_first | string | UTC ISO-8601 of earliest measurement | Unchanged from bronze |
| datetime_last | string | UTC ISO-8601 of latest measurement | Unchanged from bronze |

---

### `silver_openaq_measurements`
Transformations: cast datetime to timestamp, filter value > 0 and value not null,
restrict `parameter` to the pollutant set (drops `temperature`, `relativehumidity`, `um003`),
deduped by (location_id, parameter, datetime), ppm gas values converted to µg/m³,
ordered by location_id, datetime.
Partitioned by: `year`, `month`.

Note: gas parameters (no2, o3, co, no, nox, so2) are stored in ppm in the S3 archive;
silver_etl normalizes them to µg/m³ using EPA conversion factors at 25°C
(no2×1882, o3×1962, co×1145, no×1227, nox×1882, so2×2619).

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| location_id | long | OpenAQ sensor location ID | Unchanged |
| sensors_id | long | Individual sensor ID | Unchanged |
| location | string | Location name | Unchanged |
| datetime | timestamp | Measurement datetime (UTC) | Cast to timestamp |
| lat | double | Sensor latitude | Unchanged |
| lon | double | Sensor longitude | Unchanged |
| parameter | string | pm25, pm10, pm1, no2, o3, co, so2, no, nox | Restricted to pollutant set |
| units | string | Unit of measurement (always µg/m³ after normalization) | ppm → µg/m³ for gas parameters |
| value | double | Measured pollutant value in µg/m³ | Filtered > 0; ppm gas values multiplied by EPA factor |
| year | integer | Calendar year (partition key) | Derived: YEAR(datetime) |
| month | integer | Month 1–12 (partition key) | Derived: MONTH(datetime) |

---

### `silver_weather`
Transformations: cast datetime to timestamp, null-filter (datetime + temperature_c not null), dedup on (latitude, longitude, datetime), rename Open-Meteo columns with explicit unit suffixes, derive `is_rainy` flag.
Default mode (force_refresh=False): watermark `MAX(datetime)` + Delta `MERGE INTO` with `whenMatchedUpdateAll` (Open-Meteo retroactively refines recent observations, so updates are required). force_refresh=True: full read of bronze + partition replace.
Partitioned by: `year`, `month`.

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| datetime | timestamp | Hourly UTC timestamp | Cast from bronze string |
| latitude | double | Grid centroid latitude | Unchanged from bronze |
| longitude | double | Grid centroid longitude | Unchanged from bronze |
| temperature_c | double | Air temperature at 2 m (°C) | Renamed from `temperature_2m` |
| feels_like_c | double | Apparent ("feels like") temperature (°C) | Renamed from `apparent_temperature` |
| precipitation_mm | double | Hourly precipitation total (mm) | Renamed from `precipitation` |
| wind_speed_kmh | double | Wind speed at 10 m (km/h) | Renamed from `wind_speed_10m` |
| humidity_pct | long | Relative humidity at 2 m (%) | Renamed from `relative_humidity_2m` |
| weather_code | long | WMO weather interpretation code | Unchanged |
| is_rainy | boolean | True when `precipitation_mm > 0` | Derived |
| year | integer | Calendar year | Partition key — `YEAR(datetime)` |
| month | integer | Month 1–12 | Partition key — `MONTH(datetime)` |

---

### `silver_gdp`
Transformations: year cast to int, gdp_usd cast to double,
deduped by (country_code, year), rows with null country_code or gdp_usd dropped,
ordered by country_code, year.

| Column | Type | Description |
|--------|------|-------------|
| country_code | string | ISO-3 code |
| country_name | string | Full name |
| year | integer | Calendar year |
| gdp_usd | double | GDP in current USD |

---

### `silver_taxi_zones`
Transformations: defensive cast `location_id` to int, drop nulls, dedup by `location_id`, ordered by `location_id`.
Reference data (~265 rows, static); full overwrite each run.
Added for medallion strictness — `gold_etl.DimZone` reads from silver instead of bronze.

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| location_id | int | TLC zone ID (1–265) | Cast to int (defensive); nulls dropped |
| borough | string | Manhattan / Brooklyn / Queens / Bronx / Staten Island / EWR | Unchanged |
| zone | string | Zone name (e.g., "JFK Airport") | Unchanged |
| service_zone | string | Boro Zone / Yellow Zone / Airports | Unchanged |

---

### `silver_fx_rates`
Transformations: date cast to date type, usd_eur_rate cast to double,
deduped by date, null rates dropped, ordered by date.

| Column | Type | Description |
|--------|------|-------------|
| date | date | Trading date |
| usd_eur_rate | double | EUR per 1 USD |

---

## Gold Layer — Warehouse Star Schema

> Gold tables live in `gold_warehouse` (Fabric Warehouse, T-SQL endpoint). Written by `gold_etl` via `synapsesql`. Spark types map to SQL: `integer`→`INT`, `long`→`BIGINT`, `double`→`FLOAT(53)`.

### `FactTaxiDaily`
Grain: one row per day per pickup zone.

| Column | Type | Description |
|--------|------|-------------|
| date_key | integer | FK → DimDate.date_key |
| zone_key | integer | FK → DimZone.zone_key |
| fx_key | integer | FK → DimFX.fx_key |
| trip_count | long | Total trips that day in zone |
| total_fare_usd | double | Sum of fare_amount_usd |
| total_fare_eur | double | total_fare_usd * usd_eur_rate |
| avg_fare_usd | double | Average fare |
| avg_trip_duration_min | double | Average trip duration |
| avg_trip_distance_mi | double | Average distance |
| total_passengers | integer | Sum of passenger_count |

---

### `FactAirQualityDaily`
Grain: one row per day per location per pollutant.

| Column | Type | Description |
|--------|------|-------------|
| date_key | integer | FK → DimDate.date_key |
| location_id | long | OpenAQ sensor ID |
| city | string | Station name (joined from `silver_openaq_locations.location_name`) — semantic-model rename to `station_name` planned (see CLAUDE.md backlog) |
| country | string | Country |
| latitude | double | Station latitude (joined from `silver_openaq_locations`) — for map visuals |
| longitude | double | Station longitude (joined from `silver_openaq_locations`) — for map visuals |
| parameter | string | pm25 / pm10 / pm1 / no2 / o3 / co / so2 / no / nox |
| avg_value | double | Daily average |
| max_value | double | Daily maximum |
| min_value | double | Daily minimum |
| measurement_count | long | Number of readings that day |

---

### `DimDate`
Grain: one row per calendar day.

| Column | Type | Description |
|--------|------|-------------|
| date_key | integer | PK — YYYYMMDD integer |
| date | date | Calendar date |
| year | integer | Year |
| quarter | integer | 1–4 |
| month | integer | 1–12 |
| month_name | string | January–December |
| week_of_year | integer | ISO week number |
| day_of_month | integer | 1–31 |
| day_of_week | integer | 1=Monday, 7=Sunday |
| day_name | string | Monday–Sunday |
| is_weekend | boolean | True if Sat or Sun |

---

### `DimZone`
Source: `silver_taxi_zones` (cleaned in `silver_etl`; originally from TLC zone lookup CSV via `bronze_ingest_taxi_zones` notebook).

| Column | Type | Description |
|--------|------|-------------|
| zone_key | integer | PK |
| location_id | integer | TLC zone ID (1–265) |
| zone_name | string | Zone name (e.g., "JFK Airport") |
| borough | string | Manhattan / Brooklyn / Queens / Bronx / Staten Island / EWR |
| service_zone | string | Boro Zone / Yellow Zone / Airports |

---

### `DimFX`

| Column | Type | Description |
|--------|------|-------------|
| fx_key | integer | PK |
| date_key | integer | FK → DimDate.date_key |
| date | date | Trading date |
| usd_eur_rate | double | EUR per 1 USD |

---

### `DimGDP`

| Column | Type | Description |
|--------|------|-------------|
| gdp_key | integer | PK |
| country_code | string | ISO-3 code |
| country_name | string | Full name |
| year | integer | Calendar year |
| gdp_usd | double | GDP in current USD |
| gdp_trillion_usd | double | gdp_usd / 1e12 (for display) |

---

## External stores

- **InfluxDB** (`nyc_analytics` bucket) — `app/weather_sync.py` mirrors `silver_weather` (most recent observations) into a single `weather_hourly` measurement for Grafana dashboards. See `app/weather_sync.py` for the exact field projection.
- **Great Expectations** — Suites are defined in code at `app/ge/suites.py` (Silver + Gold). The Telegram bot's `/report` command exposes the latest run. See that file for the authoritative check list per table.
