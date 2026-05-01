# Data Dictionary

## Bronze Layer — Raw Tables

### `bronze_taxi_trips`
Source: NYC TLC — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

| Column | Type | Description |
|--------|------|-------------|
| VendorID | int | Taxi vendor (1 = Creative Mobile, 2 = VeriFone) |
| tpep_pickup_datetime | timestamp | Trip start datetime |
| tpep_dropoff_datetime | timestamp | Trip end datetime |
| passenger_count | int | Number of passengers |
| trip_distance | float | Distance in miles |
| RatecodeID | int | Rate code (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride) |
| store_and_fwd_flag | string | Y/N — trip record held in vehicle memory before send |
| PULocationID | int | Pickup TLC zone ID |
| DOLocationID | int | Dropoff TLC zone ID |
| payment_type | int | 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided |
| fare_amount | float | Metered fare (USD) |
| extra | float | Extras and surcharges |
| mta_tax | float | MTA tax |
| tip_amount | float | Tip (auto-populated for credit card only) |
| tolls_amount | float | Toll charges |
| improvement_surcharge | float | $0.30 improvement surcharge |
| total_amount | float | Total charged (USD) |
| congestion_surcharge | float | NYC congestion surcharge |
| airport_fee | float | JFK/LaGuardia airport fee |

---

### `bronze_air_quality`
Source: OpenAQ API — https://docs.openaq.org

| Column | Type | Description |
|--------|------|-------------|
| location_id | int | OpenAQ sensor location ID |
| location | string | Human-readable location name |
| city | string | City name |
| country | string | ISO-2 country code |
| latitude | float | Sensor latitude |
| longitude | float | Sensor longitude |
| parameter | string | Pollutant name (pm25, no2, o3, co, so2, pm10) |
| value | float | Measured value |
| unit | string | Unit of measurement (µg/m³, ppm) |
| date_utc | timestamp | Measurement datetime (UTC) |
| date_local | timestamp | Measurement datetime (local timezone) |

---

### `bronze_gdp`
Source: World Bank API

| Column | Type | Description |
|--------|------|-------------|
| country_code | string | ISO-3 country code (e.g., USA) |
| country_name | string | Full country name |
| indicator_id | string | World Bank indicator (NY.GDP.MKTP.CD) |
| indicator_name | string | "GDP (current US$)" |
| year | int | Calendar year |
| value | float | GDP in current USD |

---

### `bronze_fx_rates`
Source: ECB Data Portal

| Column | Type | Description |
|--------|------|-------------|
| date | date | Trading date |
| currency_pair | string | "USD/EUR" |
| rate | float | Exchange rate (how many EUR per 1 USD) |
| freq | string | Frequency (D = daily) |

---

### `bronze_weather`
Source: Open-Meteo API — https://api.open-meteo.com

| Column | Type | Description |
|--------|------|-------------|
| timestamp | timestamp | Hourly timestamp (UTC) |
| latitude | float | 40.71 (NYC) |
| longitude | float | -74.01 (NYC) |
| temperature_2m | float | Air temperature at 2m height (°C) |
| precipitation | float | Precipitation amount (mm) |
| windspeed_10m | float | Wind speed at 10m (km/h) |
| weather_code | int | WMO weather interpretation code |
| fetched_at | timestamp | When this record was ingested |

---

## Silver Layer — Cleaned Tables

### `silver_taxi_trips`
Transformations: parsed timestamps, removed nulls, deduped, added derived time fields

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| trip_id | string | Synthetic unique key | SHA hash of pickup_datetime + PULocationID + fare |
| pickup_datetime | timestamp | Trip start | Parsed from tpep_pickup_datetime |
| dropoff_datetime | timestamp | Trip end | Parsed from tpep_dropoff_datetime |
| trip_duration_min | float | Duration in minutes | (dropoff - pickup) / 60 |
| pickup_date | date | Date only | DATE(pickup_datetime) |
| pickup_hour | int | Hour 0–23 | HOUR(pickup_datetime) |
| day_of_week | string | Mon–Sun | DAYOFWEEK(pickup_datetime) |
| month | int | 1–12 | MONTH(pickup_datetime) |
| year | int | Calendar year | YEAR(pickup_datetime) |
| pu_location_id | int | Pickup zone ID | = PULocationID |
| do_location_id | int | Dropoff zone ID | = DOLocationID |
| passenger_count | int | Passengers | Nulls replaced with 1 |
| trip_distance_mi | float | Distance in miles | = trip_distance |
| fare_amount_usd | float | Base fare | = fare_amount |
| total_amount_usd | float | Total charge | = total_amount |
| payment_type | int | Payment method | = payment_type |
| vendor_id | int | Vendor | = VendorID |

---

### `silver_air_quality`
Transformations: flattened, standardized units, removed invalid readings

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| measurement_id | string | Synthetic key | SHA hash of location_id + parameter + date_utc |
| location_id | int | Sensor ID | = location_id |
| location_name | string | Location label | = location |
| city | string | City | = city |
| country | string | ISO-2 code | = country |
| latitude | float | Latitude | = latitude |
| longitude | float | Longitude | = longitude |
| parameter | string | Pollutant | = parameter (lowercased) |
| value | float | Measurement | Negatives removed |
| unit | string | Unit | Standardized to µg/m³ where possible |
| measured_at_utc | timestamp | UTC datetime | = date_utc |
| measured_date | date | Date only | DATE(date_utc) |
| hour | int | Hour 0–23 | HOUR(date_utc) |
| year | int | Calendar year | YEAR(date_utc) |
| month | int | 1–12 | MONTH(date_utc) |

---

### `silver_weather`
Transformations: validated ranges, added derived time fields

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| weather_id | string | Synthetic key | SHA hash of timestamp + latitude + longitude |
| measured_at_utc | timestamp | Hourly UTC datetime | = timestamp |
| measured_date | date | Date only | DATE(timestamp) |
| hour | int | Hour 0–23 | HOUR(timestamp) |
| year | int | Year | YEAR(timestamp) |
| month | int | Month 1–12 | MONTH(timestamp) |
| temperature_c | float | Temperature (°C) | Clamped to [-40, 60] |
| precipitation_mm | float | Precipitation (mm) | Negatives removed |
| windspeed_kmh | float | Wind speed (km/h) | = windspeed_10m |
| weather_code | int | WMO code | = weather_code |
| weather_label | string | Human-readable condition | Mapped from weather_code |
| is_rainy | boolean | True if precipitation > 0.5mm | Derived |

---

### `silver_gdp`
| Column | Type | Description |
|--------|------|-------------|
| country_code | string | ISO-3 code |
| country_name | string | Full name |
| year | int | Calendar year |
| gdp_usd | float | GDP in USD |

---

### `silver_fx_rates`
| Column | Type | Description |
|--------|------|-------------|
| date | date | Trading date |
| usd_eur_rate | float | EUR per 1 USD |
| year | int | Year |
| month | int | Month |

---

## Gold Layer — Warehouse Star Schema

### `FactTaxiDaily`
Grain: one row per day per pickup zone

| Column | Type | Description |
|--------|------|-------------|
| date_key | int | FK → DimDate.date_key |
| zone_key | int | FK → DimZone.zone_key |
| fx_key | int | FK → DimFX.fx_key |
| trip_count | int | Total trips that day in zone |
| total_fare_usd | float | Sum of fare_amount_usd |
| total_fare_eur | float | total_fare_usd * usd_eur_rate |
| avg_fare_usd | float | Average fare |
| avg_trip_duration_min | float | Average trip duration |
| avg_trip_distance_mi | float | Average distance |
| total_passengers | int | Sum of passenger_count |

---

### `FactAirQualityDaily`
Grain: one row per day per location per pollutant

| Column | Type | Description |
|--------|------|-------------|
| date_key | int | FK → DimDate.date_key |
| location_id | int | OpenAQ sensor ID |
| city | string | City |
| country | string | Country |
| parameter | string | pm25 / no2 / o3 / etc. |
| avg_value | float | Daily average |
| max_value | float | Daily maximum |
| min_value | float | Daily minimum |
| measurement_count | int | Number of readings that day |

---

### `DimDate`
Grain: one row per calendar day

| Column | Type | Description |
|--------|------|-------------|
| date_key | int | PK — YYYYMMDD integer |
| date | date | Calendar date |
| year | int | Year |
| quarter | int | 1–4 |
| month | int | 1–12 |
| month_name | string | January–December |
| week_of_year | int | ISO week number |
| day_of_month | int | 1–31 |
| day_of_week | int | 1=Monday, 7=Sunday |
| day_name | string | Monday–Sunday |
| is_weekend | boolean | True if Sat or Sun |
| is_us_holiday | boolean | US federal holidays |

---

### `DimZone`
Source: TLC Zone Lookup CSV (taxi_zone_lookup.csv)

| Column | Type | Description |
|--------|------|-------------|
| zone_key | int | PK |
| location_id | int | TLC zone ID (1–265) |
| zone_name | string | Zone name (e.g., "JFK Airport") |
| borough | string | Manhattan / Brooklyn / Queens / Bronx / Staten Island / EWR |
| service_zone | string | Boro Zone / Yellow Zone / Airports |

---

### `DimFX`
| Column | Type | Description |
|--------|------|-------------|
| fx_key | int | PK |
| date_key | int | FK → DimDate.date_key |
| date | date | Trading date |
| usd_eur_rate | float | EUR per 1 USD |

---

### `FactWeatherDaily`
Grain: one row per day (NYC)

| Column | Type | Description |
|--------|------|-------------|
| date_key | int | FK → DimDate.date_key |
| avg_temperature_c | float | Daily average temperature |
| min_temperature_c | float | Daily minimum temperature |
| max_temperature_c | float | Daily maximum temperature |
| total_precipitation_mm | float | Daily total precipitation |
| avg_windspeed_kmh | float | Daily average wind speed |
| rainy_hours | int | Hours with precipitation > 0.5mm |
| is_rainy_day | boolean | True if total_precipitation > 2mm |

---

### `DimGDP`
| Column | Type | Description |
|--------|------|-------------|
| gdp_key | int | PK |
| country_code | string | ISO-3 code |
| country_name | string | Full name |
| year | int | Calendar year |
| gdp_usd | float | GDP in current USD |
| gdp_trillion_usd | float | gdp_usd / 1e12 (for display) |

---

## InfluxDB — Time-Series Measurements

### Bucket: `nyc_analytics`

#### Measurement: `weather_hourly`
Tags: `city` (always "NYC"), `source` ("open-meteo")

| Field | Type | Description |
|-------|------|-------------|
| temperature_c | float | Temperature at 2m (°C) |
| precipitation_mm | float | Precipitation (mm) |
| windspeed_kmh | float | Wind speed (km/h) |
| weather_code | int | WMO code |

#### Measurement: `nyc_weather_enriched`
Enriched: weather joined with taxi trip counts for same hour

Tags: `city`, `data_source`

| Field | Type | Description |
|-------|------|-------------|
| temperature_c | float | Temperature |
| precipitation_mm | float | Precipitation |
| taxi_trips | int | Total taxi trips in NYC that hour |
| avg_fare_usd | float | Average taxi fare that hour |

---

## Great Expectations — Expectation Suites

### Suite: `silver_taxi_trips`
| Expectation | Parameters |
|-------------|-----------|
| expect_column_values_to_not_be_null | columns: pickup_datetime, fare_amount_usd |
| expect_column_values_to_be_between | fare_amount_usd: min=0, max=500 |
| expect_column_values_to_be_between | trip_distance_mi: min=0, max=200 |
| expect_column_values_to_be_in_set | payment_type: [1,2,3,4,5,6] |
| expect_table_row_count_to_be_between | min=100000 (sanity check per month) |

### Suite: `silver_air_quality`
| Expectation | Parameters |
|-------------|-----------|
| expect_column_values_to_not_be_null | columns: measured_at_utc, parameter, value |
| expect_column_values_to_be_between | value: min=0, max=10000 |
| expect_column_values_to_be_in_set | parameter: [pm25, pm10, no2, o3, co, so2] |

### Suite: `silver_weather`
| Expectation | Parameters |
|-------------|-----------|
| expect_column_values_to_not_be_null | columns: measured_at_utc, temperature_c |
| expect_column_values_to_be_between | temperature_c: min=-40, max=60 |
| expect_column_values_to_be_between | precipitation_mm: min=0, max=500 |
