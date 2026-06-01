# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cc456ca8-0bdb-48e0-b22e-523d9267e9c6",
# META       "default_lakehouse_name": "silver_lakehouse",
# META       "default_lakehouse_workspace_id": "d5f75821-ae8f-4a0a-b235-74982716aa0b",
# META       "known_lakehouses": [
# META         {
# META           "id": "cc456ca8-0bdb-48e0-b22e-523d9267e9c6"
# META         },
# META         {
# META           "id": "c9741aec-56ed-41b9-9025-c45ee072256d"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "649dd1b2-d040-4523-9b1a-9cb45a00c0c9",
# META       "known_warehouses": [
# META         {
# META           "id": "649dd1b2-d040-4523-9b1a-9cb45a00c0c9",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver ETL — Bronze → Silver Transformations
# Reads raw data from `bronze_lakehouse`, applies schema standardization, deduplication, null filtering, unit normalization, and column enrichment, then writes clean Delta tables to `silver_lakehouse`. One notebook covers all 7 Silver tables, section by section. Each section is independently runnable after the Imports + Config cells. The principle: **Silver owns cleaning** — Bronze stays immutable, Gold gets ready-to-aggregate data.
# ### Input
# - `bronze_fx_rates`, `bronze_gdp`.<p>
# - `bronze_openaq_locations`, `bronze_openaq_measurements`.<p>
# - `bronze_taxi_zones`, `Files/raw/taxi/` (raw TLC Parquet files).<p>
# - `bronze_weather`.
# ### Output
# - `silver_fx_rates`, `silver_gdp`.<p>
# - `silver_openaq_locations`, `silver_openaq_measurements`.<p>
# - `silver_taxi_zones`, `silver_taxi_trips`.<p>
# - `silver_weather`.
# ### Parameters
# - `year_start` (int) — lower bound of year range.<p>
# - `year_end` (int) — upper bound of year range.<p>
# - `force_refresh` (bool) — incremental vs full rebuild.<p>
# Parameter behavior varies per source. Three large tables (`silver_taxi_trips`, `silver_openaq_measurements`, `silver_weather`) use them only on full rebuilds; four small reference tables (FX, GDP, locations, taxi zones) ignore them entirely. See each table's section for specifics.


# PARAMETERS CELL ********************

year_start = 2021
year_end = 2026
force_refresh = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports
# - `pyspark.sql.functions` — column functions for renames, casts, derived columns, filters.<p>
# - `pyspark.sql.utils.AnalysisException` — caught when reading a Silver table that doesn't exist yet (first run fallback).<p>
# - `DeltaTable` — MERGE INTO for incremental writes on the three large tables.<p>
# - `spark_max` — alias for `pyspark.sql.functions.max` to avoid clashing with Python's built-in.

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, to_date, to_timestamp, when, year, month, max as spark_max
from pyspark.sql.utils import AnalysisException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Config
# Short aliases for the two Lakehouses, then full identifiers for every Bronze/Silver table.<p>
# `BRONZE_FILES` is the cross-lakehouse `abfss://` URI for raw taxi Parquet files — resolved via `notebookutils.lakehouse.get` because the Silver notebook attaches `silver_lakehouse` as default, so reading Bronze files needs an explicit path rather than the relative `Files/...`.

# CELL ********************

BRONZE = "bronze_lakehouse"
SILVER = "silver_lakehouse"

_b = notebookutils.lakehouse.get(BRONZE)
BRONZE_FILES = f"abfss://{_b.workspaceId}@onelake.dfs.fabric.microsoft.com/{_b.id}/Files"

BRONZE_FX_RATES              = f"{BRONZE}.bronze_fx_rates"
BRONZE_GDP                   = f"{BRONZE}.bronze_gdp"
BRONZE_OPENAQ_LOCATIONS      = f"{BRONZE}.bronze_openaq_locations"
BRONZE_OPENAQ_MEASUREMENTS   = f"{BRONZE}.bronze_openaq_measurements"
BRONZE_TAXI_ZONES            = f"{BRONZE}.bronze_taxi_zones"
BRONZE_WEATHER               = f"{BRONZE}.bronze_weather"
BRONZE_TAXI_FILES            = f"{BRONZE_FILES}/raw/taxi/"

SILVER_FX_RATES              = f"{SILVER}.silver_fx_rates"
SILVER_GDP                   = f"{SILVER}.silver_gdp"
SILVER_OPENAQ_LOCATIONS      = f"{SILVER}.silver_openaq_locations"
SILVER_OPENAQ_MEASUREMENTS   = f"{SILVER}.silver_openaq_measurements"
SILVER_TAXI_TRIPS            = f"{SILVER}.silver_taxi_trips"
SILVER_TAXI_ZONES            = f"{SILVER}.silver_taxi_zones"
SILVER_WEATHER               = f"{SILVER}.silver_weather"

print(f"Year range: {year_start} - {year_end}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper
# `write_silver(df, table_name, partition_by, replace_where, merge_schema)` — universal Silver writer wrapping `df.write.format("delta").mode("overwrite")` with three optional levers:
# - `partition_by` — list of column names for `partitionBy` (used by the three large tables).<p>
# - `replace_where` — Delta partition-level overwrite predicate (only the matching partitions get replaced; everything else stays untouched). The full-mode equivalent of MERGE for partitioned tables.<p>
# - `merge_schema=True` — additive schema evolution; new upstream columns get NULL on old rows. Used for sources known to drift (e.g. TLC adds `cbd_congestion_fee` from 2025).

# CELL ********************

def write_silver(df, table_name: str, partition_by: list = None, replace_where: str = None,
                 merge_schema: bool = False) -> None:
    print(f"[{table_name}] rows before write: {df.count()}")

    writer = df.write.format("delta").mode("overwrite")
    if replace_where:
        writer = writer.option("replaceWhere", replace_where)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.saveAsTable(table_name)
    print(f"[{table_name}] write done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ECB FX Rates
# Daily USD/EUR exchange rates from ECB. Cast `date` to date type, `usd_eur_rate` to double, deduplicate by date, drop null rates. Small table (~7k rows) — full overwrite each run.<p>
# Used downstream as `DimFX` and to convert taxi revenue from USD to EUR in `FactTaxiDaily`.

# CELL ********************

df = spark.read.table(BRONZE_FX_RATES)

df_silver = (
    df
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("usd_eur_rate", col("usd_eur_rate").cast("double"))
    .dropDuplicates(["date"])
    .filter(col("usd_eur_rate").isNotNull())
)

write_silver(df_silver, SILVER_FX_RATES)
display(df_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## World Bank GDP
# Yearly GDP per country (USD) from World Bank API. Cast `year` to int, `gdp_usd` to double, deduplicate by `(country_code, year)`, drop rows with null country or GDP. ~6k rows covering all countries from 2000 to ~2024 (World Bank lags ~1 year). Full overwrite each run.<p>
# Used downstream as `DimGDP` and by the `USA GDP (USD)` DAX measure on the Economic Impact page.

# CELL ********************

df = spark.read.table(BRONZE_GDP)

df_silver = (
    df
    .withColumn("year", col("year").cast("int"))
    .withColumn("gdp_usd", col("gdp_usd").cast("double"))
    .dropDuplicates(["country_code", "year"])
    .filter(col("country_code").isNotNull() & col("gdp_usd").isNotNull())
)

write_silver(df_silver, SILVER_GDP)
display(df_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## OpenAQ Locations
# All OpenAQ stations worldwide (~24,500 rows) — location metadata only, no measurements. Source: `bronze_openaq_locations` populated by `bronze_ingest_openaq_locations`.<p>
# Deduplicate by `location_id`, drop records missing `location_id` or `country_id`. Full overwrite each run. Includes activity-window columns `datetime_first` / `datetime_last` that the OpenAQ measurements ingestion uses to pre-filter inactive stations and save S3 list calls.

# CELL ********************

df = spark.read.table(BRONZE_OPENAQ_LOCATIONS)

df_silver = (
    df
    .dropDuplicates(["location_id"])
    .filter(col("location_id").isNotNull() & col("country_id").isNotNull())
)

write_silver(df_silver, SILVER_OPENAQ_LOCATIONS)
display(df_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## NYC Taxi Zones
# TLC zone lookup (~265 rows of static reference data: zone name, borough, service zone per `location_id`). Source: `bronze_taxi_zones` populated by `bronze_ingest_taxi_zones`.<p>
# Defensive cast `location_id` (string → int) for medallion strictness, drop nulls, dedup, sort. Full overwrite each run — data essentially never changes. Used downstream as `DimZone` and as the RLS filter column (`service_zone`) in the semantic model.

# CELL ********************

df = spark.read.table(BRONZE_TAXI_ZONES)

df_silver = (
    df
    .withColumn("location_id", col("location_id").cast("int"))
    .filter(col("location_id").isNotNull())
    .dropDuplicates(["location_id"])
    .orderBy("location_id")
)

write_silver(df_silver, SILVER_TAXI_ZONES)
display(df_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## NYC Taxi Trips
# Daily yellow-taxi trip records — pickup/dropoff timestamps, fares, distance, passenger counts. Source: monthly Parquet files in `bronze_lakehouse/Files/raw/taxi/` named `yellow_tripdata_YYYY-MM.parquet` (one file per month, ~3-4M rows each, ~60 files for 2021-2026).
# ### Mode scenarios
# - **`force_refresh=True`** — re-process all files whose year is in `year_start..year_end`. Partition-overwrite via `replaceWhere`; other years stay untouched.<p>
# - **`force_refresh=False` + Silver has partitions** — incremental. Compare existing `(year, month)` partitions in Silver against all Bronze files; process only files with `(year, month)` not yet in Silver. Append-only write.<p>
# - **`force_refresh=False` + Silver empty / missing** — fallback to full read of the year range.
# ### Schema drift — read file-by-file
# TLC changes the Parquet schema across years:
# - 2025+ added `cbd_congestion_fee` (NYC congestion pricing went live 2025-01-05) — handled by `mergeSchema=true` on write so Delta evolves the target table; older rows get NULL.<p>
# - 2026+ capitalized `airport_fee` → `Airport_fee` — renamed back to lowercase per file before union.<p>
# - Pre-/post-mid-2023: `VendorID`, `PULocationID`, `DOLocationID`, `payment_type` use INT32 vs INT64 — explicit cast to `long`.<p>
# - 2026+: `passenger_count`, `RatecodeID` switched from long to double — explicit cast to `double`.<p>
# A bulk `spark.read.parquet(*all_files)` can't reconcile these types, so we loop per file, normalize, then `unionByName(allowMissingColumns=True)` to tolerate column-set differences.
# ### Cleaning
# Snake_case rename. `tpep_pickup_datetime` / `tpep_dropoff_datetime` → `pickup_datetime` / `dropoff_datetime` and cast `TIMESTAMP_NTZ → TIMESTAMP` (Fabric Lakehouse SQL endpoint hides `TIMESTAMP_NTZ` from the T-SQL surface, blocking Gold reads). Derive `year` / `month` partition keys. Dedup on natural key `(pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, fare_amount)`.
# ### Sanity filters
# - `pickup_datetime`, `pu_location_id`, `do_location_id` not null.<p>
# - `trip_distance` in `(0, 100]` miles (negatives + cross-city outliers dropped).<p>
# - `fare_amount` in `(0, 10_000]` USD — TLC occasionally publishes corrupted rows with $187k–$863k fares for short trips; the upper bound catches corruption while keeping any legit large airport fare.


# CELL ********************

def _file_year_month(name: str) -> tuple:
    # yellow_tripdata_YYYY-MM.parquet → (YYYY, MM)
    parts = name.replace("yellow_tripdata_", "").replace(".parquet", "").split("-")
    return int(parts[0]), int(parts[1])

if force_refresh:
    use_full_mode = True
    existing_partitions = set()
    print(f"[{SILVER_TAXI_TRIPS}] force_refresh=True — processing year range {year_start}-{year_end}")
else:
    try:
        existing_partitions = set(
            (row.year, row.month)
            for row in spark.read.table(SILVER_TAXI_TRIPS).select("year", "month").distinct().collect()
        )
        use_full_mode = len(existing_partitions) == 0
        if use_full_mode:
            print(f"[{SILVER_TAXI_TRIPS}] silver is empty — falling back to full read of year range {year_start}-{year_end}")
        else:
            print(f"[{SILVER_TAXI_TRIPS}] incremental — {len(existing_partitions)} existing partitions in silver")
    except AnalysisException:
        use_full_mode = True
        existing_partitions = set()
        print(f"[{SILVER_TAXI_TRIPS}] table doesn't exist — falling back to full read of year range {year_start}-{year_end}")

all_bronze_files = [f for f in notebookutils.fs.ls(BRONZE_TAXI_FILES) if f.name.endswith(".parquet")]

if use_full_mode:
    taxi_files = sorted(f.path for f in all_bronze_files if year_start <= _file_year_month(f.name)[0] <= year_end)
else:
    taxi_files = sorted(f.path for f in all_bronze_files if _file_year_month(f.name) not in existing_partitions)

print(f"Taxi files to process: {len(taxi_files)}")

if not taxi_files:
    print(f"[{SILVER_TAXI_TRIPS}] no files to process — skipping")
else:
    dfs = []
    for path in taxi_files:
        df_f = spark.read.parquet(path)
        if "Airport_fee" in df_f.columns:
            df_f = df_f.withColumnRenamed("Airport_fee", "airport_fee")
        df_f = (
            df_f
            .withColumn("VendorID",        col("VendorID").cast("long"))
            .withColumn("RatecodeID",      col("RatecodeID").cast("double"))
            .withColumn("PULocationID",    col("PULocationID").cast("long"))
            .withColumn("DOLocationID",    col("DOLocationID").cast("long"))
            .withColumn("payment_type",    col("payment_type").cast("long"))
            .withColumn("passenger_count", col("passenger_count").cast("double"))
        )
        dfs.append(df_f)

    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d, allowMissingColumns=True)

    df_silver = (
        df
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("RatecodeID", "ratecode_id")
        .withColumnRenamed("PULocationID", "pu_location_id")
        .withColumnRenamed("DOLocationID", "do_location_id")
        .withColumn("pickup_datetime",  col("pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_datetime", col("dropoff_datetime").cast("timestamp"))
        .withColumn("year", year(col("pickup_datetime")))
        .withColumn("month", month(col("pickup_datetime")))
        .dropDuplicates(["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id", "fare_amount"])
        .filter(
            col("pickup_datetime").isNotNull()
            & col("pu_location_id").isNotNull()
            & col("do_location_id").isNotNull()
            & (col("trip_distance") > 0)
            & (col("trip_distance") <= 100)
            & (col("fare_amount") > 0)
            & (col("fare_amount") <= 10_000)
        )
    )

    if use_full_mode:
        df_silver = df_silver.filter(col("year").between(year_start, year_end))
        write_silver(df_silver, SILVER_TAXI_TRIPS, partition_by=["year", "month"],
                     replace_where=f"year >= {year_start} AND year <= {year_end}",
                     merge_schema=True)
    else:
        rows_before = spark.read.table(SILVER_TAXI_TRIPS).count()
        df_silver.write.format("delta").mode("append").saveAsTable(SILVER_TAXI_TRIPS)
        rows_after = spark.read.table(SILVER_TAXI_TRIPS).count()
        print(f"[{SILVER_TAXI_TRIPS}] appended {rows_after - rows_before:,} rows; silver total: {rows_after:,}")

    display(df_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## OpenAQ Measurements
# Hourly pollutant readings from NYC stations: PM2.5, PM10, PM1, NO2, O3, CO, SO2, NO, NOx. Source: `bronze_openaq_measurements` (raw CSV data from OpenAQ public S3 archive).
# ### Mode scenarios
# - **`force_refresh=True`** — read full Bronze for `year_start..year_end`, partition-overwrite via `replaceWhere` + `mergeSchema=True`.<p>
# - **`force_refresh=False` + Silver has data** — incremental via `MAX(datetime)` watermark. Read only Bronze rows where `datetime > max_dt`, then Delta `MERGE INTO` on natural key `(location_id, parameter, datetime)` with `whenNotMatchedInsertAll`. **Insert-only** — never update matched rows, because the OpenAQ S3 archive is immutable.<p>
# - **`force_refresh=False` + Silver empty / missing** — fallback to full read.
# ### Pollutant whitelist
# OpenAQ co-hosts context parameters (`temperature`, `relativehumidity`, `um003`) on the same endpoint — they're filtered out via `parameter.isin(POLLUTANTS)` so downstream consumers (Power BI, Great Expectations) only see real pollutants.
# ### EPA unit conversion ppm → µg/m³
# OpenAQ stores gases (`no2`, `o3`, `co`, `no`, `nox`, `so2`) in ppm and particulates (`pm25`, `pm10`, `pm1`) in µg/m³. To make pollutants comparable on dashboards and against WHO thresholds (all in µg/m³), gases are converted using EPA factors at 25 °C, 1 atm — e.g. NO2 factor 1882 (= molar_mass × 1000 / 24.45). The factor table is iterated in Python to build a single nested CASE WHEN expression; adding a new gas means only adding an entry to `PPM_TO_UGM3`. The `units` column is rewritten from `ppm` → `µg/m³`.
# ### Cleaning
# Drop `value <= 0` (sensor errors), cast `datetime` to timestamp, dedup on natural key, derive `year` / `month` partition keys.


# CELL ********************

if not force_refresh:
    try:
        max_dt_row = spark.read.table(SILVER_OPENAQ_MEASUREMENTS).agg(spark_max("datetime")).collect()
        max_dt = max_dt_row[0][0] if max_dt_row and max_dt_row[0][0] is not None else None
    except AnalysisException:
        max_dt = None

    if max_dt is None:
        print(f"[{SILVER_OPENAQ_MEASUREMENTS}] no existing data — falling back to full read")
        df = spark.read.table(BRONZE_OPENAQ_MEASUREMENTS)
    else:
        print(f"[{SILVER_OPENAQ_MEASUREMENTS}] incremental — watermark: {max_dt}")
        df = spark.read.table(BRONZE_OPENAQ_MEASUREMENTS).filter(to_timestamp(col("datetime")) > lit(max_dt))
else:
    max_dt = None
    print(f"[{SILVER_OPENAQ_MEASUREMENTS}] force_refresh=True — full read for year range {year_start}-{year_end}")
    df = spark.read.table(BRONZE_OPENAQ_MEASUREMENTS)

PPM_TO_UGM3 = {
    "no2": 1882, "o3": 1962, "co": 1145,
    "no": 1227,  "nox": 1882, "so2": 2619,
}

POLLUTANTS = ["pm25", "pm10", "pm1", "no2", "o3", "co", "so2", "no", "nox"]

value_expr = col("value")
for param, factor in PPM_TO_UGM3.items():
    value_expr = when(
        (col("units") == "ppm") & (col("parameter") == param),
        col("value") * factor
    ).otherwise(value_expr)

df_silver = (
    df
    .filter(col("parameter").isin(POLLUTANTS))
    .filter(col("value") > 0)
    .withColumn("value", value_expr)
    .withColumn("units", when(col("units") == "ppm", lit("µg/m³")).otherwise(col("units")))
    .withColumn("datetime", to_timestamp(col("datetime")))
    .dropDuplicates(["location_id", "parameter", "datetime"])
    .filter(col("location_id").isNotNull() & col("parameter").isNotNull() & col("datetime").isNotNull())
    .withColumn("year", year(col("datetime")))
    .withColumn("month", month(col("datetime")))
)

if force_refresh:
    df_silver = df_silver.filter(col("year").between(year_start, year_end))

if not force_refresh and max_dt is not None:
    target = DeltaTable.forName(spark, SILVER_OPENAQ_MEASUREMENTS)
    (
        target.alias("t")
        .merge(
            df_silver.alias("s"),
            "t.location_id = s.location_id AND t.parameter = s.parameter AND t.datetime = s.datetime"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[{SILVER_OPENAQ_MEASUREMENTS}] incremental merge done")
else:
    write_silver(df_silver, SILVER_OPENAQ_MEASUREMENTS, partition_by=["year", "month"],
                 replace_where=f"year >= {year_start} AND year <= {year_end}",
                 merge_schema=True)

display(df_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Weather
# Hourly NYC weather observations from Open-Meteo (single point — Manhattan, lat 40.7128 / lon -74.0060). Source: `bronze_weather` (raw Open-Meteo API payloads).
# ### Mode scenarios
# Same pattern as OpenAQ Measurements:
# - **`force_refresh=True`** — full read + `replaceWhere` partition overwrite.<p>
# - **`force_refresh=False` + Silver has data** — watermark `MAX(datetime)` + Delta MERGE.<p>
# - **`force_refresh=False` + Silver empty / missing** — fallback to full read.<p>
# **MERGE uses BOTH `whenMatchedUpdateAll` AND `whenNotMatchedInsertAll`** — different from OpenAQ. Open-Meteo retroactively refines recent observations (e.g. 13:00 temperature might be 22.5 °C at 14:00 publication, refined to 22.7 °C at 18:00). Matched rows must be **updated**, not skipped. Natural MERGE key: `(latitude, longitude, datetime)`.
# ### Column enrichment — unit suffixes
# - `temperature_2m` → `temperature_c` (drop WMO altitude suffix, add unit).<p>
# - `apparent_temperature` → `feels_like_c`.<p>
# - `precipitation` → `precipitation_mm`.<p>
# - `wind_speed_10m` → `wind_speed_kmh`.<p>
# - `relative_humidity_2m` → `humidity_pct`.<p>
# Makes columns self-documenting — a Power BI user dragging `temperature_c` knows the unit immediately, no docs lookup needed.
# ### Derived columns
# - `is_rainy` — boolean `precipitation_mm > 0`. Cheap to derive once in Silver instead of recomputing in every downstream consumer (DAX, Grafana).<p>
# **Explicit `.select(...)`** at the end pins down column order and column set, so the Delta files always have predictable schema and accidentally-added `withColumn` calls don't silently drift in.


# CELL ********************

if not force_refresh:
    try:
        max_dt_row = spark.read.table(SILVER_WEATHER).agg(spark_max("datetime")).collect()
        max_dt = max_dt_row[0][0] if max_dt_row and max_dt_row[0][0] is not None else None
    except AnalysisException:
        max_dt = None

    if max_dt is None:
        print(f"[{SILVER_WEATHER}] no existing data — falling back to full read")
        df = spark.read.table(BRONZE_WEATHER)
    else:
        print(f"[{SILVER_WEATHER}] incremental — watermark: {max_dt}")
        df = spark.read.table(BRONZE_WEATHER).filter(to_timestamp(col("datetime")) > lit(max_dt))
else:
    max_dt = None
    print(f"[{SILVER_WEATHER}] force_refresh=True — full read for year range {year_start}-{year_end}")
    df = spark.read.table(BRONZE_WEATHER)

df_silver = (
    df
    .withColumn("datetime", to_timestamp(col("datetime")))
    .withColumnRenamed("temperature_2m",       "temperature_c")
    .withColumnRenamed("apparent_temperature", "feels_like_c")
    .withColumnRenamed("precipitation",        "precipitation_mm")
    .withColumnRenamed("wind_speed_10m",       "wind_speed_kmh")
    .withColumnRenamed("relative_humidity_2m", "humidity_pct")
    .withColumn("is_rainy", col("precipitation_mm") > 0)
    .filter(col("datetime").isNotNull() & col("temperature_c").isNotNull())
    .dropDuplicates(["latitude", "longitude", "datetime"])
    .withColumn("year",  year(col("datetime")))
    .withColumn("month", month(col("datetime")))
    .select(
        "datetime", "latitude", "longitude",
        "temperature_c", "feels_like_c", "precipitation_mm",
        "wind_speed_kmh", "humidity_pct", "weather_code", "is_rainy",
        "year", "month",
    )
)

if force_refresh:
    df_silver = df_silver.filter(col("year").between(year_start, year_end))

if not force_refresh and max_dt is not None:
    target = DeltaTable.forName(spark, SILVER_WEATHER)
    (
        target.alias("t")
        .merge(
            df_silver.alias("s"),
            "t.latitude = s.latitude AND t.longitude = s.longitude AND t.datetime = s.datetime"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[{SILVER_WEATHER}] incremental merge done")
else:
    write_silver(df_silver, SILVER_WEATHER, partition_by=["year", "month"],
                 replace_where=f"year >= {year_start} AND year <= {year_end}")

display(df_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification
# Confirm all five Silver tables were created successfully.

# CELL ********************

spark.sql(f"SHOW TABLES IN {SILVER}").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
