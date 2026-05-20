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
# Reads raw data from `bronze_lakehouse`, applies type casting, deduplication, and null filtering,
# writes clean Delta tables to `silver_lakehouse`.
# **Input:** `bronze_fx_rates`, `bronze_gdp`, `bronze_openaq_locations`, `bronze_openaq_measurements`, `bronze_taxi_zones`, `bronze_weather`, `Files/raw/taxi/`
# **Output:** `silver_fx_rates`, `silver_gdp`, `silver_openaq_locations`, `silver_openaq_measurements`, `silver_taxi_trips`, `silver_taxi_zones`, `silver_weather`
# **`year_start`/`year_end`:** semantics vary by source:
# - `silver_taxi_trips`, `silver_openaq_measurements`, `silver_weather` — **ignored when `force_refresh=False`**
#   (incremental via partition diff / watermark MERGE). **Used only when `force_refresh=True`** to scope
#   the full rebuild.
# - `silver_fx_rates`, `silver_gdp`, `silver_openaq_locations`, `silver_taxi_zones` — **never used**
#   (full overwrite of small reference tables every run).

# PARAMETERS CELL ********************

year_start = 2023
year_end = 2023
force_refresh = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports
# PySpark functions and types used across all ETL cells.

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
# `write_silver` — writes a cleaned DataFrame to Silver Lakehouse as a Delta table.
# Accepts optional `partition_by` for partitioned writes.

# CELL ********************

def write_silver(df, table_name: str, partition_by: list = None, replace_where: str = None,
                 merge_schema: bool = False) -> None:
    """Write DataFrame to Silver Lakehouse as Delta table.
    replace_where enables partition-level overwrite (Delta replaceWhere) instead of full table overwrite.
    merge_schema=True allows additive schema evolution (new upstream columns get NULL on old rows);
    use for sources known to drift over time (e.g. TLC adds new fee columns mid-year).
    """
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
# Cast date and rate to correct types, deduplicate by date, drop null rates.

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
# Cast year and GDP value to correct types, deduplicate by (country_code, year), drop nulls.

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
# Source: `bronze_openaq_locations` populated by `bronze_ingest_openaq_locations` notebook.
# Deduplicate by location_id, drop records missing location_id or country_id.

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
# Source: `bronze_taxi_zones` populated by `bronze_ingest_taxi_zones` notebook (~265 static rows).
# Defensive cast on `location_id` (string → int) for medallion strictness; drop nulls; dedup.
# Full overwrite each run — reference data is small and rarely changes.

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
# Rename columns to snake_case, add year/month for partitioning, filter out invalid trips.
# Sanity filter on `fare_amount`: keep `(0, 10_000]`. TLC source occasionally publishes
# corrupted rows with fares of $187k–$863k for short trips; real NYC taxi fares cannot
# realistically exceed a few thousand dollars.
# Default mode (force_refresh=False): process only bronze files whose (year, month) is not yet in silver — append new partitions.
# force_refresh=True: re-process all files in year_start..year_end range (partition overwrite).
# Note: TLC adds new fee columns over time (e.g. `cbd_congestion_fee` from 2023+), so the
# write uses `mergeSchema=true` to let Delta evolve the schema; older rows get NULL for
# columns introduced after their ingestion.

# CELL ********************

def _file_year_month(name: str) -> tuple:
    # yellow_tripdata_YYYY-MM.parquet → (YYYY, MM)
    return int(name[16:20]), int(name[21:23])

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
        # Normalise Airport_fee capitalisation introduced in 2026 TLC files
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
        # Cast TIMESTAMP_NTZ → TIMESTAMP so the columns are visible to the Lakehouse SQL endpoint
        # (Fabric hides TIMESTAMP_NTZ from T-SQL surface). Wall-clock values are preserved.
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
# Filter out non-positive readings, deduplicate by (location_id, parameter, datetime),
# cast datetime, add year/month partition keys.
# Restrict to pollutant parameters only — OpenAQ co-hosts context parameters
# (`temperature`, `relativehumidity`, `um003`) that are out of scope for this project's
# pollution analytics; they are dropped here so downstream consumers and DQ checks
# only see real pollutants.
# Default mode (force_refresh=False): read only bronze rows newer than MAX(datetime) in silver, then MERGE INTO target — fast incremental processing for scheduled runs.
# force_refresh=True: read full bronze for the year range and overwrite partitions — for manual backfill or recovery.

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
# Rename columns with explicit unit suffixes (enrichment), cast datetime, add year/month partition keys,
# derive `is_rainy` flag.
# Default mode (force_refresh=False): read only bronze rows newer than MAX(datetime) in silver, then MERGE INTO target.
# force_refresh=True: read full bronze for the year range and overwrite partitions — for manual backfill.

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
