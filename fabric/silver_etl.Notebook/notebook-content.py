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
# **Input:** `bronze_fx_rates`, `bronze_gdp`, `bronze_openaq_locations`, `bronze_openaq_measurements`, `Files/raw/taxi/`
# **Output:** `silver_fx_rates`, `silver_gdp`, `silver_openaq_locations`, `silver_openaq_measurements`, `silver_taxi_trips`

# MARKDOWN ********************

# ## Imports
# PySpark functions used across all ETL cells.

# CELL ********************

from pyspark.sql.functions import col, to_date, to_timestamp, year, month

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
BRONZE_TAXI_FILES            = f"{BRONZE_FILES}/raw/taxi/"

SILVER_FX_RATES              = f"{SILVER}.silver_fx_rates"
SILVER_GDP                   = f"{SILVER}.silver_gdp"
SILVER_OPENAQ_LOCATIONS      = f"{SILVER}.silver_openaq_locations"
SILVER_OPENAQ_MEASUREMENTS   = f"{SILVER}.silver_openaq_measurements"
SILVER_TAXI_TRIPS            = f"{SILVER}.silver_taxi_trips"

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

def write_silver(df, table_name: str, partition_by: list = None) -> None:
    """Write DataFrame to Silver Lakehouse as Delta table."""
    print(f"[{table_name}] rows before write: {df.count()}")

    writer = df.write.format("delta").mode("overwrite")
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
print(f"[{BRONZE_FX_RATES}] rows read: {df.count()}")

df_silver = (
    df
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("usd_eur_rate", col("usd_eur_rate").cast("double"))
    .dropDuplicates(["date"])
    .filter(col("usd_eur_rate").isNotNull())
    .orderBy("date")
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
print(f"[{BRONZE_GDP}] rows read: {df.count()}")

df_silver = (
    df
    .withColumn("year", col("year").cast("int"))
    .withColumn("gdp_usd", col("gdp_usd").cast("double"))
    .dropDuplicates(["country_code", "year"])
    .filter(col("country_code").isNotNull() & col("gdp_usd").isNotNull())
    .orderBy("country_code", "year")
)

write_silver(df_silver, SILVER_GDP)
display(df_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## OpenAQ Air Quality
# Deduplicate by location_id, drop records missing location or country.

# CELL ********************

df = spark.read.table(BRONZE_OPENAQ_LOCATIONS)
print(f"[{BRONZE_OPENAQ_LOCATIONS}] rows read: {df.count()}")

df_silver = (
    df
    .dropDuplicates(["location_id"])
    .filter(col("location_id").isNotNull() & col("country_id").isNotNull())
    .orderBy("country_id", "location_id")
)

write_silver(df_silver, SILVER_OPENAQ_LOCATIONS)
display(df_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## NYC Taxi Trips
# Rename columns to snake_case, add year/month for partitioning, filter out invalid trips.

# CELL ********************

df = spark.read.parquet(BRONZE_TAXI_FILES)
print(f"[{BRONZE}.taxi_files] rows read: {df.count()}")

df_silver = (
    df
    .withColumnRenamed("VendorID", "vendor_id")
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    .withColumnRenamed("RatecodeID", "ratecode_id")
    .withColumnRenamed("PULocationID", "pu_location_id")
    .withColumnRenamed("DOLocationID", "do_location_id")
    .withColumn("year", year(col("pickup_datetime")))
    .withColumn("month", month(col("pickup_datetime")))
    .dropDuplicates(["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id", "fare_amount"])
    .filter(
        col("pickup_datetime").isNotNull()
        & col("pu_location_id").isNotNull()
        & col("do_location_id").isNotNull()
        & (col("trip_distance") > 0)
        & (col("fare_amount") > 0)
    )
)

write_silver(df_silver, SILVER_TAXI_TRIPS, partition_by=["year", "month"])
display(df_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## OpenAQ Measurements
# Filter out non-positive readings, deduplicate by (location_id, parameter, datetime),
# cast datetime, add year/month partition keys. Partitioned by year/month.

# CELL ********************

df = spark.read.table(BRONZE_OPENAQ_MEASUREMENTS)
print(f"[{BRONZE_OPENAQ_MEASUREMENTS}] rows read: {df.count()}")

df_silver = (
    df
    .filter(col("value") > 0)
    .withColumn("datetime", to_timestamp(col("datetime")))
    .dropDuplicates(["location_id", "parameter", "datetime"])
    .filter(col("location_id").isNotNull() & col("parameter").isNotNull() & col("datetime").isNotNull())
    .withColumn("year", year(col("datetime")))
    .withColumn("month", month(col("datetime")))
    .orderBy("location_id", "parameter", "datetime")
)

write_silver(df_silver, SILVER_OPENAQ_MEASUREMENTS, partition_by=["year", "month"])
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
