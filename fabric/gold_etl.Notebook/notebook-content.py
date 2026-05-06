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
# META       "default_warehouse": "544920bc-713b-90c9-4289-8411de80194d",
# META       "known_warehouses": [
# META         {
# META           "id": "544920bc-713b-90c9-4289-8411de80194d",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold ETL — Silver → Fabric Warehouse (star schema)
# Builds star schema from Silver tables and writes to `gold_warehouse` via synapsesql.
# **Input:** silver_taxi_trips, silver_openaq_measurements, silver_fx_rates, silver_gdp
# **Output:** gold_warehouse.dbo — DimDate, DimZone, DimFX, DimGDP, FactTaxiDaily, FactAirQualityDaily

# MARKDOWN ********************

# ## Imports

# CELL ********************

import com.microsoft.spark.fabric
import urllib.request
from pyspark.sql.functions import (
    col, explode, sequence, to_date,
    year, quarter, month, date_format,
    weekofyear, dayofmonth, dayofweek,
    avg, max, min, count, sum as spark_sum,
    round as spark_round
)

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
GOLD   = "gold_warehouse"

YEAR_START = 2019
YEAR_END   = 2025

_b = notebookutils.lakehouse.get(BRONZE)
BRONZE_FILES = f"abfss://{_b.workspaceId}@onelake.dfs.fabric.microsoft.com/{_b.id}/Files"

SILVER_TAXI_TRIPS           = f"{SILVER}.silver_taxi_trips"
SILVER_OPENAQ_MEASUREMENTS  = f"{SILVER}.silver_openaq_measurements"
SILVER_FX_RATES             = f"{SILVER}.silver_fx_rates"
SILVER_GDP                  = f"{SILVER}.silver_gdp"

print(f"Date spine: {YEAR_START}-01-01 → {YEAR_END}-12-31")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper

# CELL ********************

def write_gold(df, table: str) -> None:
    print(f"[{table}] rows before write: {df.count()}")
    df.write.mode("overwrite").synapsesql(f"{GOLD}.dbo.{table}")
    print(f"[{table}] write done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DimDate
# Full date spine 2019-01-01 → 2025-12-31. day_of_week: 1=Monday … 7=Sunday.

# CELL ********************

df_dim_date = (
    spark.sql(f"""
        SELECT explode(sequence(
            to_date('{YEAR_START}-01-01'),
            to_date('{YEAR_END}-12-31'),
            interval 1 day
        )) AS date
    """)
    .select(
        (year(col("date")) * 10000
         + month(col("date")) * 100
         + dayofmonth(col("date"))).cast("int").alias("date_key"),
        col("date"),
        year(col("date")).alias("year"),
        quarter(col("date")).alias("quarter"),
        month(col("date")).alias("month"),
        date_format(col("date"), "MMMM").alias("month_name"),
        weekofyear(col("date")).alias("week_of_year"),
        dayofmonth(col("date")).alias("day_of_month"),
        ((dayofweek(col("date")) + 5) % 7 + 1).alias("day_of_week"),
        date_format(col("date"), "EEEE").alias("day_name"),
        (((dayofweek(col("date")) + 5) % 7 + 1)).isin([6, 7]).alias("is_weekend"),
    )
)

write_gold(df_dim_date, "DimDate")
display(df_dim_date.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DimZone
# TLC taxi zone lookup CSV → 265 rows. zone_key = LocationID (natural PK, 1–265).
# CSV is downloaded once to bronze_lakehouse Files and read via Spark.

# CELL ********************

ZONE_CSV_URL  = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_CSV_PATH = f"{BRONZE_FILES}/raw/taxi_zones/taxi_zone_lookup.csv"

urllib.request.urlretrieve(ZONE_CSV_URL, "/tmp/taxi_zone_lookup.csv")
notebookutils.fs.cp("file:///tmp/taxi_zone_lookup.csv", ZONE_CSV_PATH, overwrite=True)
print(f"Zone CSV written to {ZONE_CSV_PATH}")

df_dim_zone = (
    spark.read.option("header", True).csv(ZONE_CSV_PATH)
    .select(
        col("LocationID").cast("int").alias("zone_key"),
        col("LocationID").cast("int").alias("location_id"),
        col("Zone").alias("zone_name"),
        col("Borough").alias("borough"),
        col("service_zone"),
    )
    .filter(col("zone_key").isNotNull())
    .orderBy("zone_key")
)

write_gold(df_dim_zone, "DimZone")
display(df_dim_zone.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

df_check = spark.read.synapsesql(f"{GOLD}.dbo.DimDate")
print(f"DimDate rows: {df_check.count()}")
df_check.groupBy("is_weekend").count().show()
display(df_check.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check = spark.read.synapsesql(f"{GOLD}.dbo.DimZone")
print(f"DimZone rows: {df_check.count()}")
df_check.groupBy("borough").count().orderBy("borough").show()
display(df_check.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
