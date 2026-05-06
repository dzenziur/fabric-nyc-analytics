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
from datetime import datetime
from pyspark.sql.functions import (
    col, explode, sequence, to_date,
    year, quarter, month, date_format,
    weekofyear, dayofmonth, dayofweek,
    avg, max, min, count, sum as spark_sum,
    round as spark_round, row_number, unix_timestamp
)
from pyspark.sql.window import Window

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

YEAR_END   = datetime.now().year - 1
YEAR_START = YEAR_END - 4

_b = notebookutils.lakehouse.get(BRONZE)
BRONZE_FILES = f"abfss://{_b.workspaceId}@onelake.dfs.fabric.microsoft.com/{_b.id}/Files"

SILVER_TAXI_TRIPS           = f"{SILVER}.silver_taxi_trips"
SILVER_OPENAQ_MEASUREMENTS  = f"{SILVER}.silver_openaq_measurements"
SILVER_OPENAQ_LOCATIONS     = f"{SILVER}.silver_openaq_locations"
SILVER_FX_RATES             = f"{SILVER}.silver_fx_rates"
SILVER_GDP                  = f"{SILVER}.silver_gdp"

print(f"Year range: {YEAR_START} - {YEAR_END}")

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
notebookutils.fs.cp("file:///tmp/taxi_zone_lookup.csv", ZONE_CSV_PATH)
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

# ## DimFX
# One row per trading date. fx_key is a sequential surrogate key ordered by date.

# CELL ********************

df_dim_fx = (
    spark.read.table(SILVER_FX_RATES)
    .select(
        col("date"),
        (year(col("date")) * 10000
         + month(col("date")) * 100
         + dayofmonth(col("date"))).cast("int").alias("date_key"),
        col("usd_eur_rate"),
    )
    .withColumn("fx_key", row_number().over(Window.orderBy("date")))
    .select("fx_key", "date_key", "date", "usd_eur_rate")
)

write_gold(df_dim_fx, "DimFX")
display(df_dim_fx.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DimGDP
# One row per (country, year). gdp_trillion_usd is a display-friendly derived column.

# CELL ********************

df_dim_gdp = (
    spark.read.table(SILVER_GDP)
    .withColumn("gdp_trillion_usd", spark_round(col("gdp_usd") / 1e12, 4))
    .withColumn("gdp_key", row_number().over(Window.orderBy("country_code", "year")))
    .select("gdp_key", "country_code", "country_name", "year", "gdp_usd", "gdp_trillion_usd")
)

write_gold(df_dim_gdp, "DimGDP")
display(df_dim_gdp.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FactTaxiDaily
# Grain: one row per day × pickup zone. FX join is left — weekend/holiday dates have no ECB rate (null fx_key).

# CELL ********************

df_fx   = spark.read.synapsesql(f"{GOLD}.dbo.DimFX").select("fx_key", "date", "usd_eur_rate")
df_zone = spark.read.synapsesql(f"{GOLD}.dbo.DimZone").select("zone_key", "location_id")

df_agg = (
    spark.read.table(SILVER_TAXI_TRIPS)
    .withColumn("trip_date", to_date(col("pickup_datetime")))
    .withColumn("duration_min",
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
    )
    .groupBy("trip_date", "pu_location_id")
    .agg(
        count("*").alias("trip_count"),
        spark_round(spark_sum("fare_amount"), 2).alias("total_fare_usd"),
        spark_round(avg("fare_amount"), 4).alias("avg_fare_usd"),
        spark_round(avg("duration_min"), 4).alias("avg_trip_duration_min"),
        spark_round(avg("trip_distance"), 4).alias("avg_trip_distance_mi"),
        spark_sum("passenger_count").cast("int").alias("total_passengers"),
    )
)

df_fact_taxi = (
    df_agg
    .join(df_fx,   df_agg["trip_date"]      == df_fx["date"],        "left")
    .join(df_zone, df_agg["pu_location_id"] == df_zone["location_id"], "left")
    .withColumn("date_key",
        (year(col("trip_date")) * 10000
         + month(col("trip_date")) * 100
         + dayofmonth(col("trip_date"))).cast("int")
    )
    .withColumn("total_fare_eur", spark_round(col("total_fare_usd") * col("usd_eur_rate"), 2))
    .select(
        "date_key", "zone_key", "fx_key",
        "trip_count", "total_fare_usd", "total_fare_eur",
        "avg_fare_usd", "avg_trip_duration_min", "avg_trip_distance_mi",
        "total_passengers",
    )
)

write_gold(df_fact_taxi, "FactTaxiDaily")
display(df_fact_taxi.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FactAirQualityDaily
# Grain: one row per day × location × parameter. city/country joined from silver_openaq_locations.

# CELL ********************

df_loc = spark.read.table(SILVER_OPENAQ_LOCATIONS).select("location_id", "country_name")

df_fact_aq = (
    spark.read.table(SILVER_OPENAQ_MEASUREMENTS)
    .withColumn("meas_date", to_date(col("datetime")))
    .groupBy("meas_date", "location_id", "location", "parameter")
    .agg(
        spark_round(avg("value"), 4).alias("avg_value"),
        spark_round(max("value"), 4).alias("max_value"),
        spark_round(min("value"), 4).alias("min_value"),
        count("*").alias("measurement_count"),
    )
    .join(df_loc, "location_id", "left")
    .withColumn("date_key",
        (year(col("meas_date")) * 10000
         + month(col("meas_date")) * 100
         + dayofmonth(col("meas_date"))).cast("int")
    )
    .select(
        "date_key",
        "location_id",
        col("location").alias("city"),
        col("country_name").alias("country"),
        "parameter",
        "avg_value",
        "max_value",
        "min_value",
        "measurement_count",
    )
)

write_gold(df_fact_aq, "FactAirQualityDaily")
display(df_fact_aq.limit(10))

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

# CELL ********************

df_check = spark.read.synapsesql(f"{GOLD}.dbo.DimFX")
print(f"DimFX rows: {df_check.count()}")
display(df_check.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check = spark.read.synapsesql(f"{GOLD}.dbo.DimGDP")
print(f"DimGDP rows: {df_check.count()}")
df_check.groupBy("year").count().orderBy("year").show(5)
display(df_check.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check = spark.read.synapsesql(f"{GOLD}.dbo.FactTaxiDaily")
print(f"FactTaxiDaily rows: {df_check.count()}")
print(f"Null zone_key: {df_check.filter(col('zone_key').isNull()).count()}")
print(f"Null fx_key:   {df_check.filter(col('fx_key').isNull()).count()}")
df_check.withColumn("year", (col("date_key") / 10000).cast("int")).groupBy("year").count().orderBy("year").show()
display(df_check.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check = spark.read.synapsesql(f"{GOLD}.dbo.FactAirQualityDaily")
print(f"FactAirQualityDaily rows: {df_check.count()}")
df_check.groupBy("parameter").count().orderBy("parameter").show()
display(df_check.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
