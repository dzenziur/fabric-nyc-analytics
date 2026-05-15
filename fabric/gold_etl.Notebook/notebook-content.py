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
# **Input:** silver_taxi_trips, silver_openaq_measurements, silver_openaq_locations, silver_fx_rates, silver_gdp
# **Output:** gold_warehouse.dbo — DimDate, DimZone, DimFX, DimGDP, FactTaxiDaily, FactAirQualityDaily

# PARAMETERS CELL ********************

year_start = 2023
year_end = 2023

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports

# CELL ********************

import com.microsoft.spark.fabric
from pyspark.sql.functions import (
    col, explode, sequence, to_date,
    year, quarter, month, date_format,
    weekofyear, dayofmonth, dayofweek,
    avg, max, min, count, sum as spark_sum,
    round as spark_round, row_number, unix_timestamp
)
from py4j.protocol import Py4JJavaError
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

YEAR_START = year_start
YEAR_END   = year_end

BRONZE_TAXI_ZONES           = f"{BRONZE}.bronze_taxi_zones"

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

def write_gold(df_new, table: str, exclude_filter: str = None) -> None:
    if exclude_filter:
        try:
            df_existing = spark.read.synapsesql(f"{GOLD}.dbo.{table}")
            df_final = df_existing.filter(exclude_filter).unionByName(df_new)
        except Py4JJavaError as e:
            if "source is invalid" in str(e) or "read access" in str(e):
                print(f"[{table}] not found (first run), creating fresh")
                df_final = df_new
            else:
                raise
    else:
        df_final = df_new
    print(f"[{table}] rows before write: {df_final.count()}")
    df_final.write.mode("overwrite").synapsesql(f"{GOLD}.dbo.{table}")
    print(f"[{table}] write done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DimDate
# Full date spine covering the accumulated range across all runs. day_of_week: 1=Monday … 7=Sunday.
# Range extends to include existing DimDate years so previous years are never lost on a partial run.

# CELL ********************

try:
    _row = spark.read.synapsesql(f"{GOLD}.dbo.DimDate").agg(
        min(col("year")).alias("min_y"),
        max(col("year")).alias("max_y"),
    ).collect()[0]
    _dim_year_start = _row["min_y"] if _row["min_y"] < YEAR_START else YEAR_START
    _dim_year_end   = _row["max_y"] if _row["max_y"] > YEAR_END   else YEAR_END
except Py4JJavaError as e:
    if "source is invalid" in str(e) or "read access" in str(e):
        print("DimDate not found (first run), using parameter range")
        _dim_year_start = YEAR_START
        _dim_year_end   = YEAR_END
    else:
        raise

print(f"DimDate range: {_dim_year_start} - {_dim_year_end}")

df_dim_date = (
    spark.sql(f"""
        SELECT explode(sequence(
            to_date('{_dim_year_start}-01-01'),
            to_date('{_dim_year_end}-12-31'),
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
# Reads from `bronze_taxi_zones` (ingested separately by `bronze_ingest_taxi_zones` notebook).
# 265 rows. zone_key = location_id (natural PK, 1–265).

# CELL ********************

df_dim_zone = (
    spark.read.table(BRONZE_TAXI_ZONES)
    .select(
        col("location_id").cast("int").alias("zone_key"),
        col("location_id").cast("int").alias("location_id"),
        col("zone").alias("zone_name"),
        col("borough"),
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
    .filter(col("year").between(YEAR_START, YEAR_END))
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

write_gold(df_fact_taxi, "FactTaxiDaily",
           exclude_filter=f"date_key < {YEAR_START * 10000 + 101} OR date_key > {YEAR_END * 10000 + 1231}")
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

df_loc = spark.read.table(SILVER_OPENAQ_LOCATIONS).select("location_id", "location_name", "country_name")

df_fact_aq = (
    spark.read.table(SILVER_OPENAQ_MEASUREMENTS)
    .filter(col("year").between(YEAR_START, YEAR_END))
    .withColumn("meas_date", to_date(col("datetime")))
    .groupBy("meas_date", "location_id", "parameter")
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
        col("location_name").alias("city"),
        col("country_name").alias("country"),
        "parameter",
        "avg_value",
        "max_value",
        "min_value",
        "measurement_count",
    )
)

write_gold(df_fact_aq, "FactAirQualityDaily",
           exclude_filter=f"date_key < {YEAR_START * 10000 + 101} OR date_key > {YEAR_END * 10000 + 1231}")
display(df_fact_aq.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# MARKDOWN ********************

# ### DimDate

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

# MARKDOWN ********************

# ### DimZone

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

# MARKDOWN ********************

# ### DimFX

# CELL ********************

df_check = spark.read.synapsesql(f"{GOLD}.dbo.DimFX")
print(f"DimFX rows: {df_check.count()}")
display(df_check.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DimGDP

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

# MARKDOWN ********************

# ### FactTaxiDaily

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

# MARKDOWN ********************

# ### FactAirQualityDaily

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
