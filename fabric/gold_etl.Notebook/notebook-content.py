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
# Transforms Silver tables into a star schema in `gold_warehouse`.
# Python cells handle all transformations and register temp views.
# Scala cells write each temp view to gold_warehouse via synapsesql.
# **Input:** silver_taxi_trips, silver_openaq_measurements, silver_fx_rates, silver_gdp
# **Output:** gold_warehouse.dbo — DimDate, DimZone, DimFX, DimGDP, FactTaxiDaily, FactAirQualityDaily

# MARKDOWN ********************

# ## Imports

# CELL ********************

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

SILVER = "silver_lakehouse"
GOLD   = "gold_warehouse"

YEAR_START = 2019
YEAR_END   = 2025

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

def stage(df, table: str) -> None:
    """Count rows, register temp view for Scala synapsesql write."""
    print(f"[{table}] rows staged: {df.count()}")
    df.createOrReplaceTempView(f"v_{table}")

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

stage(df_dim_date, "DimDate")
display(df_dim_date.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%%scala
spark.table("v_DimDate").write.mode("overwrite").synapsesql("gold_warehouse.dbo.DimDate")
println("[DimDate] write done")

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_spark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

%%scala
spark.sql("SELECT COUNT(*) AS total_days, MIN(date) AS min_date, MAX(date) AS max_date FROM gold_warehouse.dbo.DimDate").show()
spark.sql("SELECT is_weekend, COUNT(*) AS cnt FROM gold_warehouse.dbo.DimDate GROUP BY is_weekend").show()

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_spark"
# META }
