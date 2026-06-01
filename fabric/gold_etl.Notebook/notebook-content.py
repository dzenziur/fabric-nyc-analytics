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

# # Gold ETL — Silver → Fabric Warehouse
# Builds the star schema in `gold_warehouse` from Silver tables. Two fact tables and four dimensions, all written via `df.write.synapsesql`. Idempotent through the **read-filter-union-overwrite** pattern in the `write_gold` helper.
# ### Input
# - `silver_taxi_trips`, `silver_taxi_zones`.<p>
# - `silver_openaq_measurements`, `silver_openaq_locations`.<p>
# - `silver_fx_rates`, `silver_gdp`.
# ### Output
# All written to `gold_warehouse.dbo.*`:
# - Dimensions — `DimDate`, `DimZone`, `DimFX`, `DimGDP`.<p>
# - Facts — `FactTaxiDaily`, `FactAirQualityDaily`.
# ### Parameters
# - `year_start` (int) — lower bound of year range.<p>
# - `year_end` (int) — upper bound of year range.<p>
# - `force_refresh` (bool) — controls incremental vs full rebuild.<p>
# Parameter behavior varies per output table — see each table's section below for the specifics.

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
# - `com.microsoft.spark.fabric` — Fabric-specific Scala module that makes `df.write.synapsesql` available in PySpark. Without this import the call raises `AttributeError`, and Fabric documentation doesn't surface this requirement.<p>
# - `pyspark.sql.functions` — column functions for casts, derived columns, date keys, aggregations.<p>
# - `pyspark.sql.Window` + `row_number()` — sequential surrogate keys for `DimFX`, `DimGDP`.<p>
# - `py4j.protocol.Py4JJavaError` — caught by `write_gold` and read paths when a Warehouse table doesn't exist yet (first-run fallback).

# CELL ********************

import com.microsoft.spark.fabric
from datetime import date, timedelta
from pyspark.sql.functions import (
    col, lit, to_date,
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
# Short uppercase aliases for the lakehouse/warehouse names, full Silver table identifiers, and `LATE_ARRIVING_LOOKBACK_DAYS = 7` — the incremental re-aggregation window for both fact tables. Seven days handles late-arriving Silver data plus any short missed-run gap on the schedule.

# CELL ********************

SILVER = "silver_lakehouse"
GOLD   = "gold_warehouse"

LATE_ARRIVING_LOOKBACK_DAYS = 7

SILVER_TAXI_TRIPS           = f"{SILVER}.silver_taxi_trips"
SILVER_TAXI_ZONES           = f"{SILVER}.silver_taxi_zones"
SILVER_OPENAQ_MEASUREMENTS  = f"{SILVER}.silver_openaq_measurements"
SILVER_OPENAQ_LOCATIONS     = f"{SILVER}.silver_openaq_locations"
SILVER_FX_RATES             = f"{SILVER}.silver_fx_rates"
SILVER_GDP                  = f"{SILVER}.silver_gdp"

print(f"Year range: {year_start} - {year_end}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper
# `write_gold(df_new, table, exclude_filter)` — universal Warehouse writer implementing the **read-filter-union-overwrite** pattern that gives idempotent writes on top of `synapsesql` (which only supports `mode("overwrite")` / `mode("append")`).
# Used with `exclude_filter` for facts:
# 1. Read existing Warehouse table.<p>
# 2. Filter to rows matching `exclude_filter` (rows NOT being refreshed in this run).<p>
# 3. Union the filtered set with `df_new`.<p>
# 4. Overwrite the whole table with the result.<p>
# Without `exclude_filter` (dims): plain full overwrite. First-run is handled by catching the specific `Py4JJavaError` raised when the Warehouse table doesn't exist yet.<p>
# Why not just append or overwrite directly: `append` is non-idempotent (re-run duplicates rows), full `overwrite` erases history outside the refresh range. Delta `replaceWhere` isn't available through `synapsesql`. This pattern is the workaround.

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
# Full date spine — one row per day across the accumulated range. `date_key` is an integer `YYYYMMDD` (e.g. `20240315` for 2024-03-15) — same formula used by all facts and `DimFX`, so keys match by construction and no DimDate join is needed during ETL.<p>
# `day_of_week`: 1 = Monday … 7 = Sunday.
# ### Range logic
# `year_start` / `year_end` are **always used** as a floor/ceiling. Then the range is **extended outward** by reading the existing DimDate min/max from the Warehouse and taking the union — so a partial re-run for one year doesn't erase the others. DimDate never shrinks.

# CELL ********************

try:
    _row = spark.read.synapsesql(f"{GOLD}.dbo.DimDate").agg(
        min(col("year")).alias("min_y"),
        max(col("year")).alias("max_y"),
    ).collect()[0]
    _dim_year_start = _row["min_y"] if _row["min_y"] < year_start else year_start
    _dim_year_end   = _row["max_y"] if _row["max_y"] > year_end   else year_end
except Py4JJavaError as e:
    if "source is invalid" in str(e) or "read access" in str(e):
        print("DimDate not found (first run), using parameter range")
        _dim_year_start = year_start
        _dim_year_end   = year_end
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
# 265 rows of TLC zone lookup, read from `silver_taxi_zones` (cleaned + cast in `silver_etl`).<p>
# `zone_key = location_id` — natural PK (1–265). TLC's IDs are stable and dense, so no surrogate is needed. Used as the FK target for `FactTaxiDaily[zone_key]` and as the RLS filter column (`service_zone`) in the semantic model — five roles map service zones to taxi-operations teams.

# CELL ********************

df_dim_zone = (
    spark.read.table(SILVER_TAXI_ZONES)
    .select(
        col("location_id").alias("zone_key"),
        col("location_id"),
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
# One row per ECB trading date with the USD/EUR rate. Weekends and ECB holidays are **absent** — no rate published. `FactTaxiDaily` joins via LEFT JOIN so weekend trips survive with a null `fx_key` and null `total_fare_eur`.<p>
# `fx_key` is a sequential surrogate via `row_number().over(Window.orderBy("date"))` — compact FK instead of using the date itself.

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
# One row per (country, year). Source: World Bank annual GDP, ~6k rows covering all countries from 2000 to ~2024 (World Bank lags ~1 year).<p>
# `gdp_key` is a sequential surrogate. `gdp_trillion_usd` is a display-friendly derived column (`gdp_usd / 1e12`, rounded to 4 decimals) for Power BI cards.<p>
# **DimGDP has no relationship in the semantic model.** GDP is yearly + country-level, so joining to daily zone-level facts would create grain mismatch and unwanted cross-filter side effects. The `USA GDP (USD)` DAX measure uses a year-aware `SELECTEDVALUE` virtual relationship instead.

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
# Daily aggregate of NYC taxi trips. **Grain:** one row per (day × pickup zone).
# ### Aggregations
# `trip_count`, `total_fare_usd`, `avg_fare_usd`, `avg_trip_duration_min`, `avg_trip_distance_mi`, `total_passengers`. `total_fare_eur` is derived via LEFT JOIN to `DimFX` (`total_fare_usd × usd_eur_rate`) — weekends and holidays have null FX → null EUR (acceptable).
# ### Dimension joins
# - `DimFX` (LEFT) — gives `fx_key` + `usd_eur_rate` for EUR conversion.<p>
# - `DimZone` (LEFT) — translates `pu_location_id` → `zone_key`.<p>
# - `DimDate` — **not joined**; `date_key` is computed directly from `trip_date` (YYYYMMDD formula).<p>
# - `DimGDP` — **not joined**; no fact-level relationship.
# ### Mode scenarios
# - **`force_refresh=True`** — full rebuild for `year_start..year_end`. `exclude_filter` keeps rows OUTSIDE that range (`date_key < YYYY0101 OR date_key > YYYY1231`).<p>
# - **`force_refresh=False` + table has data** — incremental. Re-aggregate Silver from `MAX(gold.date_key) - LATE_ARRIVING_LOOKBACK_DAYS (=7)` forward; `exclude_filter = "date_key < cutoff"` keeps everything older than the lookback window. The 7-day lookback covers late-arriving Silver data and short missed-run gaps.<p>
# - **`force_refresh=False` + table empty/missing** — fallback to full rebuild for the year range.


# CELL ********************

df_fx   = spark.read.synapsesql(f"{GOLD}.dbo.DimFX").select("fx_key", "date", "usd_eur_rate")
df_zone = spark.read.synapsesql(f"{GOLD}.dbo.DimZone").select("zone_key", "location_id")

if force_refresh:
    df_silver_taxi = (
        spark.read.table(SILVER_TAXI_TRIPS)
        .filter(col("year").between(year_start, year_end))
    )
    fact_taxi_exclude_filter = f"date_key < {year_start * 10000 + 101} OR date_key > {year_end * 10000 + 1231}"
    print(f"[FactTaxiDaily] force_refresh=True — full rebuild for {year_start}-{year_end}")
else:
    try:
        max_dt_key_row = spark.read.synapsesql(f"{GOLD}.dbo.FactTaxiDaily").agg(max("date_key")).collect()
        max_dt_key = max_dt_key_row[0][0] if max_dt_key_row and max_dt_key_row[0][0] is not None else None
    except Py4JJavaError as e:
        if "source is invalid" in str(e) or "read access" in str(e):
            max_dt_key = None
        else:
            raise

    if max_dt_key is None:
        df_silver_taxi = (
            spark.read.table(SILVER_TAXI_TRIPS)
                .filter(col("year").between(year_start, year_end))
        )
        fact_taxi_exclude_filter = f"date_key < {year_start * 10000 + 101} OR date_key > {year_end * 10000 + 1231}"
        print(f"[FactTaxiDaily] no existing data — falling back to full rebuild for {year_start}-{year_end}")
    else:
        max_dt = date(max_dt_key // 10000, (max_dt_key // 100) % 100, max_dt_key % 100)
        cutoff_dt = max_dt - timedelta(days=LATE_ARRIVING_LOOKBACK_DAYS)
        cutoff_dt_key = int(cutoff_dt.strftime("%Y%m%d"))
        df_silver_taxi = (
            spark.read.table(SILVER_TAXI_TRIPS)
                .filter(col("pickup_datetime") >= lit(cutoff_dt.strftime("%Y-%m-%d")))
        )
        fact_taxi_exclude_filter = f"date_key < {cutoff_dt_key}"
        print(f"[FactTaxiDaily] incremental — gold max date_key: {max_dt_key}, re-aggregating from {cutoff_dt_key} ({LATE_ARRIVING_LOOKBACK_DAYS}-day lookback)")

df_agg = (
    df_silver_taxi
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

write_gold(df_fact_taxi, "FactTaxiDaily", exclude_filter=fact_taxi_exclude_filter)
display(df_fact_taxi.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FactAirQualityDaily
# Daily aggregate of OpenAQ pollutant measurements. **Grain:** one row per (day × location × parameter). Three keys because one station measures multiple pollutants — each parameter (PM2.5, NO2, O3, …) is a separate row.
# ### Aggregations
# `avg_value`, `max_value`, `min_value`, `measurement_count`. Both max and min matter — short peaks can exceed WHO thresholds even when daily averages look safe. `measurement_count` is a quality indicator (typically 24 hourly readings per day).
# ### Dimension joins
# Locations are **denormalized into the fact** (`city`, `country`, `latitude`, `longitude`) instead of a separate `DimLocation`. Only one fact uses these columns, and the Power BI Azure Map needs lat/lon directly on the fact. `DimDate` is implicit via the computed `date_key`. In the semantic model the `city` column is exposed as `station_name` (sourceColumn alias), since OpenAQ stations aren't cities.
# ### Mode scenarios
# Same as `FactTaxiDaily`:
# - **`force_refresh=True`** — full rebuild for `year_start..year_end`.<p>
# - **`force_refresh=False` + table has data** — incremental with 7-day lookback from `MAX(gold.date_key)`.<p>
# - **`force_refresh=False` + table empty/missing** — fallback to full rebuild.


# CELL ********************

df_loc = spark.read.table(SILVER_OPENAQ_LOCATIONS).select(
    "location_id", "location_name", "country_name", "latitude", "longitude"
)

if force_refresh:
    df_silver_aq = (
        spark.read.table(SILVER_OPENAQ_MEASUREMENTS)
        .filter(col("year").between(year_start, year_end))
    )
    fact_aq_exclude_filter = f"date_key < {year_start * 10000 + 101} OR date_key > {year_end * 10000 + 1231}"
    print(f"[FactAirQualityDaily] force_refresh=True — full rebuild for {year_start}-{year_end}")
else:
    try:
        max_dt_key_row = spark.read.synapsesql(f"{GOLD}.dbo.FactAirQualityDaily").agg(max("date_key")).collect()
        max_dt_key = max_dt_key_row[0][0] if max_dt_key_row and max_dt_key_row[0][0] is not None else None
    except Py4JJavaError as e:
        if "source is invalid" in str(e) or "read access" in str(e):
            max_dt_key = None
        else:
            raise

    if max_dt_key is None:
        df_silver_aq = (
            spark.read.table(SILVER_OPENAQ_MEASUREMENTS)
                .filter(col("year").between(year_start, year_end))
        )
        fact_aq_exclude_filter = f"date_key < {year_start * 10000 + 101} OR date_key > {year_end * 10000 + 1231}"
        print(f"[FactAirQualityDaily] no existing data — falling back to full rebuild for {year_start}-{year_end}")
    else:
        max_dt = date(max_dt_key // 10000, (max_dt_key // 100) % 100, max_dt_key % 100)
        cutoff_dt = max_dt - timedelta(days=LATE_ARRIVING_LOOKBACK_DAYS)
        cutoff_dt_key = int(cutoff_dt.strftime("%Y%m%d"))
        df_silver_aq = (
            spark.read.table(SILVER_OPENAQ_MEASUREMENTS)
                .filter(col("datetime") >= lit(cutoff_dt.strftime("%Y-%m-%d")))
        )
        fact_aq_exclude_filter = f"date_key < {cutoff_dt_key}"
        print(f"[FactAirQualityDaily] incremental — gold max date_key: {max_dt_key}, re-aggregating from {cutoff_dt_key} ({LATE_ARRIVING_LOOKBACK_DAYS}-day lookback)")

df_fact_aq = (
    df_silver_aq
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
        "latitude",
        "longitude",
        "parameter",
        "avg_value",
        "max_value",
        "min_value",
        "measurement_count",
    )
)

write_gold(df_fact_aq, "FactAirQualityDaily", exclude_filter=fact_aq_exclude_filter)
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
