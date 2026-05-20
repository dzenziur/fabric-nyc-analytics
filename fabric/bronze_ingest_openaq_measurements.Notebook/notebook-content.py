# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c9741aec-56ed-41b9-9025-c45ee072256d",
# META       "default_lakehouse_name": "bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "d5f75821-ae8f-4a0a-b235-74982716aa0b",
# META       "known_lakehouses": [
# META         {
# META           "id": "c9741aec-56ed-41b9-9025-c45ee072256d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze — OpenAQ Measurements Ingestion
# Reads pollutant measurement history for NYC stations from the OpenAQ public S3 archive.
# **Input:** `bronze_openaq_locations` (NYC stations filtered by bounding box) + S3 archive
# **Output:** `bronze_openaq_measurements` (raw CSV data, no transformations)
# **Note:** Fabric Spark S3A cannot access public S3 anonymously — boto3 is used instead.

# PARAMETERS CELL ********************

year_start = 2023
year_end = 2023
force_refresh = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import subprocess
subprocess.run(["pip", "install", "boto3", "-q"], check=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports

# CELL ********************

import io
import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.client import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from delta.tables import DeltaTable
from pyspark.sql.functions import col, year, to_timestamp
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

NYC_LAT_MIN, NYC_LAT_MAX = 40.4, 40.9
NYC_LON_MIN, NYC_LON_MAX = -74.3, -73.7

YEAR_END   = year_end
YEAR_START = year_start

S3_BUCKET = "openaq-data-archive"
S3_BASE   = "records/csv.gz"

BRONZE_OPENAQ_LOCATIONS    = f"{BRONZE}.bronze_openaq_locations"
BRONZE_OPENAQ_MEASUREMENTS = f"{BRONZE}.bronze_openaq_measurements"

# Parallelism: STATION_WORKERS outer stations × MAX_WORKERS inner S3 keys per station.
# Total concurrent S3 GETs = 8 × 16 = 128, bounded by the boto3 connection pool below.
STATION_WORKERS = 8
MAX_WORKERS     = 16

print(f"Year range: {YEAR_START} - {YEAR_END}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper

# CELL ********************

def list_keys(s3_client: object, bucket: str, loc_id: int, months: list) -> list:
    """List S3 keys for given (year, month) tuples. Uses month-level prefix for narrower listing."""
    keys = []
    for y, m in months:
        prefix = f"{S3_BASE}/locationid={loc_id}/year={y}/month={m:02d}/"
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def download_file(s3_client: object, bucket: str, key: str) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()), compression="gzip")


def read_location(s3_client: object, bucket: str, loc_id: int, months: list) -> pd.DataFrame:
    keys = list_keys(s3_client, bucket, loc_id, months)
    if not keys:
        return pd.DataFrame()
    dfs = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_file, s3_client, bucket, k): k for k in keys}
        for future in as_completed(futures):
            dfs.append(future.result())
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def months_in_year_range(year_start: int, year_end: int) -> list:
    """Generate all (year, month) tuples for full year range."""
    return [(y, m) for y in range(year_start, year_end + 1) for m in range(1, 13)]


def current_and_previous_month() -> list:
    """Return [(prev_year, prev_month), (curr_year, curr_month)] for incremental fetching."""
    today = date.today()
    if today.month == 1:
        prev = (today.year - 1, 12)
    else:
        prev = (today.year, today.month - 1)
    return [prev, (today.year, today.month)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## S3 Client
# Anonymous access via boto3 — `openaq-data-archive` is a public AWS Open Data Registry bucket.

# CELL ********************

s3 = boto3.client(
    "s3",
    config=Config(
        signature_version=UNSIGNED,
        # Connection pool must accommodate STATION_WORKERS × MAX_WORKERS concurrent GETs;
        # boto3 default of 10 would cause "Connection pool is full" warnings + queueing.
        max_pool_connections=STATION_WORKERS * MAX_WORKERS,
    ),
    region_name="us-east-1",
)
print(f"S3 client ready (max_pool_connections={STATION_WORKERS * MAX_WORKERS})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## NYC Stations
# Read location IDs from `bronze_openaq_locations`, filter by NYC bounding box.
# Pre-filter by station activity window — drop stations whose `[datetime_first, datetime_last]`
# doesn't overlap the requested year range. Saves the bulk of S3 LIST calls (most NYC stations
# came online post-2023, so 2021–2022 probes are otherwise just "no data, skipping").
# Falls back to no filter (with a warning) if the activity columns are missing.

# CELL ********************

from datetime import datetime
from pyspark.sql.functions import lit

df_locations = spark.read.table(BRONZE_OPENAQ_LOCATIONS)

df_nyc = df_locations.filter(
    (col("latitude")  >= NYC_LAT_MIN) & (col("latitude")  <= NYC_LAT_MAX) &
    (col("longitude") >= NYC_LON_MIN) & (col("longitude") <= NYC_LON_MAX)
)

has_activity_cols = "datetime_first" in df_locations.columns and "datetime_last" in df_locations.columns
if has_activity_cols:
    range_start = datetime(YEAR_START, 1, 1)
    range_end   = datetime(YEAR_END, 12, 31, 23, 59, 59)
    df_active = df_nyc.filter(
        (col("datetime_first").isNull() | (to_timestamp(col("datetime_first")) <= lit(range_end))) &
        (col("datetime_last").isNull()  | (to_timestamp(col("datetime_last"))  >= lit(range_start)))
    )
    nyc_total  = df_nyc.count()
    nyc_active = df_active.count()
    print(f"NYC stations in bbox: {nyc_total}; active for {YEAR_START}-{YEAR_END}: {nyc_active} (dropped {nyc_total - nyc_active} inactive)")
    df_filtered = df_active
else:
    print(f"WARNING: bronze_openaq_locations is missing datetime_first/datetime_last — skipping activity pre-filter. Re-run bronze_ingest_openaq_locations to enable.")
    df_filtered = df_nyc

nyc_ids = (
    df_filtered
    .select("location_id")
    .rdd.flatMap(lambda x: x)
    .collect()
)

print(f"NYC stations to probe: {len(nyc_ids)}")
print(f"IDs: {sorted(nyc_ids)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read Measurements from S3 and Write to Bronze
# Default mode (force_refresh=False): fetch only current + previous month from S3, MERGE INTO bronze on natural key.
#   Most efficient for scheduled daily runs — historical months in S3 archive are immutable.
# force_refresh=True: fetch full year range, replace year partitions (existing behavior). For manual backfill.
# First run (table doesn't exist): falls back to full year range mode regardless of force_refresh.

# CELL ********************

# Determine which months to fetch
try:
    bronze_exists = spark.catalog.tableExists(BRONZE_OPENAQ_MEASUREMENTS)
except Exception:
    bronze_exists = False

if force_refresh or not bronze_exists:
    months_to_fetch = months_in_year_range(YEAR_START, YEAR_END)
    use_incremental = False
    print(f"Full mode — fetching year range {YEAR_START}-{YEAR_END} ({len(months_to_fetch)} months)")
else:
    months_to_fetch = current_and_previous_month()
    use_incremental = True
    print(f"Incremental mode — fetching months: {months_to_fetch}")

dfs_new = []
total_rows = 0

# Outer parallelism: fetch up to STATION_WORKERS stations concurrently. S3 networking is
# the dominant cost; spark.createDataFrame must run on the driver and stays serial via
# as_completed (cheap, in-memory). Log order is now completion-order, not nyc_ids order.
with ThreadPoolExecutor(max_workers=STATION_WORKERS) as executor:
    futures = {
        executor.submit(read_location, s3, S3_BUCKET, loc_id, months_to_fetch): loc_id
        for loc_id in nyc_ids
    }
    print(f"Submitted {len(futures)} station fetches; outer parallelism = {STATION_WORKERS}, inner per-station = {MAX_WORKERS}")
    for future in as_completed(futures):
        loc_id = futures[future]
        df_pd = future.result()
        if df_pd.empty:
            print(f"[location {loc_id}] no data, skipping")
            continue
        dfs_new.append(
            spark.createDataFrame(df_pd)
            .withColumn("year", year(to_timestamp(col("datetime"))))
        )
        total_rows += len(df_pd)
        print(f"[location {loc_id}] rows fetched: {len(df_pd)}")

print(f"New rows fetched from S3: {total_rows}")

if not dfs_new:
    if use_incremental:
        print(f"[{BRONZE_OPENAQ_MEASUREMENTS}] no new data for months {months_to_fetch} — skipping write")
    else:
        raise ValueError(f"No data found in S3 for any NYC station in year range {YEAR_START}–{YEAR_END}. Aborting.")
else:
    df_new = dfs_new[0]
    for d in dfs_new[1:]:
        df_new = df_new.unionByName(d, allowMissingColumns=True)

    if use_incremental:
        target = DeltaTable.forName(spark, BRONZE_OPENAQ_MEASUREMENTS)
        (
            target.alias("t")
            .merge(
                df_new.alias("s"),
                "t.location_id = s.location_id AND t.sensors_id = s.sensors_id "
                "AND t.datetime = s.datetime AND t.parameter = s.parameter"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[{BRONZE_OPENAQ_MEASUREMENTS}] incremental merge done")
    else:
        try:
            df_existing = spark.read.table(BRONZE_OPENAQ_MEASUREMENTS)
            if "year" not in df_existing.columns:
                df_existing = df_existing.withColumn("year", year(to_timestamp(col("datetime"))))
            df_existing = df_existing.filter(
                (col("year") < YEAR_START) | (col("year") > YEAR_END)
            )
            df_final = df_existing.unionByName(df_new, allowMissingColumns=True)
        except AnalysisException:
            df_final = df_new

        print(f"[{BRONZE_OPENAQ_MEASUREMENTS}] rows before write: {df_final.count()}")
        (
            df_final.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("year")
            .saveAsTable(BRONZE_OPENAQ_MEASUREMENTS)
        )
        print(f"[{BRONZE_OPENAQ_MEASUREMENTS}] write done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

spark.sql(f"SELECT year, COUNT(*) as cnt FROM {BRONZE_OPENAQ_MEASUREMENTS} GROUP BY year ORDER BY year").show()
spark.sql(f"SELECT parameter, COUNT(*) as cnt FROM {BRONZE_OPENAQ_MEASUREMENTS} GROUP BY parameter ORDER BY cnt DESC").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
