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
# Reads hourly pollutant measurements for NYC stations from the OpenAQ public S3 archive
# (`s3://openaq-data-archive/`). Largest and most parallelized Bronze notebook.
# ### Input
# - `bronze_openaq_locations` — station list, filtered downstream to NYC bbox + active window.<p>
# - `s3://openaq-data-archive/records/csv.gz/locationid=*/year=*/month=*/...` — public, anonymous.
# ### Output
# - `bronze_openaq_measurements` — raw CSV data, no transformations (cleaning happens in Silver).
# ### Parameters
# - `year_start` (int) — lower bound; used only on full rebuilds.<p>
# - `year_end` (int) — upper bound; used only on full rebuilds.<p>
# - `force_refresh` (bool) — selects which months to fetch and how to write; details in the **OpenAQ Measurements** section below.

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

# ## Install
# `boto3` isn't bundled with Fabric Spark runtimes — install it inline so the notebook is self-contained. Quiet flag (`-q`) keeps the output clean.

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
# - `boto3` + `botocore.UNSIGNED` — anonymous S3 access.<p>
# - `pandas` — accumulating per-station CSV chunks before one bulk conversion to Spark.<p>
# - `ThreadPoolExecutor` / `as_completed` — two-level parallelism for fetching stations and S3 keys concurrently.<p>
# - `DeltaTable` — incremental MERGE in default mode.

# CELL ********************

import io
import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.client import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, year, to_timestamp
from pyspark.sql.utils import AnalysisException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Config
# **NYC bounding box** — lat/lon range covering the five boroughs plus immediate suburbs. Used to filter the global station list from `bronze_openaq_locations` down to ~22 NYC-area stations.<p>
# **S3 archive layout** — `s3://openaq-data-archive/records/csv.gz/locationid={id}/year={y}/month={m}/...`. Month-level prefix listing is much cheaper than scanning the whole `locationid={id}/` tree.<p>
# **Parallelism** — two-level: `STATION_WORKERS=8` outer workers (one station each) × `MAX_WORKERS=16` inner workers (S3 keys per station) = up to 128 concurrent S3 GETs. The boto3 connection pool below is sized to match; the default of 10 would queue and warn "Connection pool is full".

# CELL ********************

BRONZE = "bronze_lakehouse"

NYC_LAT_MIN, NYC_LAT_MAX = 40.4, 40.9
NYC_LON_MIN, NYC_LON_MAX = -74.3, -73.7

S3_BUCKET = "openaq-data-archive"
S3_BASE   = "records/csv.gz"

BRONZE_OPENAQ_LOCATIONS    = f"{BRONZE}.bronze_openaq_locations"
BRONZE_OPENAQ_MEASUREMENTS = f"{BRONZE}.bronze_openaq_measurements"

STATION_WORKERS = 8
MAX_WORKERS     = 16

print(f"Year range: {year_start} - {year_end}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper
# - `list_keys` — list S3 object keys for given `(year, month)` tuples under one station's prefix. Uses month-level prefix to keep the LIST cheap (much faster than scanning the whole station tree).<p>
# - `download_file` — download one S3 object and read its gzipped CSV into a pandas DataFrame.<p>
# - `read_location` — combine both: list all keys for a station's months, download them in parallel via the inner `ThreadPoolExecutor`, return one concatenated DataFrame.<p>
# - `months_in_year_range` — `[(y, m) for y in start..end for m in 1..12]` for full-rebuild mode.<p>
# - `current_and_previous_month` — `[(prev_year, prev_month), (curr_year, curr_month)]` for incremental mode. Handles year boundary (January → previous month is December of last year).

# CELL ********************

def list_keys(s3_client: object, bucket: str, loc_id: int, months: list) -> list:
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
    return [(y, m) for y in range(year_start, year_end + 1) for m in range(1, 13)]


def current_and_previous_month() -> list:
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
# `openaq-data-archive` is a public AWS Open Data Registry bucket — anonymous access only. `signature_version=UNSIGNED` tells boto3 to skip credential signing, which Spark's S3A connector can't do (Fabric Spark hardcodes the AWS credential provider chain).<p>
# `max_pool_connections` is sized to match the maximum number of concurrent GETs we can launch (`STATION_WORKERS × MAX_WORKERS = 128`). The boto3 default of 10 would queue requests and emit "Connection pool is full" warnings.

# CELL ********************

s3 = boto3.client(
    "s3",
    config=Config(
        signature_version=UNSIGNED,
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
# Build the list of stations to probe in this run:<p>
# 1. Read all stations from `bronze_openaq_locations`.<p>
# 2. Spatial filter: keep those inside the NYC bounding box (`NYC_LAT/LON_*`) → ~22 stations.<p>
# 3. Temporal filter (activity window): drop stations whose `[datetime_first, datetime_last]` does NOT overlap the requested year range. Saves the bulk of S3 LIST calls — most NYC stations came online post-2023, so 2021–2022 probes would otherwise be just "no data, skipping".<p>
# Falls back to no temporal filter (with a WARNING) if the activity columns are missing — they're populated by `bronze_ingest_openaq_locations`.

# CELL ********************

df_locations = spark.read.table(BRONZE_OPENAQ_LOCATIONS)

df_nyc = df_locations.filter(
    (col("latitude")  >= NYC_LAT_MIN) & (col("latitude")  <= NYC_LAT_MAX) &
    (col("longitude") >= NYC_LON_MIN) & (col("longitude") <= NYC_LON_MAX)
)

has_activity_cols = "datetime_first" in df_locations.columns and "datetime_last" in df_locations.columns
if has_activity_cols:
    range_start = datetime(year_start, 1, 1)
    range_end   = datetime(year_end, 12, 31, 23, 59, 59)
    df_active = df_nyc.filter(
        (col("datetime_first").isNull() | (to_timestamp(col("datetime_first")) <= lit(range_end))) &
        (col("datetime_last").isNull()  | (to_timestamp(col("datetime_last"))  >= lit(range_start)))
    )
    nyc_total  = df_nyc.count()
    nyc_active = df_active.count()
    print(f"NYC stations in bbox: {nyc_total}; active for {year_start}-{year_end}: {nyc_active} (dropped {nyc_total - nyc_active} inactive)")
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

# ## OpenAQ Measurements
# Fetch + write loop. Mode logic decides which months to fetch and how to write.
# ### Mode scenarios
# - **`force_refresh=True`** → fetch full `year_start..year_end` range. Partition overwrite by `year` via read-filter-union-overwrite (read existing, filter out year range, union new, overwrite).<p>
# - **`force_refresh=False` + Bronze has data** → fetch only current + previous month from S3 (since the archive is immutable for closed months, older data can't have changed). Delta `MERGE INTO` on natural key `(location_id, sensors_id, datetime, parameter)` with `whenNotMatchedInsertAll` (insert-only — never updates matched rows).<p>
# - **`force_refresh=False` + first run / empty Bronze** → falls back to full year range.
# ### Parallel fetch
# Outer `ThreadPoolExecutor` of `STATION_WORKERS=8` stations runs concurrently; each station spawns inner `MAX_WORKERS=16` downloads. Up to 128 concurrent S3 GETs total. Results accumulate as per-station pandas DataFrames and are concatenated once at the end via `pd.concat` — much cheaper than a long Spark `unionByName` chain.


# CELL ********************

# Determine which months to fetch
try:
    bronze_exists = spark.catalog.tableExists(BRONZE_OPENAQ_MEASUREMENTS)
except Exception:
    bronze_exists = False

if force_refresh or not bronze_exists:
    months_to_fetch = months_in_year_range(year_start, year_end)
    use_incremental = False
    print(f"Full mode — fetching year range {year_start}-{year_end} ({len(months_to_fetch)} months)")
else:
    months_to_fetch = current_and_previous_month()
    use_incremental = True
    print(f"Incremental mode — fetching months: {months_to_fetch}")

pd_dfs = []
total_rows = 0

# Outer parallelism: fetch up to STATION_WORKERS stations concurrently. S3 networking is
# the dominant cost; results accumulate as pandas DataFrames and are merged into a single
# Spark DataFrame at the end (one createDataFrame instead of a long unionByName chain).
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
        pd_dfs.append(df_pd)
        total_rows += len(df_pd)
        print(f"[location {loc_id}] rows fetched: {len(df_pd)}")

print(f"New rows fetched from S3: {total_rows}")

if not pd_dfs:
    if use_incremental:
        print(f"[{BRONZE_OPENAQ_MEASUREMENTS}] no new data for months {months_to_fetch} — skipping write")
    else:
        raise ValueError(f"No data found in S3 for any NYC station in year range {year_start}–{year_end}. Aborting.")
else:
    df_new = (
        spark.createDataFrame(pd.concat(pd_dfs, ignore_index=True))
        .withColumn("year", year(to_timestamp(col("datetime"))))
    )

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
                (col("year") < year_start) | (col("year") > year_end)
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
