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
from pyspark.sql.functions import col, year, to_timestamp

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

MAX_WORKERS = 50

print(f"Year range: {YEAR_START} - {YEAR_END}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper

# CELL ********************

def list_keys(s3_client: object, bucket: str, loc_id: int, year_start: int, year_end: int) -> list:
    keys = []
    for year in range(year_start, year_end + 1):
        prefix = f"{S3_BASE}/locationid={loc_id}/year={year}/"
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def download_file(s3_client: object, bucket: str, key: str) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()), compression="gzip")


def read_location(s3_client: object, bucket: str, loc_id: int, year_start: int, year_end: int) -> pd.DataFrame:
    keys = list_keys(s3_client, bucket, loc_id, year_start, year_end)
    if not keys:
        return pd.DataFrame()
    dfs = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_file, s3_client, bucket, k): k for k in keys}
        for future in as_completed(futures):
            dfs.append(future.result())
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

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
    config=Config(signature_version=UNSIGNED),
    region_name="us-east-1"
)
print("S3 client ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## NYC Stations
# Read location IDs from `bronze_openaq_locations`, filter by NYC bounding box.

# CELL ********************

df_locations = spark.read.table(BRONZE_OPENAQ_LOCATIONS)

nyc_ids = (
    df_locations
    .filter(
        (col("latitude")  >= NYC_LAT_MIN) & (col("latitude")  <= NYC_LAT_MAX) &
        (col("longitude") >= NYC_LON_MIN) & (col("longitude") <= NYC_LON_MAX)
    )
    .select("location_id")
    .rdd.flatMap(lambda x: x)
    .collect()
)

print(f"NYC stations found: {len(nyc_ids)}")
print(f"IDs: {sorted(nyc_ids)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read Measurements from S3 and Write to Bronze
# read-filter-union-overwrite: preserves rows outside the target year range.
# Re-running for the same range replaces only those years — idempotent.

# CELL ********************

dfs_new = []
total_rows = 0

for loc_id in nyc_ids:
    df_pd = read_location(s3, S3_BUCKET, loc_id, YEAR_START, YEAR_END)
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
    raise ValueError(f"No data found in S3 for any NYC station in year range {YEAR_START}–{YEAR_END}. Aborting.")

df_new = dfs_new[0]
for d in dfs_new[1:]:
    df_new = df_new.unionByName(d, allowMissingColumns=True)

try:
    df_existing = spark.read.table(BRONZE_OPENAQ_MEASUREMENTS)
    if "year" not in df_existing.columns:
        df_existing = df_existing.withColumn("year", year(to_timestamp(col("datetime"))))
    df_existing = df_existing.filter(
        (col("year") < YEAR_START) | (col("year") > YEAR_END)
    )
    df_final = df_existing.unionByName(df_new, allowMissingColumns=True)
except Exception:
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
