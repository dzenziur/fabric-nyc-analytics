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

# CELL ********************

from datetime import datetime

BRONZE = "bronze_lakehouse"

# NYC bounding box
NYC_LAT_MIN, NYC_LAT_MAX = 40.4, 40.9
NYC_LON_MIN, NYC_LON_MAX = -74.3, -73.7

# Year range — configurable
YEAR_END   = datetime.now().year - 1   # last fully completed year
YEAR_START = YEAR_END - 4              # 5 years back

# S3 source
OPENAQ_S3_BASE = "s3a://openaq-data-archive/records/csv.gz"

# Tables
BRONZE_OPENAQ_LOCATIONS    = f"{BRONZE}.bronze_openaq_locations"
BRONZE_OPENAQ_MEASUREMENTS = f"{BRONZE}.bronze_openaq_measurements"

print(f"Year range: {YEAR_START} – {YEAR_END}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## S3 Client
# Anonymous access via boto3 — `openaq-data-archive` is a public AWS Open Data Registry bucket.
# Fabric Spark S3A does not support anonymous credentials, so we use boto3 directly.

# CELL ********************

import boto3
import io
import pandas as pd
from botocore import UNSIGNED
from botocore.client import Config

S3_BUCKET = "openaq-data-archive"

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
# Process one location at a time via boto3.
# First location: overwrite (idempotent re-runs). Subsequent: append.
# Files are daily CSV.gz: `location-{id}-YYYYMMDD.csv.gz`

# CELL ********************

def read_location_year(s3_client: object, bucket: str, loc_id: int, year: int) -> pd.DataFrame:
    """Download all monthly CSV.gz files for one location/year, return as pandas DataFrame."""
    dfs = []
    for month in range(1, 13):
        prefix = f"records/csv.gz/locationid={loc_id}/year={year}/month={month:02d}/"
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in resp.get("Contents", []):
            raw = s3_client.get_object(Bucket=bucket, Key=obj["Key"])
            df = pd.read_csv(io.BytesIO(raw["Body"].read()), compression="gzip")
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


total_rows = 0
years = list(range(YEAR_START, YEAR_END + 1))

for i, loc_id in enumerate(nyc_ids):
    loc_rows = 0
    for year in years:
        df_pd = read_location_year(s3, S3_BUCKET, loc_id, year)
        if df_pd.empty:
            continue
        write_mode = "overwrite" if (i == 0 and year == years[0]) else "append"
        overwrite_schema = write_mode == "overwrite"
        (
            spark.createDataFrame(df_pd)
            .write.format("delta")
            .mode(write_mode)
            .option("overwriteSchema", str(overwrite_schema).lower())
            .saveAsTable(BRONZE_OPENAQ_MEASUREMENTS)
        )
        loc_rows += len(df_pd)
    print(f"[location {loc_id}] rows written: {loc_rows}")
    total_rows += loc_rows

print(f"[{BRONZE_OPENAQ_MEASUREMENTS}] total rows: {total_rows}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

spark.sql(f"SELECT parameter, COUNT(*) as cnt FROM {BRONZE_OPENAQ_MEASUREMENTS} GROUP BY parameter ORDER BY cnt DESC").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
