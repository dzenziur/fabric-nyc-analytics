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

# # Bronze — TLC Taxi Zones Ingestion
# Downloads the TLC taxi zone lookup — ~265 rows of static reference data mapping `location_id` to zone name, borough, and service zone. Used downstream as `DimZone` in the Gold layer.
# ### Input
# - TLC CloudFront — `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`.
# ### Output
# - `bronze_taxi_zones` — Delta table, ~265 rows.
# ### Parameters
# - `force_refresh` (bool) — controls the skip-when-populated behavior; details in the **Up-to-Date Check** section below.

# PARAMETERS CELL ********************

force_refresh = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports
# - `requests` — HTTP GET (handles redirects + cookies better than `urllib` for TLC's strict `/misc/` path).<p>
# - `pyspark.sql.functions.col` — for the `location_id` int cast.

# CELL ********************

import requests
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Config
# - `ZONE_CSV_URL` — TLC's static CSV endpoint.<p>
# - `ZONE_CSV_TMP` — local temp path on the driver (Spark `read.csv` can't read directly from HTTP).<p>
# - `BROWSER_HEADERS` — full browser-like header set. TLC's `/misc/` rejects a bare User-Agent with HTTP 403; we send `User-Agent`, `Accept`, `Accept-Language`, and `Referer: nyc.gov` — that's what `nyc.gov` actually sends.

# CELL ********************

BRONZE = "bronze_lakehouse"
BRONZE_TAXI_ZONES = f"{BRONZE}.bronze_taxi_zones"

ZONE_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_CSV_TMP = "/tmp/taxi_zone_lookup.csv"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,application/octet-stream,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Up-to-Date Check
# - **When `force_refresh=False`** (default) — if the Bronze table already has rows, skip the download entirely via `notebookutils.notebook.exit`. Zone data is essentially static, so re-running daily would be wasteful.<p>
# - **When `force_refresh=True`** — bypass the skip; always re-download and overwrite.

# CELL ********************

if not force_refresh:
    try:
        existing_count = spark.read.table(BRONZE_TAXI_ZONES).count()
    except Exception:
        existing_count = 0
    if existing_count > 0:
        print(f"[{BRONZE_TAXI_ZONES}] table already populated ({existing_count} rows) — skipping download. Pass force_refresh=True to override.")
        notebookutils.notebook.exit("skipped: table populated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## TLC Taxi Zones
# Download the CSV to a temp file (Spark can't read HTTP directly), then read it with the header option, rename columns to snake_case, cast `location_id` to int, and write as a Delta overwrite. Cast is done here in Bronze rather than Silver because `DimZone` downstream expects an integer key and there's no point delaying.

# CELL ********************

resp = requests.get(ZONE_CSV_URL, headers=BROWSER_HEADERS, timeout=30)
resp.raise_for_status()
with open(ZONE_CSV_TMP, "wb") as fh:
    fh.write(resp.content)
print(f"Downloaded {len(resp.content):,} bytes from {ZONE_CSV_URL}")

df = (
    spark.read.option("header", True).csv(f"file://{ZONE_CSV_TMP}")
    .withColumnRenamed("LocationID", "location_id")
    .withColumnRenamed("Borough", "borough")
    .withColumnRenamed("Zone", "zone")
    .withColumn("location_id", col("location_id").cast("int"))
)

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_TAXI_ZONES)
print(f"[{BRONZE_TAXI_ZONES}] write done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

display(spark.read.table(BRONZE_TAXI_ZONES).limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
