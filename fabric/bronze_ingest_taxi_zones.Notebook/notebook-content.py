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
# # Static CSV from TLC CloudFront (~265 rows, rarely changes).
# # **Input:** TLC `taxi_zone_lookup.csv`
# # **Output:** `bronze_taxi_zones`
# # Default (force_refresh=False) skips the download entirely when the table already
# # has rows — zones are essentially static. Pass force_refresh=True to force a re-fetch.

# PARAMETERS CELL ********************

force_refresh = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import urllib.request

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE = "bronze_lakehouse"
BRONZE_TAXI_ZONES = f"{BRONZE}.bronze_taxi_zones"

ZONE_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_CSV_TMP = "/tmp/taxi_zone_lookup.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Skip download when bronze already has rows and force_refresh is False.
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

# CELL ********************

# TLC CloudFront rejects the default `Python-urllib/*` User-Agent with HTTP 403.
# Send a full realistic Chrome UA — a bare `Mozilla/5.0` is sometimes still blocked.
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
req = urllib.request.Request(ZONE_CSV_URL, headers={"User-Agent": BROWSER_UA})
with urllib.request.urlopen(req) as resp, open(ZONE_CSV_TMP, "wb") as fh:
    fh.write(resp.read())

df = (
    spark.read.option("header", True).csv(f"file://{ZONE_CSV_TMP}")
    .withColumnRenamed("LocationID", "location_id")
    .withColumnRenamed("Borough", "borough")
    .withColumnRenamed("Zone", "zone")
    .withColumnRenamed("service_zone", "service_zone")
)

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_TAXI_ZONES)
print(f"[{BRONZE_TAXI_ZONES}] write done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.read.table(BRONZE_TAXI_ZONES).limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
