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

# # Bronze — OpenAQ Locations Ingestion
# Fetches location metadata from OpenAQ API v3, writes to `bronze_lakehouse`.
# **Input:** OpenAQ API `/v3/locations` (paginated)
# **Output:** `bronze_openaq_locations` (includes `datetime_first`/`datetime_last` activity window per station)
# Default (force_refresh=False) does a lightweight `limit=1` probe of `meta.found` and
# skips the full paginated fetch when it matches the existing bronze row count.

# PARAMETERS CELL ********************

openaq_api_key = ""
force_refresh = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports

# CELL ********************

import requests
import pandas as pd
from pyspark.sql.functions import col
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Config

# CELL ********************

BRONZE = "bronze_lakehouse"
BRONZE_OPENAQ_LOCATIONS = f"{BRONZE}.bronze_openaq_locations"

BASE_URL = "https://api.openaq.org/v3/locations"
PAGE_LIMIT = 1000
PAGE_CAP = 500
REQUEST_TIMEOUT = 30

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fetch
# Paginated fetch with retries on transient errors (5xx, 429, network).
# Hard page cap prevents runaway loops; WARNING logged if cap is hit with a full last page.

# CELL ********************

session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)))

headers = {"X-API-Key": openaq_api_key}

# Lightweight up-to-date check: one limit=1 call returns meta.found.
# If bronze already has the same count and force_refresh is False, skip the full paginated fetch.
if not force_refresh:
    try:
        existing_count = spark.read.table(BRONZE_OPENAQ_LOCATIONS).count()
    except Exception:
        existing_count = 0
    if existing_count > 0:
        probe = session.get(BASE_URL, params={"limit": 1, "page": 1},
                            headers=headers, timeout=REQUEST_TIMEOUT)
        probe.raise_for_status()
        meta_found = probe.json().get("meta", {}).get("found")
        if meta_found == existing_count:
            print(f"[{BRONZE_OPENAQ_LOCATIONS}] up to date — {existing_count} rows match OpenAQ meta.found={meta_found}; skipping fetch")
            notebookutils.notebook.exit("skipped: up to date")
        else:
            print(f"[{BRONZE_OPENAQ_LOCATIONS}] stale — bronze has {existing_count} rows, OpenAQ has {meta_found}; refetching")

records = []
last_page_size = 0

for page in range(1, PAGE_CAP + 1):
    resp = session.get(BASE_URL, params={"limit": PAGE_LIMIT, "page": page},
                       headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        break
    records.extend(results)
    last_page_size = len(results)
    print(f"Page {page}: {len(results)} locations fetched")
    if len(results) < PAGE_LIMIT:
        break
else:
    if last_page_size == PAGE_LIMIT:
        print(f"WARNING: hit hard page cap of {PAGE_CAP} with a full last page — data may be truncated; increase PAGE_CAP")

print(f"Total locations fetched: {len(records)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Flatten

# CELL ********************

rows = []
for r in records:
    country  = r.get("country") or {}
    coords   = r.get("coordinates") or {}
    dt_first = r.get("datetimeFirst") or {}
    dt_last  = r.get("datetimeLast")  or {}
    rows.append({
        "location_id":    r.get("id"),
        "location_name":  r.get("name"),
        "timezone":       r.get("timezone"),
        "country_id":     country.get("code"),
        "country_name":   country.get("name"),
        "latitude":       coords.get("latitude"),
        "longitude":      coords.get("longitude"),
        "datetime_first": dt_first.get("utc"),
        "datetime_last":  dt_last.get("utc"),
    })

df_pd = pd.DataFrame(rows)
print(f"Rows to write: {len(df_pd)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write

# CELL ********************

df_spark = spark.createDataFrame(df_pd)
print(f"[{BRONZE_OPENAQ_LOCATIONS}] rows before write: {df_spark.count()}")

df_spark.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_OPENAQ_LOCATIONS)
print(f"[{BRONZE_OPENAQ_LOCATIONS}] write done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

display(spark.read.table(BRONZE_OPENAQ_LOCATIONS).limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
