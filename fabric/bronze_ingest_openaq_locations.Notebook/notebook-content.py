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
# Fetches location metadata for every OpenAQ station worldwide (~24,500 records) and writes them to `bronze_lakehouse`. Includes activity-window columns (`datetime_first` / `datetime_last`) that downstream `bronze_ingest_openaq_measurements` uses to pre-filter inactive stations.
# ### Input
# - OpenAQ API v3 `/v3/locations` — paginated, requires API key in `X-API-Key` header.
# ### Output
# - `bronze_openaq_locations` — full station snapshot, written as Delta overwrite.
# ### Parameters
# - `openaq_api_key` (string) — SecureString from the orchestrator trigger.<p>
# - `force_refresh` (bool) — controls the up-to-date check; details in the **Fetch** section below.

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
# - `requests` + `urllib3.util.retry.Retry` mounted on `HTTPAdapter` — transient-error retries with exponential backoff.<p>
# - `pandas` — accumulating per-page results before one bulk `spark.createDataFrame`.

# CELL ********************

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Config
# - `BASE_URL` — OpenAQ v3 paginated locations endpoint.<p>
# - `PAGE_LIMIT = 1000` — number of station records per HTTP request (OpenAQ max).<p>
# - `PAGE_CAP = 500` — hard safety cap on number of pages (500 × 1000 = 500k records max). If the loop hits this with a full last page, a WARNING is logged about possible data truncation.<p>
# - `REQUEST_TIMEOUT = 30` — per-request timeout in seconds.

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

# ## OpenAQ Locations
# ### Fetch
# Two stages: an optional up-to-date check, then the paginated fetch loop.<p>
# **When `force_refresh=False`** (default) — do a one-row `limit=1` probe call first. The response's `meta.found` is OpenAQ's total record count across all pages. If it equals the existing row count in Bronze, exit early via `notebookutils.notebook.exit("skipped: up to date")`. A single GET decides the run — daily scheduled execution is essentially free when nothing changed.<p>
# **When `force_refresh=True`** — skip the probe; always proceed to the paginated fetch.<p>
# **Paginated fetch loop:** request pages 1..`PAGE_CAP` with `limit=PAGE_LIMIT=1000`. Each response's `results` array holds up to 1000 station dicts; accumulate them. Exit when a page returns empty OR a partial page (fewer than `PAGE_LIMIT` rows = last page). Defensive double-condition because different APIs end pagination differently. The HTTPS session retries 3× with exponential backoff on HTTP 429 / 5xx. If the loop hits `PAGE_CAP` with a full last page, a WARNING is logged.


# CELL ********************

session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)))

headers = {"X-API-Key": openaq_api_key}

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

# ### Flatten
# OpenAQ returns deeply nested JSON per station — `country.code`, `coordinates.latitude`, etc. Build a flat row-per-station dict, then convert to pandas once. Defensive `or {}` on each nested object handles edge cases where a record might be missing fields.

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

# ### Write
# Convert pandas → Spark once, then full Delta overwrite with `overwriteSchema=True` (lets new upstream columns from OpenAQ land without manual schema migration).

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
