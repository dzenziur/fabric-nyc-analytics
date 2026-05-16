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

# # Bronze — TLC Taxi Files Pre-flight & Ingestion Plan
# Pre-flight check (HEAD on latest expected TLC file) + list missing files in bronze.
# Outputs JSON list of {year, month} pairs to download via ForEach in master orchestrator.
# **Input:** TLC CloudFront (HEAD) + `Files/raw/taxi/` (list existing)
# **Output:** notebook exitValue — JSON array of months to download

# PARAMETERS CELL ********************

year_start = 2023
year_end = 2023
force_refresh = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports

# CELL ********************

import json
import urllib.request
from urllib.error import HTTPError

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Config

# CELL ********************

URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{:04d}-{:02d}.parquet"
REQUEST_TIMEOUT = 15
TAXI_FILES_PATH = "Files/raw/taxi/"

print(f"Year range: {year_start} - {year_end}, force_refresh: {force_refresh}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pre-flight Check
# HEAD request on each month in the requested range. Months that return HTTP 403/404 (not yet published by TLC)
# are skipped — we proceed with whatever is available. Only fails if NO months in range are available.

# CELL ********************

months_available_at_source = []

for y in range(year_start, year_end + 1):
    for m in range(1, 13):
        url = URL_TEMPLATE.format(y, m)
        req = urllib.request.Request(url, method="HEAD")
        try:
            urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT)
            months_available_at_source.append({"year": y, "month": m})
        except HTTPError as e:
            if e.code in (403, 404):
                continue
            raise

total_possible = (year_end - year_start + 1) * 12
print(f"Months available on TLC: {len(months_available_at_source)} / {total_possible}")

if not months_available_at_source:
    raise ValueError(
        f"No taxi files available on TLC for year range {year_start}-{year_end}. "
        f"All months returned HTTP 403/404 (TLC has ~2-month publishing lag). "
        f"Adjust year_start/year_end to a range with completed data."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Plan Ingestion
# From available months, exclude ones already in bronze. Result is the list of files to actually download.

# CELL ********************

if force_refresh:
    existing_files = set()
    print("force_refresh=True — will download all available files")
else:
    try:
        files = notebookutils.fs.ls(TAXI_FILES_PATH)
        existing_files = {f.name for f in files if f.name.endswith(".parquet")}
        print(f"Existing files in bronze: {len(existing_files)}")
    except Exception:
        existing_files = set()
        print("No existing files (first run)")

months_to_download = []
for item in months_available_at_source:
    filename = f"yellow_tripdata_{item['year']:04d}-{item['month']:02d}.parquet"
    if filename not in existing_files:
        months_to_download.append(item)

print(f"Files to download: {len(months_to_download)} / {len(months_available_at_source)} available")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Exit
# Pass list of months to ForEach in pl_master_orchestrator via notebook exit value.

# CELL ********************

notebookutils.notebook.exit(json.dumps(months_to_download))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
