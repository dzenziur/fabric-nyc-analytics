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
# Pre-flight notebook that decides which TLC taxi monthly Parquet files to actually download in this run. Does two jobs: probe TLC CloudFront with HTTP HEAD per `(year, month)` to find which months are published, then list existing Bronze files and exclude already-downloaded months. The final list of `{year, month}` pairs is returned as a JSON string via `notebookutils.notebook.exit`, which `pl_master_orchestrator`'s ForEach reads as its `items` source.
# ### Input
# - TLC CloudFront — HEAD requests against each month's URL in `year_start..year_end`.<p>
# - `Files/raw/taxi/` — list of files already downloaded to Bronze.
# ### Output
# - Notebook exit value — JSON array, e.g. `[{"year": 2024, "month": 3}, {"year": 2024, "month": 4}]`.
# ### Parameters
# - `year_start` (int) — lower bound; **always used** to scope the HEAD probe and existing-file diff.<p>
# - `year_end` (int) — upper bound; **always used** (update each January for the new calendar year).<p>
# - `force_refresh` (bool) — controls the "already downloaded" filter; details in the **Ingestion Plan** section below.


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
# - `urllib.request` + `urllib.error.HTTPError` — HTTP HEAD probes. TLC's `/trip-data/` accepts a Chrome UA, unlike the stricter `/misc/` path that needs the full `requests` setup from `bronze_ingest_taxi_zones`.<p>
# - `json` — serializing the exit value.

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
# - `FILENAME_TEMPLATE` / `URL_TEMPLATE` — TLC's monthly Parquet naming convention. Files at `yellow_tripdata_YYYY-MM.parquet` under the CloudFront `/trip-data/` path.<p>
# - `REQUEST_TIMEOUT = 15` — per-HEAD timeout, kept short because we're hitting it 60+ times.<p>
# - `TAXI_FILES_PATH` — Bronze landing folder for downloaded Parquets.<p>
# - `BROWSER_UA` — full Chrome UA string. TLC CloudFront rejects the default `Python-urllib/*` UA with HTTP 403, and a short `Mozilla/5.0` is sometimes still blocked, so we send a complete realistic UA. The `/trip-data/` path is permissive enough that this UA alone is sufficient — unlike the stricter `/misc/` path in `bronze_ingest_taxi_zones` which needs full browser headers.

# CELL ********************

FILENAME_TEMPLATE = "yellow_tripdata_{:04d}-{:02d}.parquet"
URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/" + FILENAME_TEMPLATE
REQUEST_TIMEOUT = 15
TAXI_FILES_PATH = "Files/raw/taxi/"
BROWSER_UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

print(f"Year range: {year_start} - {year_end}, force_refresh: {force_refresh}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pre-flight Check
# Two parts:<p>
# 1. **Reachability probe** — HEAD against a known-published reference month (`2024-01`) that must return 200. CloudFront returns HTTP **403** (not 404) for missing keys, so a per-month 403 is ambiguous: it could mean "file not yet published" OR "anti-bot block". The probe disambiguates — if the known-good URL also 403s, we abort loudly (anti-bot, update UA); otherwise per-month 403/404 is silently treated as "month not yet available".<p>
# 2. **Per-month probe loop** — HEAD against every `(year, month)` in the requested range. Months that return 200 are added to `months_available_at_source`. 403/404 are skipped (publishing lag — TLC lags ~2 months). The notebook fails loudly only if **no** months in the range come back available — that means the year range is wrong (e.g. 2027-2028 in 2026).

# CELL ********************

REFERENCE_URL = URL_TEMPLATE.format(2024, 1)
try:
    urllib.request.urlopen(
        urllib.request.Request(REFERENCE_URL, method="HEAD", headers=BROWSER_UA),
        timeout=REQUEST_TIMEOUT,
    )
    print(f"TLC reachability OK (reference {REFERENCE_URL})")
except HTTPError as e:
    raise RuntimeError(
        f"TLC CloudFront unreachable for reference URL {REFERENCE_URL} (HTTP {e.code}). "
        f"Likely anti-bot block — update BROWSER_UA or check connectivity."
    ) from e

months_available_at_source = []

for y in range(year_start, year_end + 1):
    for m in range(1, 13):
        url = URL_TEMPLATE.format(y, m)
        req = urllib.request.Request(url, method="HEAD", headers=BROWSER_UA)
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

# ## Ingestion Plan
# Combine the "available on TLC" list with what's already in Bronze.
# - **When `force_refresh=False`** (default) — list `Files/raw/taxi/` for existing `.parquet` filenames and keep only months whose filename is NOT in the existing set. Default incremental behavior — avoids re-downloading the 60+ files we already have on every run.<p>
# - **When `force_refresh=True`** — `existing_files = set()` (treat Bronze as empty); re-download every month that's currently available on TLC. Used for manual backfill or recovery.<p>
# Result: `months_to_download` is the actual ForEach iteration list — months published AND not yet downloaded.

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
    filename = FILENAME_TEMPLATE.format(item["year"], item["month"])
    if filename not in existing_files:
        months_to_download.append(item)

print(f"Files to download: {len(months_to_download)} / {len(months_available_at_source)} available")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Output
# Return the list as a JSON-serialized string via `notebookutils.notebook.exit`. The orchestrator's ForEach reads it with `@json(activity('prepare_taxi_ingestion').output.result.exitValue)` and iterates one `(year, month)` per parallel branch.<p>
# **Example exit value:**
# ```json
# [
#   {"year": 2024, "month": 3},
#   {"year": 2024, "month": 4},
#   {"year": 2024, "month": 5}
# ]
# ```
# Empty list `[]` is also valid — means everything is already downloaded; ForEach iterates zero times.

# CELL ********************

notebookutils.notebook.exit(json.dumps(months_to_download))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
