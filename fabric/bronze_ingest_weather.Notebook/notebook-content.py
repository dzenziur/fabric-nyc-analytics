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

# # Bronze — Open-Meteo Weather Ingestion
# Fetches hourly weather observations for NYC (single point — Manhattan) from Open-Meteo and writes raw rows to `bronze_lakehouse`. Open-Meteo is free with no API key and no rate limit for the volumes we need (one point × hourly resolution).
# ### Input
# - Open-Meteo Archive API (`archive-api.open-meteo.com`) — historical, lags ~5 days.<p>
# - Open-Meteo Forecast API (`api.open-meteo.com/v1/forecast`) — recent days.
# ### Output
# - `bronze_weather` — raw hourly observations, partitioned by year.
# ### Parameters
# - `year_start` (int) — lower bound; used only in full-rebuild mode.<p>
# - `year_end` (int) — upper bound; used only in full-rebuild mode.<p>
# - `force_refresh` (bool) — selects which endpoint to hit and how to write; details in the **Weather** section below.

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
# - `requests` + `urllib3.util.retry.Retry` mounted on an `HTTPAdapter` — retry-on-transient-error behaviour.<p>
# - `pandas` — flatten Open-Meteo's parallel hourly arrays into a row-per-hour DataFrame.<p>
# - `DeltaTable` — MERGE in incremental mode.

# CELL ********************

import requests
import pandas as pd
from datetime import date, timedelta
from delta.tables import DeltaTable
from pyspark.sql.functions import col, year, to_timestamp, current_timestamp
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Config
# - `NYC_LAT` / `NYC_LON` — Manhattan coordinates (40.7128, -74.0060). Single point coverage is enough for city-wide context with taxi data; a finer grid would not change conclusions on this project.<p>
# - `HOURLY_PARAMS` — six hourly variables we request from Open-Meteo. The API returns each as a parallel array; the `fetch_hourly` helper zips them into a row-per-hour DataFrame.<p>
# - `ARCHIVE_URL` vs `FORECAST_URL` — two different Open-Meteo endpoints. Archive covers history but lags ~5 days; Forecast covers recent days. Mode logic below picks the right one.<p>
# - `INCREMENTAL_PAST_DAYS = 2` — how many days the Forecast endpoint reaches back in incremental mode. Two days of overlap means a single missed scheduled run doesn't create a gap; three would be safer but more redundant.

# CELL ********************

BRONZE = "bronze_lakehouse"
BRONZE_WEATHER = f"{BRONZE}.bronze_weather"

NYC_LAT = 40.7128
NYC_LON = -74.0060

HOURLY_PARAMS = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "wind_speed_10m",
    "relative_humidity_2m",
    "weather_code",
]

ARCHIVE_URL  = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

INCREMENTAL_PAST_DAYS = 2
REQUEST_TIMEOUT       = 60

print(f"Year range: {year_start} - {year_end}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper
# - `session` — HTTPS session with retry-on-transient-error mounted: up to 3 retries with exponential backoff (factor 2) on 429 / 5xx. Saves us from writing manual `try/except` around every GET.<p>
# - `fetch_hourly(url, params)` — calls an Open-Meteo endpoint, then flattens the response's parallel hourly arrays into a row-per-hour pandas DataFrame. Open-Meteo returns `{"hourly": {"time": [...], "temperature_2m": [...], ...}}` — we zip the arrays column-wise. Returns an empty DataFrame if no `time` array is present (rare but defensive).

# CELL ********************

session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)))


def fetch_hourly(url: str, params: dict) -> pd.DataFrame:
    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        return pd.DataFrame()
    rows = {"datetime": times}
    for p in HOURLY_PARAMS:
        rows[p] = hourly.get(p) or [None] * len(times)
    df = pd.DataFrame(rows)
    df["latitude"]  = payload.get("latitude", NYC_LAT)
    df["longitude"] = payload.get("longitude", NYC_LON)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Weather
# Mode logic decides which endpoint to hit and how to write.
# ### Mode scenarios
# - **`force_refresh=True`** — Archive endpoint for the full `year_start..year_end` range. Clamps `end_date` to `today - 2 days` if the requested end is in the future (Archive lags ~5 days). Partition overwrite via `replaceWhere` on `year`.<p>
# - **`force_refresh=False` + Bronze has data** — Forecast endpoint with `past_days=2` (the last 48 hours). MERGE on `(latitude, longitude, datetime)` with **both** `whenMatchedUpdateAll` AND `whenNotMatchedInsertAll`. The update half is critical — Open-Meteo retroactively refines recent hours (a 13:00 temperature published at 14:00 can be revised at 16:00 and finalized at 18:00), so matched rows must be **updated** with newer values, not skipped.<p>
# - **`force_refresh=False` + first run** — falls back to Archive endpoint for the full year range.<p>
# `ingestion_timestamp` is added as an audit-trail column — when this row was written to Bronze.

# CELL ********************

try:
    bronze_exists = spark.catalog.tableExists(BRONZE_WEATHER)
except Exception:
    bronze_exists = False

if force_refresh or not bronze_exists:
    end_date = date(year_end, 12, 31)
    today    = date.today()
    if end_date > today:
        end_date = today - timedelta(days=2)
    params = {
        "latitude":   NYC_LAT,
        "longitude":  NYC_LON,
        "start_date": f"{year_start}-01-01",
        "end_date":   end_date.isoformat(),
        "hourly":     ",".join(HOURLY_PARAMS),
        "timezone":   "GMT",
    }
    print(f"Full mode — archive endpoint, {params['start_date']} → {params['end_date']}")
    df_pd = fetch_hourly(ARCHIVE_URL, params)
    use_incremental = False
else:
    params = {
        "latitude":      NYC_LAT,
        "longitude":     NYC_LON,
        "past_days":     INCREMENTAL_PAST_DAYS,
        "forecast_days": 0,
        "hourly":        ",".join(HOURLY_PARAMS),
        "timezone":      "GMT",
    }
    print(f"Incremental mode — forecast endpoint, past_days={INCREMENTAL_PAST_DAYS}")
    df_pd = fetch_hourly(FORECAST_URL, params)
    use_incremental = True

print(f"Rows fetched: {len(df_pd)}")

if df_pd.empty:
    if use_incremental:
        print(f"[{BRONZE_WEATHER}] no new data — skipping write")
    else:
        raise ValueError(f"No weather data returned from Open-Meteo for {year_start}–{year_end}. Aborting.")
else:
    df_new = (
        spark.createDataFrame(df_pd)
        .withColumn("year", year(to_timestamp(col("datetime"))))
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    if use_incremental:
        target = DeltaTable.forName(spark, BRONZE_WEATHER)
        (
            target.alias("t")
            .merge(
                df_new.alias("s"),
                "t.latitude = s.latitude AND t.longitude = s.longitude AND t.datetime = s.datetime"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[{BRONZE_WEATHER}] incremental merge done")
    else:
        print(f"[{BRONZE_WEATHER}] rows before write: {df_new.count()}")
        (
            df_new.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("year")
            .saveAsTable(BRONZE_WEATHER)
        )
        print(f"[{BRONZE_WEATHER}] write done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

spark.sql(f"SELECT year, COUNT(*) AS cnt FROM {BRONZE_WEATHER} GROUP BY year ORDER BY year").show()
display(spark.read.table(BRONZE_WEATHER).orderBy(col("datetime").desc()).limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
