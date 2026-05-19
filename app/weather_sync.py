"""Weather sync — pull silver_weather from Fabric, push to InfluxDB.

Watermark-based incremental:
  1. Query InfluxDB for the latest `_time` of measurement `weather` (last point).
  2. Read silver_weather where datetime > watermark, ordered ascending.
  3. Convert each row to an InfluxDB Point and write in a single batch.

The first run sees an empty InfluxDB → backfills the full silver_weather history
(~50k hourly rows for 2021–2026, single HTTP write batch, completes in seconds).
"""
from datetime import datetime, timezone

from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from app import config
from app.fabric_client import get_connection
from app.influx_client import get_client

MEASUREMENT  = "weather"
LOCATION_TAG = "nyc"

# Columns pulled from silver_weather, in select order. `datetime` is the
# time index; the rest become InfluxDB fields.
SILVER_COLUMNS = [
    "datetime",
    "temperature_c",
    "feels_like_c",
    "precipitation_mm",
    "wind_speed_kmh",
    "humidity_pct",
    "weather_code",
    "is_rainy",
]


def _get_watermark(influx) -> datetime | None:
    """Return the latest `_time` of the weather measurement, or None if empty."""
    query = f'''
from(bucket: "{config.INFLUXDB_BUCKET}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
  |> filter(fn: (r) => r._field == "temperature_c")
  |> last()
  |> keep(columns: ["_time"])
'''
    tables = influx.query_api().query(query, org=config.INFLUXDB_ORG)
    for table in tables:
        for record in table.records:
            return record.get_time()
    return None


def _row_to_point(row) -> Point:
    """Convert a pyodbc Row (matching SILVER_COLUMNS order) to an InfluxDB Point."""
    dt = row[0]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (
        Point(MEASUREMENT)
        .tag("location", LOCATION_TAG)
        .field("temperature_c",    float(row[1]) if row[1] is not None else None)
        .field("feels_like_c",     float(row[2]) if row[2] is not None else None)
        .field("precipitation_mm", float(row[3]) if row[3] is not None else None)
        .field("wind_speed_kmh",   float(row[4]) if row[4] is not None else None)
        .field("humidity_pct",     float(row[5]) if row[5] is not None else None)
        .field("weather_code",     int(row[6])   if row[6] is not None else None)
        .field("is_rainy",         bool(row[7])  if row[7] is not None else None)
        .time(dt, WritePrecision.S)
    )


def run() -> None:
    print(f"[weather_sync] starting at {datetime.now(timezone.utc).isoformat()}")

    with get_client() as influx:
        watermark = _get_watermark(influx)
        if watermark is None:
            print("[weather_sync] InfluxDB empty — full backfill")
            where = ""
            params: tuple = ()
        else:
            print(f"[weather_sync] watermark: {watermark.isoformat()}")
            where = "WHERE datetime > ?"
            params = (watermark,)

        sql = f"""
            SELECT {", ".join(SILVER_COLUMNS)}
            FROM   {config.SILVER_LAKEHOUSE_DB}.dbo.silver_weather
            {where}
            ORDER BY datetime ASC
        """

        with get_connection(config.SILVER_LAKEHOUSE_DB) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()

        print(f"[weather_sync] silver rows to sync: {len(rows)}")
        if not rows:
            print("[weather_sync] nothing to do")
            return

        points = [_row_to_point(r) for r in rows]
        with influx.write_api(write_options=SYNCHRONOUS) as write_api:
            write_api.write(
                bucket=config.INFLUXDB_BUCKET,
                org=config.INFLUXDB_ORG,
                record=points,
            )
        print(f"[weather_sync] wrote {len(points)} points to {config.INFLUXDB_BUCKET}")
