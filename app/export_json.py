"""Export a monthly slice of Gold data to JSON and upload it to Dropbox.

One-shot job for the Power Automate integration (external stack, part 3):
aggregate FactTaxiDaily over the last full month available in the data, write a
JSON document, and upload it to Dropbox via the HTTP content API. A Power
Automate cloud flow watches the Dropbox folder and fans the file out to a Gmail
e-mail + a mobile push notification. Fabric stays the source of truth — this job
only reads from it.
"""
import calendar
import datetime as dt
import json
from decimal import Decimal

import requests

from app import config
from app.fabric_client import get_connection

DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload"
MAX_ROWS = 500
MIN_DAILY_TRIPS = 1000


def _json_default(value: object) -> object:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _anchor_date(conn) -> dt.date:
    """Latest day with real volume — skips sparse tail records (e.g. stray 1-trip days)."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT TOP (1) d.date
        FROM dbo.FactTaxiDaily f
        JOIN dbo.DimDate d ON f.date_key = d.date_key
        GROUP BY d.date
        HAVING SUM(f.trip_count) >= ?
        ORDER BY d.date DESC
        """,
        MIN_DAILY_TRIPS,
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("No day in FactTaxiDaily meets the volume threshold — nothing to export")
    return row[0]


def _fetch_rows(conn, period_start: dt.date, period_end: dt.date) -> list[dict]:
    """Top pickup zones aggregated over the whole period."""
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT TOP ({MAX_ROWS})
            z.zone_name                       AS zone,
            z.borough                         AS borough,
            SUM(CAST(f.trip_count AS BIGINT)) AS trips,
            SUM(f.total_fare_usd)             AS revenue_usd
        FROM dbo.FactTaxiDaily f
        JOIN dbo.DimDate d ON f.date_key = d.date_key
        JOIN dbo.DimZone z ON f.zone_key = z.zone_key
        WHERE d.date >= ? AND d.date <= ?
        GROUP BY z.zone_name, z.borough
        ORDER BY trips DESC
        """,
        period_start,
        period_end,
    )
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _fetch_summary(conn, period_start: dt.date, period_end: dt.date) -> dict:
    """Period-wide KPIs (over all rows, not just the exported top-N)."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            SUM(CAST(f.trip_count AS BIGINT)) AS total_trips,
            SUM(f.total_fare_usd)             AS total_revenue_usd,
            COUNT(DISTINCT f.zone_key)        AS zone_count
        FROM dbo.FactTaxiDaily f
        JOIN dbo.DimDate d ON f.date_key = d.date_key
        WHERE d.date >= ? AND d.date <= ?
        """,
        period_start,
        period_end,
    )
    row = cursor.fetchone()
    return {
        "total_trips": int(row[0] or 0),
        "total_revenue_usd": round(float(row[1] or 0.0), 2),
        "zone_count": int(row[2] or 0),
    }


def _upload_to_dropbox(payload: bytes, filename: str) -> str:
    if not config.DROPBOX_ACCESS_TOKEN:
        raise RuntimeError("DROPBOX_ACCESS_TOKEN is not set")
    path = f"{config.DROPBOX_UPLOAD_DIR}/{filename}"
    headers = {
        "Authorization": f"Bearer {config.DROPBOX_ACCESS_TOKEN}",
        "Dropbox-API-Arg": json.dumps(
            {"path": path, "mode": "add", "autorename": True, "mute": False}
        ),
        "Content-Type": "application/octet-stream",
    }
    resp = requests.post(DROPBOX_UPLOAD_URL, headers=headers, data=payload, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Dropbox upload failed ({resp.status_code}): {resp.text}")
    return resp.json()["path_display"]


def _last_full_month(anchor: dt.date) -> tuple[dt.date, dt.date]:
    """Resolve the anchor day to the last calendar month that is fully present."""
    month_end = anchor.replace(day=calendar.monthrange(anchor.year, anchor.month)[1])
    if anchor >= month_end:
        return anchor.replace(day=1), month_end
    prev_month_end = anchor.replace(day=1) - dt.timedelta(days=1)
    return prev_month_end.replace(day=1), prev_month_end


def run() -> None:
    conn = get_connection(config.GOLD_WAREHOUSE_DB)
    try:
        anchor = _anchor_date(conn)
        period_start, period_end = _last_full_month(anchor)
        rows = _fetch_rows(conn, period_start, period_end)
        summary = _fetch_summary(conn, period_start, period_end)
    finally:
        conn.close()

    generated_at = dt.datetime.now(dt.timezone.utc)
    document = {
        "dataset": "FactTaxiDaily",
        "generated_at": generated_at.isoformat(),
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "period_label": period_start.strftime("%B %Y"),
        "row_count": len(rows),
        "summary": summary,
        "rows": rows,
    }
    payload = json.dumps(document, default=_json_default, indent=2).encode("utf-8")
    print(
        f"[export_json] built {len(rows)} rows for {period_start}..{period_end} "
        f"({len(payload)} bytes)"
    )

    filename = f"nyc_taxi_export_{generated_at:%Y%m%d_%H%M%S}.json"
    path = _upload_to_dropbox(payload, filename)
    print(f"[export_json] uploaded to Dropbox: {path}")
