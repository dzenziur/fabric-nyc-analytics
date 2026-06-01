"""Environment-driven configuration. Loads .env in local dev; relies on env vars in containers."""
import os
from dotenv import load_dotenv

load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required env var {name} is not set")
    return value


FABRIC_SQL_SERVER       = os.getenv("FABRIC_SQL_SERVER", "")
FABRIC_SP_CLIENT_ID     = os.getenv("FABRIC_SP_CLIENT_ID", "")
FABRIC_SP_CLIENT_SECRET = os.getenv("FABRIC_SP_CLIENT_SECRET", "")
SILVER_LAKEHOUSE_DB     = os.getenv("SILVER_LAKEHOUSE_DB", "silver_lakehouse")
GOLD_WAREHOUSE_DB       = os.getenv("GOLD_WAREHOUSE_DB", "gold_warehouse")

INFLUXDB_URL    = os.getenv("INFLUXDB_URL", "")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG", "")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "weather_nyc")


DROPBOX_ACCESS_TOKEN = os.getenv("DROPBOX_ACCESS_TOKEN", "")
DROPBOX_UPLOAD_DIR   = os.getenv("DROPBOX_UPLOAD_DIR", "/nyc-analytics")


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")


def _parse_chat_ids(raw: str) -> list[int]:
    """Comma-separated chat IDs. Skip invalid tokens with a warning."""
    result: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            result.append(int(token))
        except ValueError:
            print(f"[config] ignoring invalid TELEGRAM_ALLOWED_CHAT_IDS entry: {token!r}")
    return result


TELEGRAM_ALLOWED_CHAT_IDS = _parse_chat_ids(os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", ""))
