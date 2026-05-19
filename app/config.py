"""Environment-driven configuration. Loads .env in local dev; relies on env vars in containers."""
import os
from dotenv import load_dotenv

load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required env var {name} is not set")
    return value


# Fabric SQL endpoint (shared workspace server, one DB per Lakehouse/Warehouse)
FABRIC_SQL_SERVER       = os.getenv("FABRIC_SQL_SERVER", "")
FABRIC_SP_CLIENT_ID     = os.getenv("FABRIC_SP_CLIENT_ID", "")
FABRIC_SP_CLIENT_SECRET = os.getenv("FABRIC_SP_CLIENT_SECRET", "")
SILVER_LAKEHOUSE_DB     = os.getenv("SILVER_LAKEHOUSE_DB", "silver_lakehouse")
GOLD_WAREHOUSE_DB       = os.getenv("GOLD_WAREHOUSE_DB", "gold_warehouse")

# InfluxDB
INFLUXDB_URL    = os.getenv("INFLUXDB_URL", "")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG", "")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "weather_nyc")
