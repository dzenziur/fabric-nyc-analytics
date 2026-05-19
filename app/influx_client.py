"""InfluxDB client factory."""
from influxdb_client import InfluxDBClient

from app import config


def get_client() -> InfluxDBClient:
    return InfluxDBClient(
        url=config.INFLUXDB_URL,
        token=config.INFLUXDB_TOKEN,
        org=config.INFLUXDB_ORG,
        timeout=30_000,
    )
