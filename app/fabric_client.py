"""Fabric SQL endpoint connection helper. Uses Entra ID Service Principal auth."""
import pyodbc

from app import config


def get_connection(database: str) -> pyodbc.Connection:
    """Open a pyodbc connection to the Fabric SQL endpoint for the given database.

    `database` is the Lakehouse SQL endpoint name (e.g. "silver_lakehouse") or
    the Warehouse name (e.g. "gold_warehouse"). All databases in a workspace
    share the same FABRIC_SQL_SERVER host.
    """
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{config.FABRIC_SQL_SERVER},1433;"
        f"Database={database};"
        "Authentication=ActiveDirectoryServicePrincipal;"
        f"UID={config.FABRIC_SP_CLIENT_ID};"
        f"PWD={config.FABRIC_SP_CLIENT_SECRET};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)
