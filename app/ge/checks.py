"""Reusable check helpers — SQL-aggregate style and pandas+GE style."""
from typing import Any, Sequence

import pandas as pd

from app.ge.result import CheckResult


# ---------------- SQL-aggregate checks (for large tables) ----------------


def _scalar(conn, sql: str, params: tuple = ()) -> Any:
    cur = conn.cursor()
    cur.execute(sql, params) if params else cur.execute(sql)
    return cur.fetchone()[0]


def sql_not_null(conn, table: str, table_fq: str, column: str) -> CheckResult:
    n = _scalar(conn, f"SELECT COUNT_BIG(*) FROM {table_fq} WHERE {column} IS NULL")
    return CheckResult(
        table=table,
        name=f"{column} not null",
        passed=(n == 0),
        observed=f"{n:,} nulls",
    )


def sql_range(conn, table: str, table_fq: str, column: str,
              min_value: float | None = None,
              max_value: float | None = None) -> CheckResult:
    parts = []
    params: list = []
    if min_value is not None:
        parts.append(f"{column} < ?")
        params.append(min_value)
    if max_value is not None:
        parts.append(f"{column} > ?")
        params.append(max_value)
    where = " OR ".join(parts) if parts else "1 = 0"
    n = _scalar(conn, f"SELECT COUNT_BIG(*) FROM {table_fq} WHERE {where}", tuple(params))
    rng = f"[{min_value}, {max_value}]"
    return CheckResult(
        table=table,
        name=f"{column} in {rng}",
        passed=(n == 0),
        observed=f"{n:,} rows outside range",
    )


def sql_in_set(conn, table: str, table_fq: str, column: str, allowed: Sequence[Any]) -> CheckResult:
    placeholders = ", ".join("?" * len(allowed))
    n = _scalar(
        conn,
        f"SELECT COUNT_BIG(*) FROM {table_fq} "
        f"WHERE {column} IS NOT NULL AND {column} NOT IN ({placeholders})",
        tuple(allowed),
    )
    return CheckResult(
        table=table,
        name=f"{column} in set ({len(allowed)} values)",
        passed=(n == 0),
        observed=f"{n:,} rows with disallowed values",
    )


def sql_row_count_min(conn, table: str, table_fq: str, min_count: int) -> CheckResult:
    n = _scalar(conn, f"SELECT COUNT_BIG(*) FROM {table_fq}")
    return CheckResult(
        table=table,
        name=f"row count >= {min_count:,}",
        passed=(n >= min_count),
        observed=f"actual: {n:,}",
    )


def sql_fk_integrity(conn, table: str, child_fq: str, child_col: str,
                     parent_fq: str, parent_col: str) -> CheckResult:
    """Count child rows whose FK has no match in parent."""
    n = _scalar(
        conn,
        f"SELECT COUNT_BIG(*) FROM {child_fq} c "
        f"LEFT JOIN {parent_fq} p ON c.{child_col} = p.{parent_col} "
        f"WHERE c.{child_col} IS NOT NULL AND p.{parent_col} IS NULL",
    )
    return CheckResult(
        table=table,
        name=f"FK {child_col} -> {parent_fq.split('.')[-1]}.{parent_col}",
        passed=(n == 0),
        observed=f"{n:,} orphans",
    )


# ---------------- Pandas + Great Expectations (for small tables) ----------------


def _load_df(conn, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, conn)


def ge_validate_df(table: str, df: pd.DataFrame, expectations: list) -> list[CheckResult]:
    """Run a list of GE expectations against a pandas DataFrame.

    `expectations` is a list of (display_name, expectation_instance) tuples.
    Uses an ephemeral GE context — no filesystem stores or checkpoints.
    """
    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(name=f"pd_{table}")
    data_asset = data_source.add_dataframe_asset(name=table)
    batch_def = data_asset.add_batch_definition_whole_dataframe(name=f"batch_{table}")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    results: list[CheckResult] = []
    for display_name, expectation in expectations:
        validation = batch.validate(expectation)
        success = bool(validation.success)
        result_info = validation.result or {}
        observed = _summarise_ge_result(result_info, success)
        results.append(CheckResult(table=table, name=display_name, passed=success, observed=observed))
    return results


def _summarise_ge_result(result: dict, success: bool) -> str:
    """Compact one-line summary from a GE result dict."""
    if success:
        n = result.get("element_count")
        return f"{n:,} rows OK" if n is not None else "OK"
    n_bad = result.get("unexpected_count")
    n_total = result.get("element_count")
    if n_bad is not None and n_total is not None:
        return f"{n_bad:,} / {n_total:,} rows failed"
    return "failed"


# Convenience re-exports — readers reach for these from suites.py
__all__ = [
    "_load_df",
    "sql_not_null",
    "sql_range",
    "sql_in_set",
    "sql_row_count_min",
    "sql_fk_integrity",
    "ge_validate_df",
]
