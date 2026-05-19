"""Top-level DQ runner — opens connections, runs all suites, returns formatted report."""
from app import config
from app.fabric_client import get_connection
from app.ge.result import CheckResult, format_report
from app.ge.suites import GOLD_SUITES, SILVER_SUITES


def _run_layer(database: str, suites: list) -> list[CheckResult]:
    results: list[CheckResult] = []
    with get_connection(database) as conn:
        for suite in suites:
            try:
                results.extend(suite(conn))
            except Exception as exc:
                # Don't kill the whole report if a single suite blows up — surface it as a failure row.
                results.append(CheckResult(
                    table=suite.__name__,
                    name="suite execution",
                    passed=False,
                    observed=f"error: {type(exc).__name__}: {exc}",
                ))
    return results


def run_report() -> str:
    results: list[CheckResult] = []
    print("[ge] running Silver suites...")
    results.extend(_run_layer(config.SILVER_LAKEHOUSE_DB, SILVER_SUITES))
    print("[ge] running Gold suites...")
    results.extend(_run_layer(config.GOLD_WAREHOUSE_DB,   GOLD_SUITES))
    print("[ge] done")
    return format_report(results)
