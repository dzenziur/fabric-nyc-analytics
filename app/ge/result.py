"""CheckResult dataclass + report formatter."""
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class CheckResult:
    table: str        # e.g. "silver_weather"
    name: str         # short human-readable expectation, e.g. "temperature_c in [-30, 50]"
    passed: bool
    observed: str     # short description, e.g. "47112/47112 OK" or "12 rows outside range"


def format_report(results: list[CheckResult]) -> str:
    """Group results by table, render a Telegram-friendly text report."""
    by_table: dict[str, list[CheckResult]] = {}
    for r in results:
        by_table.setdefault(r.table, []).append(r)

    total = len(results)
    failures = sum(1 for r in results if not r.passed)
    passed = total - failures

    header = (
        f"Data Quality Report - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"Tables: {len(by_table)} | Checks: {total} | Passed: {passed} | Failed: {failures}\n"
        f"{'-' * 60}"
    )

    lines = [header]
    for table in sorted(by_table.keys()):
        checks = by_table[table]
        n_pass = sum(1 for c in checks if c.passed)
        status = "OK " if n_pass == len(checks) else "FAIL"
        lines.append(f"[{status}] {table:30s} {n_pass}/{len(checks)} checks")
        for c in checks:
            if c.passed:
                continue
            lines.append(f"        - {c.name}: {c.observed}")

    return "\n".join(lines)
