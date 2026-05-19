"""CLI entry point for the external NYC Analytics app.

Usage:
    python -m app weather-sync   # Pull silver_weather from Fabric -> push to InfluxDB
    python -m app ge-report      # Run DQ checks on Silver + Gold, print report to stdout

Environment knobs:
    WEATHER_SYNC_INTERVAL_SECONDS   If set and > 0, weather-sync loops with this delay
                                    between runs. Default 0 (one-shot).
"""
import os
import sys
import time


def _weather_sync_loop() -> None:
    from app.weather_sync import run

    interval = int(os.getenv("WEATHER_SYNC_INTERVAL_SECONDS", "0"))
    if interval <= 0:
        run()
        return
    print(f"[weather_sync] scheduler enabled — interval {interval}s")
    while True:
        try:
            run()
        except Exception as exc:
            print(f"[weather_sync] error: {exc!r} — will retry next tick")
        time.sleep(interval)


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "weather-sync":
        _weather_sync_loop()
    elif cmd == "ge-report":
        from app.ge import run_report
        print(run_report())
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
