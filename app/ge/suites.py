"""Per-table expectation suites. SQL-aggregate for large tables; GE PandasDataset for small."""
from app import config
from app.ge.checks import (
    _load_df,
    ge_validate_df,
    sql_fk_integrity,
    sql_in_set,
    sql_not_null,
    sql_range,
    sql_row_count_min,
)
from app.ge.result import CheckResult

SILVER = config.SILVER_LAKEHOUSE_DB
GOLD   = config.GOLD_WAREHOUSE_DB


# ---------------- Silver Layer ----------------


def silver_taxi_trips(conn) -> list[CheckResult]:
    # pickup_datetime / dropoff_datetime are cast to TIMESTAMP in silver_etl so they
    # are visible to the Lakehouse SQL endpoint and can be checked here.
    t  = "silver_taxi_trips"
    fq = f"{SILVER}.dbo.{t}"
    return [
        sql_not_null(conn, t, fq, "pickup_datetime"),
        sql_not_null(conn, t, fq, "pu_location_id"),
        sql_not_null(conn, t, fq, "do_location_id"),
        sql_not_null(conn, t, fq, "fare_amount"),
        sql_range  (conn, t, fq, "trip_distance",  min_value=0.01, max_value=100),
        sql_range  (conn, t, fq, "fare_amount",    min_value=0.01, max_value=10_000),
        sql_in_set (conn, t, fq, "payment_type",   [0, 1, 2, 3, 4, 5, 6]),
        sql_row_count_min(conn, t, fq, 100_000_000),
    ]


def silver_openaq_measurements(conn) -> list[CheckResult]:
    # Units vary by parameter (µg/m³ for PM family, ppm normalised to µg/m³ for gases),
    # so we can't impose a global upper bound on `value` — silver_etl already enforces
    # value > 0 and restricts `parameter` to the pollutant set, which is the meaningful
    # invariant.
    t  = "silver_openaq_measurements"
    fq = f"{SILVER}.dbo.{t}"
    return [
        sql_not_null(conn, t, fq, "datetime"),
        sql_not_null(conn, t, fq, "parameter"),
        sql_not_null(conn, t, fq, "value"),
        sql_range  (conn, t, fq, "value", min_value=0),
        sql_in_set (conn, t, fq, "parameter",
                    ["pm25", "pm10", "pm1", "no2", "o3", "co", "so2", "no", "nox"]),
        sql_row_count_min(conn, t, fq, 100_000),
    ]


def silver_openaq_locations(conn) -> list[CheckResult]:
    import great_expectations as gx
    t  = "silver_openaq_locations"
    df = _load_df(conn, f"SELECT location_id, country_id, latitude, longitude FROM {SILVER}.dbo.{t}")
    return ge_validate_df(t, df, [
        ("location_id not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="location_id")),
        ("country_id not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="country_id")),
        ("latitude in [-90, 90]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="latitude",  min_value=-90,  max_value=90)),
        ("longitude in [-180, 180]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180, max_value=180)),
        ("location_id unique",
         gx.expectations.ExpectColumnValuesToBeUnique(column="location_id")),
    ])


def silver_gdp(conn) -> list[CheckResult]:
    import great_expectations as gx
    t  = "silver_gdp"
    df = _load_df(conn, f"SELECT country_code, year, gdp_usd FROM {SILVER}.dbo.{t}")
    return ge_validate_df(t, df, [
        ("country_code not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="country_code")),
        ("gdp_usd > 0",
         gx.expectations.ExpectColumnValuesToBeBetween(column="gdp_usd", min_value=0, strict_min=True)),
        ("year in [1960, 2030]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="year", min_value=1960, max_value=2030)),
    ])


def silver_fx_rates(conn) -> list[CheckResult]:
    import great_expectations as gx
    t  = "silver_fx_rates"
    df = _load_df(conn, f"SELECT date, usd_eur_rate FROM {SILVER}.dbo.{t}")
    return ge_validate_df(t, df, [
        ("date not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="date")),
        ("date unique",
         gx.expectations.ExpectColumnValuesToBeUnique(column="date")),
        ("usd_eur_rate in (0, 5]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="usd_eur_rate", min_value=0, max_value=5, strict_min=True)),
    ])


def silver_weather(conn) -> list[CheckResult]:
    import great_expectations as gx
    t  = "silver_weather"
    df = _load_df(conn, f"SELECT datetime, temperature_c, precipitation_mm, wind_speed_kmh, humidity_pct, weather_code FROM {SILVER}.dbo.{t}")
    return ge_validate_df(t, df, [
        ("datetime not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="datetime")),
        ("temperature_c in [-30, 50]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="temperature_c",    min_value=-30, max_value=50)),
        ("precipitation_mm in [0, 200]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="precipitation_mm", min_value=0,   max_value=200)),
        ("wind_speed_kmh in [0, 200]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="wind_speed_kmh",   min_value=0,   max_value=200)),
        ("humidity_pct in [0, 100]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="humidity_pct",     min_value=0,   max_value=100)),
        ("weather_code in [0, 99]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="weather_code",     min_value=0,   max_value=99)),
    ])


SILVER_SUITES = [
    silver_taxi_trips,
    silver_openaq_measurements,
    silver_openaq_locations,
    silver_gdp,
    silver_fx_rates,
    silver_weather,
]


# ---------------- Gold Layer ----------------


def dim_date(conn) -> list[CheckResult]:
    import great_expectations as gx
    t  = "DimDate"
    df = _load_df(conn, f"SELECT date_key, date, year, month, day_of_week FROM {GOLD}.dbo.{t}")
    return ge_validate_df(t, df, [
        ("date_key unique",
         gx.expectations.ExpectColumnValuesToBeUnique(column="date_key")),
        ("date not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="date")),
        ("month in [1, 12]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="month",       min_value=1, max_value=12)),
        ("day_of_week in [1, 7]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="day_of_week", min_value=1, max_value=7)),
    ])


def dim_zone(conn) -> list[CheckResult]:
    import great_expectations as gx
    t  = "DimZone"
    df = _load_df(conn, f"SELECT zone_key, location_id, borough, service_zone FROM {GOLD}.dbo.{t}")
    return ge_validate_df(t, df, [
        ("zone_key unique",
         gx.expectations.ExpectColumnValuesToBeUnique(column="zone_key")),
        ("location_id not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="location_id")),
        ("borough in expected set",
         gx.expectations.ExpectColumnValuesToBeInSet(column="borough",
              value_set=["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR", "Unknown", "N/A"])),
        ("service_zone in expected set",
         gx.expectations.ExpectColumnValuesToBeInSet(column="service_zone",
              value_set=["Yellow Zone", "Boro Zone", "Airports", "EWR", "N/A"])),
    ])


def dim_fx(conn) -> list[CheckResult]:
    import great_expectations as gx
    t  = "DimFX"
    df = _load_df(conn, f"SELECT fx_key, date, usd_eur_rate FROM {GOLD}.dbo.{t}")
    return ge_validate_df(t, df, [
        ("fx_key unique",
         gx.expectations.ExpectColumnValuesToBeUnique(column="fx_key")),
        ("date unique",
         gx.expectations.ExpectColumnValuesToBeUnique(column="date")),
        ("usd_eur_rate in (0, 5]",
         gx.expectations.ExpectColumnValuesToBeBetween(column="usd_eur_rate", min_value=0, max_value=5, strict_min=True)),
    ])


def dim_gdp(conn) -> list[CheckResult]:
    import great_expectations as gx
    t  = "DimGDP"
    df = _load_df(conn, f"SELECT gdp_key, country_code, year, gdp_usd FROM {GOLD}.dbo.{t}")
    return ge_validate_df(t, df, [
        ("gdp_key unique",
         gx.expectations.ExpectColumnValuesToBeUnique(column="gdp_key")),
        ("gdp_usd > 0",
         gx.expectations.ExpectColumnValuesToBeBetween(column="gdp_usd", min_value=0, strict_min=True)),
    ])


def fact_taxi_daily(conn) -> list[CheckResult]:
    t   = "FactTaxiDaily"
    fq  = f"{GOLD}.dbo.{t}"
    return [
        sql_not_null     (conn, t, fq, "date_key"),
        sql_not_null     (conn, t, fq, "zone_key"),
        sql_range        (conn, t, fq, "trip_count",     min_value=0),
        sql_range        (conn, t, fq, "total_fare_usd", min_value=0),
        sql_fk_integrity (conn, t, fq, "date_key", f"{GOLD}.dbo.DimDate", "date_key"),
        sql_fk_integrity (conn, t, fq, "zone_key", f"{GOLD}.dbo.DimZone", "zone_key"),
        sql_row_count_min(conn, t, fq, 100_000),
    ]


def fact_air_quality_daily(conn) -> list[CheckResult]:
    # Same unit-heterogeneity caveat as silver_openaq_measurements — avg_value
    # is meaningful per parameter, not globally bounded.
    t  = "FactAirQualityDaily"
    fq = f"{GOLD}.dbo.{t}"
    return [
        sql_not_null     (conn, t, fq, "date_key"),
        sql_not_null     (conn, t, fq, "location_id"),
        sql_not_null     (conn, t, fq, "parameter"),
        sql_range        (conn, t, fq, "avg_value", min_value=0),
        sql_fk_integrity (conn, t, fq, "date_key", f"{GOLD}.dbo.DimDate", "date_key"),
        sql_row_count_min(conn, t, fq, 1_000),
    ]


GOLD_SUITES = [
    dim_date,
    dim_zone,
    dim_fx,
    dim_gdp,
    fact_taxi_daily,
    fact_air_quality_daily,
]
