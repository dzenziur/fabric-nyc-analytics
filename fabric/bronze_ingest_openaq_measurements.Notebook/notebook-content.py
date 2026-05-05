# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c9741aec-56ed-41b9-9025-c45ee072256d",
# META       "default_lakehouse_name": "bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "d5f75821-ae8f-4a0a-b235-74982716aa0b",
# META       "known_lakehouses": [
# META         {
# META           "id": "c9741aec-56ed-41b9-9025-c45ee072256d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze — OpenAQ Measurements Ingestion
# Reads pollutant measurement history for NYC stations from the OpenAQ public S3 archive.
# **Input:** `bronze_openaq_locations` (NYC stations filtered by bounding box) + S3 archive
# **Output:** `bronze_openaq_measurements` (raw CSV data, no transformations)

# CELL ********************

from datetime import datetime

BRONZE = "bronze_lakehouse"

# NYC bounding box
NYC_LAT_MIN, NYC_LAT_MAX = 40.4, 40.9
NYC_LON_MIN, NYC_LON_MAX = -74.3, -73.7

# Year range — configurable
YEAR_END   = datetime.now().year - 1   # last fully completed year
YEAR_START = YEAR_END - 4              # 5 years back

# S3 source
OPENAQ_S3_BASE = "s3a://openaq-data-archive/records/csv.gz"

# Tables
BRONZE_OPENAQ_LOCATIONS    = f"{BRONZE}.bronze_openaq_locations"
BRONZE_OPENAQ_MEASUREMENTS = f"{BRONZE}.bronze_openaq_measurements"

print(f"Year range: {YEAR_START} – {YEAR_END}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## S3 Configuration
# Anonymous access — `openaq-data-archive` is a public AWS Open Data Registry bucket.

# CELL ********************

spark.conf.set(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
)
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## NYC Stations
# Read location IDs from `bronze_openaq_locations`, filter by NYC bounding box.

# CELL ********************

df_locations = spark.read.table(BRONZE_OPENAQ_LOCATIONS)

nyc_ids = (
    df_locations
    .filter(
        (col("latitude")  >= NYC_LAT_MIN) & (col("latitude")  <= NYC_LAT_MAX) &
        (col("longitude") >= NYC_LON_MIN) & (col("longitude") <= NYC_LON_MAX)
    )
    .select("location_id")
    .rdd.flatMap(lambda x: x)
    .collect()
)

print(f"NYC stations found: {len(nyc_ids)}")
print(f"IDs: {sorted(nyc_ids)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read Measurements from S3
# Build paths for each station × year, read all CSV.gz files in one pass.

# CELL ********************

paths = [
    f"{OPENAQ_S3_BASE}/locationid={loc_id}/year={year}/"
    for loc_id in nyc_ids
    for year in range(YEAR_START, YEAR_END + 1)
]

print(f"S3 paths to read: {len(paths)}")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("recursiveFileLookup", "true")
    .csv(paths)
)

print(f"[bronze_openaq_measurements] rows read: {df.count()}")
display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Bronze
# Raw data, no transformations. Overwrite for idempotent runs.

# CELL ********************

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_OPENAQ_MEASUREMENTS)
print(f"[{BRONZE_OPENAQ_MEASUREMENTS}] write done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification

# CELL ********************

spark.sql(f"SELECT parameter, COUNT(*) as cnt FROM {BRONZE_OPENAQ_MEASUREMENTS} GROUP BY parameter ORDER BY cnt DESC").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
