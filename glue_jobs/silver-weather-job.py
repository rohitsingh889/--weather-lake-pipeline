#these are created on the aws glue...
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, explode, arrays_zip, to_timestamp,
    to_date, input_file_name, regexp_extract,
    count, when
)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'process_date', 'bucket_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args['bucket_name']
process_date = args['process_date']
year, month, day = process_date.split("-")

raw_path = f"s3://{bucket}/bronze/weather/city=*/year={year}/month={month}/day={day}/"
silver_path = f"s3://{bucket}/silver/weather/"

print(f"Reading Raw JSON → {raw_path}")


# SAFE READ

try:
    raw_df = spark.read.json(raw_path)
except Exception:
    print(f"No raw data for {process_date}")
    job.commit()
    sys.exit(0)

if raw_df.rdd.isEmpty():
    print("Raw dataframe empty")
    job.commit()
    sys.exit(0)

# Extract city from file path
raw_df = raw_df.withColumn("_source_file", input_file_name())
raw_df = raw_df.withColumn(
    "city",
    regexp_extract(col("_source_file"), r"city=([^/]+)", 1)
)

# Flatten hourly arrays
hourly_df = raw_df.select(
    col("city"),
    explode(arrays_zip(
        col("hourly.time"),
        col("hourly.temperature_2m"),
        col("hourly.precipitation"),
        col("hourly.windspeed_10m")
    )).alias("row")
)

silver_df = hourly_df.select(
    col("city"),
    to_timestamp(col("row.time")).alias("timestamp"),
    col("row.temperature_2m").cast("double").alias("temperature_2m"),
    col("row.precipitation").cast("double").alias("precipitation"),
    col("row.windspeed_10m").cast("double").alias("windspeed_10m")
)

silver_df = silver_df.filter(col("timestamp").isNotNull())

silver_df = silver_df.withColumn("date", to_date(col("timestamp")))

silver_df = silver_df.dropDuplicates(["city", "timestamp"])


#  DATA QUALITY CHECKS

print("Running Data Quality Checks…")

# Null Checks
null_counts = silver_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in ["timestamp", "temperature_2m", "windspeed_10m"]
])

null_counts.show()

#  Range Checks (Realistic weather limits)
invalid_temp = silver_df.filter(
    (col("temperature_2m") < -80) | (col("temperature_2m") > 80)
).count()

invalid_wind = silver_df.filter(
    (col("windspeed_10m") < 0) | (col("windspeed_10m") > 150)
).count()

print(f"Invalid temperature rows → {invalid_temp}")
print(f"Invalid windspeed rows → {invalid_wind}")

if invalid_temp > 0 or invalid_wind > 0:
    raise Exception("Data Quality Check Failed: Invalid metric values detected")

#  Duplicate Check
dup_count = silver_df.groupBy("city", "timestamp").count() \
    .filter(col("count") > 1).count()

print(f"Duplicate rows → {dup_count}")

if dup_count > 0:
    raise Exception("Data Quality Check Failed: Duplicate records detected")

print("Data Quality Checks Passed ✔")


# INCREMENTAL WRITE (Partition Overwrite)

print(f"Overwriting Silver partition → {process_date}")

(
    silver_df.write
    .mode("overwrite")
    .option("replaceWhere", f"date = '{process_date}'")
    .partitionBy("date")
    .parquet(silver_path)
)

print("Silver write complete")

job.commit()