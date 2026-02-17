#these are created on the aws glue...

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import avg, max, sum, to_date, col

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'process_date', 'bucket_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args['bucket_name']
process_date = args['process_date']

silver_path = f"s3://{bucket}/silver/weather/date={process_date}/"
gold_path = f"s3://{bucket}/gold/weather/"

print(f"Reading Silver → {silver_path}")

# ----------------------------------------------------
# SAFE READ
# ----------------------------------------------------
try:
    df = spark.read.parquet(silver_path)
except Exception:
    print(f"No Silver data for {process_date}")
    job.commit()
    sys.exit(0)

if df.rdd.isEmpty():
    print("Silver partition empty")
    job.commit()
    sys.exit(0)

df = df.withColumn("date", to_date(col("timestamp")))

gold_df = df.groupBy("city", "date").agg(
    avg("temperature_2m").alias("avg_temperature"),
    max("temperature_2m").alias("max_temperature"),
    sum("precipitation").alias("total_precipitation"),
    avg("windspeed_10m").alias("avg_windspeed")
)

print(f"Overwriting Gold partition → {process_date}")

(
    gold_df.write
    .mode("overwrite")
    .option("replaceWhere", f"date = '{process_date}'")
    .partitionBy("date")
    .parquet(gold_path)
)

print("Gold write complete")

job.commit()