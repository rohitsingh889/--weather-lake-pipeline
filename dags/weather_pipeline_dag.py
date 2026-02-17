from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import time

from extraction import run_extraction

REGION = "us-east-1"
BUCKET_NAME = "rohitbucket1weather"


# DATE HELPER (Single Source of Truth)

def get_process_date(ds):
    """Return yesterday relative to Airflow execution date."""
    return (
        datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")


# ----------------------------------------------------
# UNIVERSAL GLUE RUNNER
# ----------------------------------------------------
def run_glue_job_and_wait(job_name, arguments=None):
    glue = boto3.client("glue", region_name=REGION)

    kwargs = {"JobName": job_name}
    if arguments:
        kwargs["Arguments"] = arguments

    response = glue.start_job_run(**kwargs)
    run_id = response["JobRunId"]

    print(f"{job_name} started → {run_id}")

    while True:
        job_run = glue.get_job_run(JobName=job_name, RunId=run_id)
        state = job_run["JobRun"]["JobRunState"]

        print(f"{job_name} state → {state}")

        if state == "SUCCEEDED":
            print(f"{job_name} succeeded")
            return

        if state in ["FAILED", "STOPPED", "TIMEOUT"]:
            raise Exception(f"{job_name} ended with state → {state}")

        time.sleep(30)



# TASK FUNCTIONS

def trigger_extraction_task(**context):
    print("Starting API extraction…")
    result = run_extraction()
    print(f"Extraction complete → {result}")


def trigger_silver_glue_job(ds, **context):
    process_date = get_process_date(ds)

    print(f"Triggering Silver Job for → {process_date}")

    run_glue_job_and_wait(
        job_name="silver-weather-job",
        arguments={
            "--process_date": process_date,
            "--bucket_name": BUCKET_NAME,
        },
    )


def trigger_gold_glue_job(ds, **context):
    process_date = get_process_date(ds)

    print(f"Triggering Gold Job for → {process_date}")

    run_glue_job_and_wait(
        job_name="gold-weather-job",
        arguments={
            "--process_date": process_date,
            "--bucket_name": BUCKET_NAME,
        },
    )


def trigger_crawler(**context):
    glue = boto3.client("glue", region_name=REGION)
    crawler_name = "weather-data-crawler"

    print("Starting Glue Crawler…")

    glue.start_crawler(Name=crawler_name)

    while True:
        crawler = glue.get_crawler(Name=crawler_name)
        state = crawler["Crawler"]["State"]

        print(f"Crawler state → {state}")

        if state == "READY":
            print("Crawler completed")
            return

        time.sleep(15)


# ----------------------------------------------------
# DAG CONFIG
# ----------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_pipeline_v3",
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["weather", "aws", "glue"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_api_to_bronze",
        python_callable=trigger_extraction_task,
    )

    silver_task = PythonOperator(
        task_id="run_silver_glue_job",
        python_callable=trigger_silver_glue_job,
    )

    gold_task = PythonOperator(
        task_id="run_gold_glue_job",
        python_callable=trigger_gold_glue_job,
    )

    crawler_task = PythonOperator(
        task_id="run_glue_crawler",
        python_callable=trigger_crawler,
    )

    extract_task >> silver_task >> gold_task >> crawler_task