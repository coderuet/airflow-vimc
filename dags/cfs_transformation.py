from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from helpers.helper import send_error_to_discord

BASE_DIR = Path(__file__).resolve().parents[1]
SPARK_APP = BASE_DIR / "spark-code" / "cfs_transformation.py"
CONFIG_PATH = BASE_DIR / "configs" / "cfs.toml"

DEFAULT_BUCKET = Variable.get("MINIO_BUCKET", default_var="data-lake")
BRONZE_BUCKET = Variable.get("CFS_BRONZE_BUCKET", default_var=DEFAULT_BUCKET)
SILVER_BUCKET = Variable.get("CFS_SILVER_BUCKET", default_var=DEFAULT_BUCKET)
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT", default_var="http://minio-server:9000")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY", default_var="MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY", default_var="MINIO_SECRET_KEY")
SPARK_CONN_ID = Variable.get("CFS_SPARK_CONN_ID", default_var="spark_default")
BRONZE_PREFIX = Variable.get("CFS_BRONZE_PREFIX", default_var="bronze")
SILVER_PREFIX = Variable.get("CFS_SILVER_PREFIX", default_var="silver")


def failure_callback(context):
    error_message = str(context.get("exception", "Unknown failure"))
    send_error_to_discord(error_message)


def build_application_args():
    return [
        "--bronze-bucket", BRONZE_BUCKET,
        "--bronze-prefix", BRONZE_PREFIX,
        "--silver-bucket", SILVER_BUCKET,
        "--silver-prefix", SILVER_PREFIX,
        "--endpoint", MINIO_ENDPOINT,
        "--access-key", MINIO_ACCESS_KEY,
        "--secret-key", MINIO_SECRET_KEY,
        "--config-path", str(CONFIG_PATH),
    ]


definition = {
    "dag_id": "cfs_transformation",
    "default_args": {
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": failure_callback,
    },
    "description": "Submit the Spark-based CFS bronze+silver transformation job",
    "schedule": "@monthly",
    "start_date": datetime(2021, 1, 1),
    "catchup": False,
    "tags": ["cfs", "spark", "transformation"],
}

with DAG(**definition) as dag:
    run_cfs_transformation = SparkSubmitOperator(
        task_id="run_cfs_transformation",
        conn_id=SPARK_CONN_ID,
        application=str(SPARK_APP),
        name="cfs_transformation_job",
        verbose=True,
        application_args=build_application_args(),
    )

    run_cfs_transformation
