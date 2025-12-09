from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from helpers.helper import send_error_to_discord

BASE_DIR = Path(__file__).resolve().parents[1]
SPARK_APP = BASE_DIR / "spark-code" / "cfs_full_load.py"
CONFIG_PATH = BASE_DIR / "configs" / "cfs.toml"
# Add the APIs module path for py-files
APIS_MODULE = BASE_DIR / "apis" / "cfs.py"

DEFAULT_BUCKET = Variable.get("MINIO_BUCKET", default_var="data-lake")
DEFAULT_BRONZE_PATH = f"{DEFAULT_BUCKET}/bronze"
DEFAULT_SILVER_PATH = f"{DEFAULT_BUCKET}/silver"

BRONZE_PATH = Variable.get("CFS_FULL_LOAD_BRONZE_PATH", default_var=DEFAULT_BRONZE_PATH)
SILVER_PATH = Variable.get("CFS_FULL_LOAD_SILVER_PATH", default_var=DEFAULT_SILVER_PATH)
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT", default_var="http://minio-server:9000")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY", default_var="MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY", default_var="MINIO_SECRET_KEY")
SPARK_CONN_ID = Variable.get("CFS_FULL_LOAD_SPARK_CONN_ID", default_var="spark_default")
FULL_LOAD_START = Variable.get("CFS_FULL_LOAD_START_DATE", default_var="20230101")

# Discord for development alerts
# Change to Email or other channels as needed
def failure_callback(context):
    """Handle task failures by sending alerts to Discord."""
    error_message = str(context.get("exception", "Unknown failure"))
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    
    formatted_message = (
        f"❌ DAG Failure: {dag_id}\n"
        f"Task: {task_instance.task_id if task_instance else 'unknown'}\n"
        f"Error: {error_message}"
    )
    send_error_to_discord(formatted_message)


def build_application_args():
    """Build command-line arguments for the Spark application."""
    return [
        "--bronze-path", BRONZE_PATH,
        "--silver-path", SILVER_PATH,
        "--config-path", str(CONFIG_PATH),
        "--start-date", FULL_LOAD_START,
        "--endpoint", MINIO_ENDPOINT,
        "--access-key", MINIO_ACCESS_KEY,
        "--secret-key", MINIO_SECRET_KEY,
    ]


default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback,
    "execution_timeout": timedelta(hours=6),
}

with DAG(
    dag_id="cfs_full_load",
    default_args=default_args,
    description="Submit Spark job for CFS full load (bronze + silver).",
    schedule="@monthly",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["cfs", "spark", "full_load", "etl"],
) as dag:
    
    run_cfs_full_load = SparkSubmitOperator(
        task_id="run_cfs_full_load",
        conn_id=SPARK_CONN_ID,
        application=str(SPARK_APP),
        name="cfs_full_load_job",
        verbose=True,
        application_args=build_application_args(),
        # Ship the config file and APIs module to Spark executors
        files=str(CONFIG_PATH),
        py_files=str(APIS_MODULE),
        # Spark configurations for better performance
        conf={
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.dynamicAllocation.maxExecutors": "10",
        },
    )