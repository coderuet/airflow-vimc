from __future__ import annotations
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from textwrap import dedent
from helpers.spark_helper import (
    render_env_yaml,
    build_spark_application_yaml,
    create_spark_k8s_operator,
    create_spark_k8s_sensor,
)

import datetime as dt
import pendulum
import logging


SPARK_MAIN_CLASS = "vn.viettel.BRONZE_ZONE"

ENV_VARS = {"ENV_JOB_RUN": "dev"}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "retries": 0,
}


def startBatch():
    print("##### startBatch #####")


def done():
    print("##### done #####")


with DAG(
    dag_id="spark_ingest_demo",
    default_args=default_args,
    # schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 11, 24, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "k8s", "bronze-zone", "batch"],
    description="ETL Pipeline: Ingest -> Bronze Zone ",
) as raw_zone_batch_dag:
    # Tạo cấu hình spark job format yaml
    ingest_bronze_batch, ingest_bronze_batch_app_name = build_spark_application_yaml(
        job_suffix="dev-vimc-ingest-bronze-zone-batch",
        main_class=SPARK_MAIN_CLASS,
        env_vars=ENV_VARS,
        spark_image="192.168.74.14:80/vimc-vlp-project/thanh-spark-vimc-dev",
    )

    raw_batch_submit = create_spark_k8s_operator(
        "submit_raw_zone_batch", ingest_bronze_batch
    )

    raw_batch_wait = create_spark_k8s_sensor(
        "wait_raw_zone_batch", ingest_bronze_batch_app_name
    )

    start_batch_task = PythonOperator(task_id="startBatch", python_callable=startBatch)
    done_task = PythonOperator(task_id="done", python_callable=done)

    start_batch_task >> raw_batch_submit >> raw_batch_wait >> done_task
