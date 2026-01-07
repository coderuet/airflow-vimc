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


SPARK_MAIN_CLASS = "vn.viettel.RAW_ZONE"

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


def build_spark_config(**context):
    """Build Spark configuration with conditional arguments"""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    start_date = conf.get("start_date")
    end_date = conf.get("end_date")

    arguments = None
    if start_date and end_date:
        print(f"Found start_date and end_date in config, passing arguments to Scala")
        arguments = [
            context["dag"].dag_id,
            context["run_id"],
            "REPROCESS",
            start_date,
            end_date,
        ]
    else:
        print("No start_date or end_date found, NOT passing arguments to Scala")

    # Build Spark manifest
    manifest, app_name = build_spark_application_yaml(
        job_suffix="dev-vimc-ingest-raw-zone-batch",
        main_class=SPARK_MAIN_CLASS,
        env_vars=ENV_VARS,
        arguments=arguments,
        spark_image="192.168.74.14:80/vimc-vlp-project/thanh-spark-vimc-dev",
    )

    # Push to XCom
    context["task_instance"].xcom_push(key="spark_manifest", value=manifest)
    context["task_instance"].xcom_push(key="spark_app_name", value=app_name)

    return app_name


with DAG(
    dag_id="spark_ingest_demo",
    default_args=default_args,
    # schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 11, 24, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "k8s", "raw-zone", "batch"],
    description="ETL Pipeline: Ingest -> Raw Zone ",
) as raw_zone_batch_dag:

    start_batch_task = PythonOperator(task_id="startBatch", python_callable=startBatch)

    # Build Spark config với conditional arguments
    build_config_task = PythonOperator(
        task_id="build_spark_config",
        python_callable=build_spark_config,
        provide_context=True,
    )

    # Submit Spark job
    def submit_spark(**context):
        manifest = context["task_instance"].xcom_pull(
            task_ids="build_spark_config", key="spark_manifest"
        )
        operator = create_spark_k8s_operator("submit_raw_zone_batch", manifest)
        operator.execute(context)

    raw_batch_submit = PythonOperator(
        task_id="submit_raw_zone_batch",
        python_callable=submit_spark,
        provide_context=True,
    )

    # Wait for Spark job
    def wait_spark(**context):
        app_name = context["task_instance"].xcom_pull(
            task_ids="build_spark_config", key="spark_app_name"
        )
        sensor = create_spark_k8s_sensor("wait_raw_zone_batch", app_name)
        sensor.execute(context)

    raw_batch_wait = PythonOperator(
        task_id="wait_raw_zone_batch", python_callable=wait_spark, provide_context=True
    )

    done_task = PythonOperator(task_id="done", python_callable=done)

    (
        start_batch_task
        >> build_config_task
        >> raw_batch_submit
        >> raw_batch_wait
        >> done_task
    )
