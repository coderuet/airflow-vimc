from __future__ import annotations
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from textwrap import dedent
from helpers.spark_helper import render_env_yaml, build_spark_application_yaml, create_spark_k8s_operator, create_spark_k8s_sensor

import datetime as dt
import pendulum
import logging



SPARK_MAIN_CLASS = "vn.viettel.ingestion.api.finance.CFS_PURCHASE"

ENV_VARS = {
    "ENV_JOB_RUN": "dev"
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0,
}

def startBatch(value):
    print(f'##### {value} #####')

def done():
    print('##### done #####')

with DAG(
    dag_id="test_ingest_to_bronze_job",
    default_args=default_args,
    # schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 11, 24, tz='Asia/Ho_Chi_Minh'),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "k8s", "api", "bronze-zone", "batch"],
    description="ETL Pipeline: API -> Bronze Zone"
) as bronze_zone_batch_dag:
    # Tạo cấu hình spark job format yaml
    bronze_batch_manifest, bronze_batch_app_name = build_spark_application_yaml(
        job_suffix="dev-vimc-bronze-zone-batch",
        main_class=SPARK_MAIN_CLASS,
        env_vars=ENV_VARS
    )

    print(bronze_batch_manifest)
    print("-----")
    print(bronze_batch_app_name)

    bronze_batch_submit = create_spark_k8s_operator('submit_bronze_zone_batch', bronze_batch_manifest)

    bronze_batch_wait = create_spark_k8s_sensor('wait_bronze_zone_batch', bronze_batch_app_name)

    start_batch_task = PythonOperator(
        task_id='startBatch',
        python_callable=startBatch,
        params={"value": bronze_batch_manifest}
    )
    done_task = PythonOperator(
        task_id='done',
        python_callable=done
    )


    start_batch_task >> bronze_batch_submit >> bronze_batch_wait >> done_task