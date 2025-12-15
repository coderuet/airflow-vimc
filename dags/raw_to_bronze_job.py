from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago
from __future__ import annotations
from textwrap import dedent

import datetime as dt
import pendulum
import logging

def _get_var(name: str, default: str) -> str:
    return Variable.get(name, default_var=default)

SPARK_NAMESPACE = _get_var("VIMC_SPARK_NAMESPACE", "vlp-tenantdvak01g-wsghwlhlt-ingestion")
SPARK_K8S_CONN_ID = _get_var("VIMC_K8S_CONN_ID", "kubernetes_default")
SPARK_IMAGE = _get_var("VIMC_SPARK_IMAGE", "192.168.74.14:80/vimc-vlp-project/spark:3.5.1-scala2.12-java11-v1.5.2.1")
SPARK_IMAGE_PULL_POLICY = _get_var("VIMC_SPARK_IMAGE_PULL_POLICY", "Always")
SPARK_IMAGE_PULL_SECRET = _get_var("VIMC_SPARK_IMAGE_PULL_SECRET", "vlp-registry")
SPARK_MAIN_JAR = _get_var("VIMC_SPARK_MAIN_JAR", "local:///opt/spark/jars/app.jar")
SPARK_SERVICE_ACCOUNT = _get_var("VIMC_SPARK_SA", "spark-application-sa")
SPARK_VERSION = _get_var("VIMC_SPARK_VERSION", "3.5.1")

DRIVER_CORES = _get_var("VIMC_DRIVER_CORES", "2")
DRIVER_CORE_LIMIT = _get_var("VIMC_DRIVER_CORE_LIMIT", "2")
DRIVER_MEMORY = _get_var("VIMC_DRIVER_MEMORY", "4g")
DRIVER_MEMORY_OVERHEAD = _get_var("VIMC_DRIVER_MEMORY_OVERHEAD", "512m")

EXECUTOR_CORES = _get_var("VIMC_EXECUTOR_CORES", "3")
EXECUTOR_CORE_LIMIT = _get_var("VIMC_EXECUTOR_CORE_LIMIT", "3")
EXECUTOR_MEMORY = _get_var("VIMC_EXECUTOR_MEMORY", "6g")
EXECUTOR_MEMORY_OVERHEAD = _get_var("VIMC_EXECUTOR_MEMORY_OVERHEAD", "512m")
EXECUTOR_INSTANCES = _get_var("VIMC_EXECUTOR_INSTANCES", "2")

ENV_VARS = {
    "ENV_JOB_RUN": "dev"
}

def _render_env_yaml(indent_spaces: int = 12) -> str:
    pad = " " * indent_spaces
    env_lines = []
    for key, value in ENV_VARS.items():
        env_lines.append(f"{pad}- name: {key}\n{pad}  value: \"{value}\"")
    return "\n".join(env_lines)

def _build_spark_application_yaml(job_suffix: str, main_class: str) -> tuple[str, str]:
    app_name = f"poc-VIMC-spark-batch-{job_suffix}"

    pull_secret_block = ""
    if SPARK_IMAGE_PULL_SECRET:
        pull_secret_block = f"imagePullSecrets:\n            - {SPARK_IMAGE_PULL_SECRET}"

    env_block = _render_env_yaml(12)

    manifest = dedent(
        f"""
        apiVersion: "sparkoperator.k8s.io/v1beta2"
        kind: SparkApplication
        metadata:
          name: {app_name}
          namespace: {SPARK_NAMESPACE}
        spec:
          type: Scala
          mode: cluster
          image: "{SPARK_IMAGE}"
          imagePullPolicy: {SPARK_IMAGE_PULL_POLICY}
          {pull_secret_block}
          mainApplicationFile: {SPARK_MAIN_JAR}
          mainClass: {main_class}
          sparkVersion: "{SPARK_VERSION}"
          restartPolicy:
            type: Never
          sparkConf:
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            "spark.sql.adaptive.enabled": "true"
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
          driver:
            serviceAccount: {SPARK_SERVICE_ACCOUNT}
            cores: {DRIVER_CORES}
            coreLimit: "{DRIVER_CORE_LIMIT}"
            memory: "{DRIVER_MEMORY}"
            memoryOverhead: "{DRIVER_MEMORY_OVERHEAD}"
            env:
{env_block}
          executor:
            cores: {EXECUTOR_CORES}
            instances: {EXECUTOR_INSTANCES}
            coreLimit: "{EXECUTOR_CORE_LIMIT}"
            memory: "{EXECUTOR_MEMORY}"
            memoryOverhead: "{EXECUTOR_MEMORY_OVERHEAD}"
            env:
{env_block}
        """
    ).strip()
    return manifest, app_name

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


def startBatch():
    print('##### startBatch #####')

def done():
    print('##### done #####')

with DAG(
    dag_id="spark_batch_airflow",
    default_args=default_args,
    # schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 11, 24, tz='Asia/Ho_Chi_Minh'),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "k8s", "raw-zone", "bronze-zone", "batch"],
    description="ETL Pipeline: Raw Zone -> Bronze Zone"
) as raw_zone_batch_dag:
    raw_batch_manifest, raw_batch_app_name = _build_spark_application_yaml(
        job_suffix="raw-zone-batch",
        main_class="vn.viettel.vlp_load.RAW_ZONE",
    )

    raw_batch_submit = SparkKubernetesOperator(
        task_id="submit_raw_zone_batch",
        namespace=SPARK_NAMESPACE,
        application_file=raw_batch_manifest,
        kubernetes_conn_id=SPARK_K8S_CONN_ID,
        do_xcom_push=False,
    )

    raw_batch_wait = SparkKubernetesSensor(
        task_id="wait_raw_zone_batch",
        namespace=SPARK_NAMESPACE,
        application_name=raw_batch_app_name,
        kubernetes_conn_id=SPARK_K8S_CONN_ID,
        attach_log=True,
        poke_interval=30,
        timeout=180000,
    )

    start_batch_task = PythonOperator(
        task_id='startBatch',
        python_callable=startBatch
    )
    done_task = PythonOperator(
        task_id='done',
        python_callable=done
    )


    start_batch_task >> raw_batch_submit >> raw_batch_wait >> done_task