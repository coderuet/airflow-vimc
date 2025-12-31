from __future__ import annotations
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from textwrap import dedent
from airflow.providers.cncf.kubernetes.operators.kubernetes_manifest import KubernetesManifestOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.models import Variable
import yaml

import datetime as dt
import pendulum
import logging

def _get_var(name: str, default: str) -> str:
    return Variable.get(name, default_var=default)

SPARK_NAMESPACE = _get_var("VIMC_SPARK_NAMESPACE", "vlp-tenantdvak01g-wsghwlhlt-ingestion")
SPARK_K8S_CONN_ID = _get_var("VIMC_K8S_CONN_ID", "kubernetes_default")
SPARK_IMAGE = _get_var("VIMC_SPARK_IMAGE", "192.168.74.14:80/vimc-vlp-project/spark-base:1.0")
SPARK_IMAGE_PULL_POLICY = _get_var("VIMC_SPARK_IMAGE_PULL_POLICY", "IfNotPresent")
SPARK_IMAGE_PULL_SECRET = _get_var("VIMC_SPARK_IMAGE_PULL_SECRET", "vlp-registry")
SPARK_MAIN_JAR = _get_var("VIMC_SPARK_MAIN_JAR", "local:///opt/spark/jars/app.jar")
SPARK_SERVICE_ACCOUNT = _get_var("VIMC_SPARK_SA", "spark-application-sa")
SPARK_VERSION = _get_var("VIMC_SPARK_VERSION", "3.5.1")

DRIVER_CORES = _get_var("VIMC_DRIVER_CORES", "1")
DRIVER_CORE_LIMIT = _get_var("VIMC_DRIVER_CORE_LIMIT", "1")
DRIVER_MEMORY = _get_var("VIMC_DRIVER_MEMORY", "4g")
DRIVER_MEMORY_OVERHEAD = _get_var("VIMC_DRIVER_MEMORY_OVERHEAD", "512m")

EXECUTOR_CORES = _get_var("VIMC_EXECUTOR_CORES", "1")
EXECUTOR_CORE_LIMIT = _get_var("VIMC_EXECUTOR_CORE_LIMIT", "1")
EXECUTOR_MEMORY = _get_var("VIMC_EXECUTOR_MEMORY", "4g")
EXECUTOR_MEMORY_OVERHEAD = _get_var("VIMC_EXECUTOR_MEMORY_OVERHEAD", "512m")
EXECUTOR_INSTANCES = _get_var("VIMC_EXECUTOR_INSTANCES", "1")

spark_yaml = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {"name": "spark-base-app", "namespace": SPARK_NAMESPACE},
    "spec": {
        "type": "Scala",
        "mode": "cluster",
        "image": "192.168.74.14:80/vimc-vlp-project/spark-base:1.0",
        "mainClass": "vn.viettel.code.ingestion.vimc.cfs.Purchase",
        "mainApplicationFile": "s3://vimc/spark-artifacts/jobs/sparkscalavimc_2.12-0.1.0-SNAPSHOT.jar",
        "sparkVersion": "3.5.1",
        "timeToLiveSeconds": 3600,
        "restartPolicy": {"type": "Never"},
        "deps": {
            "sparkConf":
                "spark.jars": "s3://vimc/spark-artifacts/libs/*.jar"
        }
    }
}


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
    dag_id="spark_base_demo",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 24, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "k8s", "test"],
    description="ETL Pipeline: Ingest -> Bronze Zone ",
) as bronze_zone_batch_dag:
    submit_spark = KubernetesManifestOperator(
        task_id="submit_spark",
        manifest=spark_yaml
    )

    raw_batch_wait = SparkKubernetesSensor(
        task_id="sensor_task",
        namespace=SPARK_NAMESPACE,
        application_name="spark-base-app",
        kubernetes_conn_id=SPARK_K8S_CONN_ID,
        attach_log=True,
        poke_interval=30,
        timeout=180000,
    )

    start_batch_task = PythonOperator(task_id="startBatch", python_callable=startBatch)
    done_task = PythonOperator(task_id="done", python_callable=done)

    start_batch_task >> submit_spark >> raw_batch_wait >> done_task
