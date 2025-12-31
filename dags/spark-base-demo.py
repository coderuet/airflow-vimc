from __future__ import annotations
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from textwrap import dedent
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
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

def render_env_yaml(indent_spaces: int = 12, env_vars: dict = {}) -> str:
    pad = " " * indent_spaces
    env_lines = []
    for key, value in env_vars.items():
        env_lines.append(f"{pad}- name: {key}\n{pad}  value: \"{value}\"")
    return "\n".join(env_lines)


def build_spark_application_yaml(
        job_suffix: str = 'spark_suffix',
        main_class: str = 'vn.viettel.vlp_load.example',
        env_vars: dict = {},
        spark_image_pull_secret: str = SPARK_IMAGE_PULL_SECRET,
        spark_namespace: str = SPARK_NAMESPACE,
        spark_image: str = SPARK_IMAGE,
        spark_image_pull_policy: str = SPARK_IMAGE_PULL_POLICY,
        spark_main_jar: str = SPARK_MAIN_JAR,
        spark_version: str = SPARK_VERSION,
        spark_service_account: str = SPARK_SERVICE_ACCOUNT,
        driver_cores: str = DRIVER_CORES,
        driver_core_limit: str = DRIVER_CORE_LIMIT,
        driver_memory: str = DRIVER_MEMORY,
        driver_memory_overhead: str = DRIVER_MEMORY_OVERHEAD,
        executor_cores: str = EXECUTOR_CORES,
        executor_instances: str = EXECUTOR_INSTANCES,
        executor_core_limit: str = EXECUTOR_CORE_LIMIT,
        executor_memory: str = EXECUTOR_MEMORY,
        executor_memory_overhead: str = EXECUTOR_MEMORY_OVERHEAD,
    ):
    rundate_str = datetime.now().strftime("%Y%m%d%H%M")
    app_name = f"vimc-spark-batch-{job_suffix}-{rundate_str}"

    pull_secret_block = ""
    if spark_image_pull_secret:
        pull_secret_block = f"imagePullSecrets:\n            - {spark_image_pull_secret}"

    env_block = render_env_yaml(12, env_vars)

    manifest = dedent(
        f"""
        apiVersion: "sparkoperator.k8s.io/v1beta2"
        kind: SparkApplication
        metadata:
          name: spark-base-app
          namespace: {SPARK_NAMESPACE}
        spec:
          type: Scala
          mode: cluster
          image: "{SPARK_IMAGE}"
          imagePullPolicy: {SPARK_IMAGE_PULL_POLICY}
          {pull_secret_block}
          mainApplicationFile: s3://vimc/spark-artifacts/jobs/sparkscalavimc_2.12-0.1.0-SNAPSHOT.jar
          mainClass: vn.viettel.code.ingestion.vimc.cfs.Purchase
          sparkVersion: "{SPARK_VERSION}"
          restartPolicy:
            type: Never
          sparkConf:
            spark.jars: s3://vimc/spark-artifacts/libs/*.jar
          sparkConf:
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            "spark.sql.adaptive.enabled": "true"
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
            "spark.eventLog.enabled": "true"
            "spark.eventLog.dir": "s3a://vimc/vmic/spark_history"
            "spark.hadoop.fs.s3a.endpoint": "http://192.168.74.16:30090"
            "spark.hadoop.fs.s3a.path.style.access": "true"
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
          driver:
            serviceAccount: {SPARK_SERVICE_ACCOUNT}
            cores: {DRIVER_CORES}
            coreLimit: "{DRIVER_CORE_LIMIT}"
            memory: "{DRIVER_MEMORY}"
            memoryOverhead: "{DRIVER_MEMORY_OVERHEAD}"
            env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
              secretKeyRef:
                name: minio-creds
                key: access-key
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
              secretKeyRef:
                name: minio-creds
                key: secret-key
    {env_block}
          executor:
            cores: {EXECUTOR_CORES}
            instances: {EXECUTOR_INSTANCES}
            coreLimit: "{EXECUTOR_CORE_LIMIT}"
            memory: "{EXECUTOR_MEMORY}"
            memoryOverhead: "{EXECUTOR_MEMORY_OVERHEAD}"
            env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: access-key
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: secret-key
    {env_block}
        """
    ).strip()
    print(manifest)
    return manifest, app_name



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
    raw_batch_manifest, raw_batch_app_name = build_spark_application_yaml(
        job_suffix="dev-vimc-raw-zone-batch",
        main_class=SPARK_MAIN_CLASS,
        env_vars=ENV_VARS
    )

    raw_batch_submit = create_spark_k8s_operator('submit_raw_zone_batch', raw_batch_manifest)

    raw_batch_wait = create_spark_k8s_sensor('wait_raw_zone_batch', raw_batch_app_name)

    start_batch_task = PythonOperator(
        task_id='startBatch',
        python_callable=startBatch
    )
    done_task = PythonOperator(
        task_id='done',
        python_callable=done
    )


    start_batch_task >> raw_batch_submit >> raw_batch_wait >> done_task