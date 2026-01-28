from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

from airflow.models import Variable
from helpers.spark_helper import build_spark_application_yaml


default_args = {
    "owner": "vimc_dev",
    "start_date": datetime(2024, 1, 1),
}

ENV_VARS = {
    "ENV_JOB_RUN": "dev",
    "MINIO_ENDPOINT": Variable.get("MINIO_ENDPOINT"),
    "MINIO_BUCKET": Variable.get("MINIO_BUCKET"),
    "MINIO_ACCESS_KEY": Variable.get("MINIO_ACCESS_KEY"),
    "MINIO_SECRET_KEY": Variable.get("MINIO_SECRET_KEY"),
    "MINIO_PATH_STYLE_ACCESS": Variable.get("MINIO_PATH_STYLE_ACCESS"),
    "BATCH_START_DATE": Variable.get("BATCH_START_DATE"),
    "BATCH_END_DATE": Variable.get("BATCH_END_DATE"),
    "DEFAULT_START_DATE": Variable.get("DEFAULT_START_DATE"),
    "CHP_API_ENDPOINT": Variable.get("CHP_API_ENDPOINT"),
    "CHP_API_USERNAME": Variable.get("CHP_API_USERNAME"),
    "CHP_API_PASSWORD": Variable.get("CHP_API_PASSWORD"),
    "CHP_API_KEY": Variable.get("CHP_API_KEY"),
    "RUN_TYPE": Variable.get("RUN_TYPE"),
    "SPARK_APP_NAME": Variable.get("SPARK_APP_NAME"),
    "HIVE_METASTORE_URI": Variable.get("HIVE_METASTORE_URI"),
}


def _extract_env_names(manifest):
    sections = {"driver": [], "executor": []}
    current = None
    for line in manifest.splitlines():
        stripped = line.strip()
        if stripped == "driver:":
            current = "driver"
            continue
        if stripped == "executor:":
            current = "executor"
            continue
        if stripped.startswith("- name:") and current:
            name = stripped.split(":", 1)[1].strip()
            sections[current].append(name)
    return sections


def _log_envs(task_label, manifest):
    logger = logging.getLogger(__name__)
    envs = _extract_env_names(manifest)
    logger.info("=" * 60)
    logger.info("SPARK YAML ENVS: %s", task_label)
    logger.info("=" * 60)
    for section in ("driver", "executor"):
        values = envs.get(section, [])
        logger.info("%s envs (%s): %s", section, len(values), ", ".join(values))
    logger.info("=" * 60)


def log_sl_chp_yaml_envs():
    manifest, app_name = build_spark_application_yaml(
        job_suffix="full-ingest-sl-to-bronze",
        main_class="vn.viettel.vlp_load.ingestion.Ingest",
        env_vars=ENV_VARS,
        spark_main_jar="s3a://vimc/vimc/spark-artifacts/jobs/minhnvq/dev-chp/spark-ops-latest.jar",
        arguments=[],
        executor_instances="2",
        executor_memory="2g",
    )
    _log_envs("full_load_ingest_sl_chp_to_bronze", manifest)
    return app_name


with DAG(
    dag_id="log_spark_yaml_envs",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["utility", "logging"],
) as dag:
    log_sl_chp = PythonOperator(
        task_id="log_sl_chp_yaml_envs",
        python_callable=log_sl_chp_yaml_envs,
    )
