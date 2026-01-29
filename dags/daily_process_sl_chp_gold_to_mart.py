from airflow import DAG
from datetime import datetime
from airflow.models import Variable

from helpers.spark_helper import build_spark_application_yaml, create_spark_k8s_operator, create_spark_k8s_sensor

default_args = {
    'owner': 'vimc_dev',
    'start_date': datetime(2024, 1, 1),
}

ENV_VARS = {
    "ENV_JOB_RUN": "dev",
    "POSTGRES_HOST": Variable.get("POSTGRES_HOST"),
    "POSTGRES_PORT": Variable.get("POSTGRES_PORT"),
    "POSTGRES_DATABASE": Variable.get("POSTGRES_DATABASE"),
    "POSTGRES_SCHEMA": Variable.get("POSTGRES_SCHEMA"),
    "POSTGRES_USERNAME": Variable.get("POSTGRES_USERNAME"),
    "POSTGRES_PASSWORD": Variable.get("POSTGRES_PASSWORD"),
    "MINIO_ENDPOINT": Variable.get("MINIO_ENDPOINT"),
    "MINIO_BUCKET": Variable.get("MINIO_BUCKET"),
    "MINIO_ACCESS_KEY": Variable.get("MINIO_ACCESS_KEY"),
    "MINIO_SECRET_KEY": Variable.get("MINIO_SECRET_KEY"),
    "MINIO_PATH_STYLE_ACCESS": Variable.get("MINIO_PATH_STYLE_ACCESS"),
    "MINIO_LAKE_HOUSE_PATH": Variable.get("MINIO_LAKE_HOUSE_PATH"),
    "BATCH_START_DATE": Variable.get("BATCH_START_DATE"),
    "BATCH_END_DATE": Variable.get("BATCH_END_DATE"),
    "DEFAULT_START_DATE": Variable.get("DEFAULT_START_DATE"),
    "SPARK_APP_NAME": Variable.get("SPARK_APP_NAME"),
    "HIVE_METASTORE_URI": Variable.get("HIVE_METASTORE_URI"),
    "DES_PATH": Variable.get("DES_PATH"),
    "SPARK_APP_NAME": Variable.get("SPARK_APP_NAME"),
    "RUN_TYPE": Variable.get("RUN_TYPE"),
    # "API_URL" : Variable.get("API_URL"),
    # "API_USERNAME" : Variable.get("API_USERNAME"),
    # "API_PASSWORD" : Variable.get("API_PASSWORD"),
    # "LIST_OF_COMPANY": Variable.get("LIST_OF_COMPANY"),
}

with DAG(
    dag_id='daily_process_sl_chp_gold_to_mart',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['daily', 'process', 'sl_chp','mart']
) as dag:

    # 1. Định nghĩa Manifest cho Spark Job
    # Helper sẽ tự động điền các thông tin về S3, Image, và Credentials
    raw_manifest, app_name = build_spark_application_yaml(
        job_suffix='daily-process-sl-chp-gold-to-mart',
        main_class='vn.viettel.vlp_load.jobs.GoldToMartJob',
        env_vars=ENV_VARS,
        spark_main_jar='s3a://vimc/vimc/spark-artifacts/jobs/muoilv/transform-chp/spark-ops-latest.jar',
        arguments=[],
        executor_instances="2",  # Tùy chỉnh số lượng executor nếu cần
        executor_memory="2g"
    )

    # 2. Tạo Task Submit
    submit_job = create_spark_k8s_operator(
        task_id='submit_spark_job',
        raw_batch_manifest=raw_manifest
    )

    # 3. Tạo Task Sensor (Để theo dõi log và trạng thái Job)
    wait_for_job = create_spark_k8s_sensor(
        task_id='wait_for_spark_job',
        raw_batch_app_name=app_name
    )

    submit_job >> wait_for_job