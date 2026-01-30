from airflow import DAG
from datetime import datetime
from airflow.models import Variable

from helpers.spark_helper import build_spark_application_yaml, create_spark_k8s_operator, create_spark_k8s_sensor

# Airflow Variables
ENV_VARS = {"ENV_JOB_RUN": "dev",
                          "MINIO_ENDPOINT": Variable.get("MINIO_ENDPOINT"),
                          "MINIO_BUCKET": Variable.get("MINIO_BUCKET"),
                          "MINIO_ACCESS_KEY": Variable.get("MINIO_ACCESS_KEY"),
                          "MINIO_SECRET_KEY": Variable.get("MINIO_SECRET_KEY"),
                          "MINIO_PATH_STYLE_ACCESS": Variable.get("MINIO_PATH_STYLE_ACCESS"),
            "MINIO_LAKE_HOUSE_PATH": Variable.get("MINIO_LAKE_HOUSE_PATH"),
                          "BATCH_START_DATE": "2025-12-01",
                            "BATCH_END_DATE": "2025-12-31",
                          "DEFAULT_START_DATE": "2024-01-01",
                          "CHP_API_ENDPOINT": Variable.get("CHP_API_ENDPOINT"),
                          "CHP_API_USERNAME": Variable.get("CHP_API_USERNAME"),
                          "CHP_API_PASSWORD": Variable.get("CHP_API_PASSWORD"),
                          "CHP_API_KEY": Variable.get("CHP_API_KEY"),
                          "RUN_TYPE": Variable.get("RUN_TYPE"),
                          "SPARK_APP_NAME": Variable.get("SPARK_APP_NAME"),
                          "HIVE_METASTORE_URI": Variable.get("HIVE_METASTORE_URI"),
                            "DES_PATH": Variable.get("DES_PATH"),
                        "SPARK_APP_NAME": Variable.get("SPARK_APP_NAME"),
            "ENVIROMENT": Variable.get("ENVIROMENT"),
            "COMPANY_NAME": "CHP_HDCV"
                          }

default_args = {
    'owner': 'vimc_dev',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='full_load_ingest_sl_chp_hdcv_to_bronze',
    default_args=default_args,
    catchup=False,
    tags=['full', 'ingest', 'sl', 'chp_hdcv']
) as dag:

    # 1. Định nghĩa Manifest cho Spark Job
    # Helper sẽ tự động điền các thông tin về S3, Image, và Credentials
    raw_manifest, app_name = build_spark_application_yaml(
        job_suffix='full-ingest-sl-to-bronze',
        main_class='vn.viettel.vlp_load.ingestion.CHP.CHPHDCVIngestion',
        env_vars=ENV_VARS,
        spark_main_jar='s3a://vimc/vimc/spark-artifacts/jobs/minhnvq/dev-chp/spark-ops-latest.jar',
        arguments=[],
        driver_memory="2g",
        executor_instances="1",  # Tùy chỉnh số lượng executor nếu cần
        executor_memory="6g"
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