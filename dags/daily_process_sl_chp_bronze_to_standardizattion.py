from airflow import DAG
from datetime import datetime

from helpers.spark_helper import build_spark_application_yaml, create_spark_k8s_operator, create_spark_k8s_sensor

default_args = {
    'owner': 'vimc_dev',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='daily_process_sl_chp_bronze_to_standardization',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['daily', 'process', 'sl_chp','standardization']
) as dag:

    # 1. Định nghĩa Manifest cho Spark Job
    # Helper sẽ tự động điền các thông tin về S3, Image, và Credentials
    raw_manifest, app_name = build_spark_application_yaml(
        job_suffix='daily-process-sl-chp-bronze-to-standardization',
        main_class='vn.viettel.vlp_load.jobs.BronzeToStandardizationJob',
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