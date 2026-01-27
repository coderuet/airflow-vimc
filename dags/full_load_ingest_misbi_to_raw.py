from airflow import DAG
from datetime import datetime
from pathlib import Path

from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context

from helpers.spark_helper import build_spark_application_yaml, create_spark_k8s_operator, create_spark_k8s_sensor

default_args = {
    'owner': 'vimc_dev',
    'start_date': datetime(2024, 1, 1),
}


def _build_manifest_file(job_suffix: str, main_class: str, spark_main_jar: str, **kwargs) -> str:
    """Build manifest at runtime using dag_run.conf for parameters and write to a temp file.

    Returns path to the manifest file (string)."""
    conf = {}
    try:
        context = get_current_context()
        dag_run = context.get('dag_run')
        if dag_run and getattr(dag_run, 'conf', None):
            conf = dag_run.conf
    except Exception:
        conf = {}

    runtime_start = conf.get('start_date') if conf else None

    env_vars = {}
    args = []
    if runtime_start:
        env_vars['PROCESS_START_DATE'] = runtime_start
        args.append(runtime_start)

    manifest, app_name = build_spark_application_yaml(
        job_suffix=job_suffix,
        main_class=main_class,
        spark_main_jar=spark_main_jar,
        arguments=args,
        env_vars=env_vars,
        executor_instances="2",
        executor_memory="2g",
    )

    out_path = Path(f"/tmp/{app_name}.yaml")
    out_path.write_text(manifest)
    return str(out_path)


with DAG(
    dag_id='full_load_ingest_misbi_to_raw',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['full_load', 'ingest', 'misbi']
) as dag:

    build_manifest = PythonOperator(
        task_id='build_manifest',
        python_callable=_build_manifest_file,
        op_kwargs={
            'job_suffix': 'ingest-full-load-misbi-to-raw',
            'main_class': 'vn.viettel.vlp_load.ingestion.full_load.file.sharepoint.SharePoint',
            'spark_main_jar': 's3a://vimc/vimc/spark-artifacts/jobs/thiennt/ingest_crm_v2/spark-ops-latest.jar',
        },
    )

    submit_job = create_spark_k8s_operator(
        task_id='submit_spark_job',
        raw_batch_manifest="{{ ti.xcom_pull(task_ids='build_manifest') }}",
    )

    wait_for_job = create_spark_k8s_sensor(
        task_id='wait_for_spark_job',
        raw_batch_app_name="{{ ti.xcom_pull(task_ids='build_manifest') | replace('/tmp/','') | replace('.yaml','') }}",
    )

    build_manifest >> submit_job >> wait_for_job