from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import logging

default_args = {
    'owner': 'vimc_dev',
    'start_date': datetime(2024, 1, 1),
}

def log_all_variables():
    logger = logging.getLogger(__name__)

    # Lấy tất cả variables dưới dạng dictionary
    all_vars = Variable.get_all()

    logger.info("=" * 50)
    logger.info("AIRFLOW VARIABLES")
    logger.info("=" * 50)

    if not all_vars:
        logger.info("No variables found")
        return

    for key, value in all_vars.items():
        logger.info(f"{key}: {value}")

    logger.info("=" * 50)
    logger.info(f"Total variables: {len(all_vars)}")

with DAG(
    dag_id='log_airflow_variables',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['utility', 'logging']
) as dag:

    log_vars_task = PythonOperator(
        task_id='log_all_variables',
        python_callable=log_all_variables,
    )