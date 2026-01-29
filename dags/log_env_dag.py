from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import logging

try:
    from airflow.utils.session import create_session
except Exception:
    create_session = None

default_args = {
    'owner': 'vimc_dev',
    'start_date': datetime(2024, 1, 1),
}

def _get_all_variables():
    if hasattr(Variable, "get_all"):
        return Variable.get_all()
    if create_session is None:
        return {}

    try:
        with create_session() as session:
            keys = [row.key for row in session.query(Variable).all()]
    except Exception:
        return {}

    return {key: Variable.get(key) for key in keys}


def log_all_variables():
    logger = logging.getLogger(__name__)

    # Lấy tất cả variables dưới dạng dictionary
    all_vars = _get_all_variables()

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