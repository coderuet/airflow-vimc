from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import platform
import os

# Hàm Python để in thông tin hệ thống
def print_python_info():
    print(f"--- PYTHON SYSTEM INFO ---")
    print(f"Python Version: {platform.python_version()}")
    print(f"OS: {platform.system()} {platform.release()}")
    print(f"Current PID: {os.getpid()}")
    print(f"--------------------------")

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'dev-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'test_cicd_gitlab_v2', # ID này sẽ hiện trên UI
    default_args=default_args,
    description='DAG kiểm tra luồng CI/CD tự động từ GitLab',
    schedule_interval=None, # Chạy thủ công (Manual Trigger)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['cicd', 'test', 'system'],
) as dag:

    # Task 1: Dùng Bash để kiểm tra User và Hostname của Pod
    t1 = BashOperator(
        task_id='check_pod_identity',
        bash_command='echo "Task đang chạy bởi User: $(whoami) trên Pod: $(hostname)"'
    )

    # Task 2: Dùng Python để in thông tin môi trường
    t2 = PythonOperator(
        task_id='check_python_env',
        python_callable=print_python_info
    )

    # Task 3: Liệt kê file trong thư mục hiện tại để chắc chắn Worker thấy code
    t3 = BashOperator(
        task_id='list_current_files',
        bash_command='ls -la'
    )

    # Định nghĩa luồng chạy: t1 xong -> t2 xong -> t3
    t1 >> t2 >> t3