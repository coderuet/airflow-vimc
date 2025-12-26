from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago

# --- CẤU HÌNH CHUNG ---
NAMESPACE = 'vlp-tenantdvak01g-wsghwlhlt-data'
HARBOR_HOST = '192.168.74.14:80'
IMAGE_NAME = 'vimc-vlp-project/spark-ops'
# Luôn lấy tag 'latest' để tự cập nhật code mới nhất từ CI/CD
FULL_IMAGE = f"{HARBOR_HOST}/{IMAGE_NAME}:latest" 

# --- HÀM HELPER: TẠO YAML CHO SPARK APP ---
def get_spark_yaml(job_name, main_class, driver_mem="1024m", executor_mem="1024m"):
    return f"""
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: "spark-{job_name}-{{{{ run_id | replace('_', '-') | lower | truncate(20, True, '') }}}}"
  namespace: {NAMESPACE}
spec:
  type: Scala
  mode: cluster
  image: "{FULL_IMAGE}"
  imagePullPolicy: Always  # BẮT BUỘC: Để Kubernetes luôn tải Image mới nhất
  
  mainClass: "{main_class}"  
  mainApplicationFile: "local:///opt/spark/jars/app.jar"
  
  sparkVersion: "3.1.1"
  restartPolicy:
    type: Never
  
  imagePullSecrets:
    - "vlp-registry"
  timeToLiveSeconds: 3600
  
  driver:
    cores: 1
    memory: "{driver_mem}"
    serviceAccount: spark-operator-sa
    labels:
      version: 3.1.1
  
  executor:
    instances: 1
    cores: 1
    memory: "{executor_mem}"
    labels:
      version: 3.1.1
"""

# --- ĐỊNH NGHĨA DAG ---
with DAG(
    dag_id='daily_data_processing', 
    default_args={'owner': 'trungnp'},
    schedule_interval='@daily',      # Tự động chạy hàng ngày
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'production'],
) as dag:

    # --- JOB 1: Tính toán PI ---
    task_pi = SparkKubernetesOperator(
        task_id='run_pi_calculation',
        namespace=NAMESPACE,
        application_file=get_spark_yaml(
            job_name="pi",
            main_class="com.example.SparkPi" 
        )
    )

    # --- JOB 2: Work Count  ---
    task_newCI = SparkKubernetesOperator(
        task_id='run_newCI',
        namespace=NAMESPACE,
        application_file=get_spark_yaml(
            job_name="newCI",
            main_class="vn.viettel.code.ingestion.vimc.cfs.Purchase",
            driver_mem="2g"
        )
    )

    task_pi >> task_newCI
