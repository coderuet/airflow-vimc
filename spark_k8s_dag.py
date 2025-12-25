from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago

# --- CẤU HÌNH ---
NAMESPACE = 'vlp-tenantdvak01g-wsghwlhlt-data'
HARBOR_HOST = '192.168.74.14:80' # Thêm port 80 cho rõ ràng (tuỳ chọn)
IMAGE_NAME = 'vimc-vlp-project/spark-ops' 

with DAG(
    dag_id='my_spark_dag',
    default_args={'owner': 'trungnp'},
    schedule_interval=None, 
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'k8s', 'cicd'],
) as dag:

    spark_task = SparkKubernetesOperator(
        task_id='submit_spark_job',
        namespace=NAMESPACE,
        application_file="""
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: "spark-app-{{ run_id | replace('_', '-') | lower | truncate(40, True, '') }}"
  namespace: {{ params.namespace }}
spec:
  type: Scala
  mode: cluster
  
  # Ghép chuỗi: 192.168.74.14:80/vimc-vlp-project/spark-ops:<tag_từ_gitlab>
  image: "{{ params.harbor }}/{{ params.image_name }}:{{ dag_run.conf.get('image_tag', 'latest') }}"
  
  imagePullPolicy: Always
  
  mainClass: "{{ dag_run.conf.get('main_class', 'com.company.project.DataPipelineEntry') }}"
  mainApplicationFile: "local:///opt/spark/jars/app.jar"
  
  sparkVersion: "3.1.1"
  restartPolicy:
    type: Never
  
  imagePullSecrets:
    - "vlp-registry"
  
  driver:
    cores: 1
    memory: "1024m"
    serviceAccount: spark-operator-sa
    labels:
      version: 3.1.1
  
  executor:
    instances: 1
    cores: 1
    memory: "1024m"
    labels:
      version: 3.1.1
""",
        # Truyền biến Python vào Template YAML
        params={
            'namespace': NAMESPACE,
            'harbor': HARBOR_HOST,
            'image_name': IMAGE_NAME
        }
    )
