from datetime import datetime
from textwrap import dedent
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor


def _get_var(name: str, default: str) -> str:
    return Variable.get(name, default_var=default)

SPARK_NAMESPACE = _get_var("VIMC_SPARK_NAMESPACE", "vlp-tenantdvak01g-wsghwlhlt-ingestion")
SPARK_K8S_CONN_ID = _get_var("VIMC_K8S_CONN_ID", "kubernetes_default")
SPARK_IMAGE = _get_var("VIMC_SPARK_IMAGE", "192.168.74.14:80/vimc-vlp-project/spark:3.5.1-scala2.12-java11-v1.5.2.1")
SPARK_IMAGE_PULL_POLICY = _get_var("VIMC_SPARK_IMAGE_PULL_POLICY", "Always")
SPARK_IMAGE_PULL_SECRET = _get_var("VIMC_SPARK_IMAGE_PULL_SECRET", "vlp-registry")
SPARK_MAIN_JAR = _get_var("VIMC_SPARK_MAIN_JAR", "local:///opt/spark/jars/app.jar")
SPARK_SERVICE_ACCOUNT = _get_var("VIMC_SPARK_SA", "spark-application-sa")
SPARK_VERSION = _get_var("VIMC_SPARK_VERSION", "3.5.1")

DRIVER_CORES = _get_var("VIMC_DRIVER_CORES", "1")
DRIVER_CORE_LIMIT = _get_var("VIMC_DRIVER_CORE_LIMIT", "1")
DRIVER_MEMORY = _get_var("VIMC_DRIVER_MEMORY", "4g")
DRIVER_MEMORY_OVERHEAD = _get_var("VIMC_DRIVER_MEMORY_OVERHEAD", "512m")

EXECUTOR_CORES = _get_var("VIMC_EXECUTOR_CORES", "1")
EXECUTOR_CORE_LIMIT = _get_var("VIMC_EXECUTOR_CORE_LIMIT", "1")
EXECUTOR_MEMORY = _get_var("VIMC_EXECUTOR_MEMORY", "4g")
EXECUTOR_MEMORY_OVERHEAD = _get_var("VIMC_EXECUTOR_MEMORY_OVERHEAD", "512m")
EXECUTOR_INSTANCES = _get_var("VIMC_EXECUTOR_INSTANCES", "1")

def render_env_yaml(indent_spaces: int = 12, env_vars: dict = {}) -> str:
    pad = " " * indent_spaces
    env_lines = []
    for key, value in env_vars.items():
        env_lines.append(f"{pad}- name: {key}\n{pad}  value: \"{value}\"")
    return "\n".join(env_lines)

def build_spark_application_yaml(
        job_suffix: str = 'spark_suffix',
        main_class: str = 'vn.viettel.vlp_load.example',
        env_vars: dict = {},
        arguments: list = None,
        spark_image_pull_secret: str = SPARK_IMAGE_PULL_SECRET,
        spark_namespace: str = SPARK_NAMESPACE,
        spark_image: str = SPARK_IMAGE,
        spark_image_pull_policy: str = SPARK_IMAGE_PULL_POLICY,
        spark_main_jar: str = SPARK_MAIN_JAR,
        spark_version: str = SPARK_VERSION,
        spark_service_account: str = SPARK_SERVICE_ACCOUNT,
        driver_cores: str = DRIVER_CORES,
        driver_core_limit: str = DRIVER_CORE_LIMIT,
        driver_memory: str = DRIVER_MEMORY,
        driver_memory_overhead: str = DRIVER_MEMORY_OVERHEAD,
        executor_cores: str = EXECUTOR_CORES,
        executor_instances: str = EXECUTOR_INSTANCES,
        executor_core_limit: str = EXECUTOR_CORE_LIMIT,
        executor_memory: str = EXECUTOR_MEMORY,
        executor_memory_overhead: str = EXECUTOR_MEMORY_OVERHEAD,
    ):
    rundate_str = datetime.now().strftime("%Y%m%d%H%M")
    app_name = f"vimc-spark-batch-{job_suffix}-{rundate_str}"

    pull_secret_block = ""
    if spark_image_pull_secret:
        pull_secret_block = f"imagePullSecrets:\n            - {spark_image_pull_secret}"

    env_block = render_env_yaml(12, env_vars)

    # Build arguments block
    arguments_block = ""
    if arguments:
        args_lines = "\n".join([f"            - \"{arg}\"" for arg in arguments])
        arguments_block = f"arguments:\n{args_lines}"

    manifest = dedent(
        f"""
        apiVersion: "sparkoperator.k8s.io/v1beta2"
        kind: SparkApplication
        metadata:
          name: {app_name}
          namespace: {spark_namespace}
        spec:
          type: Scala
          mode: cluster
          image: "{spark_image}"
          imagePullPolicy: {spark_image_pull_policy}
          {pull_secret_block}
          mainApplicationFile: {spark_main_jar}
          mainClass: {main_class}
          sparkVersion: "{spark_version}"
          {arguments_block}
          restartPolicy:
            type: Never
          sparkConf:
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            "spark.sql.adaptive.enabled": "true"
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
            "spark.eventLog.enabled": "true"
            "spark.eventLog.dir": "s3a://vimc/vmic/spark_history"
            "spark.hadoop.fs.s3a.endpoint": "http://192.168.74.16:30090"
            "spark.hadoop.fs.s3a.path.style.access": "true"
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
          driver:
            serviceAccount: {spark_service_account}
            cores: {driver_cores}
            coreLimit: "{driver_core_limit}"
            memory: "{driver_memory}"
            memoryOverhead: "{driver_memory_overhead}"
            env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: access-key
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: secret-key
{env_block}
          executor:
            cores: {executor_cores}
            instances: {executor_instances}
            coreLimit: "{executor_core_limit}"
            memory: "{executor_memory}"
            memoryOverhead: "{executor_memory_overhead}"
            env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: access-key
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: secret-key
{env_block}
        """
    ).strip()
    print(manifest)
    return manifest, app_name

def create_spark_k8s_operator(task_id, raw_batch_manifest):
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=SPARK_NAMESPACE,
        application_file=raw_batch_manifest,
        kubernetes_conn_id=SPARK_K8S_CONN_ID,
        do_xcom_push=False,
    )

def create_spark_k8s_sensor(task_id, raw_batch_app_name):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=SPARK_NAMESPACE,
        application_name=raw_batch_app_name,
        kubernetes_conn_id=SPARK_K8S_CONN_ID,
        attach_log=True,
        poke_interval=30,
        timeout=180000,
    )

def generate_date_path_from_time(start_date : datetime, end_date : datetime):
    """Generate list of dates split by month between start_date and end_date.

    Args:
        start_date (datetime): The start date to process.
        end_date (datetime): The end date to process.
    """
    print("Processing dates...")
    # Logic to split dates by month and generate minio paths goes here
    list_paths = []
    current_date = start_date
    while current_date <= end_date:
        path_date = f"{current_date.year}/{current_date.month:02d}"
        list_paths.append(path_date)
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1, day=1)
    return list_paths