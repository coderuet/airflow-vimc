from datetime import datetime

from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka import KafkaSensor
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
# from airflow

with DAG(
    dag_id="kafka_sensor",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "ingestion"],
) as dag:

    # 1. Wait for a message on a Kafka topic
    wait_for_kafka_message = KafkaSensor(
        task_id="wait_for_kafka_message",
        topic="batch_ready",
        kafka_conn_id="kafka_sftp",
        consumer_config={
            "bootstrap.servers": "kafka:9092",
            "group.id": "airflow-kafka-sensor",
            "auto.offset.reset": "earliest",
        },
        # how often to poll (seconds)
        poll_timeout=5,
        # maximum time to wait (seconds)
        timeout=60 * 10,  # 10 minutes
        # fail the task if timeout happens
        soft_fail=False,
    )

    # 2. Run a pod in Kubernetes after the Kafka message is seen
    process_message = KubernetesPodOperator(
        task_id="process_message_in_k8s",
        name="process-message-pod",
        namespace="airflow",            # your namespace
        image="python:3.11-slim",
        cmds=["python", "-c"],
        arguments=["print('Processing message from Kafka...')"],
        get_logs=True,
        is_delete_operator_pod=True,    # delete pod after completion
        in_cluster=True,                # True if Airflow runs inside the cluster
    )
    # process_message_task = PythonOperator(
    #     task_id="process_kafka_message",
    #     python_callable=process_message,
    #     op_args=["{{ ti.xcom_pull('wait_for_kafka_message') }}"],
    # )

    wait_for_kafka_message >> process_message
