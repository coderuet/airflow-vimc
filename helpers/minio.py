from minio import Minio
from typing import Dict
import json
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def save_json_to_minio(
    client: Minio,
    bucket: str,
    object_name: str,
    json_data: Dict,
) -> None:
    """Upload a JSON payload to MinIO."""
    payload_bytes = json.dumps(json_data).encode("utf-8")
    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=io.BytesIO(payload_bytes),
        length=len(payload_bytes),
        content_type="application/json",
    )


def create_minio_client(endpoint, access_key, secret_key, use_https=False) -> Minio:

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=use_https,
    )


def ensure_bucket(client: Minio, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def save_json_to_s3(hook: S3Hook, bucket: str, key: str, payload: dict):
    raw_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    hook.load_bytes(
        raw_bytes,
        key=key,
        bucket_name=bucket,
        replace=True,
    )
