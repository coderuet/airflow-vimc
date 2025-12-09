from minio import Minio
from typing import Dict
import json
import io


def save_json_to_minio(
    client: Minio,
    bucket: str,
    object_name: str,
    json_data: Dict,
) -> None:
    """Upload a JSON payload to MinIO."""
    payload_bytes = json.dumps(json_data).encode("utf-8")
    print("xxx", json_data)
    print("running upload")
    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=io.BytesIO(payload_bytes),
        length=len(payload_bytes),
        content_type="application/json",
    )
    print("upload successfully")


def create_minio_client(endpoint, access_key, secret_key, use_https=False) -> Minio:
    print("endpoint", endpoint)
    print("access_key", access_key)
    print("secret_key", secret_key)
    print("use_https", use_https)

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=use_https,
    )


def ensure_bucket(client: Minio, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
