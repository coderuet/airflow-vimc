import io
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import tomli as tomllib
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio

from apis.category import CategoryAPI
from helpers.helper import send_error_to_discord
from helpers.logger import get_logger

logger = get_logger()
CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "category.toml"
try:
    DEFAULT_UPLOAD_WORKERS = max(
        1, int(os.getenv("MINIO_UPLOAD_WORKERS", "4")))
except ValueError:
    DEFAULT_UPLOAD_WORKERS = 4


def failure_callback(context):
    error_message = str(context.get("exception", "Unknown failure"))
    send_error_to_discord(error_message)


def load_config():
    with CONFIG_PATH.open("rb") as f:
        config = tomllib.load(f)
    logger.info("Loaded %d company configs from %s",
                len(config.get("company", [])), CONFIG_PATH)
    return config


def create_minio_client(minio_config: dict) -> Minio:
    return Minio(
        minio_config["endpoint"],
        access_key=os.getenv("MINIO_ACCESS_KEY", minio_config["access_key"]),
        secret_key=os.getenv("MINIO_SECRET_KEY", minio_config["secret_key"]),
        secure=minio_config.get("secure", True),
    )


def _ensure_bucket(client: Minio, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def save_json_to_minio(client: Minio, bucket: str, object_name: str, payload: dict):
    raw_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket,
        object_name,
        io.BytesIO(raw_bytes),
        length=len(raw_bytes),
        content_type="application/json",
    )


def upload_payloads(client: Minio, bucket: str, payload_specs: list, max_workers: int = DEFAULT_UPLOAD_WORKERS):
    if not payload_specs:
        logger.info("No payloads queued for upload")
        return

    worker_count = min(max_workers, len(payload_specs))
    logger.info(
        "Uploading %d payloads to bucket %s using %d workers",
        len(payload_specs),
        bucket,
        worker_count,
    )

    errors = []
    # with ThreadPoolExecutor(max_workers=worker_count) as executor:
    #     future_map = {
    #         executor.submit(save_json_to_minio, client, bucket, object_name, payload): (company_id, object_name)
    #         for company_id, object_name, payload in payload_specs
    #     }

    #     for future in as_completed(future_map):
    #         company_id, object_name = future_map[future]
    #         try:
    #             future.result()
    #             logger.info(
    #                 "Uploaded aggregated payload for company %s to %s/%s",
    #                 company_id,
    #                 bucket,
    #                 object_name,
    #             )
    #         except Exception as exc:  # noqa: BLE001 - we want to surface upload failures
    #             logger.exception(
    #                 "Failed uploading payload for company %s to %s/%s", company_id, bucket, object_name
    #             )
    #             errors.append((company_id, object_name, exc))

    # if errors:
    #     error_summaries = ", ".join(
    #         f"{company_id}:{object_name} -> {error}" for company_id, object_name, error in errors)
    #     raise RuntimeError(
    #         f"Failed to upload {len(errors)} payload(s) to MinIO: {error_summaries}")


def ingest_category(**context):
    config = load_config()
    companies = config.get("company", [])
    if not companies:
        logger.warning("No company configuration found; nothing to ingest.")
        return

    minio_config = config.get("minio")
    if not minio_config:
        raise ValueError(
            "Missing [minio] configuration block in category.toml")

    minio_client = create_minio_client(minio_config)
    bucket = minio_config["bucket"]
    _ensure_bucket(minio_client, bucket)

    execution_date = context['execution_date']
    # date format example : 2025/01/11
    formatted_date = execution_date.strftime("%Y/%m/%d")
    upload_jobs = []
    for company in companies:
        company_id = company["id"]
        prefix = f"bronze/{company_id}/{formatted_date}"
        base_url = company["base_url"]
        apis = company.get("category_apis", [])
        if not apis:
            logger.info(
                "Company %s does not have any APIs configured; skipping", company_id)
            continue

        logger.info("Fetching %d Category APIs for company %s",
                    len(apis), company_id)
        api_client = CategoryAPI(base_url=base_url)

        company_payload = api_client.fetch_many(apis)
        for resource_name, data_api in company_payload.items():
            object_name = f"{prefix}/{resource_name}.json"
            upload_jobs.append((company_id, object_name, data_api))

    upload_payloads(minio_client, bucket, upload_jobs)


with DAG(
    "category",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": failure_callback,
        "trigger_rule": "all_success",
    },
    description="Fetch Category APIs for all configured companies and store them on MinIO.",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["category", "ingestion"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_category",
        python_callable=ingest_category,
    )
    # transformation use spark -> delta lake
