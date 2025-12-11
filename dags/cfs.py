import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from apis.cfs import CfsAPI

import tomli as tomllib
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

from helpers.helper import send_error_to_discord
from helpers.logger import get_logger

logger = get_logger()
CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "cfs.toml"

# Use your Airflow connection ID
AWS_CONN_ID = "minio"  # <--- Airflow connection name
BUCKET_NAME = Variable.get("MINIO_BUCKET", default_var="")


def failure_callback(context):
    error_message = str(context.get("exception", "Unknown failure"))
    send_error_to_discord(error_message)


def load_config():
    with CONFIG_PATH.open("rb") as f:
        config = tomllib.load(f)
    logger.info(
        "Loaded %d company configs from %s", len(config.get("company", [])), CONFIG_PATH
    )
    return config


def save_json_to_s3(hook: S3Hook, bucket: str, key: str, payload: dict):
    raw_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    hook.load_bytes(
        raw_bytes,
        key=key,
        bucket_name=bucket,
        replace=True,
    )


with DAG(
    "cfs",
    default_args={
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": failure_callback,
    },
    description="Fetch CFS APIs and store result in MinIO via Airflow S3 connection",
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["cfs", "ingestion"],
) as dag:

    @task
    def download_api(spec):
        """
        spec = {
            "label": ...,
            "company_id": ...,
            "base_url": ...,
            "username": ...,
            "password": ...,
            "book_id": ...,
            "api_name": ...,
            "start_date": ...,
            "end_date": ...,
            "prefix": ...,
            "load_ts": ...,
            "load_type": ...,
        }
        """
        api_client = CfsAPI(
            base_url=spec["base_url"],
            username=spec.get("username", ""),
            password=spec.get("password", ""),
        )

        data_post = {
            "start_date": spec["start_date"],
            "end_date": spec["end_date"],
            "set_of_book_id": spec.get("book_id", ""),
        }
        payload = api_client.fetch_resource(spec["api_name"], json=data_post)
        year = spec["start_date"][:4]
        month = spec["start_date"][4:6]
        day = spec["start_date"][6:8]
        ms = int(time.time_ns() / 1_000_000)
        prefix = (
            f"vmic/data/bronze_zone/{spec['company_id']}/cfs/incremental/"
            f"event_dt={year}/{month}/{day}/load_ts={ms}"
        )
        return {
            "label": spec["label"],
            "company_id": spec["company_id"],
            "key": f"{prefix}/{spec['api_name']}.json",
            "payload": payload,
        }

    @task
    def build_download_specs(**context):
        start_date_param = context["dag_run"].conf.get("start_date")
        end_date_param = context["dag_run"].conf.get("end_date")
        config = load_config()
        companies = config.get("company", [])
        if not companies:
            return []

        ds = context["ds"]  # execution date in "YYYY-MM-DD" format
        execution_date = datetime.strptime(ds, "%Y-%m-%d")
        # check if start_date_param and end_date_param are provided
        if start_date_param and end_date_param:
            start_date = datetime.strptime(start_date_param, "%Y%m%d")
            end_date = datetime.strptime(end_date_param, "%Y%m%d")
        else:
            start_date = execution_date - timedelta(days=1)
            end_date = execution_date

        specs = []

        for company in companies:
            company_id = company["id"]

            for api_name in company.get("cfs_apis", []):
                current_date = start_date
                while current_date <= end_date:
                    current_date = current_date + timedelta(days=1)
                    specs.append(
                        {
                            "label": f"__{company_id}__{api_name}",
                            "company_id": company_id,
                            "api_name": api_name,
                            "base_url": company["base_url"],
                            "username": company.get("username", ""),
                            "password": company.get("password", ""),
                            "book_id": company.get("book_id", ""),
                            "start_date": datetime.strftime(
                                current_date.replace(day=1), "%Y%m%d"
                            ),
                            "end_date": datetime.strftime(current_date, "%Y%m%d"),
                        }
                    )

        return specs

    @task
    def upload_to_s3(spec):
        """
        spec = { "company_id": ..., "key": ..., "payload": ... }
        """
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        try:
            save_json_to_s3(
                hook, bucket=BUCKET_NAME, key=spec["key"], payload=spec["payload"]
            )
            logger.info("Uploaded %s", spec["key"])
        except Exception as exc:
            logger.exception("Failed upload for %s", spec["key"])
            raise exc

    download_specs = build_download_specs()
    downloaded_results = download_api.expand(
        spec=download_specs,
    )

    upload_to_s3.expand(
        spec=downloaded_results,
    )
