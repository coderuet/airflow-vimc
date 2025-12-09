"""
CFS Full Load Spark Job
Extracts data from CFS API for multiple companies and saves to bronze/silver layers.
"""
import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import tomllib
from pyspark import SparkFiles
from pyspark.sql import SparkSession, DataFrame, functions as F

# Import will work because APIs module is shipped via py_files in spark-submit
from apis.cfs import CfsAPI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "cfs.toml"
DEFAULT_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio-server:9000")
DEFAULT_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "MINIO_ACCESS_KEY")
DEFAULT_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "MINIO_SECRET_KEY")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the CFS API full load job with Spark."
    )
    parser.add_argument(
        "--bronze-path",
        required=True,
        help="S3 path (without scheme) for bronze landing zone.",
    )
    parser.add_argument(
        "--silver-path",
        required=True,
        help="S3 path (without scheme) for silver Delta zone.",
    )
    parser.add_argument(
        "--config-path",
        help="Path to cfs.toml config file.",
    )
    parser.add_argument(
        "--start-date",
        default="20230101",
        help="Start date (YYYYMMDD) for extraction.",
    )
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help="MinIO/S3 endpoint.",
    )
    parser.add_argument(
        "--access-key",
        default=DEFAULT_ACCESS_KEY,
        help="MinIO/S3 access key.",
    )
    parser.add_argument(
        "--secret-key",
        default=DEFAULT_SECRET_KEY,
        help="MinIO/S3 secret key.",
    )
    return parser.parse_args()


def resolve_config_path(override_path: Optional[str] = None) -> Path:
    """
    Resolve the config file path from multiple sources.
    Priority: CLI arg > env var > SparkFiles > default path
    """
    candidates = []
    
    if override_path:
        candidates.append(override_path)
    
    env_path = os.environ.get("CFS_CONFIG_PATH")
    if env_path:
        candidates.append(env_path)
    
    # Check if file was shipped with spark-submit via --files
    try:
        spark_file = SparkFiles.get("cfs.toml")
        candidates.append(spark_file)
    except Exception:
        pass
    
    candidates.append(CONFIG_PATH)

    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if path.exists():
            logger.info(f"Using config file: {path}")
            return path
    
    raise FileNotFoundError(
        "Unable to locate cfs.toml. Options: "
        "1) Pass --config-path, "
        "2) Set CFS_CONFIG_PATH env var, "
        "3) Ship with spark-submit --files"
    )


def load_config(config_path: Optional[str] = None) -> Dict:
    """Load TOML configuration file."""
    path = resolve_config_path(config_path)
    with path.open("rb") as f:
        config = tomllib.load(f)
    logger.info(f"Loaded config with {len(config.get('company', []))} companies")
    return config


def create_spark_session(endpoint: str, access_key: str, secret_key: str) -> SparkSession:
    """Initialize Spark session with S3/MinIO and Delta Lake configurations."""
    logger.info("Initializing Spark session...")
    return (
        SparkSession.builder
        .appName("CFS Full Load")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def normalize_records(data) -> List[Dict]:
    """Normalize API response to list of records."""
    if not data:
        return []
    if isinstance(data, list):
        return data
    return [data]


def generate_month_ranges(start: str = "20230101") -> Tuple[str, str]:
    """
    Generate month ranges from start date to today.
    Yields: (start_date, end_date) tuples in YYYYMMDD format.
    """
    start_dt = datetime.strptime(start, "%Y%m%d")
    today = datetime.today()

    while start_dt <= today:
        month_start = start_dt.replace(day=1)
        next_month = (month_start.replace(day=28) + timedelta(days=5)).replace(day=1)
        month_end = next_month - timedelta(days=1)
        
        yield month_start.strftime("%Y%m%d"), month_end.strftime("%Y%m%d")
        start_dt = next_month


def build_cfs_client(company: Dict) -> CfsAPI:
    """Create CFS API client from company configuration."""
    return CfsAPI(
        base_url=company["base_url"],
        username=company.get("username"),
        password=company.get("password"),
    )


def write_bronze_data(
    spark: SparkSession,
    records: List[Dict],
    output_path: str
) -> None:
    """Write raw records to bronze layer as JSON."""
    if not records:
        logger.warning(f"No records to write to {output_path}")
        return
    
    json_rdd = spark.sparkContext.parallelize(
        [json.dumps(record) for record in records]
    )
    df = spark.read.json(json_rdd)
    df.write.mode("overwrite").json(output_path)
    logger.info(f"✓ Bronze: {len(records)} records → {output_path}")


def write_silver_data(
    spark: SparkSession,
    bronze_path: str,
    silver_base: str,
    api_name: str,
    company_id: str,
    ts_label: str,
    month_label: str,
) -> None:
    """Read bronze data and write enriched data to silver Delta layer."""
    df = spark.read.json(bronze_path)
    
    if df.rdd.isEmpty():
        logger.warning(f"Empty bronze data at {bronze_path}, skipping silver write")
        return

    # Enrich with metadata
    enriched = (
        df
        .withColumn("from_file", F.lit(bronze_path))
        .withColumn("company_id", F.lit(company_id))
        .withColumn("load_time", F.current_timestamp())
        .withColumn("month", F.lit(month_label))
    )
    
    output_path = f"{silver_base}/{ts_label}/{api_name}"
    
    # Use Delta Lake's replace where for efficient partition updates
    enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"month = '{month_label}'") \
        .partitionBy("month") \
        .save(output_path)
    
    logger.info(f"✓ Silver: Delta table → {output_path}")


def process_company(
    spark: SparkSession,
    company: Dict,
    bronze_path: str,
    silver_path: str,
    start_date: str,
) -> None:
    """Process all APIs for a single company."""
    company_id = company["id"]
    book_id = company.get("book_id", "")
    
    logger.info(f"Processing company: {company_id}")
    
    bronze_base = f"s3a://{bronze_path}/{company_id}/cfs/full_load"
    silver_base = f"s3a://{silver_path}/{company_id}/cfs/full_load"
    
    api_client = build_cfs_client(company)
    
    try:
        for api_name in company.get("cfs_apis", []):
            logger.info(f"  API: {api_name}")
            ts_label = f"full_load_ts={int(time.time_ns() / 1_000_000)}"
            
            for start, end in generate_month_ranges(start_date):
                month_label = datetime.strptime(start, "%Y%m%d").strftime("%Y-%m")
                
                # Fetch data from API
                payload = {
                    "start_date": start,
                    "end_date": end,
                    "set_of_book_id": book_id,
                }
                
                try:
                    data = api_client.fetch_resource(api_name, payload)
                    records = normalize_records(data)
                    
                    if not records:
                        logger.info(f"    {month_label}: No data")
                        continue
                    
                    # Write to bronze
                    bronze_output = (
                        f"{bronze_base}/{ts_label}/{api_name}/month={month_label}"
                    )
                    write_bronze_data(spark, records, bronze_output)
                    
                    # Write to silver
                    write_silver_data(
                        spark,
                        bronze_output,
                        silver_base,
                        api_name,
                        company_id,
                        ts_label,
                        month_label,
                    )
                    
                except Exception as e:
                    logger.error(
                        f"    {month_label}: Failed - {str(e)}",
                        exc_info=True
                    )
                    # Continue processing other months
                    continue
                    
    finally:
        api_client.close()


def main(
    bronze_path: str,
    silver_path: str,
    config_path: Optional[str] = None,
    start_date: str = "20230101",
    endpoint: str = DEFAULT_ENDPOINT,
    access_key: str = DEFAULT_ACCESS_KEY,
    secret_key: str = DEFAULT_SECRET_KEY,
) -> None:
    """Main entry point for the Spark job."""
    logger.info("=" * 80)
    logger.info("CFS Full Load Job Started")
    logger.info("=" * 80)
    
    spark = create_spark_session(endpoint, access_key, secret_key)
    
    try:
        config = load_config(config_path)
        companies = config.get("company", [])
        
        if not companies:
            logger.warning("No companies configured in cfs.toml")
            return
        
        for company in companies:
            try:
                process_company(
                    spark,
                    company,
                    bronze_path,
                    silver_path,
                    start_date,
                )
            except Exception as e:
                logger.error(
                    f"Failed to process company {company.get('id', 'unknown')}: {str(e)}",
                    exc_info=True
                )
                # Continue with next company
                continue
        
        logger.info("=" * 80)
        logger.info("CFS Full Load Job Completed Successfully")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    args = parse_args()
    main(
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        config_path=args.config_path,
        start_date=args.start_date,
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )