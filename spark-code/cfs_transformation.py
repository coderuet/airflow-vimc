import argparse
import os
from pathlib import Path

from pyspark import SparkFiles
from pyspark.sql import SparkSession, functions as F

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python <3.11 fallback
    import tomli as tomllib

CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "cfs.toml"
DEFAULT_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio-server:9000")
DEFAULT_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "MINIO_ACCESS_KEY")
DEFAULT_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "MINIO_SECRET_KEY")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform bronze CFS payloads to Delta silver outputs."
    )
    parser.add_argument("--bronze-bucket", required=True, help="Bucket that stores bronze data")
    parser.add_argument("--bronze-prefix", default="bronze", help="Root prefix under the bucket containing bronze loads")
    parser.add_argument("--silver-bucket", required=True, help="Bucket that stores the silver Delta tables")
    parser.add_argument("--silver-prefix", default="silver", help="Root prefix for silver outputs")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="MinIO/S3 endpoint")
    parser.add_argument("--access-key", default=DEFAULT_ACCESS_KEY, help="Credential for the endpoint")
    parser.add_argument("--secret-key", default=DEFAULT_SECRET_KEY, help="Credential for the endpoint")
    parser.add_argument("--config-path", help="Optional override for cfs.toml when not shipped via --files")
    return parser.parse_args()


def resolve_config_path(override_path=None):
    candidates = []
    if override_path:
        candidates.append(override_path)
    env_path = os.environ.get("CFS_CONFIG_PATH")
    if env_path:
        candidates.append(env_path)
    try:
        spark_file = SparkFiles.get("cfs.toml")
    except Exception:
        spark_file = None
    if spark_file:
        candidates.append(spark_file)
    candidates.append(CONFIG_PATH)

    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if path.exists():
            return path
    raise FileNotFoundError(
        "Unable to locate cfs.toml. Provide --config-path, set CFS_CONFIG_PATH, or ship the file with spark-submit."
    )


def load_config(config_path=None):
    path = resolve_config_path(config_path)
    with path.open("rb") as fp:
        return tomllib.load(fp)


def spark_init(endpoint, access_key, secret_key):
    return (
        SparkSession.builder
        .appName("CFS Bronze to Silver Transformation")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def _fs_and_path(spark, uri):
    """Return (FileSystem, Path) helper for s3a URIs."""
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hadoop_conf)
    return fs, path


def _path_exists(spark, uri):
    fs, path = _fs_and_path(spark, uri)
    return fs.exists(path)


def list_full_load_dirs(spark, base_uri):
    fs, path = _fs_and_path(spark, base_uri)
    if not fs.exists(path):
        return []

    statuses = fs.listStatus(path)
    candidates = []
    for status in statuses:
        if not status.isDirectory():
            continue
        name = status.getPath().getName()
        if not name.startswith("full_load_ts="):
            continue
        try:
            ts_value = int(name.split("=", 1)[1])
            candidates.append((ts_value, name))
        except ValueError:
            continue

    candidates.sort(key=lambda item: item[0], reverse=True)
    return [name for _, name in candidates]


def transform_dataset(spark, bronze_file, silver_base, api_name, company_id, ts_label):
    if not _path_exists(spark, bronze_file):
        print(f"Skipping missing bronze file: {bronze_file}")
        return

    df = spark.read.json(bronze_file)
    if df.rdd.isEmpty():
        print(f"No rows found in {bronze_file}; nothing to write")
        return

    enriched = (
        df.withColumn("from_file", F.lit(api_name))
        .withColumn("company_id", F.lit(company_id))
        .withColumn("load_time", F.current_timestamp())
    )
    output = f"{silver_base}/{ts_label}/{api_name}"
    enriched.write.format("delta").mode("overwrite").save(output)
    print(f"Saved silver Delta output to {output}")


def process_company(spark, company, args):
    company_id = company["id"]
    bronze_base = f"s3a://{args.bronze_bucket}/{args.bronze_prefix}/{company_id}/cfs"
    silver_base = f"s3a://{args.silver_bucket}/{args.silver_prefix}/{company_id}/cfs"
    available_dirs = list_full_load_dirs(spark, bronze_base)
    if not available_dirs:
        print(f"No bronze loads found for company {company_id}; skipping")
        return

    for api_name in company.get("cfs_apis", []):
        ts_label = None
        for directory in available_dirs:
            candidate = f"{bronze_base}/{directory}/{api_name}.json"
            if _path_exists(spark, candidate):
                ts_label = directory
                bronze_file = candidate
                break
        if not ts_label:
            print(f"No bronze file found for {company_id}::{api_name}; skipping")
            continue
        transform_dataset(spark, bronze_file, silver_base, api_name, company_id, ts_label)


def main():
    args = parse_args()
    spark = spark_init(args.endpoint, args.access_key, args.secret_key)
    try:
        config = load_config(args.config_path)
        companies = config.get("company", [])
        if not companies:
            print("No companies configured in cfs.toml; exiting.")
            return
        for company in companies:
            process_company(spark, company, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
