
%idle_timeout 60
%glue_version 4.0
%worker_type G.1X
%number_of_workers 5

import sys
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("sf_attachments_notebook_job", {})

print("Glue session ready.")


# =============================================================================
# CELL 2 — Hardcoded config (edit these values for your environment)
# =============================================================================
import boto3
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from pyspark.sql import functions as F

# --- Secrets Manager: only client_id / client_secret come from here ---
SECRET_NAME = "salesforce/connected-app"     # <-- edit: your Secrets Manager secret name

# --- Everything else: hardcoded ---
SF_LOGIN_URL = "https://login.salesforce.com"       # <-- use https://test.salesforce.com for sandbox
SF_API_VERSION = "60.0"

INPUT_CSV_PATH = "s3://your-bucket/input/attachment_ids.csv"   # <-- edit
OUTPUT_BUCKET = "your-bucket"                                   # <-- edit
OUTPUT_PREFIX = "salesforce-attachments"                        # <-- edit
BENCHMARK_OUTPUT_PATH = "s3://your-bucket/benchmarks/"          # <-- edit
FAILED_IDS_OUTPUT_PATH = "s3://your-bucket/benchmarks/failed_ids/"  # <-- edit: failed attachment Ids for retry

MAX_WORKERS = 30  # threads per Spark partition — I/O-bound (network wait), so this can exceed
                   # core count. Push higher (40-50+) if Salesforce has no concurrency limit and
                   # you still see headroom in the benchmark throughput numbers.

# --- Row selection for this run (useful for benchmarking before scaling to millions) ---
# ROW_SELECT_MODE options:
#   "ALL"     -> process every row in the input CSV
#   "LIMIT"   -> process only the first ROW_LIMIT rows (deterministic, good for quick tests)
#   "SAMPLE"  -> process a random sample, fraction of rows = ROW_SAMPLE_FRACTION (0.0-1.0)
ROW_SELECT_MODE = "LIMIT"          # <-- edit: "ALL" | "LIMIT" | "SAMPLE"
ROW_LIMIT = 1000                   # <-- edit: used when ROW_SELECT_MODE == "LIMIT"
ROW_SAMPLE_FRACTION = 0.01         # <-- edit: used when ROW_SELECT_MODE == "SAMPLE" (1% here)
ROW_SAMPLE_SEED = 42               # <-- edit: for reproducible sampling


def get_sf_client_credentials(secret_name):
    """Fetch {client_id, client_secret} JSON from Secrets Manager."""
    sm = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=secret_name)
    secret = json.loads(resp["SecretString"])
    return secret["client_id"], secret["client_secret"]


SF_CLIENT_ID, SF_CLIENT_SECRET = get_sf_client_credentials(SECRET_NAME)
print("Pulled Salesforce client_id/client_secret from Secrets Manager.")

sf_config = {
    "client_id": SF_CLIENT_ID,
    "client_secret": SF_CLIENT_SECRET,
    "login_url": SF_LOGIN_URL,
    "api_version": SF_API_VERSION,
}

sf_config_bc = sc.broadcast(sf_config)
output_bucket_bc = sc.broadcast(OUTPUT_BUCKET)
output_prefix_bc = sc.broadcast(OUTPUT_PREFIX)
max_workers_bc = sc.broadcast(MAX_WORKERS)


# =============================================================================
# CELL 3 — Auth + per-file download/upload + partition worker
# =============================================================================
def get_salesforce_session(cfg):
    """
    OAuth2 Client Credentials flow — no username/password/security token needed.
    Requires the Connected App to have the "Client Credentials Flow" enabled,
    with a "Run As" user configured (Setup -> App Manager -> your Connected App
    -> Edit Policies -> Client Credentials Flow).
    """
    token_url = f"{cfg['login_url']}/services/oauth2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": cfg["client_id"],
        "client_secret": cfg["client_secret"],
    }
    resp = requests.post(token_url, data=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data["access_token"], data["instance_url"]


def download_and_upload_one(row_dict, access_token, instance_url, cfg, s3_client):
    record_id = row_dict["Id"]
    parent_id = row_dict.get("ParentId") or "unfiled"
    name = row_dict.get("Name")
    api_ver = cfg["api_version"]
    headers = {"Authorization": f"Bearer {access_token}"}

    metrics = {
        "id": record_id, "parent_id": parent_id, "name": name,
        "status": "failed", "bytes": 0,
        "download_seconds": 0.0, "upload_seconds": 0.0, "total_seconds": 0.0,
        "s3_key": None, "error": None,
    }

    t_start = time.time()
    try:
        if not name:
            meta_url = f"{instance_url}/services/data/v{api_ver}/sobjects/Attachment/{record_id}"
            meta_resp = requests.get(meta_url, headers=headers, timeout=30)
            meta_resp.raise_for_status()
            name = meta_resp.json().get("Name", f"{record_id}.bin")

        body_url = f"{instance_url}/services/data/v{api_ver}/sobjects/Attachment/{record_id}/Body"

        t_dl_start = time.time()
        file_resp = requests.get(body_url, headers=headers, stream=True, timeout=60)
        file_resp.raise_for_status()
        content = file_resp.content
        t_dl_end = time.time()

        s3_key = f"{output_prefix_bc.value}/{parent_id}/{name}"

        t_ul_start = time.time()
        s3_client.put_object(Bucket=output_bucket_bc.value, Key=s3_key, Body=content)
        t_ul_end = time.time()

        metrics.update({
            "status": "success",
            "bytes": len(content),
            "download_seconds": round(t_dl_end - t_dl_start, 4),
            "upload_seconds": round(t_ul_end - t_ul_start, 4),
            "s3_key": s3_key,
        })
    except Exception as e:
        metrics["error"] = str(e)
    finally:
        metrics["total_seconds"] = round(time.time() - t_start, 4)

    return metrics


def process_partition(rows_iter):
    cfg = sf_config_bc.value
    rows = [r.asDict() for r in rows_iter]
    if not rows:
        return iter([])

    access_token, instance_url = get_salesforce_session(cfg)
    s3_client = boto3.client("s3")

    results = []
    with ThreadPoolExecutor(max_workers=max_workers_bc.value) as executor:
        futures = {
            executor.submit(download_and_upload_one, row, access_token, instance_url, cfg, s3_client): row
            for row in rows
        }
        for future in as_completed(futures):
            results.append(future.result())

    return iter(results)


# =============================================================================
# CELL 4 — Read input CSV, run the distributed job
# =============================================================================
input_df = spark.read.option("header", "true").csv(INPUT_CSV_PATH)

for col in input_df.columns:
    if col.lower() == "id" and col != "Id":
        input_df = input_df.withColumnRenamed(col, "Id")
    if col.lower() == "parentid" and col != "ParentId":
        input_df = input_df.withColumnRenamed(col, "ParentId")
    if col.lower() == "name" and col != "Name":
        input_df = input_df.withColumnRenamed(col, "Name")

assert "Id" in input_df.columns, "Input CSV must contain an 'Id' column"

full_row_count = input_df.count()
print(f"Full input CSV row count: {full_row_count}")

if ROW_SELECT_MODE == "LIMIT":
    input_df = input_df.limit(ROW_LIMIT)
    print(f"ROW_SELECT_MODE=LIMIT -> processing first {ROW_LIMIT} rows")
elif ROW_SELECT_MODE == "SAMPLE":
    input_df = input_df.sample(withReplacement=False, fraction=ROW_SAMPLE_FRACTION, seed=ROW_SAMPLE_SEED)
    print(f"ROW_SELECT_MODE=SAMPLE -> sampling ~{ROW_SAMPLE_FRACTION * 100:.2f}% of rows")
elif ROW_SELECT_MODE == "ALL":
    print("ROW_SELECT_MODE=ALL -> processing every row (use this once benchmarking looks good)")
else:
    raise ValueError(f"Unknown ROW_SELECT_MODE: {ROW_SELECT_MODE}")

num_input_rows = input_df.count()
print(f"Rows selected for this run: {num_input_rows}")

# Scales to millions of attachments: aim for ROWS_PER_PARTITION rows per
# partition (each partition fans out MAX_WORKERS concurrent downloads via the
# ThreadPoolExecutor), instead of capping total partitions at a small number.
# e.g. 5,000,000 rows / 300 per partition = ~16,700 partitions.
# With G.1X x5 workers (~4 executors x 4 cores = ~16 concurrent partitions),
# effective concurrent Salesforce calls ≈ 16 x MAX_WORKERS.
ROWS_PER_PARTITION = 300
target_partitions = max(1, -(-num_input_rows // ROWS_PER_PARTITION))  # ceil division
input_df = input_df.repartition(target_partitions)

print(f"Total attachments to process: {num_input_rows}")
print(f"Spark partitions: {target_partitions}, threads/partition: {MAX_WORKERS}")
print(f"Effective max concurrency: ~{target_partitions * MAX_WORKERS} "
      f"(bounded by cluster's actual executor/core count)")

job_start = time.time()
metrics_rdd = input_df.rdd.mapPartitions(process_partition)

metrics_schema = StructType([
    StructField("id", StringType(), True),
    StructField("parent_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("bytes", LongType(), True),
    StructField("download_seconds", DoubleType(), True),
    StructField("upload_seconds", DoubleType(), True),
    StructField("total_seconds", DoubleType(), True),
    StructField("s3_key", StringType(), True),
    StructField("error", StringType(), True),
])

metrics_rows = metrics_rdd.map(lambda m: Row(**m))
metrics_df = spark.createDataFrame(metrics_rows, schema=metrics_schema)

# Full s3:// URI for each uploaded file, in its own column for easy downstream use
metrics_df = metrics_df.withColumn(
    "s3_path",
    F.when(
        F.col("s3_key").isNotNull(),
        F.concat(F.lit(f"s3://{OUTPUT_BUCKET}/"), F.col("s3_key")),
    ).otherwise(F.lit(None).cast(StringType())),
)

metrics_df.cache()
job_elapsed = time.time() - job_start

print(f"Done. Wall-clock time: {job_elapsed:.2f}s")


# =============================================================================
# CELL 5 — Benchmark insights + write results
# =============================================================================
success_df = metrics_df.filter(F.col("status") == "success")
failed_df = metrics_df.filter(F.col("status") == "failed")

total_files = metrics_df.count()
success_count = success_df.count()
failed_count = failed_df.count()
total_bytes = success_df.agg(F.sum("bytes")).collect()[0][0] or 0

summary = success_df.agg(
    F.avg("download_seconds").alias("avg_download_s"),
    F.avg("upload_seconds").alias("avg_upload_s"),
    F.avg("total_seconds").alias("avg_total_s"),
    F.max("total_seconds").alias("max_total_s"),
    F.min("total_seconds").alias("min_total_s"),
).collect()[0]

throughput_mb_s = (total_bytes / (1024 * 1024)) / job_elapsed if job_elapsed > 0 else 0

print("=" * 60)
print("BENCHMARK SUMMARY")
print("=" * 60)
print(f"Total files attempted     : {total_files}")
print(f"Succeeded                 : {success_count}")
print(f"Failed                    : {failed_count}")
print(f"Total bytes transferred   : {total_bytes} ({total_bytes / (1024*1024):.2f} MB)")
print(f"Wall-clock job time       : {job_elapsed:.2f} s")
print(f"Overall throughput        : {throughput_mb_s:.2f} MB/s")
if summary["avg_download_s"] is not None:
    print(f"Avg download time/file    : {summary['avg_download_s']:.4f} s")
    print(f"Avg upload time/file      : {summary['avg_upload_s']:.4f} s")
    print(f"Avg total time/file       : {summary['avg_total_s']:.4f} s")
    print(f"Slowest file (s)          : {summary['max_total_s']:.4f}")
    print(f"Fastest file (s)          : {summary['min_total_s']:.4f}")
print("=" * 60)

if failed_count > 0:
    print("Sample of failed files:")
    failed_df.select("id", "name", "error").show(20, truncate=False)

# Scalable CSV write: partitioned by status, one file per Spark task (NOT
# coalesce(1) — at millions of rows that would force everything through a
# single task and bottleneck the driver). Produces many part-*.csv files
# under BENCHMARK_OUTPUT_PATH/status=success/ and /status=failed/.
metrics_df.write.mode("overwrite").partitionBy("status").option("header", "true").csv(BENCHMARK_OUTPUT_PATH)

# --- Failed attachment Ids, written separately for easy retry ---
# Just the columns you need to re-run: Id (+ ParentId/Name if you want them
# preserved), so this file can be fed straight back in as INPUT_CSV_PATH.
retry_df = failed_df.select(
    F.col("id").alias("Id"),
    F.col("parent_id").alias("ParentId"),
    F.col("name").alias("Name"),
    F.col("error").alias("last_error"),
)
retry_df.write.mode("overwrite").option("header", "true").csv(FAILED_IDS_OUTPUT_PATH)
print(f"Failed attachment Ids ({failed_count}) written to: {FAILED_IDS_OUTPUT_PATH}")
print("Re-run the job with INPUT_CSV_PATH set to this path to retry only the failures.")

summary_json = {
    "total_files": total_files,
    "success_count": success_count,
    "failed_count": failed_count,
    "total_bytes": total_bytes,
    "wall_clock_seconds": round(job_elapsed, 2),
    "throughput_mb_s": round(throughput_mb_s, 2),
}
s3 = boto3.client("s3")
summary_bucket = BENCHMARK_OUTPUT_PATH.split("/")[2]
summary_key_prefix = "/".join(BENCHMARK_OUTPUT_PATH.split("/")[3:]).rstrip("/")
s3.put_object(
    Bucket=summary_bucket,
    Key=f"{summary_key_prefix}/benchmark_summary.json",
    Body=json.dumps(summary_json, indent=2),
)

print("Benchmark CSV (partitioned by status) + summary JSON written to:", BENCHMARK_OUTPUT_PATH)