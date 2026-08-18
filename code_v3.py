import sys
import time
import threading
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ---------- Glue job boilerplate ----------

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET',
    'BASE_PREFIX',
    'YEAR',
    'THREADS_PER_TASK',
    'DISCOVERY_THREADS',   # new: threads for the driver-side discovery phase
    'NUM_SLICES'
])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args['BUCKET']
base_prefix = args['BASE_PREFIX']
year = args['YEAR']
threads_per_task = int(args['THREADS_PER_TASK'])
discovery_threads = int(args['DISCOVERY_THREADS'])
num_slices_cap = int(args['NUM_SLICES'])

year_prefix = f"{base_prefix}year={year}/"
print(f"Processing single year prefix: {year_prefix}")

# ---------- Reusable thread-local S3 client (avoids re-creating clients constantly) ----------

_thread_local = threading.local()

def get_s3_client():
    if not hasattr(_thread_local, 's3'):
        _thread_local.s3 = boto3.client('s3')
    return _thread_local.s3

# ---------- Discovery helpers ----------

def list_common_prefixes(prefix):
    s3 = get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        prefixes += [cp['Prefix'] for cp in page.get('CommonPrefixes', [])]
    return prefixes

def discover_parallel(parent_prefixes, max_workers):
    """Given a list of parent prefixes, list their child prefixes concurrently."""
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(list_common_prefixes, p): p for p in parent_prefixes}
        for future in as_completed(futures):
            parent = futures[future]
            children = future.result()
            results.append((parent, children))
    return results

# ---------- Step 1: month prefixes under this year (single call, cheap) ----------

month_prefixes = list_common_prefixes(year_prefix)
print(f"Found {len(month_prefixes)} month prefixes under {year_prefix}")

if not month_prefixes:
    print(f"No month prefixes found under {year_prefix} — check YEAR value. Exiting.")
    job.commit()
    sys.exit(0)

# ---------- Step 2: id prefixes under each month — PARALLELIZED on driver ----------
# This used to be a sequential for-loop; now runs concurrently.

id_prefixes = []
month_results = discover_parallel(month_prefixes, max_workers=discovery_threads)
for month_prefix, subs in month_results:
    id_prefixes += subs if subs else [month_prefix]

print(f"Found {len(id_prefixes)} leaf-level prefixes for year={year}")

# ---------- Counting function with reusable client + retry ----------

def count_single_prefix(prefix, max_retries=5):
    s3 = get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    retries = 0
    while retries < max_retries:
        try:
            count = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                count += page.get('KeyCount', 0)
            return (prefix, count)
        except ClientError as e:
            if e.response['Error']['Code'] in ('SlowDown', 'RequestLimitExceeded'):
                time.sleep(2 ** retries)
                retries += 1
            else:
                raise
    return (prefix, None)

def count_partition(prefixes_iter, max_workers):
    prefixes = list(prefixes_iter)
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(count_single_prefix, p): p for p in prefixes}
        for future in as_completed(futures):
            results.append(future.result())
    return results

def run_counting_pass(prefixes, num_slices, threads_per_task):
    rdd = sc.parallelize(prefixes, numSlices=num_slices)
    return rdd.mapPartitions(lambda it: iter(count_partition(it, threads_per_task))).collect()

# ---------- Distributed counting (first pass) ----------

num_slices = min(len(id_prefixes), num_slices_cap)
print(f"Distributing {len(id_prefixes)} prefixes across {num_slices} partitions "
      f"({threads_per_task} threads/task)")

results = run_counting_pass(id_prefixes, num_slices, threads_per_task)

# ---------- Retry failures once, at lower concurrency (same function reused) ----------

failed = [p for p, c in results if c is None]
if failed:
    print(f"WARNING: {len(failed)} prefixes failed on first pass — retrying at lower concurrency")
    retry_results = run_counting_pass(failed, min(len(failed), 20), threads_per_task=5)
    retry_map = dict(retry_results)
    results = [(p, retry_map.get(p, c) if c is None else c) for p, c in results]

    still_failed = [p for p, c in results if c is None]
    if still_failed:
        print(f"WARNING: {len(still_failed)} prefixes still failed after retry: {still_failed[:10]}")

total = sum(c for _, c in results if c is not None)
print(f"\nTotal files for year={year}: {total}")

# ---------- Lightweight month rollup — plain RDD aggregation, no DataFrame ----------

import re

def extract_month(prefix):
    m = re.search(r"month=(\d+)", prefix)
    return m.group(1) if m else "unknown"

valid_results = [(p, c) for p, c in results if c is not None]
month_rdd = sc.parallelize(valid_results) \
    .map(lambda pc: (extract_month(pc[0]), pc[1])) \
    .reduceByKey(lambda a, b: a + b)

month_counts = sorted(month_rdd.collect())  # list of (month, count) tuples

print(f"\n===== Month summary for year={year} =====")
for month, count in month_counts:
    print(f"{month}: {count}")

# ---------- Write summary CSV directly via boto3 — no Spark write, no coalesce ----------

import csv
import io

csv_buffer = io.StringIO()
writer = csv.writer(csv_buffer)
writer.writerow(['year', 'month', 'file_count'])
for month, count in month_counts:
    writer.writerow([year, month, count])
writer.writerow([year, 'TOTAL', total])

output_key = f"reports/partition_file_counts_summary/year={year}/summary.csv"
s3 = boto3.client('s3')
s3.put_object(Bucket=bucket, Key=output_key, Body=csv_buffer.getvalue(), ContentType='text/csv')

print(f"Summary CSV written to s3://{bucket}/{output_key}")
print(f"Done for year={year}.")
job.commit()
