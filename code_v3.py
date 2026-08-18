import sys
import time
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_extract, col

# ---------- Glue job boilerplate ----------

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET',
    'BASE_PREFIX',
    'YEAR',
    'THREADS_PER_TASK',
    'NUM_SLICES'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args['BUCKET']
base_prefix = args['BASE_PREFIX']
year = args['YEAR']
threads_per_task = int(args['THREADS_PER_TASK'])
num_slices_cap = int(args['NUM_SLICES'])

year_prefix = f"{base_prefix}year={year}/"
print(f"Processing single year prefix: {year_prefix}")

# ---------- Discovery scoped to this year only ----------

def list_common_prefixes(bucket, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        prefixes += [cp['Prefix'] for cp in page.get('CommonPrefixes', [])]
    return prefixes

month_prefixes = list_common_prefixes(bucket, year_prefix)
print(f"Found {len(month_prefixes)} month prefixes under {year_prefix}")

if not month_prefixes:
    print(f"No month prefixes found under {year_prefix} — check YEAR value. Exiting.")
    job.commit()
    sys.exit(0)

id_prefixes = []
for mp in month_prefixes:
    subs = list_common_prefixes(bucket, mp)
    id_prefixes += subs if subs else [mp]
print(f"Found {len(id_prefixes)} leaf-level prefixes for year={year}")

# ---------- Counting function used inside each thread ----------

def count_single_prefix(bucket, prefix, max_retries=5):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    count = 0
    retries = 0
    while retries < max_retries:
        try:
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

# ---------- mapPartitions: each Spark task spins up its own thread pool ----------

def count_partition(prefixes_iter):
    prefixes = list(prefixes_iter)
    results = []
    with ThreadPoolExecutor(max_workers=threads_per_task) as executor:
        futures = {executor.submit(count_single_prefix, bucket, p): p for p in prefixes}
        for future in as_completed(futures):
            results.append(future.result())
    return iter(results)

num_slices = min(len(id_prefixes), num_slices_cap)
print(f"Distributing {len(id_prefixes)} prefixes across {num_slices} partitions "
      f"({threads_per_task} threads/task)")

prefixes_rdd = sc.parallelize(id_prefixes, numSlices=num_slices)
results_rdd = prefixes_rdd.mapPartitions(count_partition)
results = results_rdd.collect()

# ---------- Retry any failures once, at lower concurrency ----------

failed = [p for p, c in results if c is None]
if failed:
    print(f"WARNING: {len(failed)} prefixes failed on first pass — retrying at lower concurrency")
    retry_rdd = sc.parallelize(failed, numSlices=min(len(failed), 20))

    def count_partition_retry(prefixes_iter):
        prefixes = list(prefixes_iter)
        results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(count_single_prefix, bucket, p): p for p in prefixes}
            for future in as_completed(futures):
                results.append(future.result())
        return iter(results)

    retry_results = retry_rdd.mapPartitions(count_partition_retry).collect()
    retry_map = dict(retry_results)
    results = [(p, retry_map.get(p, c) if c is None else c) for p, c in results]

    still_failed = [p for p, c in results if c is None]
    if still_failed:
        print(f"WARNING: {len(still_failed)} prefixes still failed after retry: {still_failed[:10]}")

total = sum(c for _, c in results if c is not None)
print(f"\nTotal files for year={year}: {total}")

# ---------- Build summary (year/month rollup) only — no detail output ----------

results_df = spark.createDataFrame(
    [(p, c) for p, c in results if c is not None], ["prefix", "count"]
)
results_df = results_df \
    .withColumn("year", regexp_extract(col("prefix"), r"year=(\d+)", 1)) \
    .withColumn("month", regexp_extract(col("prefix"), r"month=(\d+)", 1))

month_counts_df = results_df.groupBy("year", "month") \
    .sum("count").withColumnRenamed("sum(count)", "file_count") \
    .orderBy("year", "month")

month_counts_df.show(50, truncate=False)

# Write ONLY the summary CSV — single file, one per year run
month_counts_df.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"s3://{bucket}/reports/partition_file_counts_summary/year={year}/")

print(f"Summary CSV written to s3://{bucket}/reports/partition_file_counts_summary/year={year}/")
print(f"Done for year={year}.")
job.commit()
