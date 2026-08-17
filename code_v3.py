from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import time
from botocore.exceptions import ClientError

spark = SparkSession.builder.appName("PartitionFileCountsDistributed").getOrCreate()

bucket = 'your-bucket-name'
base_prefix = 'attachment_files/'

# ---------- Discovery (same as before, runs on driver) ----------

def list_common_prefixes(bucket, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        prefixes += [cp['Prefix'] for cp in page.get('CommonPrefixes', [])]
    return prefixes

year_prefixes = list_common_prefixes(bucket, base_prefix)
month_prefixes = []
for yp in year_prefixes:
    month_prefixes += list_common_prefixes(yp) if False else list_common_prefixes(bucket, yp)

id_prefixes = []
for mp in month_prefixes:
    subs = list_common_prefixes(bucket, mp)
    id_prefixes += subs if subs else [mp]

print(f"Found {len(id_prefixes)} leaf-level prefixes")

# ---------- Counting function used inside each thread ----------

def count_single_prefix(bucket, prefix, max_retries=5):
    s3 = boto3.client('s3')  # one client per thread call is fine; boto3 clients are lightweight
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
    return (prefix, None)  # failed after retries — flag for reprocessing

# ---------- mapPartitions: each Spark task spins up its own thread pool ----------

def count_partition(prefixes_iter):
    prefixes = list(prefixes_iter)
    results = []
    # Threads per task — tune based on testing (start around 20-50)
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(count_single_prefix, bucket, p): p for p in prefixes}
        for future in as_completed(futures):
            results.append(future.result())
    return iter(results)

# Fewer, bigger partitions now — threading handles concurrency within each,
# so you don't need thousands of tiny RDD partitions like the non-threaded version
num_slices = min(len(id_prefixes), 100)

prefixes_rdd = spark.sparkContext.parallelize(id_prefixes, numSlices=num_slices)
results_rdd = prefixes_rdd.mapPartitions(count_partition)

results = results_rdd.collect()

# Check for any failures
failed = [p for p, c in results if c is None]
if failed:
    print(f"WARNING: {len(failed)} prefixes failed after retries: {failed[:10]}...")

total = sum(c for _, c in results if c is not None)
print(f"\nTotal files: {total}")

# ---------- Aggregate and write to S3 ----------

from pyspark.sql.functions import regexp_extract, col

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

month_counts_df.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"s3://{bucket}/reports/partition_file_counts_summary/")

results_df.write.mode("overwrite").option("header", True) \
    .csv(f"s3://{bucket}/reports/partition_file_counts_detail/")
