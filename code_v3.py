import sys
import csv
import io
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, lower, year as spark_year, month as spark_month, to_timestamp
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Glue job boilerplate ----------

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---------- HARDCODED VALUES ----------

bucket = 'your-bucket-name'
base_prefix = 'attachment_files/'
year = '2021'
month = '03'
manifest_path = 's3://your-bucket-name/manifests/2021_03_manifest.csv'
output_bucket = 'your-bucket-name'
output_prefix = 'audit/manifest-discrepancies'
created_date_col = 'CreatedDate'
list_threads = 20   # concurrency for parallel S3 listing

# ---------------------------------------------------------------------

month_prefix = f"{base_prefix}year={year}/month={month}/"
print(f"Comparing S3 objects under: {month_prefix}")

# ---------- Step 1: discover sub-prefixes (id folders) so listing can be parallelized ----------

def list_common_prefixes(prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        prefixes += [cp['Prefix'] for cp in page.get('CommonPrefixes', [])]
    return prefixes

id_prefixes = list_common_prefixes(month_prefix)
if not id_prefixes:
    id_prefixes = [month_prefix]  # no further nesting — list the month prefix directly
print(f"Found {len(id_prefixes)} sub-prefixes to list under {month_prefix}")

# ---------- Step 2: list all keys under each sub-prefix, IN PARALLEL ----------

def list_all_keys(prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])
    return keys

actual_keys = []
with ThreadPoolExecutor(max_workers=list_threads) as executor:
    futures = {executor.submit(list_all_keys, p): p for p in id_prefixes}
    for future in as_completed(futures):
        actual_keys += future.result()

print(f"Found {len(actual_keys)} actual objects in S3 under {month_prefix}")

if not actual_keys:
    print("No objects found — check year/month/base_prefix values. Exiting.")
    job.commit()
    sys.exit(0)

# ---------- Step 3: read + filter manifest to this year/month ----------

manifest_df_full = spark.read.option("header", True).csv(manifest_path)
print("Manifest columns found:", manifest_df_full.columns)

if created_date_col not in manifest_df_full.columns:
    raise Exception(f"'{created_date_col}' not found. Columns: {manifest_df_full.columns}")

path_col_candidates = [c for c in manifest_df_full.columns if "s3" in c.lower() and "path" in c.lower()]
if not path_col_candidates:
    raise Exception(f"No S3 path column found. Columns: {manifest_df_full.columns}")
path_col = path_col_candidates[0]
print(f"Using manifest column '{path_col}' as the S3 relative path")

manifest_df = manifest_df_full.withColumn("created_ts", to_timestamp(col(created_date_col)))
manifest_df = manifest_df.filter(
    (spark_year(col("created_ts")) == int(year)) &
    (spark_month(col("created_ts")) == int(month))
).withColumn("manifest_path_norm", trim(lower(col(path_col))))

# Single action here — cache and collect ONCE, instead of multiple .count() calls
# triggering repeated recomputation of the same DataFrame.
manifest_df.cache()
manifest_paths = [row['manifest_path_norm'] for row in manifest_df.select("manifest_path_norm").collect()]
manifest_count = len(manifest_paths)
print(f"Manifest rows for year={year}, month={month}: {manifest_count}")

if manifest_count == 0:
    print(f"WARNING: 0 manifest rows matched this period. Sample raw {created_date_col} values:")
    manifest_df_full.select(created_date_col).show(5, truncate=False)

# ---------- Step 4: set-based diff — no Spark join, no LIKE wildcard scan ----------
# manifest_paths are relative paths; actual S3 keys include the full prefix.
# Build a set of normalized manifest paths, then check each S3 key by suffix
# using plain Python string operations (fast — this is a small-ish list, not 70M rows).

manifest_set = set(manifest_paths)

def has_manifest_match(s3_key_norm):
    # Since exact suffix-in-set lookup isn't directly possible with a hash set
    # (paths aren't identical strings, S3 key has extra prefix), do the cheapest
    # useful check: does the S3 key end with any manifest path?
    # For performance, index manifest paths by their last path segment (filename)
    # to avoid comparing against the entire manifest set per key.
    return s3_key_norm in manifest_lookup_by_suffix.get(s3_key_norm.rsplit('/', 1)[-1], set())

# Build a lookup: filename -> set of full manifest paths with that filename
# This turns an O(N*M) suffix scan into an O(N) filename-bucketed lookup.
manifest_lookup_by_suffix = {}
for p in manifest_set:
    fname = p.rsplit('/', 1)[-1]
    manifest_lookup_by_suffix.setdefault(fname, set()).add(p)

extra_keys = []
for key in actual_keys:
    key_norm = key.strip().lower()
    fname = key_norm.rsplit('/', 1)[-1]
    candidates = manifest_lookup_by_suffix.get(fname)
    matched = False
    if candidates:
        matched = any(key_norm.endswith(c) for c in candidates)
    if not matched:
        extra_keys.append(key)

print(f"Found {len(extra_keys)} file(s) in S3 with no manifest match for year={year}, month={month}")

manifest_df.unpersist()

if not extra_keys:
    print("No discrepancies found — S3 matches date-filtered manifest exactly.")
    job.commit()
    sys.exit(0)

print(f"Extra file(s): {extra_keys}")

# ---------- Step 5: write result CSV ----------

csv_buffer = io.StringIO()
writer = csv.writer(csv_buffer)
writer.writerow(['s3_key', 'source_bucket', 'year', 'month'])
for key in extra_keys:
    writer.writerow([key, bucket, year, month])

output_key = f"{output_prefix}/year={year}/month={month}/extra_files_not_in_manifest.csv"
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue(), ContentType='text/csv')

print(f"Discrepancy CSV written to s3://{output_bucket}/{output_key}")
print("Done.")
job.commit()
