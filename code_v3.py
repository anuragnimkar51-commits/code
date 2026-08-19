import sys
import csv
import io
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, lower, expr

# ---------- Glue job boilerplate ----------

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET',
    'BASE_PREFIX',       # e.g. "attachment_files/"
    'YEAR',               # e.g. "2021"
    'MONTH',               # e.g. "03"  -- must match actual folder naming exactly
    'MANIFEST_PATH',       # e.g. "s3://your-bucket-name/manifests/2021_03_manifest.csv"
    'OUTPUT_BUCKET',       # where the discrepancy CSV should be written
    'OUTPUT_PREFIX'        # e.g. "audit/manifest-discrepancies"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args['BUCKET']
base_prefix = args['BASE_PREFIX']
year = args['YEAR']
month = args['MONTH']
manifest_path = args['MANIFEST_PATH']
output_bucket = args['OUTPUT_BUCKET']
output_prefix = args['OUTPUT_PREFIX'].rstrip('/')

month_prefix = f"{base_prefix}year={year}/month={month}/"
print(f"Comparing S3 objects under: {month_prefix}")
print(f"Against manifest: {manifest_path}")

# ---------- Step 1: list every actual object under this month prefix ----------

def list_all_keys(bucket_name, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])
    return keys

actual_keys = list_all_keys(bucket, month_prefix)
print(f"Found {len(actual_keys)} actual objects in S3 under {month_prefix}")

if not actual_keys:
    print("No objects found under this prefix — check YEAR/MONTH/BASE_PREFIX values. Exiting.")
    job.commit()
    sys.exit(0)

actual_df = spark.createDataFrame([(k,) for k in actual_keys], ["s3_key"])
actual_df = actual_df.withColumn("s3_key_norm", trim(lower(col("s3_key"))))

# ---------- Step 2: read the manifest CSV ----------

manifest_df = spark.read.option("header", True).csv(manifest_path)
print("Manifest columns found:", manifest_df.columns)

# Auto-detect the S3 relative path column — adjust/hardcode if this picks the wrong one
path_col_candidates = [c for c in manifest_df.columns if "s3" in c.lower() and "path" in c.lower()]
if not path_col_candidates:
    raise Exception(
        f"Could not find an S3 relative path column in manifest. "
        f"Columns available: {manifest_df.columns}. "
        f"Set path_col manually below to fix."
    )
path_col = path_col_candidates[0]
print(f"Using manifest column '{path_col}' as the S3 relative path")

manifest_df = manifest_df.withColumn("manifest_path_norm", trim(lower(col(path_col))))

# ---------- Step 3: find S3 keys with NO match in the manifest ----------
# Suffix match — manifest stores relative path, S3 key includes full prefix.
# If your manifest stores the FULL key instead, switch to the exact-match join
# shown commented out below.

joined = actual_df.join(
    manifest_df.select("manifest_path_norm", path_col).withColumnRenamed(path_col, "manifest_original_path"),
    expr("s3_key_norm LIKE concat('%', manifest_path_norm)"),
    how="left_outer"
)

# --- Alternative: exact match, use instead of the join above if manifest has full keys ---
# joined = actual_df.join(
#     manifest_df.select("manifest_path_norm"),
#     actual_df.s3_key_norm == manifest_df.manifest_path_norm,
#     how="left_outer"
# )

extra_files_df = joined.filter(col("manifest_path_norm").isNull()).select("s3_key")

extra_count = extra_files_df.count()
print(f"Found {extra_count} file(s) in S3 with no manifest match")

if extra_count == 0:
    print("No discrepancies found — S3 matches manifest exactly.")
    job.commit()
    sys.exit(0)

extra_files_df.show(50, truncate=False)

# ---------- Step 4: write the extra file(s) to a CSV at the configured output path ----------

extra_keys = [row['s3_key'] for row in extra_files_df.collect()]

csv_buffer = io.StringIO()
writer = csv.writer(csv_buffer)
writer.writerow(['s3_key', 'source_bucket', 'year', 'month'])
for key in extra_keys:
    writer.writerow([key, bucket, year, month])

output_key = f"{output_prefix}/year={year}/month={month}/extra_files_not_in_manifest.csv"

s3_client = boto3.client('s3')
s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue(), ContentType='text/csv')

print(f"Discrepancy CSV written to s3://{output_bucket}/{output_key}")
print(f"Extra file(s) found: {extra_keys}")
print("Done.")
job.commit()
