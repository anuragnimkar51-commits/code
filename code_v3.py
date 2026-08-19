import sys
import csv
import io
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, lower, expr, year as spark_year, month as spark_month, to_timestamp

# ---------- Glue job boilerplate ----------

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---------- HARDCODED VALUES — edit these directly for testing ----------

bucket = 'your-bucket-name'
base_prefix = 'attachment_files/'
year = '2021'
month = '03'
manifest_path = 's3://your-bucket-name/manifests/2021_03_manifest.csv'
output_bucket = 'your-bucket-name'
output_prefix = 'audit/manifest-discrepancies'
created_date_col = 'CreatedDate'   # <-- confirm exact column name from manifest_df_full.columns

# ---------------------------------------------------------------------

month_prefix = f"{base_prefix}year={year}/month={month}/"
print(f"Comparing S3 objects under: {month_prefix}")
print(f"Against manifest: {manifest_path}")
print(f"Filtering manifest by {created_date_col} for year={year}, month={month}")

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
    print("No objects found under this prefix — check year/month/base_prefix values. Exiting.")
    job.commit()
    sys.exit(0)

actual_df = spark.createDataFrame([(k,) for k in actual_keys], ["s3_key"])
actual_df = actual_df.withColumn("s3_key_norm", trim(lower(col("s3_key"))))

# ---------- Step 2: read the manifest CSV ----------

manifest_df_full = spark.read.option("header", True).csv(manifest_path)
print("Manifest columns found:", manifest_df_full.columns)

if created_date_col not in manifest_df_full.columns:
    raise Exception(
        f"created_date_col '{created_date_col}' not found in manifest. "
        f"Columns available: {manifest_df_full.columns}"
    )

# Auto-detect the S3 relative path column
path_col_candidates = [c for c in manifest_df_full.columns if "s3" in c.lower() and "path" in c.lower()]
if not path_col_candidates:
    raise Exception(
        f"Could not find an S3 relative path column in manifest. "
        f"Columns available: {manifest_df_full.columns}."
    )
path_col = path_col_candidates[0]
print(f"Using manifest column '{path_col}' as the S3 relative path")

# ---------- Step 3: filter manifest to only rows Created in this year/month ----------

manifest_df = manifest_df_full.withColumn(
    "created_ts", to_timestamp(col(created_date_col))
)

before_filter_count = manifest_df.count()

manifest_df = manifest_df.filter(
    (spark_year(col("created_ts")) == int(year)) &
    (spark_month(col("created_ts")) == int(month))
)

after_filter_count = manifest_df.count()
print(f"Manifest rows before date filter: {before_filter_count}")
print(f"Manifest rows after filtering to year={year}, month={month}: {after_filter_count}")

if before_filter_count > 0 and after_filter_count == 0:
    print(f"WARNING: Date filter matched 0 rows out of {before_filter_count}. "
          f"Check that '{created_date_col}' parses correctly with to_timestamp(). "
          f"Sample raw values:")
    manifest_df_full.select(created_date_col).show(5, truncate=False)

manifest_df = manifest_df.withColumn("manifest_path_norm", trim(lower(col(path_col))))

# ---------- Step 4: find S3 keys with NO match in the (date-filtered) manifest ----------

joined = actual_df.join(
    manifest_df.select("manifest_path_norm", path_col, created_date_col)
        .withColumnRenamed(path_col, "manifest_original_path"),
    expr("s3_key_norm LIKE concat('%', manifest_path_norm)"),
    how="left_outer"
)

extra_files_df = joined.filter(col("manifest_path_norm").isNull()).select("s3_key")

extra_count = extra_files_df.count()
print(f"Found {extra_count} file(s) in S3 with no manifest match for year={year}, month={month}")

if extra_count == 0:
    print("No discrepancies found — S3 matches date-filtered manifest exactly.")
    job.commit()
    sys.exit(0)

extra_files_df.show(50, truncate=False)

# ---------- Step 5: write the extra file(s) to a CSV at the configured output path ----------

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
