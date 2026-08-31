"""
AWS Glue Job: Bronze CSV -> Postgres Table Validation
-------------------------------------------------------
Validates that a bronze CSV file landed correctly in a Postgres table.
Checks: row counts, schema/columns, nulls, duplicates, row-level diffs,
timestamp parsing/precision/timezone, and numeric aggregate sanity checks.

Writes a JSON report + mismatched-row CSVs to S3, and optionally sends
an SNS alert on failure.
"""

import sys
import json
import boto3
from datetime import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# ------------------------------------------------------------------
# 1. Job setup
# ------------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'CSV_S3_PATH',              # e.g. s3://my-bucket/bronze/incoming/file.csv
    'PG_SECRET_NAME',           # Secrets Manager secret: host/port/dbname/username/password
    'PG_TABLE',                  # e.g. bronze.your_table
    'KEY_COLUMN',                 # primary key column for join/dedup checks
    'VALIDATION_OUTPUT_PATH',    # e.g. s3://my-bucket/validation-reports
    'SNS_TOPIC_ARN',             # optional - pass '' if unused
    'TS_FORMAT',                  # e.g. yyyy-MM-dd HH:mm:ss  (pass '' to auto-infer)
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

# Pin session timezone to UTC for consistent timestamp handling everywhere
spark.conf.set("spark.sql.session.timeZone", "UTC")

TS_FORMAT = args['TS_FORMAT'] if args.get('TS_FORMAT') else "yyyy-MM-dd HH:mm:ss"

# ------------------------------------------------------------------
# 2. Get Postgres credentials from Secrets Manager
# ------------------------------------------------------------------
def get_pg_secret(secret_name):
    client = boto3.client('secretsmanager')
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp['SecretString'])

secret = get_pg_secret(args['PG_SECRET_NAME'])
jdbc_url = f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}"

# ------------------------------------------------------------------
# 3. Read Postgres table (authoritative schema reference)
# ------------------------------------------------------------------
logger.info(f"Reading Postgres table {args['PG_TABLE']}")

df_pg = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", args['PG_TABLE']) \
    .option("user", secret['username']) \
    .option("password", secret['password']) \
    .option("driver", "org.postgresql.Driver") \
    .option("fetchsize", 10000) \
    .option("sessionInitStatement", "SET TIME ZONE 'UTC'") \
    .load()

# ------------------------------------------------------------------
# 4. Read CSV twice:
#    - df_csv_raw: everything as string (to detect parse failures later)
#    - df_csv: with inferSchema for general typing
# ------------------------------------------------------------------
logger.info(f"Reading CSV from {args['CSV_S3_PATH']}")

df_csv_raw = spark.read.option("header", True).csv(args['CSV_S3_PATH'])  # all StringType
df_csv = spark.read.option("header", True).option("inferSchema", True).csv(args['CSV_S3_PATH'])

# ------------------------------------------------------------------
# 5. Detect timestamp columns (from Postgres schema - authoritative)
# ------------------------------------------------------------------
timestamp_cols = [
    f.name for f in df_pg.schema.fields
    if isinstance(f.dataType, TimestampType) or str(f.dataType) in ("TimestampType", "TimestampNTZType")
]
logger.info(f"Detected timestamp columns (from PG schema): {timestamp_cols}")

# ------------------------------------------------------------------
# 6. Parse CSV timestamp columns explicitly + detect parse failures
# ------------------------------------------------------------------
results = {
    "job_name": args['JOB_NAME'],
    "run_timestamp": datetime.utcnow().isoformat(),
    "csv_path": args['CSV_S3_PATH'],
    "pg_table": args['PG_TABLE'],
    "ts_format_used": TS_FORMAT,
    "timestamp_columns": timestamp_cols,
    "timestamp_parse_failures": {},
    "timestamp_range_checks": {}
}

for col in timestamp_cols:
    if col in df_csv_raw.columns:
        parsed = F.to_timestamp(F.col(col), TS_FORMAT)

        # Rows where the raw string was non-blank but parsing failed
        failed_df = df_csv_raw.filter(
            F.col(col).isNotNull() & (F.trim(F.col(col)) != "")
        ).withColumn("_parsed", parsed).filter(F.col("_parsed").isNull())

        fail_count = failed_df.count()
        results["timestamp_parse_failures"][col] = fail_count
        if fail_count > 0:
            logger.warn(f"{fail_count} rows failed to parse timestamp column '{col}' using format '{TS_FORMAT}'")

        # Replace column in df_csv with the explicitly parsed + UTC-normalized version
        df_csv = df_csv.withColumn(col, F.to_utc_timestamp(parsed, "UTC"))

# Normalize Postgres timestamp columns to UTC as well (in case of local session skew)
for col in timestamp_cols:
    df_pg = df_pg.withColumn(col, F.to_utc_timestamp(F.col(col), "UTC"))

# ------------------------------------------------------------------
# 7. Basic row count check
# ------------------------------------------------------------------
csv_count = df_csv.count()
pg_count = df_pg.count()
results["csv_row_count"] = csv_count
results["pg_row_count"] = pg_count
results["row_count_match"] = csv_count == pg_count
logger.info(f"Row counts -> CSV: {csv_count}, PG: {pg_count}")

# ------------------------------------------------------------------
# 8. Schema / column check
# ------------------------------------------------------------------
csv_cols = set(df_csv.columns)
pg_cols = set(df_pg.columns)

results["missing_cols_in_pg"] = list(csv_cols - pg_cols)
results["missing_cols_in_csv"] = list(pg_cols - csv_cols)

common_cols = sorted(csv_cols & pg_cols)
key_col = args['KEY_COLUMN']

# ------------------------------------------------------------------
# 9. Null count comparison
# ------------------------------------------------------------------
def null_counts(df, cols):
    row = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in cols
    ]).collect()[0].asDict()
    return row

results["null_counts_csv"] = null_counts(df_csv, common_cols)
results["null_counts_pg"] = null_counts(df_pg, common_cols)

# ------------------------------------------------------------------
# 10. Duplicate check on key column
# ------------------------------------------------------------------
dupes_pg = df_pg.groupBy(key_col).count().filter("count > 1")
dupes_pg_count = dupes_pg.count()
results["duplicate_keys_in_pg"] = dupes_pg_count
if dupes_pg_count > 0:
    logger.warn(f"{dupes_pg_count} duplicate keys found in Postgres table on column '{key_col}'")

# ------------------------------------------------------------------
# 11. Timestamp range sanity check (min/max) per timestamp column
# ------------------------------------------------------------------
for col in timestamp_cols:
    if col in df_csv.columns and col in df_pg.columns:
        csv_min, csv_max = df_csv.select(F.min(col), F.max(col)).collect()[0]
        pg_min, pg_max = df_pg.select(F.min(col), F.max(col)).collect()[0]
        results["timestamp_range_checks"][col] = {
            "csv_min": str(csv_min), "csv_max": str(csv_max),
            "pg_min": str(pg_min), "pg_max": str(pg_max),
            "range_match": (csv_min == pg_min) and (csv_max == pg_max)
        }

# ------------------------------------------------------------------
# 12. Row-level diff (precision-safe: truncate timestamps to seconds,
#     format all columns to consistent strings before comparing)
# ------------------------------------------------------------------
TS_COMPARE_PRECISION = "second"  # change to "millisecond"/"minute" as needed

def build_comparable_df(df, cols, ts_cols):
    select_exprs = []
    for c in cols:
        if c in ts_cols:
            truncated = F.date_trunc(TS_COMPARE_PRECISION, F.col(c))
            select_exprs.append(F.date_format(truncated, "yyyy-MM-dd HH:mm:ss").alias(c))
        else:
            select_exprs.append(F.col(c).cast("string").alias(c))
    return df.select(select_exprs)

df_csv_aligned = build_comparable_df(df_csv, common_cols, timestamp_cols)
df_pg_aligned = build_comparable_df(df_pg, common_cols, timestamp_cols)

rows_only_in_csv = df_csv_aligned.exceptAll(df_pg_aligned)
rows_only_in_pg = df_pg_aligned.exceptAll(df_csv_aligned)

rows_only_in_csv_count = rows_only_in_csv.count()
rows_only_in_pg_count = rows_only_in_pg.count()

results["rows_only_in_csv"] = rows_only_in_csv_count
results["rows_only_in_pg"] = rows_only_in_pg_count

logger.info(f"Rows only in CSV: {rows_only_in_csv_count}, Rows only in PG: {rows_only_in_pg_count}")

# ------------------------------------------------------------------
# 13. Numeric aggregate sanity checks (sum/min/max on numeric cols)
# ------------------------------------------------------------------
numeric_types = ("DoubleType", "IntegerType", "LongType", "DecimalType", "FloatType")
numeric_cols = [f.name for f in df_csv.schema.fields if str(f.dataType) in numeric_types and f.name in df_pg.columns]

agg_diffs = {}
for col in numeric_cols:
    csv_sum = df_csv.select(F.sum(col)).collect()[0][0]
    pg_sum = df_pg.select(F.sum(col)).collect()[0][0]
    agg_diffs[col] = {
        "csv_sum": csv_sum,
        "pg_sum": pg_sum,
        "match": csv_sum == pg_sum
    }
results["aggregate_checks"] = agg_diffs

# ------------------------------------------------------------------
# 14. Overall pass/fail
# ------------------------------------------------------------------
ts_parse_ok = all(v == 0 for v in results["timestamp_parse_failures"].values())

results["passed"] = (
    results["row_count_match"]
    and not results["missing_cols_in_pg"]
    and not results["missing_cols_in_csv"]
    and dupes_pg_count == 0
    and rows_only_in_csv_count == 0
    and rows_only_in_pg_count == 0
    and ts_parse_ok
)

logger.info(f"Validation results: {json.dumps(results, default=str)}")

# ------------------------------------------------------------------
# 15. Write validation report + mismatched rows to S3
# ------------------------------------------------------------------
run_id = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
report_path = f"{args['VALIDATION_OUTPUT_PATH']}/report_{run_id}.json"

sc.parallelize([json.dumps(results, default=str)]).saveAsTextFile(report_path)

if rows_only_in_csv_count > 0:
    rows_only_in_csv.write.mode("overwrite").option("header", True) \
        .csv(f"{args['VALIDATION_OUTPUT_PATH']}/rows_only_in_csv_{run_id}/")

if rows_only_in_pg_count > 0:
    rows_only_in_pg.write.mode("overwrite").option("header", True) \
        .csv(f"{args['VALIDATION_OUTPUT_PATH']}/rows_only_in_pg_{run_id}/")

# ------------------------------------------------------------------
# 16. Alert on failure (optional, via SNS)
# ------------------------------------------------------------------
if not results["passed"] and args.get('SNS_TOPIC_ARN'):
    sns = boto3.client('sns')
    sns.publish(
        TopicArn=args['SNS_TOPIC_ARN'],
        Subject=f"Bronze validation FAILED: {args['JOB_NAME']}",
        Message=json.dumps(results, indent=2, default=str)
    )
    logger.error("Validation failed - alert sent via SNS")

if not results["passed"]:
    raise Exception(f"Bronze CSV -> Postgres validation failed. See report at {report_path}")

logger.info("Validation passed.")
job.commit()
