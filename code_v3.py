"""
AWS Glue Job: Salesforce Object -> Postgres Table Validation
---------------------------------------------------------------
Validates that data pulled from Salesforce (bronze layer) landed correctly
in a Postgres table.

Source: Salesforce object via Glue Salesforce connection (recommended) or
        simple-salesforce / Bulk API fallback.
Target: Postgres table via JDBC.

Checks: row counts, schema/columns, nulls, duplicates, row-level diffs,
Salesforce datetime parsing/precision/timezone handling, and numeric
aggregate sanity checks.

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
from pyspark.sql.types import TimestampType, DateType

# ------------------------------------------------------------------
# 1. Job setup
# ------------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SF_CONNECTION_NAME',        # Glue Connection name for Salesforce
    'SF_OBJECT',                  # e.g. Opportunity, Account, Custom_Object__c
    'PG_SECRET_NAME',            # Secrets Manager secret: host/port/dbname/username/password
    'PG_TABLE',                   # e.g. bronze.your_table
    'KEY_COLUMN',                  # primary key column for join/dedup checks (e.g. Id, sfid)
    'VALIDATION_OUTPUT_PATH',    # e.g. s3://my-bucket/validation-reports
    'SNS_TOPIC_ARN',              # optional - pass '' if unused
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

# Salesforce always returns datetimes in UTC (ISO 8601, e.g. 2024-01-15T10:30:00.000+0000)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Salesforce datetime format: yyyy-MM-dd'T'HH:mm:ss.SSS+0000
SF_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
# Salesforce date-only format (e.g. CloseDate): yyyy-MM-dd
SF_DATE_FORMAT = "yyyy-MM-dd"

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
# 4. Read Salesforce object via Glue Connection (native connector)
#    This uses Glue's Spark Salesforce data source (Glue 4.0+).
#    All fields come back typed by the connector; datetimes usually
#    arrive as strings in SF's ISO format unless the connector infers them.
# ------------------------------------------------------------------
logger.info(f"Reading Salesforce object {args['SF_OBJECT']} via connection {args['SF_CONNECTION_NAME']}")

dyf_sf = glueContext.create_dynamic_frame.from_options(
    connection_type="salesforce",
    connection_options={
        "connectionName": args['SF_CONNECTION_NAME'],
        "entityName": args['SF_OBJECT'],
        # "selectedFieldNames": ["Id", "Name", "CreatedDate", "LastModifiedDate", ...]  # optional: limit columns
    }
)
df_sf = dyf_sf.toDF()

# Also keep an all-string version for parse-failure detection
df_sf_raw = df_sf.select([F.col(c).cast("string").alias(c) for c in df_sf.columns])

# ------------------------------------------------------------------
# ALTERNATIVE: if not using a Glue Connection, pull via Bulk API with
# simple-salesforce inside a Python shell job / bundled library, write
# to a staging S3 path as CSV/Parquet, then read it here instead:
#
# df_sf = spark.read.parquet("s3://my-bucket/staging/salesforce/Opportunity/")
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# 5. Detect datetime vs date columns
#    - From Postgres schema (authoritative for target types)
#    - Cross-reference with Salesforce field naming convention as backup
#      (SF datetime fields commonly end in Date/DateTime, e.g. CreatedDate,
#       LastModifiedDate, CloseDate)
# ------------------------------------------------------------------
timestamp_cols = [
    f.name for f in df_pg.schema.fields
    if isinstance(f.dataType, TimestampType) or str(f.dataType) in ("TimestampType", "TimestampNTZType")
]
date_only_cols = [
    f.name for f in df_pg.schema.fields
    if isinstance(f.dataType, DateType) or str(f.dataType) == "DateType"
]

logger.info(f"Detected timestamp columns (from PG schema): {timestamp_cols}")
logger.info(f"Detected date-only columns (from PG schema): {date_only_cols}")

# ------------------------------------------------------------------
# 6. Parse Salesforce datetime/date columns explicitly + detect failures
# ------------------------------------------------------------------
results = {
    "job_name": args['JOB_NAME'],
    "run_timestamp": datetime.utcnow().isoformat(),
    "sf_object": args['SF_OBJECT'],
    "pg_table": args['PG_TABLE'],
    "sf_datetime_format_used": SF_DATETIME_FORMAT,
    "sf_date_format_used": SF_DATE_FORMAT,
    "timestamp_columns": timestamp_cols,
    "date_columns": date_only_cols,
    "timestamp_parse_failures": {},
    "timestamp_range_checks": {}
}

def parse_sf_datetime(col_name):
    """
    Salesforce datetimes: 2024-01-15T10:30:00.000+0000
    Handles both '+0000' and 'Z' suffix variants, and missing milliseconds.
    """
    raw = F.col(col_name)
    # Normalize 'Z' suffix to '+0000' so to_timestamp parses consistently
    normalized = F.regexp_replace(raw, "Z$", "+0000")
    # Try with milliseconds first
    parsed = F.to_timestamp(normalized, SF_DATETIME_FORMAT)
    # Fallback: no milliseconds present
    parsed_fallback = F.to_timestamp(normalized, "yyyy-MM-dd'T'HH:mm:ssZ")
    return F.coalesce(parsed, parsed_fallback)

for col in timestamp_cols:
    if col in df_sf_raw.columns:
        parsed = parse_sf_datetime(col)

        failed_df = df_sf_raw.filter(
            F.col(col).isNotNull() & (F.trim(F.col(col)) != "")
        ).withColumn("_parsed", parsed).filter(F.col("_parsed").isNull())

        fail_count = failed_df.count()
        results["timestamp_parse_failures"][col] = fail_count
        if fail_count > 0:
            logger.warn(f"{fail_count} rows failed to parse SF datetime column '{col}'")

        df_sf = df_sf.withColumn(col, parsed)  # already UTC since SF returns UTC

for col in date_only_cols:
    if col in df_sf_raw.columns:
        parsed_date = F.to_date(F.col(col), SF_DATE_FORMAT)
        df_sf = df_sf.withColumn(col, parsed_date)

# Normalize Postgres timestamp columns to UTC as well (defensive, in case of session skew)
for col in timestamp_cols:
    df_pg = df_pg.withColumn(col, F.to_utc_timestamp(F.col(col), "UTC"))

# ------------------------------------------------------------------
# 7. Basic row count check
# ------------------------------------------------------------------
sf_count = df_sf.count()
pg_count = df_pg.count()
results["sf_row_count"] = sf_count
results["pg_row_count"] = pg_count
results["row_count_match"] = sf_count == pg_count
logger.info(f"Row counts -> Salesforce: {sf_count}, PG: {pg_count}")

# ------------------------------------------------------------------
# 8. Schema / column check
#    Note: Salesforce field names are often CamelCase (CreatedDate) while
#    Postgres columns are often snake_case (created_date). Normalize casing
#    for comparison, but keep a mapping if you rename during load.
# ------------------------------------------------------------------
# If your bronze load renames columns (e.g. CreatedDate -> created_date),
# define that mapping here so both sides align. Otherwise assume same names.
COLUMN_NAME_MAP = {
    # "CreatedDate": "created_date",
    # "LastModifiedDate": "last_modified_date",
    # "Id": "sfid",
}

if COLUMN_NAME_MAP:
    df_sf = df_sf.select([F.col(c).alias(COLUMN_NAME_MAP.get(c, c)) for c in df_sf.columns])
    timestamp_cols = [COLUMN_NAME_MAP.get(c, c) for c in timestamp_cols]
    date_only_cols = [COLUMN_NAME_MAP.get(c, c) for c in date_only_cols]
    key_col_mapped = COLUMN_NAME_MAP.get(args['KEY_COLUMN'], args['KEY_COLUMN'])
else:
    key_col_mapped = args['KEY_COLUMN']

sf_cols = set(df_sf.columns)
pg_cols = set(df_pg.columns)

results["missing_cols_in_pg"] = list(sf_cols - pg_cols)
results["missing_cols_in_sf"] = list(pg_cols - sf_cols)

common_cols = sorted(sf_cols & pg_cols)
key_col = key_col_mapped

# ------------------------------------------------------------------
# 9. Null count comparison
# ------------------------------------------------------------------
def null_counts(df, cols):
    row = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in cols
    ]).collect()[0].asDict()
    return row

results["null_counts_sf"] = null_counts(df_sf, common_cols)
results["null_counts_pg"] = null_counts(df_pg, common_cols)

# ------------------------------------------------------------------
# 10. Duplicate check on key column (Salesforce Id should be unique)
# ------------------------------------------------------------------
dupes_pg = df_pg.groupBy(key_col).count().filter("count > 1")
dupes_pg_count = dupes_pg.count()
results["duplicate_keys_in_pg"] = dupes_pg_count
if dupes_pg_count > 0:
    logger.warn(f"{dupes_pg_count} duplicate keys found in Postgres table on column '{key_col}'")

# Also check Salesforce side - dupes here usually mean a bad extract (e.g. pagination bug)
dupes_sf = df_sf.groupBy(key_col).count().filter("count > 1")
dupes_sf_count = dupes_sf.count()
results["duplicate_keys_in_sf_extract"] = dupes_sf_count
if dupes_sf_count > 0:
    logger.warn(f"{dupes_sf_count} duplicate keys found in Salesforce extract on column '{key_col}' - check extraction logic")

# ------------------------------------------------------------------
# 11. Timestamp range sanity check (min/max) per timestamp column
# ------------------------------------------------------------------
for col in timestamp_cols:
    if col in df_sf.columns and col in df_pg.columns:
        sf_min, sf_max = df_sf.select(F.min(col), F.max(col)).collect()[0]
        pg_min, pg_max = df_pg.select(F.min(col), F.max(col)).collect()[0]
        results["timestamp_range_checks"][col] = {
            "sf_min": str(sf_min), "sf_max": str(sf_max),
            "pg_min": str(pg_min), "pg_max": str(pg_max),
            "range_match": (sf_min == pg_min) and (sf_max == pg_max)
        }

# ------------------------------------------------------------------
# 12. Row-level diff (precision-safe: truncate timestamps to seconds
#     since SF gives milliseconds but PG column may be second-precision)
# ------------------------------------------------------------------
TS_COMPARE_PRECISION = "second"  # change to "millisecond" if PG stores full precision

def build_comparable_df(df, cols, ts_cols, date_cols):
    select_exprs = []
    for c in cols:
        if c in ts_cols:
            truncated = F.date_trunc(TS_COMPARE_PRECISION, F.col(c))
            select_exprs.append(F.date_format(truncated, "yyyy-MM-dd HH:mm:ss").alias(c))
        elif c in date_cols:
            select_exprs.append(F.date_format(F.col(c), "yyyy-MM-dd").alias(c))
        else:
            select_exprs.append(F.col(c).cast("string").alias(c))
    return df.select(select_exprs)

df_sf_aligned = build_comparable_df(df_sf, common_cols, timestamp_cols, date_only_cols)
df_pg_aligned = build_comparable_df(df_pg, common_cols, timestamp_cols, date_only_cols)

rows_only_in_sf = df_sf_aligned.exceptAll(df_pg_aligned)
rows_only_in_pg = df_pg_aligned.exceptAll(df_sf_aligned)

rows_only_in_sf_count = rows_only_in_sf.count()
rows_only_in_pg_count = rows_only_in_pg.count()

results["rows_only_in_sf"] = rows_only_in_sf_count
results["rows_only_in_pg"] = rows_only_in_pg_count

logger.info(f"Rows only in Salesforce: {rows_only_in_sf_count}, Rows only in PG: {rows_only_in_pg_count}")

# ------------------------------------------------------------------
# 13. Numeric aggregate sanity checks (sum on numeric cols, e.g. Amount)
# ------------------------------------------------------------------
numeric_types = ("DoubleType", "IntegerType", "LongType", "DecimalType", "FloatType")
numeric_cols = [f.name for f in df_sf.schema.fields if str(f.dataType) in numeric_types and f.name in df_pg.columns]

agg_diffs = {}
for col in numeric_cols:
    sf_sum = df_sf.select(F.sum(col)).collect()[0][0]
    pg_sum = df_pg.select(F.sum(col)).collect()[0][0]
    agg_diffs[col] = {
        "sf_sum": sf_sum,
        "pg_sum": pg_sum,
        "match": sf_sum == pg_sum
    }
results["aggregate_checks"] = agg_diffs

# ------------------------------------------------------------------
# 14. Overall pass/fail
# ------------------------------------------------------------------
ts_parse_ok = all(v == 0 for v in results["timestamp_parse_failures"].values())

results["passed"] = (
    results["row_count_match"]
    and not results["missing_cols_in_pg"]
    and not results["missing_cols_in_sf"]
    and dupes_pg_count == 0
    and dupes_sf_count == 0
    and rows_only_in_sf_count == 0
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

if rows_only_in_sf_count > 0:
    rows_only_in_sf.write.mode("overwrite").option("header", True) \
        .csv(f"{args['VALIDATION_OUTPUT_PATH']}/rows_only_in_sf_{run_id}/")

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
        Subject=f"Salesforce->PG validation FAILED: {args['JOB_NAME']}",
        Message=json.dumps(results, indent=2, default=str)
    )
    logger.error("Validation failed - alert sent via SNS")

if not results["passed"]:
    raise Exception(f"Salesforce -> Postgres validation failed. See report at {report_path}")

logger.info("Validation passed.")
job.commit()
