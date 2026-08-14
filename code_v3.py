"""
AWS Glue Job: Salesforce Attachment Retry Prep
================================================
Companion job to glue_sf_attachment_downloader.py.

Reads the FAILED manifest (CSV, one or more part-files under a single S3
prefix) produced by that job, and reshapes it back into the same column
shape the downloader job expects as its INPUT_CSV_PATH -- so a failed run
can be retried by simply pointing the downloader job at this job's output.

FAILED manifest columns (input to this job):
    AttachmentId, ParentId, FileName, ContentType, BodyLength, CreatedDate,
    LastModifiedDate, SystemModstamp, S3RelativePath, S3AbsolutePath,
    Status, ErrorType, ErrorMessage, Attempts, Timestamp

Downloader's expected input CSV columns (output of this job):
    Id, ParentId, Name, IsPrivate, ContentType, BodyLength, OwnerId,
    CreatedDate, CreatedById, LastModifiedDate, LastModifiedById,
    SystemModstamp, IsDeleted, Description

Column mapping (FAILED manifest -> retry input):
    AttachmentId      -> Id
    ParentId          -> ParentId
    FileName          -> Name
    ContentType       -> ContentType
    BodyLength        -> BodyLength
    CreatedDate       -> CreatedDate
    LastModifiedDate  -> LastModifiedDate
    SystemModstamp    -> SystemModstamp
    (no source)       -> IsPrivate, OwnerId, CreatedById, LastModifiedById,
                          IsDeleted, Description  (not carried in the FAILED
                          manifest, so written out as null -- the downloader
                          job never reads these columns anyway; they're only
                          accepted so the CSV can double as a straight
                          Attachment-table extract)

Row filtering:
    - Every row in the FAILED manifest is carried through and retried,
      regardless of ErrorType (including OrgApiLimitExceeded, transient
      HTTP/S3 errors, BodyLengthMismatch, AuthFailure, ValidationError,
      etc.) -- the downloader job re-validates and re-attempts each row
      itself on the next run, so this job does not try to pre-judge which
      errors are "worth" retrying.
    - The one hard exception is rows with a null/blank AttachmentId or
      ParentId (mainly MalformedCsvRow rows, where the original CSV row was
      corrupt and no AttachmentId/ParentId could even be parsed out of it).
      There is nothing to retry for these -- there's no valid Salesforce ID
      to call -- so they're dropped here rather than being fed back in to
      fail the same is_valid_sf_id() check again next run. They're counted
      and logged so they aren't silently lost; they remain visible in the
      original FAILED manifest for manual follow-up.

Output:
    - A retry-ready CSV (written as a folder of part-files, which is a
      valid input to Spark's csv reader -- point the downloader job's
      INPUT_CSV_PATH straight at this output path/folder for the retry run)
    - job_name for output naming is derived from the FAILED input path the
      same way the downloader derives it from its own input, so each retry
      pass's prep output is identifiable and doesn't collide with others.
    - A small JSON summary (rows read, rows dropped as unretryable, rows
      written, breakdown of dropped rows by ErrorType) written next to the
      retry CSV for visibility into what happened during prep.

Required Glue job parameters (--arg-name value):
  --FAILED_INPUT_PATH     s3://your-output-bucket/manifests/failed/        (folder from the run being retried)
  --RETRY_OUTPUT_PATH     s3://your-output-bucket/input/retry_attachments/ (feed this to the downloader's INPUT_CSV_PATH)
  --NUM_OUTPUT_PARTITIONS 40                                               (controls part-file count/size of retry CSV)

Recommended usage:
    1. Run glue_sf_attachment_downloader.py.
    2. Run this job with FAILED_INPUT_PATH pointed at that run's
       FAILED_OUTPUT_PATH.
    3. Run glue_sf_attachment_downloader.py again with INPUT_CSV_PATH set to
       this job's RETRY_OUTPUT_PATH (and a fresh SUCCESS/FAILED output path,
       or one you're OK appending into, since the downloader writes in
       "append" mode).
    4. Repeat from step 2 against the new FAILED manifest until it's empty
       or you decide remaining failures need manual investigation.
"""

import sys
import re
import json
import datetime as dt

import boto3

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# --------------------------------------------------------------------------- #
# Glue / Spark bootstrap
# --------------------------------------------------------------------------- #

ARGS = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "FAILED_INPUT_PATH",
        "RETRY_OUTPUT_PATH",
        "NUM_OUTPUT_PARTITIONS",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(ARGS["JOB_NAME"], ARGS)

logger = glueContext.get_logger()

FAILED_INPUT_PATH = ARGS["FAILED_INPUT_PATH"]
RETRY_OUTPUT_PATH = ARGS["RETRY_OUTPUT_PATH"].rstrip("/")
NUM_OUTPUT_PARTITIONS = int(ARGS["NUM_OUTPUT_PARTITIONS"])


def derive_job_name(path: str) -> str:
    """Same derivation logic as the downloader job, applied here to the
    FAILED_INPUT_PATH, so a summary JSON for this retry-prep pass is
    identifiable and doesn't collide with summaries from other passes."""
    base = path.rstrip("/").split("/")[-1]
    if "." in base:
        base = base.rsplit(".", 1)[0]
    base = re.sub(r"[^A-Za-z0-9._-]", "_", base)
    return base or "unknown_input"


def parse_s3_path(s3_path: str):
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected an s3:// path, got: {s3_path}")
    without_scheme = s3_path[len("s3://"):]
    bucket, _, prefix = without_scheme.partition("/")
    if not bucket:
        raise ValueError(f"Could not parse bucket from path: {s3_path}")
    return bucket, prefix.strip("/")


JOB_NAME_TAG = derive_job_name(FAILED_INPUT_PATH)

# --------------------------------------------------------------------------- #
# Schema for the FAILED manifest (mirrors RESULT_FIELDS in the downloader).
# --------------------------------------------------------------------------- #

FAILED_SCHEMA = StructType([
    StructField("AttachmentId", StringType(), True),
    StructField("ParentId", StringType(), True),
    StructField("FileName", StringType(), True),
    StructField("ContentType", StringType(), True),
    StructField("BodyLength", LongType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("LastModifiedDate", StringType(), True),
    StructField("SystemModstamp", StringType(), True),
    StructField("S3RelativePath", StringType(), True),
    StructField("S3AbsolutePath", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("ErrorType", StringType(), True),
    StructField("ErrorMessage", StringType(), True),
    StructField("Attempts", LongType(), True),
    StructField("Timestamp", StringType(), True),
])


def write_summary(bucket, prefix, payload):
    """Best-effort write of a small JSON summary of this retry-prep pass.
    Never raises -- a summary write failure shouldn't fail the job after the
    actual retry CSV has already been written successfully."""
    ts = dt.datetime.utcnow().isoformat().replace(":", "-")
    key = f"{prefix}/{JOB_NAME_TAG}_retry_prep_{ts}.json"
    try:
        client = boto3.client("s3")
        client.put_object(
            Bucket=bucket, Key=key,
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info(f"Wrote retry-prep summary to s3://{bucket}/{key}")
    except Exception as e:  # noqa: BLE001 - summary is best-effort
        logger.warn(f"Failed to write retry-prep summary to s3://{bucket}/{key}: {e}")


def main():
    logger.info(f"Reading FAILED manifest from {FAILED_INPUT_PATH} (job_name={JOB_NAME_TAG})")

    df = (
        spark.read
        .option("header", "true")
        .schema(FAILED_SCHEMA)
        .csv(FAILED_INPUT_PATH)
    )

    if df.rdd.isEmpty():
        logger.warn(f"FAILED manifest at {FAILED_INPUT_PATH} is empty -- nothing to retry.")
        out_bucket, out_prefix = parse_s3_path(RETRY_OUTPUT_PATH)
        write_summary(out_bucket, out_prefix, {
            "job_name": JOB_NAME_TAG,
            "failed_input_path": FAILED_INPUT_PATH,
            "retry_output_path": RETRY_OUTPUT_PATH,
            "rows_read": 0,
            "rows_dropped_unretryable": 0,
            "rows_written": 0,
            "dropped_by_error_type": {},
            "status": "EMPTY_INPUT",
        })
        job.commit()
        return

    df = df.cache()
    total_read = df.count()

    # Rows with no usable AttachmentId/ParentId (mainly MalformedCsvRow rows)
    # can't be retried -- there's no valid Salesforce ID to call. Drop them
    # here rather than feeding them back in to fail the same validation
    # check again next run.
    retryable_df = df.filter(
        F.col("AttachmentId").isNotNull() & (F.trim(F.col("AttachmentId")) != "")
        & F.col("ParentId").isNotNull() & (F.trim(F.col("ParentId")) != "")
    )
    dropped_df = df.filter(
        F.col("AttachmentId").isNull() | (F.trim(F.col("AttachmentId")) == "")
        | F.col("ParentId").isNull() | (F.trim(F.col("ParentId")) == "")
    )

    dropped_count = dropped_df.count()
    if dropped_count:
        logger.warn(
            f"{dropped_count} FAILED row(s) have no usable AttachmentId/ParentId "
            "and cannot be retried -- excluded from retry CSV, left in original "
            "FAILED manifest for manual follow-up."
        )

    dropped_by_error_type = {
        r["ErrorType"] or "Unknown": r["count"]
        for r in dropped_df.groupBy("ErrorType").count().collect()
    } if dropped_count else {}

    # Reshape into the downloader's expected input schema. Columns not
    # carried in the FAILED manifest (IsPrivate, OwnerId, CreatedById,
    # LastModifiedById, IsDeleted, Description) are written out as null --
    # the downloader only reads Id/ParentId/Name/ContentType/BodyLength/
    # CreatedDate/LastModifiedDate/SystemModstamp from its input, so these
    # are accepted-but-unused columns, present purely so the retry CSV has
    # the same shape as a straight Attachment-table extract.
    retry_df = retryable_df.select(
        F.col("AttachmentId").alias("Id"),
        F.col("ParentId").alias("ParentId"),
        F.col("FileName").alias("Name"),
        F.lit(None).cast(StringType()).alias("IsPrivate"),
        F.col("ContentType").alias("ContentType"),
        F.col("BodyLength").alias("BodyLength"),
        F.lit(None).cast(StringType()).alias("OwnerId"),
        F.col("CreatedDate").alias("CreatedDate"),
        F.lit(None).cast(StringType()).alias("CreatedById"),
        F.col("LastModifiedDate").alias("LastModifiedDate"),
        F.lit(None).cast(StringType()).alias("LastModifiedById"),
        F.col("SystemModstamp").alias("SystemModstamp"),
        F.lit(None).cast(StringType()).alias("IsDeleted"),
        F.lit(None).cast(StringType()).alias("Description"),
    )

    written_count = retry_df.count()

    logger.info(f"Writing retry-ready input CSV ({written_count} rows) to {RETRY_OUTPUT_PATH}")
    (
        retry_df
        .coalesce(max(1, NUM_OUTPUT_PARTITIONS))
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(RETRY_OUTPUT_PATH)
    )

    out_bucket, out_prefix = parse_s3_path(RETRY_OUTPUT_PATH)
    write_summary(out_bucket, out_prefix, {
        "job_name": JOB_NAME_TAG,
        "failed_input_path": FAILED_INPUT_PATH,
        "retry_output_path": RETRY_OUTPUT_PATH,
        "rows_read": total_read,
        "rows_dropped_unretryable": dropped_count,
        "rows_written": written_count,
        "dropped_by_error_type": dropped_by_error_type,
        "status": "COMPLETED",
    })

    logger.info(f"Done. read={total_read} dropped={dropped_count} written={written_count}")
    df.unpersist()
    job.commit()


if __name__ == "__main__":
    main()
