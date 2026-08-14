"""
AWS Glue Job: Salesforce Attachment Retry Prep (multi-batch)
==============================================================
Companion job to glue_sf_attachment_downloader.py.

Reads FAILED manifests from MULTIPLE BATCH SUBFOLDERS under a single root
prefix -- e.g.:

    s3://bucket/manifests/failed/run1/part-00000....csv
    s3://bucket/manifests/failed/run1/part-00001....csv
    s3://bucket/manifests/failed/run2/part-00000....csv
    ...

-- discovers every batch subfolder under FAILED_INPUT_ROOT automatically,
reshapes ALL of their FAILED rows back into the column shape the downloader
job expects as its INPUT_CSV_PATH, and writes ONE combined retry-ready CSV
covering every batch in a single pass.

FAILED manifest columns (input to this job):
    AttachmentId, ParentId, FileName, ContentType, BodyLength, CreatedDate,
    LastModifiedDate, SystemModstamp, S3RelativePath, S3AbsolutePath,
    Status, ErrorType, ErrorMessage, Attempts, Timestamp

Final output column order (as required by the downstream consumer of this
job's retry CSV):
    Id, IsDeleted, ParentId, Name, IsPrivate, ContentType, BodyLength,
    OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById,
    SystemModstamp, Description, IsEncrypted

Column mapping (FAILED manifest -> retry input):
    AttachmentId      -> Id
    ParentId          -> ParentId
    FileName          -> Name
    ContentType       -> ContentType
    BodyLength        -> BodyLength
    CreatedDate       -> CreatedDate
    LastModifiedDate  -> LastModifiedDate
    SystemModstamp    -> SystemModstamp
    (no source)       -> IsDeleted, IsPrivate, OwnerId, CreatedById,
                          LastModifiedById, Description, IsEncrypted  (not
                          carried in the FAILED manifest, written out as
                          null -- the downloader doesn't read these columns,
                          they only exist so the CSV can double as a
                          straight Attachment extract)

Batch discovery:
    - FAILED_INPUT_ROOT is treated as a prefix containing one subfolder per
      batch/run (e.g. run1/, run2/), NOT as a single flat folder of CSVs.
      Subfolders are discovered via an S3 ListObjectsV2 "directory" listing
      (delimiter="/") on the driver -- cheap, since it only lists folder
      names, not every object inside them.
    - All batches' CSVs are then read in ONE Spark read using
      recursiveFileLookup, rather than looping and reading+writing batch by
      batch -- avoids N separate small Spark jobs (and N separate output
      writes) for N batches.
    - Each row is tagged with its source batch name (parsed from its file
      path via input_file_name()) purely for the summary breakdown below;
      the tag column is dropped before the final retry CSV is written since
      the downloader's schema doesn't include it.

Row filtering:
    - Every row from every batch is carried through and retried, regardless
      of ErrorType (OrgApiLimitExceeded, transient HTTP/S3 errors,
      BodyLengthMismatch, AuthFailure, ValidationError, etc.) -- the
      downloader job re-validates and re-attempts each row itself, so this
      job does not pre-judge which errors are "worth" retrying.
    - The one hard exception: rows with a null/blank AttachmentId or
      ParentId (mainly MalformedCsvRow rows, where the original CSV row was
      corrupt and no AttachmentId/ParentId could even be parsed out of it).
      There's no valid Salesforce ID to retry for these, so they're dropped
      here rather than being fed back in to fail the same validation check
      again. They're counted (overall and per-batch) and logged so they
      aren't silently lost; they remain in the original FAILED manifests for
      manual follow-up.
    - If the SAME AttachmentId shows up as failed in more than one batch
      (e.g. it failed in run1, and again after being retried in run2), only
      one retry row is kept for it -- deduped by AttachmentId, keeping the
      row with the latest Timestamp -- so the combined retry CSV doesn't
      feed the downloader duplicate rows for the same attachment.

Output:
    - ONE retry-ready CSV (written as a folder of part-files, which is a
      valid input to Spark's csv reader) covering every discovered batch --
      point the downloader job's INPUT_CSV_PATH straight at RETRY_OUTPUT_PATH
      for the retry run.
    - A single JSON summary with overall totals (rows read, rows dropped as
      unretryable, duplicate rows collapsed, rows written) AND a per-batch
      breakdown (same counts, plus dropped-by-error-type) so you can see
      which batches contributed how much to the retry set.

Required Glue job parameters (--arg-name value):
  --FAILED_INPUT_ROOT      s3://your-output-bucket/manifests/failed/        (root; contains run1/, run2/, ... subfolders)
  --RETRY_OUTPUT_PATH      s3://your-output-bucket/input/retry_attachments/ (feed this to the downloader's INPUT_CSV_PATH)
  --NUM_OUTPUT_PARTITIONS  40                                               (controls part-file count/size of retry CSV)

Recommended usage:
    1. Run glue_sf_attachment_downloader.py one or more times (each run's
       FAILED_OUTPUT_PATH pointed at its own batch subfolder under a shared
       root, e.g. .../manifests/failed/run1/, .../manifests/failed/run2/).
    2. Run this job once with FAILED_INPUT_ROOT set to that shared root --
       it picks up every batch subfolder automatically.
    3. Run glue_sf_attachment_downloader.py again with INPUT_CSV_PATH set to
       this job's RETRY_OUTPUT_PATH (and a fresh SUCCESS/FAILED output
       location, or one you're OK appending into).
    4. Repeat from step 2 against the new FAILED batches until the retry set
       is empty or remaining failures need manual investigation.
"""

import sys
import re
import json
import datetime as dt

import boto3

from pyspark.context import SparkContext
from pyspark.sql import Window
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
        "FAILED_INPUT_ROOT",
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

FAILED_INPUT_ROOT = ARGS["FAILED_INPUT_ROOT"].rstrip("/")
RETRY_OUTPUT_PATH = ARGS["RETRY_OUTPUT_PATH"].rstrip("/")
NUM_OUTPUT_PARTITIONS = int(ARGS["NUM_OUTPUT_PARTITIONS"])


def parse_s3_path(s3_path: str):
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected an s3:// path, got: {s3_path}")
    without_scheme = s3_path[len("s3://"):]
    bucket, _, prefix = without_scheme.partition("/")
    if not bucket:
        raise ValueError(f"Could not parse bucket from path: {s3_path}")
    return bucket, prefix.strip("/")


def derive_tag(path: str) -> str:
    base = path.rstrip("/").split("/")[-1]
    base = re.sub(r"[^A-Za-z0-9._-]", "_", base)
    return base or "unknown"


ROOT_TAG = derive_tag(FAILED_INPUT_ROOT)


def discover_batch_subfolders(root_path: str):
    """Lists immediate "subfolders" under root_path via a delimited
    ListObjectsV2 call (cheap -- returns folder names via CommonPrefixes,
    not every object inside them). Each subfolder is treated as one batch
    (one prior downloader run's FAILED_OUTPUT_PATH)."""
    bucket, prefix = parse_s3_path(root_path)
    prefix = f"{prefix}/" if prefix and not prefix.endswith("/") else prefix

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    batch_prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []) or []:
            batch_prefixes.append(f"s3://{bucket}/{cp['Prefix']}")
    return batch_prefixes


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
    """Best-effort write of the retry-prep summary JSON. Never raises -- a
    summary write failure shouldn't fail the job after the actual retry CSV
    has already been written successfully."""
    ts = dt.datetime.utcnow().isoformat().replace(":", "-")
    key = f"{prefix}/{ROOT_TAG}_retry_prep_{ts}.json"
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
    out_bucket, out_prefix = parse_s3_path(RETRY_OUTPUT_PATH)

    logger.info(f"Discovering batch subfolders under {FAILED_INPUT_ROOT}")
    batch_paths = discover_batch_subfolders(FAILED_INPUT_ROOT)

    if not batch_paths:
        logger.warn(f"No batch subfolders found under {FAILED_INPUT_ROOT} -- nothing to retry.")
        write_summary(out_bucket, out_prefix, {
            "failed_input_root": FAILED_INPUT_ROOT,
            "retry_output_path": RETRY_OUTPUT_PATH,
            "batches_found": 0,
            "rows_read": 0,
            "rows_dropped_unretryable": 0,
            "duplicate_rows_collapsed": 0,
            "rows_written": 0,
            "per_batch": {},
            "status": "NO_BATCHES_FOUND",
        })
        job.commit()
        return

    logger.info(f"Found {len(batch_paths)} batch subfolder(s): {batch_paths}")

    # Single Spark read across every batch subfolder at once, rather than
    # looping and reading+writing batch by batch -- one Spark job instead of
    # N. recursiveFileLookup lets the reader descend into each run's
    # subfolder from the shared root.
    df = (
        spark.read
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .schema(FAILED_SCHEMA)
        .csv(FAILED_INPUT_ROOT)
    )

    if df.rdd.isEmpty():
        logger.warn(f"Batch subfolders under {FAILED_INPUT_ROOT} contained no rows -- nothing to retry.")
        write_summary(out_bucket, out_prefix, {
            "failed_input_root": FAILED_INPUT_ROOT,
            "retry_output_path": RETRY_OUTPUT_PATH,
            "batches_found": len(batch_paths),
            "rows_read": 0,
            "rows_dropped_unretryable": 0,
            "duplicate_rows_collapsed": 0,
            "rows_written": 0,
            "per_batch": {},
            "status": "EMPTY_INPUT",
        })
        job.commit()
        return

    # Tag each row with its source batch (the path segment directly under
    # FAILED_INPUT_ROOT) purely for the summary breakdown -- dropped before
    # the final retry CSV is written.
    escaped_root = re.escape(FAILED_INPUT_ROOT)
    df = df.withColumn(
        "_batch_name",
        F.regexp_extract(F.input_file_name(), rf"{escaped_root}/([^/]+)/", 1),
    )
    df = df.cache()
    total_read = df.count()

    retryable_df = df.filter(
        F.col("AttachmentId").isNotNull() & (F.trim(F.col("AttachmentId")) != "")
        & F.col("ParentId").isNotNull() & (F.trim(F.col("ParentId")) != "")
    )
    dropped_df = df.filter(
        F.col("AttachmentId").isNull() | (F.trim(F.col("AttachmentId")) == "")
        | F.col("ParentId").isNull() | (F.trim(F.col("ParentId")) == "")
    )
    dropped_count = dropped_df.count()

    # Dedup: if the same AttachmentId failed in more than one batch (e.g.
    # failed in run1, retried and failed again in run2), keep only the most
    # recent failure row for it so the combined retry CSV doesn't feed the
    # downloader duplicate rows for the same attachment.
    dedup_window = Window.partitionBy("AttachmentId").orderBy(F.col("Timestamp").desc())
    deduped_df = (
        retryable_df
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    retryable_count = retryable_df.count()
    deduped_count = deduped_df.count()
    duplicates_collapsed = retryable_count - deduped_count

    if dropped_count:
        logger.warn(
            f"{dropped_count} FAILED row(s) across all batches have no usable "
            "AttachmentId/ParentId and cannot be retried -- excluded, left in "
            "original FAILED manifests for manual follow-up."
        )
    if duplicates_collapsed:
        logger.info(
            f"{duplicates_collapsed} duplicate AttachmentId row(s) across batches "
            "collapsed to their most recent failure."
        )

    # Column order here matches the required final output schema exactly:
    # Id, IsDeleted, ParentId, Name, IsPrivate, ContentType, BodyLength,
    # OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById,
    # SystemModstamp, Description, IsEncrypted
    retry_df = deduped_df.select(
        F.col("AttachmentId").alias("Id"),
        F.lit(None).cast(StringType()).alias("IsDeleted"),
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
        F.lit(None).cast(StringType()).alias("Description"),
        F.lit(None).cast(StringType()).alias("IsEncrypted"),
    )

    written_count = retry_df.count()

    logger.info(f"Writing combined retry-ready input CSV ({written_count} rows) to {RETRY_OUTPUT_PATH}")
    (
        retry_df
        .coalesce(max(1, NUM_OUTPUT_PARTITIONS))
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(RETRY_OUTPUT_PATH)
    )

    # Per-batch breakdown for the summary: read/dropped counts and
    # dropped-by-error-type, grouped by the _batch_name tag parsed earlier.
    per_batch = {}
    read_by_batch = {r["_batch_name"]: r["count"] for r in df.groupBy("_batch_name").count().collect()}
    dropped_by_batch = {r["_batch_name"]: r["count"] for r in dropped_df.groupBy("_batch_name").count().collect()}
    dropped_error_by_batch_rows = (
        dropped_df.groupBy("_batch_name", "ErrorType").count().collect() if dropped_count else []
    )
    for batch_name in read_by_batch:
        per_batch[batch_name] = {
            "rows_read": read_by_batch.get(batch_name, 0),
            "rows_dropped_unretryable": dropped_by_batch.get(batch_name, 0),
            "dropped_by_error_type": {
                r["ErrorType"] or "Unknown": r["count"]
                for r in dropped_error_by_batch_rows
                if r["_batch_name"] == batch_name
            },
        }

    dropped_by_error_type_overall = {
        r["ErrorType"] or "Unknown": r["count"]
        for r in dropped_df.groupBy("ErrorType").count().collect()
    } if dropped_count else {}

    write_summary(out_bucket, out_prefix, {
        "failed_input_root": FAILED_INPUT_ROOT,
        "retry_output_path": RETRY_OUTPUT_PATH,
        "batches_found": len(batch_paths),
        "batch_paths": batch_paths,
        "rows_read": total_read,
        "rows_dropped_unretryable": dropped_count,
        "dropped_by_error_type": dropped_by_error_type_overall,
        "duplicate_rows_collapsed": duplicates_collapsed,
        "rows_written": written_count,
        "per_batch": per_batch,
        "status": "COMPLETED",
    })

    logger.info(
        f"Done. batches={len(batch_paths)} read={total_read} dropped={dropped_count} "
        f"duplicates_collapsed={duplicates_collapsed} written={written_count}"
    )
    df.unpersist()
    job.commit()


if __name__ == "__main__":
    main()
