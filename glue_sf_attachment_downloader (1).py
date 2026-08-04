"""
AWS Glue Job: Salesforce Attachment Bulk Downloader -> S3
============================================================
Reads a CSV (replica of the Salesforce Attachment table, e.g. exported via a
prior Bulk API extract) from S3, downloads each attachment's binary Body from
Salesforce via the REST API, and lands it in S3 at:

    s3://<bucket>/<root_folder>/year=<YYYY>/month=<MM>/<ParentId>/<AttachmentId>/<filename>

Outputs:
  - SUCCESS manifest (CSV): AttachmentId, ParentId, FileName, ContentType, BodyLength,
    S3RelativePath, S3AbsolutePath, Status, Timestamp
  - FAILED manifest (CSV, same schema as SUCCESS plus ErrorType/ErrorMessage/Attempts):
    designed to be reshaped by glue_sf_attachment_retry_prep.py and re-fed into this
    same job as the input CSV for a retry pass.

Edge cases explicitly handled:
  - Malformed/corrupt CSV rows (wrong column count, bad quoting) are captured via
    Spark's PERMISSIVE CSV mode and routed to the FAILED manifest instead of
    silently dropped or crashing the job.
  - Empty input file / empty partitions short-circuit without wasting a
    Salesforce login call.
  - A Salesforce login failure at the start of a partition (bad creds, network
    down) marks every row in that partition as FAILED with AuthFailure instead
    of raising and killing the Spark task (which would otherwise retry the
    whole partition up to spark.task.maxFailures times and can abort the job).
  - Malformed Salesforce IDs (wrong length/charset) are rejected before making
    an API call, saving a wasted round trip.
  - Salesforce's *daily* API limit (403 REQUEST_LIMIT_EXCEEDED) is treated
    differently from a transient rate limit (429): it's not retryable within
    the run, and a per-partition flag short-circuits remaining rows in that
    partition once it's hit, rather than burning through retries row by row.
  - Attachment bodies are streamed directly to S3 (not buffered fully into
    memory) so large files don't blow up memory under thread concurrency.
  - S3 key length (1024-byte AWS limit) is respected by dynamically truncating
    the filename based on how much of the key the folder path already uses.
  - Zero-byte attachments are treated as a valid success, not an error.
  - The number of bytes actually streamed to S3 for each attachment is
    compared against the source row's BodyLength; a mismatch (truncated
    download, proxy interference, etc.) is treated as a retryable failure
    rather than silently landing a corrupt/incomplete file as a "success".

Designed for 4M+ rows:
  - No driver-side collect(); everything flows through Spark RDD/DataFrame ops.
  - Salesforce auth + requests.Session (HTTP keep-alive + urllib3 Retry) created
    ONCE PER PARTITION, not per row -> avoids re-login storms and re-establishes
    TCP connections efficiently.
  - Within each partition, rows are processed concurrently via a ThreadPoolExecutor
    (THREAD_POOL_SIZE threads), using a SLIDING WINDOW of in-flight futures rather
    than fixed batches -- a completed row immediately frees a worker slot for the
    next row instead of the whole pool stalling on the slowest row in a batch.
  - boto3 S3 client created once per partition (boto3 clients are not
    picklable/shareable across the driver->executor boundary, so never create
    them in the driver and pass them in). Client is thread-safe and shared across
    the partition's thread pool. Explicit connect/read timeouts are set so a
    stalled connection can't hang a partition indefinitely.
  - S3 uploads use an explicit TransferConfig with a capped internal thread count.
    Without this, boto3's default multipart upload spins up its own thread pool
    (default max_concurrency=10) INSIDE each of our THREAD_POOL_SIZE row-worker
    threads -- i.e. up to THREAD_POOL_SIZE x 10 OS threads per partition just for
    uploads, competing for the same CPU/network the row workers need.
  - Exponential backoff w/ jitter on transient Salesforce/S3 errors (429/5xx).
  - Partition count AND thread pool size are both tunable to respect Salesforce
    concurrent API limits: effective concurrency ≈ NUM_PARTITIONS_IN_FLIGHT *
    THREAD_POOL_SIZE, so turn down whichever knob keeps you under your org's limit.
  - The input CSV is read once and cached before the empty-file check, corrupt-row
    filter/count, and clean-row filter all reuse it -- avoiding three separate
    full scans of the source file in S3.

Required Glue job parameters (--arg-name value):
  --INPUT_CSV_PATH        s3://your-bucket/input/attachment_replica.csv
  --OUTPUT_BUCKET         your-output-bucket
  --ATTACHMENT_ROOT       sf-attachments               (root "parent folder")
  --SUCCESS_OUTPUT_PATH   s3://your-output-bucket/manifests/success/
  --FAILED_OUTPUT_PATH    s3://your-output-bucket/manifests/failed/
  --SF_SECRET_NAME        salesforce/attachment-etl    (AWS Secrets Manager secret)
  --SF_API_VERSION        v60.0
  --SF_LOGIN_URL          https://your-domain.my.salesforce.com
  --NUM_PARTITIONS        400
  --THREAD_POOL_SIZE      10                            (concurrent downloads per partition)

Note on SF_LOGIN_URL: auth uses the OAuth 2.0 Client Credentials Flow, which
Salesforce only supports against an org's specific My Domain host -- the
generic https://login.salesforce.com / https://test.salesforce.com hosts do
NOT work for this grant type. Use your org's My Domain URL.

Recommended Glue/Spark setting for this job:
  --conf spark.speculation=false
  Speculative execution can launch a duplicate copy of a slow task. Since this
  job's tasks perform real side effects (S3 uploads + manifest rows) rather than
  pure functions, a speculative re-run risks duplicate manifest rows for the
  same AttachmentId. Turning speculation off avoids that; it's not set inside
  this script because it's a cluster-level setting, not a job argument.

Secrets Manager secret (JSON) -- OAuth 2.0 Client Credentials Flow, backed by
a connected app configured with a "Run As" integration user in Salesforce:
    {"client_id": "...", "client_secret": "..."}

Input CSV expected columns (case-sensitive; matches a full Attachment object
extract -- Body itself is excluded since it's fetched per-row via the REST
API, not carried in the CSV):
  Id, ParentId, Name, IsPrivate, ContentType, BodyLength, OwnerId,
  CreatedDate, CreatedById, LastModifiedDate, LastModifiedById,
  SystemModstamp, IsDeleted, Description
  - "Id" = Salesforce AttachmentId (18-char)
  - "CreatedDate" used to compute year=/month= partitioning; ISO 8601 string
  - "BodyLength" used to verify the downloaded/uploaded byte count matches
    what Salesforce recorded for the attachment
  - Only Id, ParentId, Name, ContentType, BodyLength, and CreatedDate are
    actually used for processing; the remaining columns are accepted so the
    CSV can be a straight, unmodified extract of the Attachment table, but
    are dropped before processing (see clean_df.select(...) in main()) to
    keep the per-partition shuffle payload small.
"""

import sys
import io
import re
import json
import time
import random
import logging
import threading
import itertools
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait

import boto3
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
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
        "INPUT_CSV_PATH",
        "OUTPUT_BUCKET",
        "ATTACHMENT_ROOT",
        "SUCCESS_OUTPUT_PATH",
        "FAILED_OUTPUT_PATH",
        "SF_SECRET_NAME",
        "SF_API_VERSION",
        "SF_LOGIN_URL",
        "NUM_PARTITIONS",
        "THREAD_POOL_SIZE",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(ARGS["JOB_NAME"], ARGS)

logger = glueContext.get_logger()

INPUT_CSV_PATH = ARGS["INPUT_CSV_PATH"]
OUTPUT_BUCKET = ARGS["OUTPUT_BUCKET"]
ATTACHMENT_ROOT = ARGS["ATTACHMENT_ROOT"].strip("/")
SUCCESS_OUTPUT_PATH = ARGS["SUCCESS_OUTPUT_PATH"]
FAILED_OUTPUT_PATH = ARGS["FAILED_OUTPUT_PATH"]
SF_SECRET_NAME = ARGS["SF_SECRET_NAME"]
SF_API_VERSION = ARGS["SF_API_VERSION"]
SF_LOGIN_URL = ARGS["SF_LOGIN_URL"].rstrip("/")
NUM_PARTITIONS = int(ARGS["NUM_PARTITIONS"])
THREAD_POOL_SIZE = int(ARGS["THREAD_POOL_SIZE"])

MAX_ROW_RETRIES = 4          # per-attachment retry attempts on transient errors
REQUEST_CONNECT_TIMEOUT = 10  # seconds, TCP connect
REQUEST_READ_TIMEOUT = 60     # seconds, waiting on response bytes
BACKOFF_BASE = 1.5           # seconds
MAX_S3_KEY_BYTES = 1024      # AWS hard limit
S3_KEY_SAFETY_MARGIN = 16    # leave headroom for encoding edge cases

# OPTIMIZATION: how many rows a partition keeps "in flight" across its
# ThreadPoolExecutor at once. Deliberately a small multiple of THREAD_POOL_SIZE
# (not a large fixed batch) -- see the sliding-window executor below for why.
MAX_IN_FLIGHT_MULTIPLIER = 2

# OPTIMIZATION: explicit, capped transfer config for S3 uploads. use_threads
# stays True (multipart still benefits big files) but max_concurrency is
# capped low, since real concurrency in this job already comes from
# THREAD_POOL_SIZE row-level workers -- letting every one of those workers
# also spin up boto3's default 10-thread multipart pool multiplies OS thread
# count by 10x per partition for no throughput benefit.
S3_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,
    multipart_chunksize=8 * 1024 * 1024,
    max_concurrency=2,
    use_threads=True,
)

# Salesforce record IDs are always 15 (case-sensitive) or 18
# (case-insensitive, checksum-suffixed) alphanumeric characters.
SF_ID_PATTERN = re.compile(r"^[a-zA-Z0-9]{15}$|^[a-zA-Z0-9]{18}$")

# --------------------------------------------------------------------------- #
# Secrets (fetched once in the driver, then broadcast -- avoids 4M
# Secrets Manager calls, one per row/partition)
# --------------------------------------------------------------------------- #


def _load_secret(secret_name: str) -> dict:
    # No explicit region_name -- boto3 resolves it from the standard chain
    # (AWS_DEFAULT_REGION / AWS_REGION env vars, or the Glue job's runtime
    # environment), same as every other boto3 client in this script.
    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


_sf_creds = _load_secret(SF_SECRET_NAME)
SF_CREDS_BC = sc.broadcast(_sf_creds)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

_INVALID_FS_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f]')


def sanitize_filename(name: str, fallback: str) -> str:
    if not name:
        name = fallback
    name = _INVALID_FS_CHARS.sub("_", name).strip().strip(".")
    return name if name else fallback


def is_valid_sf_id(value: str) -> bool:
    return bool(value) and bool(SF_ID_PATTERN.match(value))


def parse_year_month(created_date: str):
    """Return (yyyy, mm) strings from an ISO-ish Salesforce CreatedDate."""
    if not created_date:
        now = dt.datetime.utcnow()
        return f"{now.year:04d}", f"{now.month:02d}"
    try:
        # Salesforce CreatedDate typically: 2024-01-05T12:34:56.000+0000
        cleaned = created_date.replace("Z", "+00:00")
        parsed = dt.datetime.fromisoformat(cleaned[:19])
        return f"{parsed.year:04d}", f"{parsed.month:02d}"
    except Exception:
        now = dt.datetime.utcnow()
        return f"{now.year:04d}", f"{now.month:02d}"


def build_s3_key(root: str, year: str, month: str, parent_id: str,
                  attachment_id: str, filename: str) -> str:
    """Builds the S3 key, truncating the filename (preserving its extension)
    if the full key would exceed S3's 1024-byte key length limit."""
    prefix = f"{root}/year={year}/month={month}/{parent_id}/{attachment_id}/"
    budget = MAX_S3_KEY_BYTES - S3_KEY_SAFETY_MARGIN - len(prefix.encode("utf-8"))

    encoded = filename.encode("utf-8")
    if len(encoded) <= budget:
        return prefix + filename

    stem, dot, ext = filename.rpartition(".")
    ext_suffix = f".{ext}" if dot else ""
    stem_budget = max(1, budget - len(ext_suffix.encode("utf-8")))
    truncated_stem = stem.encode("utf-8")[:stem_budget].decode("utf-8", errors="ignore") if dot else \
        filename.encode("utf-8")[:budget].decode("utf-8", errors="ignore")
    return prefix + truncated_stem + ext_suffix


class _CountingStream:
    """Wraps a readable stream and counts bytes read through it.

    Used to verify the number of bytes actually streamed to S3 matches the
    source row's BodyLength, without buffering the whole attachment body in
    memory just to check its size."""

    def __init__(self, stream):
        self._stream = stream
        self.bytes_read = 0
        self._lock = threading.Lock()

    def read(self, amt=None):
        chunk = self._stream.read(amt) if amt is not None else self._stream.read()
        if chunk:
            with self._lock:
                self.bytes_read += len(chunk)
        return chunk

    def __getattr__(self, name):
        # Delegate anything else (e.g. urllib3's .close(), .closed) to the
        # wrapped stream so upload_fileobj can treat this as a drop-in
        # replacement for resp.raw.
        return getattr(self._stream, name)


def sf_login(creds: dict, login_url: str):
    """OAuth 2.0 Client Credentials Flow login -> (access_token, instance_url).

    No username/password/security token/refresh token involved -- auth is
    just client_id + client_secret against a connected app configured with a
    "Run As" integration user in Salesforce. login_url MUST be the org's
    specific My Domain host (e.g. https://your-domain.my.salesforce.com);
    the generic login/test.salesforce.com hosts don't support this grant."""
    token_url = f"{login_url}/services/oauth2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
    }

    resp = requests.post(
        token_url, data=payload,
        timeout=(REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT),
    )
    resp.raise_for_status()
    data = resp.json()
    return data["access_token"], data["instance_url"]


# --------------------------------------------------------------------------- #
# One-time Salesforce login on the DRIVER, broadcast to all executors.
# --------------------------------------------------------------------------- #
# Without this, each of the NUM_PARTITIONS partitions would call sf_login()
# independently on first use -- e.g. 400 login API calls for a 400-partition
# job, purely to obtain what is functionally the same session token. Logging
# in once here and broadcasting the token cuts that to a single call; each
# partition only calls sf_login() again later, lazily, if it actually hits a
# 401 (token expired/revoked mid-run) -- see relogin_if_stale() below.

try:
    _initial_access_token, _initial_instance_url = sf_login(_sf_creds, SF_LOGIN_URL)
except Exception as e:
    logger.error(f"Initial Salesforce login failed -- aborting job: {e}")
    raise

SF_AUTH_BC = sc.broadcast({
    "access_token": _initial_access_token,
    "instance_url": _initial_instance_url,
})


def make_session(pool_size: int) -> requests.Session:
    """HTTP session with connection pooling. Status-code retries (429/5xx) are
    intentionally NOT configured here -- our own per-row retry loop already
    handles those with application-aware backoff (and special-cases 401/403/404).
    Stacking urllib3's status retries on top would multiply retry attempts
    (MAX_ROW_RETRIES x urllib3 retries) and hammer Salesforce harder than
    intended. We only let urllib3 retry pure connection-level failures
    (refused/reset connections, DNS blips) since those aren't visible to our
    row-level loop until the exception surfaces."""
    session = requests.Session()
    retry = Retry(
        total=None,
        connect=2,
        read=2,
        status=0,               # don't retry on HTTP status codes here
        backoff_factor=0.5,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# --------------------------------------------------------------------------- #
# Partition-level worker
# --------------------------------------------------------------------------- #

RESULT_FIELDS = [
    "AttachmentId", "ParentId", "FileName", "ContentType", "BodyLength",
    "CreatedDate", "S3RelativePath", "S3AbsolutePath", "Status", "ErrorType",
    "ErrorMessage", "Attempts", "Timestamp",
]


def process_partition(rows):
    # Peek at the first row without fully materializing the partition, so
    # empty partitions skip all setup entirely (no wasted API calls).
    row_iter = iter(rows)
    try:
        first_row = next(row_iter)
    except StopIteration:
        return  # empty partition -- nothing to do
    row_iter = itertools.chain([first_row], row_iter)

    creds = SF_CREDS_BC.value

    # Session/S3-client construction failures must NOT raise here: an
    # uncaught exception in mapPartitions fails the Spark task, which Spark
    # then retries and can eventually abort the whole job. Note there is
    # deliberately NO Salesforce login call in this block -- auth_state is
    # seeded from the token the driver already obtained once and broadcast
    # (SF_AUTH_BC), so a partition that never needs to re-authenticate makes
    # zero login calls of its own.
    try:
        session = make_session(THREAD_POOL_SIZE)
        s3_client = boto3.client(
            "s3",
            config=BotoConfig(
                max_pool_connections=THREAD_POOL_SIZE,
                # OPTIMIZATION: explicit connect/read timeouts so a stalled
                # TCP connection or hung S3 response can't block a worker
                # thread (and therefore the partition) indefinitely.
                connect_timeout=REQUEST_CONNECT_TIMEOUT,
                read_timeout=REQUEST_READ_TIMEOUT,
            ),
        )
        auth_state = dict(SF_AUTH_BC.value)  # local copy; never mutate the broadcast value itself
    except Exception as e:  # noqa: BLE001
        err_msg = str(e)[:500]
        ts = dt.datetime.utcnow().isoformat()
        for row in row_iter:
            yield Row(
                AttachmentId=(row["Id"] or "").strip(), ParentId=(row["ParentId"] or "").strip(),
                FileName=(row["Name"] or "").strip(), ContentType=row["ContentType"],
                BodyLength=row["BodyLength"], CreatedDate=row["CreatedDate"],
                S3RelativePath=None, S3AbsolutePath=None, Status="FAILED",
                ErrorType="SetupFailure", ErrorMessage=f"Partition setup failed: {err_msg}",
                Attempts=0, Timestamp=ts,
            )
        return

    auth_lock = threading.Lock()
    # Set when Salesforce's DAILY org-wide API limit is hit (403
    # REQUEST_LIMIT_EXCEEDED). That's not retryable within this run, and
    # every subsequent call will fail the same way -- so once set, remaining
    # rows in this partition fail fast instead of each burning MAX_ROW_RETRIES
    # attempts against an exhausted quota.
    org_limit_hit = threading.Event()

    def relogin_if_stale(token_seen: str):
        """Re-auth only if no other thread has already refreshed the token
        since this thread last saw it (avoids redundant re-logins when many
        threads hit a 401 around the same time -- at most one sf_login() call
        per stale-token event, not one per thread). Raises if the re-login
        itself fails (bad/revoked creds); callers treat that as a normal
        per-row failure rather than crashing the partition."""
        with auth_lock:
            if auth_state["access_token"] == token_seen:
                auth_state["access_token"], auth_state["instance_url"] = sf_login(creds, SF_LOGIN_URL)
            return auth_state["access_token"], auth_state["instance_url"]

    def process_row(row) -> Row:
        attachment_id = (row["Id"] or "").strip()
        parent_id = (row["ParentId"] or "").strip()
        raw_name = (row["Name"] or "").strip()
        content_type = (row["ContentType"] or "application/octet-stream").strip()
        body_length = row["BodyLength"]
        created_date = row["CreatedDate"]

        def fail(error_type, error_message, attempts=0, filename=None):
            return Row(
                AttachmentId=attachment_id, ParentId=parent_id,
                FileName=filename or raw_name, ContentType=content_type, BodyLength=body_length,
                CreatedDate=created_date, S3RelativePath=None, S3AbsolutePath=None,
                Status="FAILED", ErrorType=error_type, ErrorMessage=error_message,
                Attempts=attempts, Timestamp=dt.datetime.utcnow().isoformat(),
            )

        if not is_valid_sf_id(attachment_id):
            return fail("ValidationError", f"Malformed AttachmentId: '{attachment_id}'")
        if not is_valid_sf_id(parent_id):
            return fail("ValidationError", f"Malformed ParentId: '{parent_id}'")

        filename = sanitize_filename(raw_name, f"{attachment_id}.bin")
        year, month = parse_year_month(created_date)
        s3_key = build_s3_key(ATTACHMENT_ROOT, year, month, parent_id, attachment_id, filename)
        s3_abs_path = f"s3://{OUTPUT_BUCKET}/{s3_key}"

        if org_limit_hit.is_set():
            return fail("OrgApiLimitExceeded",
                        "Salesforce daily API request limit already exceeded this run; skipped without calling out")

        last_error_type, last_error_msg = None, None
        success = False
        attempt = 0

        for attempt in range(1, MAX_ROW_RETRIES + 1):
            if org_limit_hit.is_set():
                last_error_type, last_error_msg = "OrgApiLimitExceeded", "Daily API limit hit mid-retry"
                break

            token, instance_url = auth_state["access_token"], auth_state["instance_url"]
            download_url = f"{instance_url}/services/data/{SF_API_VERSION}/sobjects/Attachment/{attachment_id}/Body"
            resp = None
            try:
                resp = session.get(
                    download_url, headers={"Authorization": f"Bearer {token}"},
                    # OPTIMIZATION: (connect, read) tuple instead of a single
                    # flat timeout -- a slow-to-connect Salesforce endpoint and
                    # a slow-to-stream large attachment body are different
                    # failure modes and are worth distinguishing/tuning
                    # separately.
                    timeout=(REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT),
                    stream=True,
                )

                if resp.status_code == 401:
                    # token expired mid-partition -> re-auth (once, across
                    # threads) and retry
                    relogin_if_stale(token)
                    last_error_type, last_error_msg = "AuthExpired", "401 - re-authenticated, retrying"
                    time.sleep(BACKOFF_BASE * attempt + random.uniform(0, 0.5))
                    continue

                if resp.status_code == 403:
                    error_code = None
                    try:
                        body = resp.json()
                        if isinstance(body, list) and body:
                            error_code = body[0].get("errorCode")
                    except Exception:
                        pass
                    if error_code == "REQUEST_LIMIT_EXCEEDED":
                        org_limit_hit.set()
                        last_error_type = "OrgApiLimitExceeded"
                        last_error_msg = "Salesforce daily API request limit exceeded (403 REQUEST_LIMIT_EXCEEDED)"
                    else:
                        last_error_type, last_error_msg = "Forbidden", f"403 - {error_code or 'access denied'}"
                    break  # not retryable either way

                if resp.status_code == 404:
                    last_error_type, last_error_msg = "NotFound", f"Attachment {attachment_id} not found (404)"
                    break  # not retryable

                resp.raise_for_status()

                # Stream the body straight to S3 instead of buffering the
                # whole attachment in memory first -- important under thread
                # concurrency where several large attachments could otherwise
                # be fully buffered at once. Config caps boto3's internal
                # multipart thread pool (see S3_TRANSFER_CONFIG comment above).
                # The stream is wrapped to count bytes as they pass through,
                # so we can confirm the full body actually made it to S3.
                resp.raw.decode_content = True
                counting_stream = _CountingStream(resp.raw)
                s3_client.upload_fileobj(
                    counting_stream, OUTPUT_BUCKET, s3_key,
                    ExtraArgs={"ContentType": content_type},
                    Config=S3_TRANSFER_CONFIG,
                )

                if body_length is not None and counting_stream.bytes_read != body_length:
                    # A short/long read usually means a dropped connection
                    # mid-stream or a proxy that truncated the response.
                    # Treat it as retryable rather than landing a corrupt
                    # file as a "success" -- the next attempt's upload will
                    # overwrite this same S3 key.
                    last_error_type = "BodyLengthMismatch"
                    last_error_msg = (
                        f"Expected BodyLength={body_length} but streamed "
                        f"{counting_stream.bytes_read} bytes to S3"
                    )
                    time.sleep(BACKOFF_BASE * attempt + random.uniform(0, 0.5))
                    continue

                success = True
                break

            except requests.exceptions.RequestException as e:
                last_error_type, last_error_msg = "HTTPError", str(e)[:500]
            except Exception as e:  # noqa: BLE001 - broad catch, boto3/S3 errors etc.
                last_error_type, last_error_msg = type(e).__name__, str(e)[:500]
            finally:
                if resp is not None:
                    resp.close()

            time.sleep(BACKOFF_BASE * attempt + random.uniform(0, 0.5))

        if success:
            return Row(
                AttachmentId=attachment_id, ParentId=parent_id, FileName=filename,
                ContentType=content_type, BodyLength=body_length,
                CreatedDate=created_date,
                S3RelativePath=s3_key, S3AbsolutePath=s3_abs_path, Status="SUCCESS",
                ErrorType=None, ErrorMessage=None, Attempts=attempt,
                Timestamp=dt.datetime.utcnow().isoformat(),
            )
        return fail(last_error_type or "UnknownError",
                    last_error_msg or "Exhausted retries with no specific error captured",
                    attempts=MAX_ROW_RETRIES, filename=filename)

    # OPTIMIZATION: sliding-window submission instead of fixed batches.
    #
    # The original pattern submitted BATCH_SIZE futures, then blocked on
    # as_completed() for the *entire* batch before submitting the next one.
    # With THREAD_POOL_SIZE=10 and BATCH_SIZE=200, a single slow row (near
    # the 60s read timeout, or hitting a 401 that triggers a re-login) means
    # the other 9 workers can finish their remaining rows in the batch and
    # then sit idle waiting for that one straggler before the executor is
    # given any more work -- effective concurrency degrades to well below
    # THREAD_POOL_SIZE near every batch boundary, and this happens roughly
    # (partition_size / BATCH_SIZE) times per partition.
    #
    # Instead, keep a fixed number of futures in flight (a small multiple of
    # THREAD_POOL_SIZE, not a large batch) and top the pool back up to that
    # level every time any single future completes. A slow row then only
    # ever idles its own worker slot, never the rest of the pool.
    max_in_flight = THREAD_POOL_SIZE * MAX_IN_FLIGHT_MULTIPLIER
    try:
        with ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE) as executor:
            in_flight = set()

            # Prime the pool.
            for row in itertools.islice(row_iter, max_in_flight):
                in_flight.add(executor.submit(process_row, row))

            while in_flight:
                done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    yield future.result()
                # Top back up to max_in_flight, one row per just-freed slot.
                for row in itertools.islice(row_iter, len(done)):
                    in_flight.add(executor.submit(process_row, row))
    finally:
        session.close()


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    logger.info(f"Reading input CSV from {INPUT_CSV_PATH}")

    # Schema includes a corrupt-record column so malformed rows (wrong column
    # count, unescaped quotes, etc.) are captured rather than silently
    # dropped or causing the read to fail. enforceSchema=false makes Spark
    # match columns by header name instead of position, so column reordering
    # in the source export doesn't silently misalign fields.
    # Full Attachment object schema (minus Body, which is fetched per-row via
    # the REST API rather than carried in the CSV). Declared as StringType
    # except BodyLength (needed for the byte-count check) and _corrupt_record,
    # so a stray non-boolean/date value in a column we don't use can't itself
    # cause a parse failure -- only actually malformed CSV structure should
    # route a row to _corrupt_record.
    input_schema = StructType([
        StructField("Id", StringType(), True),
        StructField("ParentId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("IsPrivate", StringType(), True),
        StructField("ContentType", StringType(), True),
        StructField("BodyLength", LongType(), True),
        StructField("OwnerId", StringType(), True),
        StructField("CreatedDate", StringType(), True),
        StructField("CreatedById", StringType(), True),
        StructField("LastModifiedDate", StringType(), True),
        StructField("LastModifiedById", StringType(), True),
        StructField("SystemModstamp", StringType(), True),
        StructField("IsDeleted", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(input_schema)
        .csv(INPUT_CSV_PATH)
    )

    # isEmpty() is a cheap check (effectively take(1)) -- keep it as the
    # first, uncached short-circuit so a genuinely empty file never pays for
    # a cache materialization at all.
    if df.rdd.isEmpty():
        logger.warn(f"Input CSV at {INPUT_CSV_PATH} is empty -- nothing to process.")
        job.commit()
        return

    # OPTIMIZATION: cache df once we know it's non-empty. Below, df is
    # scanned twice more (once to filter+count corrupt rows, once to filter
    # clean rows for processing). Without caching, both of those re-read and
    # re-parse the full CSV from S3 independently -- for a 4M-row file that's
    # two extra full object reads + CSV parses before a single attachment has
    # even been downloaded. Unpersisted once corrupt_df is no longer needed.
    df = df.cache()

    corrupt_df = df.filter(F.col("_corrupt_record").isNotNull())
    clean_df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    corrupt_count = corrupt_df.count()
    if corrupt_count:
        logger.warn(f"{corrupt_count} malformed input row(s) detected -- routing to FAILED manifest.")

    # Drop rows with a null Id up front (cheap, avoids wasted API calls)
    clean_df = clean_df.filter(F.col("Id").isNotNull())

    # OPTIMIZATION: the CSV carries the full Attachment object, but only
    # these six columns are used for processing. Selecting down before the
    # repartition shuffle below keeps the shuffle payload small instead of
    # dragging OwnerId, Description, audit timestamps, etc. through every
    # partition boundary for 4M+ rows.
    clean_df = clean_df.select("Id", "ParentId", "Name", "ContentType", "BodyLength", "CreatedDate")

    # Repartition to control download concurrency. Effective concurrency is
    # roughly (partitions running concurrently on the cluster) x THREAD_POOL_SIZE,
    # and should stay <= your Salesforce org's concurrent long-running API
    # request limit (default is often 25, higher for larger editions -- check
    # your org's limits). Prefer scaling THREAD_POOL_SIZE for I/O concurrency
    # per executor core, and NUM_PARTITIONS mainly to spread work evenly
    # across the cluster. E.g. 400 partitions x THREAD_POOL_SIZE=10 across 40
    # G.1X workers keeps plenty of parallelism without overwhelming Salesforce.
    clean_df = clean_df.repartition(NUM_PARTITIONS)

    result_schema = StructType([
        StructField("AttachmentId", StringType(), True),
        StructField("ParentId", StringType(), True),
        StructField("FileName", StringType(), True),
        StructField("ContentType", StringType(), True),
        StructField("BodyLength", LongType(), True),
        StructField("CreatedDate", StringType(), True),
        StructField("S3RelativePath", StringType(), True),
        StructField("S3AbsolutePath", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("ErrorType", StringType(), True),
        StructField("ErrorMessage", StringType(), True),
        StructField("Attempts", LongType(), True),
        StructField("Timestamp", StringType(), True),
    ])

    result_rdd = clean_df.rdd.mapPartitions(process_partition)
    result_df = spark.createDataFrame(result_rdd, schema=result_schema)

    # Fold malformed input rows into the same FAILED shape so they show up
    # in the retry-able manifest alongside download/upload failures.
    if corrupt_count:
        corrupt_failed_df = corrupt_df.select(
            F.lit(None).cast(StringType()).alias("AttachmentId"),
            F.lit(None).cast(StringType()).alias("ParentId"),
            F.lit(None).cast(StringType()).alias("FileName"),
            F.lit(None).cast(StringType()).alias("ContentType"),
            F.lit(None).cast(LongType()).alias("BodyLength"),
            F.lit(None).cast(StringType()).alias("CreatedDate"),
            F.lit(None).cast(StringType()).alias("S3RelativePath"),
            F.lit(None).cast(StringType()).alias("S3AbsolutePath"),
            F.lit("FAILED").alias("Status"),
            F.lit("MalformedCsvRow").alias("ErrorType"),
            F.col("_corrupt_record").alias("ErrorMessage"),
            F.lit(0).cast(LongType()).alias("Attempts"),
            F.lit(dt.datetime.utcnow().isoformat()).alias("Timestamp"),
        )
        result_df = result_df.unionByName(corrupt_failed_df)

    result_df.persist()  # reused for both success + failed writes

    # OPTIMIZATION: df is no longer needed once corrupt_failed_df (built from
    # corrupt_df, itself derived from df) has been folded into result_df.
    # Freeing it here gives the executors' cache space back before the much
    # larger result_df.persist() above needs room.
    df.unpersist()

    success_df = result_df.filter(F.col("Status") == "SUCCESS")
    failed_df = result_df.filter(F.col("Status") == "FAILED")

    logger.info(f"Writing SUCCESS manifest to {SUCCESS_OUTPUT_PATH}")
    (
        success_df
        .coalesce(max(1, NUM_PARTITIONS // 10))  # fewer, larger output files
        .write
        .mode("append")
        .option("header", "true")
        .csv(SUCCESS_OUTPUT_PATH)
    )

    logger.info(f"Writing FAILED records to {FAILED_OUTPUT_PATH}")
    (
        failed_df
        .coalesce(max(1, NUM_PARTITIONS // 10))
        .write
        .mode("append")
        .option("header", "true")
        .csv(FAILED_OUTPUT_PATH)
    )

    # result_df is persisted (MEMORY_AND_DISK by default), so these counts
    # read from cache rather than re-triggering any Salesforce downloads or
    # S3 uploads -- cheap relative to the job's actual I/O work.
    success_count = success_df.count()
    failed_count = failed_df.count()
    logger.info(f"Done. SUCCESS={success_count} FAILED={failed_count}")

    result_df.unpersist()
    job.commit()


if __name__ == "__main__":
    main()
