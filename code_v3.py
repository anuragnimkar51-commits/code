# Cell 2: Everything else in one cell

import io
import csv
import json
import boto3
import psycopg2
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame

# ---------------- Setup ----------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ---------------- Config ----------------
S3_PATH = "s3://your-bucket/path/to/data.csv"
SECRET_NAME = "your/postgres/secret-name"
REGION_NAME = "us-east-1"
TARGET_TABLE = "target_table"          # schema-qualify if needed: "public.target_table"
CONFLICT_COLS = ["id"]                 # primary key / unique constraint column(s)
NUM_PARTITIONS = 8

# ---------------- Read CSV from S3 ----------------
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(S3_PATH)

df.printSchema()
df.show(5)

# ---------------- Fetch DB credentials from Secrets Manager ----------------
def get_db_secret(secret_name: str, region_name: str) -> dict:
    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

secret = get_db_secret(SECRET_NAME, REGION_NAME)

db_config = {
    "host": secret["host"],
    "dbname": secret["dbname"],
    "user": secret["username"],
    "password": secret["password"],
    "port": secret.get("port", 5432),
}

# ---------------- Upsert helpers ----------------
def build_temp_upsert_sql(temp_table, target_table, all_cols, conflict_cols):
    update_cols = [c for c in all_cols if c not in conflict_cols]
    cols_str = ", ".join(all_cols)
    conflict_str = ", ".join(conflict_cols)

    if not update_cols:
        return f"""
            INSERT INTO {target_table} ({cols_str})
            SELECT {cols_str} FROM {temp_table}
            ON CONFLICT ({conflict_str}) DO NOTHING;
        """

    update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    return f"""
        INSERT INTO {target_table} ({cols_str})
        SELECT {cols_str} FROM {temp_table}
        ON CONFLICT ({conflict_str})
        DO UPDATE SET {update_str};
    """


def upsert_dataframe_to_postgres_bulk(df: DataFrame, table, conflict_cols, db_config, num_partitions=8):
    all_cols = df.columns
    temp_table = "temp_upsert_buffer"

    df = df.repartition(num_partitions)

    def upsert_partition(rows):
        rows = list(rows)
        if not rows:
            return

        conn = psycopg2.connect(**db_config)
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TEMP TABLE {temp_table}
                    (LIKE {table} INCLUDING DEFAULTS)
                    ON COMMIT DROP;
                """)

                buffer = io.StringIO()
                writer = csv.writer(buffer)
                for r in rows:
                    writer.writerow([r[c] for c in all_cols])
                buffer.seek(0)

                cols_str = ", ".join(all_cols)
                cur.copy_expert(
                    f"COPY {temp_table} ({cols_str}) FROM STDIN WITH (FORMAT csv)",
                    buffer,
                )

                upsert_sql = build_temp_upsert_sql(temp_table, table, all_cols, conflict_cols)
                cur.execute(upsert_sql)

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    df.foreachPartition(upsert_partition)

# ---------------- Run ----------------
upsert_dataframe_to_postgres_bulk(
    df=df,
    table=TARGET_TABLE,
    conflict_cols=CONFLICT_COLS,
    db_config=db_config,
    num_partitions=NUM_PARTITIONS,
)

print("Upsert complete.")
