# ============================================================
# Cell 1: Session setup (Glue interactive session magics)
# ============================================================
%idle_timeout 60
%glue_version 4.0
%worker_type G.1X
%number_of_workers 5

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, count

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ============================================================
# Cell 2: Config
# ============================================================
INPUT_PATH  = "s3://your-input-bucket/path/to/input.csv"
OUTPUT_PATH = "s3://your-output-bucket/path/to/output/monthly_counts/"
DATE_COL    = "created_date"
DATE_FORMAT = None   # e.g. "yyyy-MM-dd" if auto-cast fails; None = let Spark infer

# ============================================================
# Cell 3: Read CSV with inferSchema
# ============================================================
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

print("Row count:", df.count())
df.printSchema()
df.show(5, truncate=False)

# ============================================================
# Cell 4: Parse created_date -> year/month columns
# ============================================================
if DATE_FORMAT:
    df = df.withColumn(DATE_COL, col(DATE_COL).cast("string"))
    df = df.withColumn(DATE_COL, col(DATE_COL).cast("timestamp"))
else:
    df = df.withColumn(DATE_COL, col(DATE_COL).cast("timestamp"))

df = df.withColumn("year", year(col(DATE_COL))) \
       .withColumn("month", month(col(DATE_COL)))

# Drop rows where date failed to parse
df = df.filter(col("year").isNotNull())

# ============================================================
# Cell 5: GroupBy year + month -> row counts
# ============================================================
monthly_counts = (
    df.groupBy("year", "month")
      .agg(count("*").alias("row_count"))
      .orderBy("year", "month")
)

monthly_counts.show(50, truncate=False)

# ============================================================
# Cell 6: Write single CSV to S3
# ============================================================
(
    monthly_counts
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(OUTPUT_PATH)
)

print(f"Output written to {OUTPUT_PATH}")

# job.commit()  # uncomment if this notebook backs a Glue Job
