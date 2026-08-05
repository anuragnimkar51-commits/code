from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

# Create Spark session (not needed in AWS Glue Job if spark already exists)
spark = SparkSession.builder.getOrCreate()

# Read CSV from S3
df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("s3://your-bucket/input/data.csv")
)

# Add row numbers based on an ordering column
window_spec = Window.orderBy("id")  # Replace "id" with your ordering column

df = df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

# First 5 lakh rows
df_part1 = (
    df.filter(col("row_num") <= 500000)
      .drop("row_num")
)

# Remaining rows
df_part2 = (
    df.filter(col("row_num") > 500000)
      .drop("row_num")
)

# Write first part as a single CSV
(
    df_part1
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv("s3://your-bucket/output/part1/")
)

# Write remaining rows as a single CSV
(
    df_part2
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv("s3://your-bucket/output/part2/")
)

print("Split completed successfully.")
