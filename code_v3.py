id_col = "AttachmentId"   # change to your actual column name

sf_id_pattern = r"^[a-zA-Z0-9]{15}$|^[a-zA-Z0-9]{18}$"

is_valid_expr = (
    F.col(id_col).isNotNull() &
    (F.trim(F.col(id_col)) != "") &
    F.col(id_col).rlike(sf_id_pattern)
)

valid_df = df.filter(is_valid_expr)
invalid_df = df.filter(~is_valid_expr)   # cleaner than subtract() — handles duplicate rows correctly

# ---------------------------------------------------------------
# 4. Counts
# ---------------------------------------------------------------
total_rows = df.count()
valid_count = valid_df.count()
distinct_valid_count = valid_df.select(id_col).distinct().count()
invalid_count = invalid_df.count()

print(f"Total rows:              {total_rows}")
print(f"Valid attachment IDs:    {valid_count}")
print(f"Distinct valid IDs:      {distinct_valid_count}")
print(f"Invalid/null IDs:        {invalid_count}")

# ---------------------------------------------------------------
# 5. Write invalid rows (entire row) to S3 as CSV
# ---------------------------------------------------------------
invalid_output_path = "s3a://your-bucket/path/to/invalid_attachment_ids/"

invalid_df.write \
    .mode("overwrite") \          # use "append" if you don't want to overwrite previous runs
    .option("header", "true") \
    .csv(invalid_output_path)

print(f"Invalid rows written to: {invalid_output_path}")
