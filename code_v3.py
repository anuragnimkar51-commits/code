id_col = "AttachmentId"   # change to your actual column name, e.g. "Id" or "attachment_id"

sf_id_pattern = r"^[a-zA-Z0-9]{15}$|^[a-zA-Z0-9]{18}$"

valid_df = df.filter(
    F.col(id_col).isNotNull() &
    (F.trim(F.col(id_col)) != "") &
    F.col(id_col).rlike(sf_id_pattern)
)

# ---------------------------------------------------------------
# 4. Counts
# ---------------------------------------------------------------
total_rows = df.count()
valid_count = valid_df.count()
distinct_valid_count = valid_df.select(id_col).distinct().count()
invalid_count = total_rows - valid_count

print(f"Total rows:              {total_rows}")
print(f"Valid attachment IDs:    {valid_count}")
print(f"Distinct valid IDs:      {distinct_valid_count}")
print(f"Invalid/null IDs:        {invalid_count}")
