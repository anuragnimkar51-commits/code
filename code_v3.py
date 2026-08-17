import boto3
import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = boto3.client('s3')
bucket = 'your-bucket-name'
base_prefix = 'attachment_files/'
output_key = 'reports/partition_file_counts.csv'  # where the CSV lands in S3

def count_prefix(prefix):
    paginator = s3.get_paginator('list_objects_v2')
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        count += page.get('KeyCount', 0)
    return prefix, count

def list_common_prefixes(prefix):
    paginator = s3.get_paginator('list_objects_v2')
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        prefixes += [cp['Prefix'] for cp in page.get('CommonPrefixes', [])]
    return prefixes

# Step 1: discover year= prefixes under attachment_files/
year_prefixes = list_common_prefixes(base_prefix)

# Step 2: discover month= prefixes under each year
month_prefixes = []
for yp in year_prefixes:
    month_prefixes += list_common_prefixes(yp)

# Step 3: count each partition in parallel
results = {}
with ThreadPoolExecutor(max_workers=30) as executor:
    futures = {executor.submit(count_prefix, p): p for p in month_prefixes}
    for future in as_completed(futures):
        prefix, count = future.result()
        results[prefix] = count
        print(f"{prefix}: {count}")

total = sum(results.values())
print(f"\nTotal files: {total}")

# Step 4: write results to CSV in memory
csv_buffer = io.StringIO()
writer = csv.writer(csv_buffer)
writer.writerow(['prefix', 'file_count'])
for prefix, count in sorted(results.items()):
    writer.writerow([prefix, count])
writer.writerow(['TOTAL', total])

# Step 5: upload CSV to S3
s3.put_object(
    Bucket=bucket,
    Key=output_key,
    Body=csv_buffer.getvalue(),
    ContentType='text/csv'
)

print(f"\nCSV uploaded to s3://{bucket}/{output_key}")
