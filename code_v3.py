import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = boto3.client('s3')
bucket = 'your-bucket-name'

def count_prefix(prefix):
    paginator = s3.get_paginator('list_objects_v2')
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        count += page.get('KeyCount', 0)
    return prefix, count

# Step 1: discover year= prefixes
years = s3.list_objects_v2(Bucket=bucket, Delimiter='/')['CommonPrefixes']
year_prefixes = [y['Prefix'] for y in years]  # e.g. 'year=2014/'

# Step 2: discover month= prefixes under each year
month_prefixes = []
for yp in year_prefixes:
    months = s3.list_objects_v2(Bucket=bucket, Prefix=yp, Delimiter='/')
    month_prefixes += [m['Prefix'] for m in months.get('CommonPrefixes', [])]
    # e.g. 'year=2014/month=01/'

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
