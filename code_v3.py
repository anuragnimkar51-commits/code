import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = boto3.client('s3')
bucket = 'your-bucket-name'
base_prefix = 'attachment_files/'  # parent folder

def count_prefix(prefix):
    paginator = s3.get_paginator('list_objects_v2')
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        count += page.get('KeyCount', 0)
    return prefix, count

def list_common_prefixes(prefix):
    """Handles pagination in case there are >1000 sub-prefixes."""
    paginator = s3.get_paginator('list_objects_v2')
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        prefixes += [cp['Prefix'] for cp in page.get('CommonPrefixes', [])]
    return prefixes

# Step 1: discover year= prefixes under attachment_files/
year_prefixes = list_common_prefixes(base_prefix)
# e.g. 'attachment_files/year=2014/'

# Step 2: discover month= prefixes under each year
month_prefixes = []
for yp in year_prefixes:
    month_prefixes += list_common_prefixes(yp)
    # e.g. 'attachment_files/year=2014/month=12/'

# Step 3: count each partition in parallel
# (list_objects_v2 without Delimiter counts everything nested below,
#  so this still correctly counts files under .../id/parentif/file.pdf)
results = {}
with ThreadPoolExecutor(max_workers=30) as executor:
    futures = {executor.submit(count_prefix, p): p for p in month_prefixes}
    for future in as_completed(futures):
        prefix, count = future.result()
        results[prefix] = count
        print(f"{prefix}: {count}")

total = sum(results.values())
print(f"\nTotal files: {total}")
