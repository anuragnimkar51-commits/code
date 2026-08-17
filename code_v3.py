import boto3
from concurrent.futures import ThreadPoolExecutor

s3 = boto3.client('s3')
bucket = 'your-bucket-name'

def count_prefix(prefix):
    paginator = s3.get_paginator('list_objects_v2')
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        count += page.get('KeyCount', 0)
    return count

# Split work across first-level prefixes (folders) run in parallel
prefixes = ['a/', 'b/', 'c/', ...]  # discover via delimiter listing first
with ThreadPoolExecutor(max_workers=20) as executor:
    results = list(executor.map(count_prefix, prefixes))

print(f"Total: {sum(results)}")
