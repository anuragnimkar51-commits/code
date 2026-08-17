import boto3

s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')

count = 0
for page in paginator.paginate(Bucket='your-bucket-name', Prefix='path/to/folder/'):
    count += page.get('KeyCount', 0)

print(f"Total files: {count}")
