"""
Salesforce Field Metadata Extractor -> S3 CSV Uploader
--------------------------------------------------------
1. Authenticates to Salesforce using OAuth 2.0 Client Credentials flow
2. Pulls field name, label, and data type for one or more sObjects
3. Uploads the result as a CSV directly to an S3 bucket

Requirements:
    pip install requests boto3
"""

import csv
import io
import sys

import boto3
import requests


# ----------------------------------------------------------------------
# CONFIG — update these values
# ----------------------------------------------------------------------
SF_INSTANCE_URL = "https://your-domain.my.salesforce.com"  # My Domain URL
SF_CLIENT_ID = "your_client_id"
SF_CLIENT_SECRET = "your_client_secret"
SF_API_VERSION = "v60.0"

SF_OBJECTS = ["Account", "Contact", "Opportunity"]  # objects to describe

S3_BUCKET = "your-bucket-name"
S3_KEY = "salesforce/field_metadata.csv"
AWS_REGION = "us-east-1"


# ----------------------------------------------------------------------
# Step 1: Authenticate via Client Credentials flow
# ----------------------------------------------------------------------
def get_access_token(instance_url, client_id, client_secret):
    token_url = f"{instance_url}/services/oauth2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    resp = requests.post(token_url, data=payload)
    resp.raise_for_status()
    return resp.json()


# ----------------------------------------------------------------------
# Step 2: Get field name / label / type for one object
# ----------------------------------------------------------------------
def get_field_info(instance_url, access_token, api_version, object_name):
    url = f"{instance_url}/services/data/{api_version}/sobjects/{object_name}/describe"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    fields = resp.json()["fields"]

    return [
        {
            "object": object_name,
            "name": f["name"],
            "label": f["label"],
            "type": f["type"],
        }
        for f in fields
    ]


def get_field_info_multi(instance_url, access_token, api_version, object_names):
    all_fields = []
    for obj in object_names:
        print(f"Describing {obj}...")
        all_fields.extend(get_field_info(instance_url, access_token, api_version, obj))
    return all_fields


# ----------------------------------------------------------------------
# Step 3: Upload as CSV to S3
# ----------------------------------------------------------------------
def save_fields_to_s3(fields, bucket_name, s3_key, aws_region):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["object", "name", "label", "type"])
    writer.writeheader()
    writer.writerows(fields)

    s3 = boto3.client("s3", region_name=aws_region)
    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="text/csv",
    )
    print(f"Uploaded to s3://{bucket_name}/{s3_key}")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    try:
        auth_data = get_access_token(SF_INSTANCE_URL, SF_CLIENT_ID, SF_CLIENT_SECRET)
    except requests.exceptions.HTTPError as e:
        print(f"Auth failed: {e}\nResponse body: {e.response.text}")
        sys.exit(1)

    access_token = auth_data["access_token"]
    instance_url = auth_data["instance_url"]  # use the returned instance_url, not the input one

    fields = get_field_info_multi(instance_url, access_token, SF_API_VERSION, SF_OBJECTS)
    print(f"Retrieved {len(fields)} fields across {len(SF_OBJECTS)} object(s).")

    save_fields_to_s3(fields, S3_BUCKET, S3_KEY, AWS_REGION)


if __name__ == "__main__":
    main()
