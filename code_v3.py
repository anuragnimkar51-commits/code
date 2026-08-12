import requests

def get_access_token(instance_url, client_id, client_secret):
    resp = requests.post(
        f"{instance_url}/services/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    resp.raise_for_status()
    return resp.json()

def describe_object(instance_url, access_token, object_name):
    url = f"{instance_url}/services/data/v60.0/sobjects/{object_name}/describe"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def describe_all_objects(instance_url, access_token):
    url = f"{instance_url}/services/data/v60.0/sobjects"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

auth = get_access_token("https://your-domain.my.salesforce.com", "CLIENT_ID", "CLIENT_SECRET")
token = auth["access_token"]
inst_url = auth["instance_url"]

account_meta = describe_object(inst_url, token, "Account")
all_objects = describe_all_objects(inst_url, token)
