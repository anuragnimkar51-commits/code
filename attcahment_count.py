import requests

SF_LOGIN_URL = 'https://your-domain.my.salesforce.com'
CLIENT_ID = 'YOUR_CONSUMER_KEY'
CLIENT_SECRET = 'YOUR_CONSUMER_SECRET'

def get_access_token():
    url = f'{SF_LOGIN_URL}/services/oauth2/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()

def get_attachment_count():
    token_data = get_access_token()
    access_token = token_data['access_token']
    instance_url = token_data['instance_url']

    query = 'SELECT COUNT() FROM Attachment'
    resp = requests.get(
        f'{instance_url}/services/data/v60.0/query',
        headers={'Authorization': f'Bearer {access_token}'},
        params={'q': query}
    )
    resp.raise_for_status()
    result = resp.json()
    print('Attachment count:', result['totalSize'])
    return result['totalSize']

if __name__ == '__main__':
    get_attachment_count()