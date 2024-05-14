import base64
import json
import requests
import os
from google.cloud import storage
from datetime import datetime
import functions_framework

@functions_framework.cloud_event
def fetch_scooter_data(cloud_event):
    message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    print("Received message:", message_data)

    # API and storage configuration
    api_urls = [
        'https://gbfs.lyft.com/gbfs/1.1/dca-cabi/en/free_bike_status.json',
        'https://data.lime.bike/api/partners/v1/gbfs/washington_dc/free_bike_status.json',
        'https://gbfs.spin.pm/api/gbfs/v1/washington_dc/free_bike_status',
        'https://s3.amazonaws.com/lyft-lastmile-production-iad/lbs/dca/free_bike_status.json'
    ]
    bucket_name = os.environ.get('BUCKET_NAME', 'scooter-data-storage') 

    # Initialize the GCS client
    client = storage.Client()

    # Fetch and store data from each API
    for api_url in api_urls:
        fetch_and_store_data(api_url, client, bucket_name)

def fetch_and_store_data(api_url, client, bucket_name):
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H%M%S')
        blob_name = f'{api_url.split("/")[-3]}-data-{timestamp}.json'
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(data), content_type='application/json')
        print(f'Successfully saved data to {blob_name} in bucket {bucket_name}')
    except Exception as e:
        print(f'Error fetching or saving data from {api_url}: {str(e)}')
