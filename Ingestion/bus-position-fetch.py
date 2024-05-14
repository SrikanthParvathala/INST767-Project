import base64
import json
import requests
from google.cloud import storage
from datetime import datetime
import functions_framework
import os

@functions_framework.cloud_event
def fetch_bus_position_data(cloud_event):
    message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    print("Received message:", message_data)
    
    # API and storage configuration
    api_key = '332e1249c3c4449e805c5a0b1a60ab9a' 
    api_url = f"https://api.wmata.com/Bus.svc/json/jBusPositions"  
    bucket_name = os.environ.get('BUCKET_NAME', 'bus-position-api-storage')  

    # Initialize the GCS client
    client = storage.Client()

    # Fetch and store data
    fetch_and_store_data(api_url, api_key, client, bucket_name)

def fetch_and_store_data(api_url, api_key, client, bucket_name):
    headers = {'api_key': api_key}
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        response.raise_for_status()  
        data = response.json()
        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H%M%S')
        blob_name = f'bus-data-{timestamp}.json'
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(data), content_type='application/json')
        print(f'Successfully saved data to {blob_name} in bucket {bucket_name}')
    except Exception as e:
        print(f'Error fetching or saving data from {api_url}: {str(e)}')
