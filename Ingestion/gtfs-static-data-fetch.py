import os
import http.client
import urllib.parse
from google.cloud import storage
from flask import escape

def fetch_gtfs_static_data(request):
    """Fetch GTFS static data from WMATA and upload it to GCS.
    
    Args:
        request (flask.Request): HTTP request object.
    
    Returns:
        str: Confirmation message of operations.
    """
    # Use the WMATA API key from environment variables
    api_key = '332e1249c3c4449e805c5a0b1a60ab9a'
    headers = {'api_key': api_key}
    params = urllib.parse.urlencode({})

    # Establish connection and request data
    try:
        conn = http.client.HTTPSConnection('api.wmata.com')
        conn.request("GET", "/gtfs/bus-gtfs-static.zip?" + params, headers=headers)
        response = conn.getresponse()
        data = response.read()
        
        # Save to Google Cloud Storage
        if save_to_gcs(data):
            return 'GTFS Data successfully fetched and uploaded to GCS.'
        else:
            return 'Failed to upload to GCS.', 500
        
    except Exception as e:
        return f"An error occurred: {e}", 500

def save_to_gcs(data):
    """Save the downloaded data to Google Cloud Storage.
    
    Args:
        data (bytes): Data to be saved.
    
    Returns:
        bool: True if success, False otherwise.
    """
    try:
        client = storage.Client()
        bucket_name = 'gtfs-static-data'
        bucket = client.bucket(bucket_name)
        blob = bucket.blob('bus-gtfs-static.zip')
        blob.upload_from_string(data, content_type='application/zip')
        print("File uploaded to GCS")
        return True
    except Exception as e:
        print(f"Failed to upload to GCS: {e}")
        return False
