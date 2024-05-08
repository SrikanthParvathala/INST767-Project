#This code Processes the Zip Files we get for the Static data from the WMATA API.

import os
import tempfile
import zipfile
from google.cloud import storage
from flask import abort

def process_gtfs_zip(request):
    client = storage.Client()
    bucket_name = 'gtfs-static-data'  
    blob_name = 'bus-gtfs-static.zip'  

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        with tempfile.NamedTemporaryFile(delete=False, dir='/tmp') as temp_file:
            blob.download_to_filename(temp_file.name)
            with zipfile.ZipFile(temp_file.name, 'r') as zip_ref:
                extract_path = '/tmp/extracted_files'  
                zip_ref.extractall(extract_path)
                
                # Upload extracted files to GCS
                for filename in os.listdir(extract_path):
                    file_path = os.path.join(extract_path, filename)
                    blob = bucket.blob(f"extracted/{filename}")
                    blob.upload_from_filename(file_path)
                    print(f"Uploaded {filename} to GCS")
                    
    except Exception as e:
        print(f"Failed to process zip file: {e}")
        abort(500, description=f"Failed to process zip file: {e}")
    finally:
        # Cleanup: remove temporary files
        if os.path.exists(temp_file.name):
            os.remove(temp_file.name)
        for root, dirs, files in os.walk(extract_path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))

    return 'Zip file processed successfully.'
