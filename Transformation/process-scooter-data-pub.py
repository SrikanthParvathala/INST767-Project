import base64
import json
import logging
from googleapiclient.discovery import build
from google.auth import default

logging.basicConfig(level=logging.DEBUG)

def start_dataproc_job(event, context):
    if 'data' in event:
        message_data = base64.b64decode(event['data']).decode('utf-8')
        try:
            message = json.loads(message_data)
            logging.info("Received message: %s", message)
        except json.JSONDecodeError:
            logging.error("Received malformed JSON")
            return "Malformed JSON", 400

    credentials, _ = default()

    project_id = 'inst767-project-big-data'
    region = 'us-west1'
    cluster_name = 'cluster-transformation'
    job_file_path = 'gs://transformation-scripts/bus_transformations_script.py'

    dataproc = build('dataproc', 'v1', credentials=credentials)

    job_details = {
        'projectId': project_id,
        'region': region,
        'job': {
            'placement': {
                'clusterName': cluster_name
            },
            'pysparkJob': {
                'mainPythonFileUri': job_file_path
            }
        }
    }

    try:
        result = dataproc.projects().regions().jobs().submit(
            projectId=project_id, region=region, body=job_details).execute()
        logging.info("Job submitted successfully, Job ID: %s", result['reference']['jobId'])
        return f"Started job: {result['reference']['jobId']}"
    except Exception as e:
        logging.error("Failed to submit job: %s", e)
        return "Failed to submit job", 500



"""
Requirements.txt File Contents:
# Function dependencies, for example:
# package>=version
google-cloud-dataproc>=2.0.0
google-auth>=1.6.3
google-api-python-client>=1.7.11
"""
