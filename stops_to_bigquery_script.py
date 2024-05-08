#This code fetches the data from bus stops location API. It is to be written in Cloud Functions.
# We get the Stops data from the API in the form of a zip file. Then another Python function extracts that zip file. And then this code takes the stops.txt file and loads data into BigQuery
from google.cloud import bigquery
import json

def load_data_into_bigquery(dataset_id, table_id, source_uri):
    # Code to load data into BigQuery
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True

    load_job = bigquery_client.load_table_from_uri(
        source_uri, table_ref, job_config=job_config
    )

    load_job.result()  # Wait for the job to complete

    print("Data loaded into BigQuery table {}".format(table_id))

def main():
    # Load data into BigQuery
    dataset_id = "stops_dataset"
    table_id = "stops_data"
    source_uri = "gs://gtfs-static-data/extracted/stops.txt"
    load_data_into_bigquery(dataset_id, table_id, source_uri)

if __name__ == "__main__":
    main()
