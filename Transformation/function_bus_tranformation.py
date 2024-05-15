from google.cloud import bigquery
from google.cloud import storage
import glob
import json
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# Function to process JSON data from Cloud Storage blob
def process_json(blob):
    logging.info("Processing file: %s", blob.name)
    json_content = blob.download_as_string().decode("utf-8")
    json_data = json.loads(json_content)
    df = pd.json_normalize(json_data['BusPositions'])
    return df

# Function to parse data from multiple JSON files stored in Cloud Storage
def parse_data(pos_df, bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    
    with ThreadPoolExecutor() as executor:
        # Process JSON files in parallel
        dfs = list(executor.map(process_json, blobs))
    
    # Concatenate DataFrame from all processed JSON files
    pos_df = pd.concat(dfs, ignore_index=True)
    logging.info("Columns in pos_df: %s", pos_df.columns)
    return pos_df



def convert_to_parquet(df):
    # Create an in-memory buffer to store the Parquet file
    buffer = BytesIO()

    # Convert the Pandas DataFrame to a PyArrow Table
    table = pa.Table.from_pandas(df)

    # Write the PyArrow Table to the in-memory buffer as a Parquet file
    pq.write_table(table, buffer)

    # Reset the buffer's position to the beginning
    buffer.seek(0)

    return buffer



def write_to_bigquery(df_new):
    schema = [
    # Vehicle ID
    bigquery.SchemaField("VehicleID", "STRING"),
    # Latitude
    bigquery.SchemaField("Lat", "FLOAT64"),
    # Longitude
    bigquery.SchemaField("Lon", "FLOAT64"),
    # Deviation (assuming numerical value)
    bigquery.SchemaField("Deviation", "FLOAT64"),
    # Date/Time (assuming timestamp format)
    bigquery.SchemaField("DateTime", "STRING"),
    # Trip ID
    bigquery.SchemaField("TripID", "STRING"),
    # Route ID
    bigquery.SchemaField("RouteID", "STRING"),
    # Direction Number (assuming numerical value)
    bigquery.SchemaField("DirectionNum", "INTEGER"),
    # Direction Text
    bigquery.SchemaField("DirectionText", "STRING"),
    # Trip Headsign
    bigquery.SchemaField("TripHeadsign", "STRING"),
    # Trip Start Time (assuming timestamp format)
    bigquery.SchemaField("TripStartTime", "STRING"),
    # Trip End Time (assuming timestamp format)
    bigquery.SchemaField("TripEndTime", "STRING"),
    # Block Number
    bigquery.SchemaField("BlockNumber", "STRING"),
    ]
    # Create a BigQuery client
    client = bigquery.Client()

    # Define the BigQuery table ID
    table_id = "inst767-project-big-data.bus_positions_dataset.bus_positions"  


    # Write Pandas DataFrame to BigQuery table
    job_config = bigquery.LoadJobConfig(
    schema=schema)

    job = client.load_table_from_dataframe(
        df_new, table_id, job_config=job_config
    )
    # job = client.load_table_from_dataframe(df_new, table_id, location="US")
    job.result()


def transform(bucket_name = 'bus-position-api-storage', prefix = 'bus-data'):
    logging.basicConfig(level=logging.INFO)
    #bus data
    bucket_name = 'bus-position-api-storage'
    prefix = 'bus-data'
    pos_df = pd.DataFrame()
    
    df_new = parse_data(pos_df, bucket_name, prefix)
    # Convert the DataFrame to Parquet
    parquet_buffer = convert_to_parquet(df_new)

    # Write the Parquet file to Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket("bus-position-parquet")
    blob = bucket.blob("bus-position.parquet")

    # Write the contents of the in-memory buffer to the Cloud Storage blob
    blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
    write_to_bigquery(df_new)
    


