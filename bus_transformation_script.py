from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, regexp_extract, max, input_file_name, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql import functions as F
from google.cloud import bigquery
from math import radians, sin, cos, sqrt, atan2
import glob
import json
import pandas as pd
from pyspark.sql.functions import udf

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StopData") \
    .getOrCreate()


def calculate_distance(lat1, lon1, lat2, lon2):
   lat1_rad = radians(lat1)
   lon1_rad = radians(lon1)
   lat2_rad = radians(lat2)
   lon2_rad = radians(lon2)
   earth_radius = 6371.0
    # Calculate the differences between the coordinates
   dlat = lat2_rad - lat1_rad
   dlon = lon2_rad - lon1_rad
    # Calculate the Haversine distance
   a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
   c = 2 * atan2(sqrt(a), sqrt(1 - a))
   distance = earth_radius * c *1000
   #dist_m = distance * 1000
   return distance


def merge_stops_df(stop_path, stop_times_path):
    stops_dataframe = pd.read_csv(stop_path, sep=",")
    stop_times_dataframe = pd.read_csv(stop_times_path, sep=",")

    merged_dataframe = pd.merge(stops_dataframe,stop_times_dataframe,on='stop_id', how='inner')
    merged_dataframe = merged_dataframe[['stop_id','stop_name','stop_lat','stop_lon','trip_id','arrival_time','departure_time']]
    return merged_dataframe


def parse_data(stops_df, pos_df, bucket_name, prefix):
    # stops_df = spark.createDataFrame(stops_df)
    path = f"gs://{bucket_name}/{prefix}*.json"
    file_paths = glob.glob(path)

    # Loop through each file path
    for file_path in file_paths:
      # Open the file
      with open(file_path, "r") as f:
        # Read the JSON content
        json_content = json.loads(f.read())
        print(json_content)
        df = pd.json_normalize(json_content['BusPositions'])
        # df = spark.createDataFrame(df)
        columns_to_drop = ['DirectionNum', 'DirectionText','BlockNumber']  # List of columns to drop

        # Drop the specified columns
        df = df.drop(columns=columns_to_drop)

        pos_df = pd.concat([pos_df, df], ignore_index=True)
  
    #merge df and stops_df on stop_id
    pos_df['TripID'] = pos_df['TripID'].astype('int64')
    pos_df = pd.merge(pos_df, stops_df, left_on='TripID', right_on='trip_id', how='left')
    # pos_df = pos_df.join(stops_df, left_on='TripID', right_on='trip_id')

    #use df and stops_df to calculate distance and store the distance in a new column based on tripID
    pos_df = spark.createDataFrame(pos_df)
    calculate_distance_udf = udf(calculate_distance) 
    pos_df = pos_df.withColumn("distance_from_stop", calculate_distance_udf(pos_df["stop_lat"], pos_df["stop_lon"], pos_df["Lat"], pos_df["Lon"]))
    return pos_df



def load_new_data(spark, bucket_name, prefix):
    path = f"gs://{bucket_name}/{prefix}*.json"
    df = spark.read.json(path)
    return df

def transform_data():
    #stops data - need to put the paths
    stops_df = merge_stops_df("gs://gtfs-static-data/extracted/stops.txt","gs://gtfs-static-data/extracted/stop_times.txt")

    #bus data
    bucket_name = 'bus-position-api-storage'
    prefix = 'bus-data'
    pos_df = pd.DataFrame()
    df_new = parse_data(stops_df,pos_df, bucket_name, prefix)

    # Save to BigQuery
    df_new.write.format("bigquery") \
        .option("table", "inst767-project-big-data.bus_dataset.bus_position_table") \
        .option("temporaryGcsBucket", "temp-bucket-bus") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    transform_data()
