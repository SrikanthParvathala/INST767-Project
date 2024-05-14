from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, regexp_extract, max, input_file_name, when, split, expr 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql import functions as F
from google.cloud import bigquery


# Initialize Spark session
spark = SparkSession.builder \
    .appName("BusPositionDataAggregation") \
    .getOrCreate()


# def load_json(spark, bucket_name_bus):
#     path = f"gs://{bucket_name_bus}/*.json"
#     df = spark.read.json(path)
#     return df

# def transform_data(df):
#     df_transformed = df.selectExpr("inline(BusPositions)").select("VehicleID",
#                                                                     "Lat", 
#                                                                     "Lon", 
#                                                                     "Deviation", 
#                                                                     "DateTime")
#     df_transformed = df_transformed.withColumn("VehicleID", 
#                                                F.col("VehicleID").cast(IntegerType()))
#     df_transformed = df_transformed.withColumn("DateTime", 
#                                                to_timestamp(col("DateTime"), 
#                                                             "yyyy-MM-dd'T'HH:mm:ss"))
#     return df_transformed


def load_transform_gtfs_data(bucket_name_gtfs):
    stops_df = spark.read.text(f"gs://{bucket_name_gtfs}/extracted/stops.txt")
    stops_df = stops_df.withColumn('stop_values', split(stops_df['value'], ','))
    for i in range(0, len(stops_df.select('stop_values').first()[0])):
        stops_df = stops_df.withColumn(f'col_{i}', stops_df['stop_values'].getItem(i))
    stops_df = stops_df.drop('value', 'stop_values')
    columns = stops_df.select('col_0', 'col_1', 'col_2', 'col_3', 'col_4', 'col_5', 'col_6', 'col_7').first()
    stops_df = stops_df.toDF(*columns)
    stops_df = stops_df.where(stops_df['stop_id'] != 'stop_id')
    stops_df = stops_df.select(['stop_id', 'stop_name', 'stop_lat', 'stop_lon'])
    stops_df = stops_df.withColumn("stop_id", 
                                    F.col("stop_id").cast(IntegerType())).withColumn("stop_lat", 
                                    F.col("stop_lat").cast(DoubleType())).withColumn("stop_lon", 
                                    F.col("stop_lon").cast(DoubleType()))
    return stops_df


def run_job():
    
#     bucket_name_bus = 'bus-position-api-storage'
    
    bucket_name_gtfs = 'gtfs-static-data'
                               
                               
#     bus_positions = load_json(spark, bucket_name_bus)
#     bus_positions = transform_data(bus_positions)
    bus_stops = load_transform_gtfs_data(bucket_name_gtfs)


    # Save to BigQuery
    bus_stops.write.format("bigquery") \
        .option("table", "inst767-project-big-data.stops_dataset.stops_data") \
        .option("temporaryGcsBucket", "temp-bucket-scooter") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .mode("append") \
        .save()
                               
#     bus_positions.write.format("bigquery") \
#         .option("table", "inst767-project-big-data.stops_dataset.bus_positions") \
#         .option("temporaryGcsBucket", "temp-bucket-scooter") \
#         .option("createDisposition", "CREATE_IF_NEEDED") \
#         .option("writeDisposition", "WRITE_APPEND") \
#         .mode("append") \
#         .save()  
                             

if __name__ == "__main__":
    run_job()
