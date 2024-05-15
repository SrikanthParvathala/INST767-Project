from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, regexp_extract, max, input_file_name, when, to_date, expr 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql import functions as F
from google.cloud import bigquery

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ScooterDataAggregation") \
    .getOrCreate()

def load_new_data(spark, bucket_name, prefix):
    path = f"gs://{bucket_name}/{prefix}*.json"
    df = spark.read.json(path)
    df = df.withColumn("filename", input_file_name())
    df = df.withColumn("datetime_str", regexp_extract("filename", r"\d{4}-\d{2}-\d{2}-\d{6}", 0))
    df = df.withColumn("timestamp_UTC", to_timestamp(col("datetime_str"), "yyyy-MM-dd-HHmmss"))
    df = df.withColumn("timestamp_EST", expr("from_utc_timestamp(timestamp_UTC, 'America/New_York')"))
#     if last_timestamp:
#         df = df.filter(col("timestamp") > last_timestamp)
    
    # Convert timestamp from UTC to Eastern Time with consideration for DST
    return df

def transform_data(df, filename_contains, vendor_name, bikes_col_name, vehicle_type_col, type_mappings=None):
    df_vendor = df.filter(F.col("filename").contains(filename_contains)).select(
        F.col(f"data.{bikes_col_name}").alias("bikes"),
        F.col("timestamp_EST")
    )
    if filename_contains in ['dca-cabi', 'lbs-data']:
        df_vendor = df_vendor.selectExpr("inline(bikes)", "timestamp_EST").select(
            'type', "lat", "lon", "is_disabled", "is_reserved", "bike_id", "timestamp_EST"
        )
    else:
        df_vendor = df_vendor.selectExpr("inline(bikes)", "timestamp_EST").select(
            vehicle_type_col, "lat", "lon", "is_disabled", "is_reserved", "bike_id", "timestamp_EST"
        )
    if vehicle_type_col != "type":
        df_vendor = df_vendor.withColumnRenamed(vehicle_type_col, "type")
    if type_mappings:
        mapping_expr = F.expr(
            "CASE " + " ".join([f"WHEN type = '{k}' THEN '{v}'" for k, v in type_mappings.items()]) + " ELSE type END"
        )
        df_vendor = df_vendor.withColumn("type", mapping_expr)
    df_vendor = df_vendor.withColumn("vendor", F.lit(vendor_name))
    if vendor_name == "Spin":
        df_vendor = df_vendor.withColumn("is_disabled", F.expr("CASE WHEN is_disabled = true THEN 1 ELSE 0 END")) \
                             .withColumn("is_reserved", F.expr("CASE WHEN is_reserved = true THEN 1 ELSE 0 END"))
    else:
        df_vendor = df_vendor.withColumn("is_disabled", F.col("is_disabled").cast(IntegerType())) \
                             .withColumn("is_reserved", F.col("is_reserved").cast(IntegerType()))
    df_vendor = df_vendor.withColumn("date", to_date(col("timestamp_EST"), "yyyy-MM-dd"))
    df_vendor = df_vendor.repartition(col("date"), col("vendor"))
    return df_vendor

def run_job():
    bucket_name = 'scooter-data-storage'
    prefix = ''
 #   last_timestamp = None 
    df_new = load_new_data(spark, bucket_name, prefix)
#    latest_timestamp = df_new.select(max("timestamp")).collect()[0][0] if df_new.count() > 0 else None
    type_mappings = {
        'bae2102b-56ba-42ba-9097-720e5990b4b2': 'electric_scooter',
        '2ea3c8b2-ed07-4c53-b87e-638c08471309': 'electric_bike', 
        'scooter': 'electric_scooter',
        'bike': 'electric_bike'
    }
    df_capital = transform_data(df_new, "dca-cabi", "Capital Bikeshare", "bikes", "type")
    df_lime = transform_data(df_new, "gbfs-data", "Lime", "bikes", "vehicle_type", type_mappings)
    df_lyft = transform_data(df_new, "lbs-data", "Lyft", "bikes", "type")
    df_spin = transform_data(df_new, "v1-data", "Spin", "bikes", "vehicle_type_id", type_mappings)
    df_combined = df_capital.union(df_lime).union(df_lyft).union(df_spin)
    df_combined = df_combined.distinct()
    df_combined.write.format("bigquery") \
        .option("table", "inst767-project-big-data.scooter_dataset.vehicle_usage_table_UPDATED") \
        .option("temporaryGcsBucket", "temp-bucket-scooter") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    run_job()
    