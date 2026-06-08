from pyspark.sql import SparkSession
from pyspark.sql.functions import col, decode, get_json_object

spark = SparkSession.builder \
    .appName("Bronze Taxi") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "file:///home/jovyan/iceberg_warehouse") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS cdc")

df_kafka = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "taxi-trips") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_kafka.select(
    col("topic"), col("partition"), col("offset"), col("timestamp").alias("kafka_timestamp"),
    decode(col("value"), "UTF-8").alias("value_str")
).select(
    "*",
    get_json_object(col("value_str"), "$.VendorID").alias("VendorID"),
    get_json_object(col("value_str"), "$.tpep_pickup_datetime").alias("pickup_datetime"),
    get_json_object(col("value_str"), "$.tpep_dropoff_datetime").alias("dropoff_datetime"),
    # add all other fields as before
    get_json_object(col("value_str"), "$.passenger_count").alias("passenger_count"),
    get_json_object(col("value_str"), "$.trip_distance").alias("trip_distance"),
    get_json_object(col("value_str"), "$.PULocationID").alias("PULocationID"),
    get_json_object(col("value_str"), "$.DOLocationID").alias("DOLocationID"),
    get_json_object(col("value_str"), "$.fare_amount").alias("fare_amount"),
    get_json_object(col("value_str"), "$.extra").alias("extra"),
    get_json_object(col("value_str"), "$.mta_tax").alias("mta_tax"),
    get_json_object(col("value_str"), "$.tip_amount").alias("tip_amount"),
    get_json_object(col("value_str"), "$.tolls_amount").alias("tolls_amount"),
    get_json_object(col("value_str"), "$.improvement_surcharge").alias("improvement_surcharge"),
    get_json_object(col("value_str"), "$.total_amount").alias("total_amount"),
    get_json_object(col("value_str"), "$.congestion_surcharge").alias("congestion_surcharge"),
    get_json_object(col("value_str"), "$.Airport_fee").alias("Airport_fee")
)

df_parsed.write.mode("append").format("iceberg").saveAsTable("cdc.bronze_taxi_trips")
print("Bronze Taxi updated")