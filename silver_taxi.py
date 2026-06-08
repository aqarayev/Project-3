from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, regexp_replace

spark = SparkSession.builder \
    .appName("Silver Taxi") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "file:///home/jovyan/iceberg_warehouse") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS cdc")

bronze_df = spark.table("cdc.bronze_taxi_trips")

# Clean timestamps (replace 'T' with space)
bronze_df = bronze_df.withColumn("pickup_clean", regexp_replace(col("pickup_datetime"), "T", " "))
bronze_df = bronze_df.withColumn("dropoff_clean", regexp_replace(col("dropoff_datetime"), "T", " "))

silver_df = bronze_df.select(
    col("VendorID").alias("vendor_id"),
    to_timestamp(col("pickup_clean"), "yyyy-MM-dd HH:mm:ss").alias("pickup_datetime"),
    to_timestamp(col("dropoff_clean"), "yyyy-MM-dd HH:mm:ss").alias("dropoff_datetime"),
    col("passenger_count").cast("double").cast("int").alias("passenger_count"),
    col("trip_distance").cast("double").alias("trip_distance"),
    col("PULocationID").cast("double").cast("int").alias("pulocation_id"),
    col("DOLocationID").cast("double").cast("int").alias("dolocation_id"),
    col("fare_amount").cast("double").alias("fare_amount"),
    col("extra").cast("double").alias("extra"),
    col("mta_tax").cast("double").alias("mta_tax"),
    col("tip_amount").cast("double").alias("tip_amount"),
    col("tolls_amount").cast("double").alias("tolls_amount"),
    col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
    col("total_amount").cast("double").alias("total_amount"),
    col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
    col("Airport_fee").cast("double").alias("airport_fee")
).filter(
    (col("pickup_datetime").isNotNull()) &
    (col("dropoff_datetime").isNotNull()) &
    (col("fare_amount") > 0)
)

# Load zones
zone_df = spark.read.parquet("file:///home/jovyan/project/data/taxi_zone_lookup.parquet")
zone_pu = zone_df.select(col("LocationID").alias("pulocation_id"), col("Zone").alias("pickup_zone"))
zone_do = zone_df.select(col("LocationID").alias("dolocation_id"), col("Zone").alias("dropoff_zone"))

silver_with_zones = silver_df.join(zone_pu, on="pulocation_id", how="left") \
                             .join(zone_do, on="dolocation_id", how="left")

silver_with_zones.write.mode("overwrite").format("iceberg").saveAsTable("cdc.silver_taxi_trips")
print("Silver taxi table updated. Row count:", silver_with_zones.count())