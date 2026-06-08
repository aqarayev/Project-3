from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Gold Taxi") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "file:///home/jovyan/iceberg_warehouse") \
    .getOrCreate()

gold_taxi = spark.sql("""
SELECT 
    pickup_zone,
    DATE_TRUNC('hour', pickup_datetime) AS pickup_hour,
    COUNT(*) AS total_trips,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM cdc.silver_taxi_trips
WHERE pickup_zone IS NOT NULL
GROUP BY pickup_zone, DATE_TRUNC('hour', pickup_datetime)
ORDER BY pickup_hour DESC, total_trips DESC
""")

gold_taxi.write.mode("overwrite").format("iceberg").saveAsTable("cdc.gold_taxi_aggregates")
print("Gold taxi table updated. Row count:", gold_taxi.count())
print("Sample top 10 rows:")
gold_taxi.show(10)