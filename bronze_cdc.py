from pyspark.sql import SparkSession
from pyspark.sql.functions import col, decode, get_json_object

spark = SparkSession.builder \
    .appName("Bronze CDC") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "file:///home/jovyan/iceberg_warehouse") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS cdc")

df_kafka = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver1.public.customers,dbserver1.public.drivers") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_kafka.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    decode(col("value"), "UTF-8").alias("value_str")
).select(
    "*",
    get_json_object(col("value_str"), "$.payload.op").alias("op"),
    get_json_object(col("value_str"), "$.payload.ts_ms").alias("ts_ms"),
    get_json_object(col("value_str"), "$.payload.source.lsn").alias("lsn"),
    get_json_object(col("value_str"), "$.payload.before").alias("before_json"),
    get_json_object(col("value_str"), "$.payload.after").alias("after_json")
)

df_parsed.write.mode("append").format("iceberg").saveAsTable("cdc.bronze_cdc_events")
print("Bronze CDC updated")