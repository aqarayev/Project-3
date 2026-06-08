from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, row_number, desc, coalesce
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Silver CDC") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "file:///home/jovyan/iceberg_warehouse") \
    .getOrCreate()

bronze = spark.table("cdc.bronze_cdc_events")
bronze_with_key = bronze.select(
    "*",
    get_json_object(col("after_json"), "$.id").cast("int").alias("id"),
    get_json_object(col("before_json"), "$.id").cast("int").alias("before_id"),
    (col("topic").contains("customers")).alias("is_customers"),
    (col("topic").contains("drivers")).alias("is_drivers")
).withColumn("primary_key", coalesce(col("id"), col("before_id"))).filter(col("primary_key").isNotNull())

window = Window.partitionBy("topic", "primary_key").orderBy(desc("ts_ms"))
latest = bronze_with_key.withColumn("rn", row_number().over(window)).filter(col("rn") == 1)

# Customers
customers = latest.filter(col("is_customers") & (col("op") != "d")) \
    .select(get_json_object(col("after_json"), "$.id").cast("int").alias("id"),
            get_json_object(col("after_json"), "$.name").alias("name"),
            get_json_object(col("after_json"), "$.email").alias("email"),
            get_json_object(col("after_json"), "$.country").alias("country"))
customers.write.mode("overwrite").format("iceberg").saveAsTable("cdc.silver_customers")

# Drivers
drivers = latest.filter(col("is_drivers") & (col("op") != "d")) \
    .select(get_json_object(col("after_json"), "$.id").cast("int").alias("id"),
            get_json_object(col("after_json"), "$.name").alias("name"),
            get_json_object(col("after_json"), "$.license_number").alias("license_number"),
            get_json_object(col("after_json"), "$.rating").alias("rating"),
            get_json_object(col("after_json"), "$.city").alias("city"),
            get_json_object(col("after_json"), "$.active").alias("active"))
drivers.write.mode("overwrite").format("iceberg").saveAsTable("cdc.silver_drivers")

print("Silver CDC updated")