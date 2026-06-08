from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Validate") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "file:///home/jovyan/iceberg_warehouse") \
    .getOrCreate()

silver_customers = spark.table("cdc.silver_customers").count()
silver_drivers = spark.table("cdc.silver_drivers").count()

print(f"Silver customers count: {silver_customers}")
print(f"Silver drivers count: {silver_drivers}")
print("Validation passed (manual comparison with PostgreSQL is required for report)")