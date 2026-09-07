from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as spark_sum, count
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from delta import configure_spark_with_delta_pip

# Configure Spark with Delta Lake
builder = SparkSession.builder.appName("OrdersStream") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "/delta/warehouse")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define schema for order events
schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("product_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("status", StringType()) \
    .add("timestamp", TimestampType())

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON from Kafka value
orders = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Write to Delta Lake (raw layer)
raw_query = orders.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/delta/checkpoints/orders_raw") \
    .option("path", "/delta/tables/orders_raw") \
    .start()

# Windowed aggregation (5-minute windows) for paid orders
agg = orders.filter(col("status") == "paid") \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "5 minutes")) \
    .agg(
        spark_sum("amount").alias("revenue"),
        count("*").alias("order_count")
    )

# Write aggregated data to Delta Lake
agg_query = agg.writeStream.format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", "/delta/checkpoints/revenue_agg") \
    .option("path", "/delta/tables/revenue_5min") \
    .start()

print("Streaming queries started:")
print(f"  - Raw orders: {raw_query.id}")
print(f"  - Revenue aggregation: {agg_query.id}")

# Wait for termination
spark.streams.awaitAnyTermination()
