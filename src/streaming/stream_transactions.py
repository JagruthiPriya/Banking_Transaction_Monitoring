from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    when,
    current_timestamp,
    input_file_name
)

# 1. CREATE SPARK SESSION

spark = (
    SparkSession.builder
    .appName("BankingTransactionStreaming")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Streaming Spark Session Created")

# 2. DEFINE INPUT SCHEMA

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("merchant", StringType(), True),
    StructField("location", StringType(), True)
])

# 3. READ INCOMING CSV FILES AS A STREAM

input_path = "data/streaming/transactions"

stream_df = (
    spark.readStream
    .format("csv")
    .option("header", True)
    .schema(transaction_schema)
    .option("maxFilesPerTrigger", 1)
    .load(input_path)
)

print("Streaming Input Folder Connected")

# 4. ADD STREAMING METADATA

processed_df = (
    stream_df
    .withColumn("source_file", input_file_name())
    .withColumn("processing_timestamp", current_timestamp())
)

# 5. FLAG HIGH-VALUE DEBIT TRANSACTIONS

processed_df = processed_df.withColumn(
    "stream_alert",
    when(
        (col("transaction_type") == "DEBIT")
        & (col("amount") >= 100000),
        "HIGH_VALUE_DEBIT"
    ).otherwise("NORMAL")
)

# 6. WRITE STREAMING RESULTS

output_path = "storage/silver/stream_transactions"

checkpoint_path = "checkpoints/banking_transactions"

stream_query = (
    processed_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="5 seconds")
    .start()
)

print("\nStreaming Pipeline Started")
print("Waiting for incoming transaction files...")
print("Press Ctrl+C to stop the streaming application.")

# 7. KEEP STREAM RUNNING

stream_query.awaitTermination()
