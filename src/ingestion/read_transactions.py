from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
    to_date
)

# 1. CREATE SPARK SESSION

spark = (
    SparkSession.builder
    .appName("BankingTransactionIngestion")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark Session Created Successfully")

# 2. DEFINE TRANSACTION SCHEMA

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("merchant", StringType(), True),
    StructField("location", StringType(), True)
])

# 3. READ TRANSACTION CSV

input_path = "data/landing/transactions/transactions.csv"

transactions_df = (
    spark.read
    .option("header", True)
    .option("mode", "FAILFAST")
    .schema(transaction_schema)
    .csv(input_path)
)

print("\nOriginal Transaction Data")

transactions_df.show(truncate=False)


# 4. ADD INGESTION METADATA

batch_id = "banking_demo_001"

bronze_df = (
    transactions_df
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("batch_id", lit(batch_id))
    .withColumn(
        "ingestion_date",
        to_date(current_timestamp())
    )
)

# 5. DISPLAY RECORD COUNT

total_records = bronze_df.count()

print("\nTotal Transactions Ingested:", total_records)

# 6. WRITE TO BRONZE LAYER

output_path = (
    "storage/bronze/transactions/"
    "batch_id=banking_demo_001"
)

(
    bronze_df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("\nBronze Data Written Successfully")

# 7. VERIFY BRONZE DATA

bronze_check = spark.read.parquet(output_path)

print("\nBronze Transaction Records")

bronze_check.show(truncate=False)

print("\nBronze Record Count:", bronze_check.count())

print("\nBronze Schema")

bronze_check.printSchema()

# 8. STOP SPARK

spark.stop()

print("\nTransaction Ingestion Completed Successfully")
