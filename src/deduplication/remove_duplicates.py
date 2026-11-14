from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    row_number
)
from pyspark.sql.window import Window

# 1. CREATE SPARK SESSION

spark = (
    SparkSession.builder
    .appName("BankingTransactionDeduplication")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Deduplication Spark Session Created")

# 2. READ BRONZE TRANSACTIONS

input_path = (
    "storage/bronze/transactions/"
    "batch_id=banking_demo_001"
)

transactions_df = spark.read.parquet(input_path)

print("\nOriginal Bronze Transactions")

transactions_df.show(truncate=False)

print(
    "\nOriginal Record Count:",
    transactions_df.count()
)

# 3. DEFINE DUPLICATE IDENTIFICATION RULE

# transaction_id is intentionally excluded.
# We compare the business details of each transaction.

duplicate_columns = [
    "account_id",
    "transaction_date",
    "transaction_type",
    "amount",
    "merchant",
    "location"
]

# 4. IDENTIFY DUPLICATE RECORDS

duplicate_window = (
    Window
    .partitionBy(*duplicate_columns)
    .orderBy("transaction_id")
)

ranked_df = transactions_df.withColumn(
    "duplicate_rank",
    row_number().over(duplicate_window)
)

# Keep the first occurrence
unique_transactions = (
    ranked_df
    .filter(col("duplicate_rank") == 1)
    .drop("duplicate_rank")
)

# Keep additional occurrences separately
duplicate_transactions = (
    ranked_df
    .filter(col("duplicate_rank") > 1)
    .drop("duplicate_rank")
)

# 5. DISPLAY RESULTS

original_count = transactions_df.count()
unique_count = unique_transactions.count()
duplicate_count = duplicate_transactions.count()

print("\nDeduplication Results")
print("---------------------")

print("Original Records:", original_count)
print("Unique Records:", unique_count)
print("Probable Duplicate Records:", duplicate_count)

print("\nProbable Duplicate Transactions")

duplicate_transactions.show(truncate=False)

# 6. SAVE UNIQUE TRANSACTIONS

unique_path = "storage/silver/deduplicated"

(
    unique_transactions.write
    .mode("overwrite")
    .parquet(unique_path)
)

print("\nUnique Transactions Saved Successfully")

# 7. SAVE DUPLICATE TRANSACTIONS

duplicate_path = "storage/silver/duplicates"

(
    duplicate_transactions.write
    .mode("overwrite")
    .parquet(duplicate_path)
)

print("\nDuplicate Transactions Saved Successfully")

# 8. VERIFY OUTPUT

unique_check = spark.read.parquet(unique_path)
duplicate_check = spark.read.parquet(duplicate_path)

print("\nVerified Unique Count:", unique_check.count())
print("Verified Duplicate Count:", duplicate_check.count())

print("\nDeduplication Completed Successfully")

spark.stop()
