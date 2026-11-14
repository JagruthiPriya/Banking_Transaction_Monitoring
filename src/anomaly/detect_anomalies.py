
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    count,
    sum as spark_sum
)

# 1. CREATE SPARK SESSION

spark = (
    SparkSession.builder
    .appName("BankingAnomalyDetection")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Anomaly Detection Spark Session Created")

# 2. READ VALID TRANSACTIONS

input_path = "storage/silver/valid"

transactions_df = spark.read.parquet(input_path)

print("\nValid Transactions:", transactions_df.count())

# 3. DEFINE HIGH-VALUE TRANSACTION RULE

# Demo threshold: debit amount >= 100,000

anomalies_df = transactions_df.withColumn(
    "anomaly_reason",
    when(
        (col("transaction_type") == "DEBIT")
        & (col("amount") >= 100000),
        "High-value debit transaction"
    ).otherwise("Normal")
)

# 4. FILTER SUSPICIOUS TRANSACTIONS

suspicious_df = anomalies_df.filter(
    col("anomaly_reason") != "Normal"
)

print("\nSuspicious Transactions")

suspicious_df.select(
    "transaction_id",
    "account_id",
    "transaction_date",
    "amount",
    "merchant",
    "location",
    "anomaly_reason"
).show(truncate=False)

print(
    "High-value Transactions:",
    suspicious_df.count()
)

# 5. IDENTIFY REPEATED HIGH-VALUE DEBITS

account_anomalies = (
    suspicious_df
    .groupBy("account_id")
    .agg(
        count("*").alias("high_value_transaction_count"),
        spark_sum("amount").alias("total_high_value_amount")
    )
    .filter(col("high_value_transaction_count") >= 2)
)

print("\nAccounts With Repeated High-Value Transactions")

account_anomalies.show(truncate=False)

# 6. SAVE ANOMALY REPORT

output_path = "storage/gold/anomaly_report"

(
    suspicious_df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("\nAnomaly Report Saved Successfully")

# 7. VERIFY OUTPUT

anomaly_check = spark.read.parquet(output_path)

print(
    "Verified Anomaly Count:",
    anomaly_check.count()
)

print("\nAnomaly Detection Completed Successfully")

spark.stop()
