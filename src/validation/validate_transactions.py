from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    upper,
    trim,
    concat_ws
)

# 1. CREATE SPARK SESSION

spark = (
    SparkSession.builder
    .appName("BankingTransactionValidation")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Validation Spark Session Created")

# 2. READ DEDUPLICATED TRANSACTIONS

transactions_path = "storage/silver/deduplicated"
accounts_path = (
    "storage/bronze/accounts/"
    "batch_id=account_demo_001"
)

transactions_df = spark.read.parquet(transactions_path)
accounts_df = spark.read.parquet(accounts_path)

print("\nTransactions Before Validation:",
      transactions_df.count())

# 3. PREPARE ACCOUNT MASTER

account_master = accounts_df.select(
    col("account_id").alias("master_account_id"),
    upper(trim(col("status"))).alias("account_status")
)

# 4. JOIN TRANSACTIONS WITH ACCOUNT MASTER

joined_df = transactions_df.join(
    account_master,
    transactions_df.account_id == account_master.master_account_id,
    "left"
)

# 5. APPLY VALIDATION RULES

validated_df = joined_df.withColumn(
    "validation_reason",
    concat_ws(
        "; ",
        when(
            col("transaction_id").isNull()
            | (trim(col("transaction_id")) == ""),
            "Missing transaction ID"
        ),
        when(
            col("transaction_date").isNull(),
            "Missing transaction date"
        ),
        when(
            col("master_account_id").isNull(),
            "Account does not exist"
        ),
        when(
            col("account_status") != "ACTIVE",
            "Account is inactive"
        ),
        when(
            col("amount").isNull() | (col("amount") <= 0),
            "Amount must be greater than zero"
        ),
        when(
            ~upper(trim(col("transaction_type"))).isin(
                "CREDIT", "DEBIT"
            ),
            "Invalid transaction type"
        )
    )
)

# 6. SEPARATE VALID AND INVALID TRANSACTIONS

validated_df = validated_df.withColumn(
    "validation_status",
    when(
        col("validation_reason") == "",
        "VALID"
    ).otherwise("INVALID")
)

valid_transactions = (
    validated_df
    .filter(col("validation_status") == "VALID")
    .drop("master_account_id", "account_status")
)

invalid_transactions = (
    validated_df
    .filter(col("validation_status") == "INVALID")
    .drop("master_account_id", "account_status")
)

# 7. DISPLAY VALIDATION RESULTS

print("\nValidation Summary")
print("------------------")

print("Valid Transactions:", valid_transactions.count())
print("Invalid Transactions:", invalid_transactions.count())

print("\nValid Transactions")

valid_transactions.show(truncate=False)

print("\nInvalid Transactions")

invalid_transactions.select(
    "transaction_id",
    "account_id",
    "amount",
    "validation_reason"
).show(truncate=False)

# 8. SAVE VALID TRANSACTIONS

valid_path = "storage/silver/valid"

(
    valid_transactions.write
    .mode("overwrite")
    .parquet(valid_path)
)

print("\nValid Transactions Saved")

# 9. SAVE INVALID TRANSACTIONS

invalid_path = "storage/silver/invalid"

(
    invalid_transactions.write
    .mode("overwrite")
    .parquet(invalid_path)
)

print("\nInvalid Transactions Saved")

# 10. VERIFY OUTPUT

valid_check = spark.read.parquet(valid_path)
invalid_check = spark.read.parquet(invalid_path)

print("\nVerified Valid Count:", valid_check.count())
print("Verified Invalid Count:", invalid_check.count())

print("\nTransaction Validation Completed Successfully")

spark.stop()
