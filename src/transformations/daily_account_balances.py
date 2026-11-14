from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    when,
    lit,
    coalesce,
    to_date
)
from pyspark.sql.window import Window

# 1. CREATE SPARK SESSION

spark = (
    SparkSession.builder
    .appName("DailyBankAccountBalances")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Daily Balance Spark Session Created")

# 2. READ VALID TRANSACTIONS AND ACCOUNTS

transactions_df = spark.read.parquet(
    "storage/silver/valid"
)

accounts_df = spark.read.parquet(
    "storage/bronze/accounts/batch_id=account_demo_001"
)

print("\nValid Transaction Count:", transactions_df.count())

# 3. CONVERT TRANSACTION DATE TO DATE FORMAT

transactions_df = transactions_df.withColumn(
    "transaction_day",
    to_date(col("transaction_date"))
)

# 4. CALCULATE DAILY CREDITS AND DEBITS

daily_transactions = (
    transactions_df
    .groupBy("account_id", "transaction_day")
    .agg(
        spark_sum(
            when(
                col("transaction_type") == "CREDIT",
                col("amount")
            ).otherwise(lit(0))
        ).alias("daily_credits"),

        spark_sum(
            when(
                col("transaction_type") == "DEBIT",
                col("amount")
            ).otherwise(lit(0))
        ).alias("daily_debits")
    )
)

print("\nDaily Transaction Summary")

daily_transactions.orderBy(
    "account_id", "transaction_day"
).show(truncate=False)

# 5. ADD OPENING BALANCES

account_balances = accounts_df.filter(
    col("status") == "ACTIVE"
).select(
    "account_id",
    "customer_name",
    "opening_balance"
)

balance_df = daily_transactions.join(
    account_balances,
    on="account_id",
    how="inner"
)

# 6. CALCULATE DAILY NET MOVEMENT

balance_df = balance_df.withColumn(
    "net_movement",
    col("daily_credits") - col("daily_debits")
)

# 7. CALCULATE RUNNING CLOSING BALANCE

balance_window = (
    Window
    .partitionBy("account_id")
    .orderBy("transaction_day")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

balance_df = balance_df.withColumn(
    "closing_balance",
    col("opening_balance") +
    spark_sum("net_movement").over(balance_window)
)

# 8. CALCULATE DAILY OPENING BALANCE

balance_df = balance_df.withColumn(
    "previous_movement",
    coalesce(
        spark_sum("net_movement").over(
            balance_window.rowsBetween(
                Window.unboundedPreceding,
                -1
            )
        ),
        lit(0)
    )
)

balance_df = balance_df.withColumn(
    "daily_opening_balance",
    col("opening_balance") + col("previous_movement")
)

# 9. DISPLAY FINAL DAILY BALANCES

final_balances = balance_df.select(
    "account_id",
    "customer_name",
    "transaction_day",
    "daily_opening_balance",
    "daily_credits",
    "daily_debits",
    "net_movement",
    "closing_balance"
)

print("\nFINAL DAILY ACCOUNT BALANCES")

final_balances.orderBy(
    "account_id",
    "transaction_day"
).show(truncate=False)

# 10. SAVE GOLD DATA

output_path = "storage/gold/daily_account_balances"

(
    final_balances.write
    .mode("overwrite")
    .parquet(output_path)
)

print("\nDaily Account Balances Saved Successfully")

# 11. VERIFY OUTPUT

balance_check = spark.read.parquet(output_path)

print(
    "Verified Daily Balance Records:",
    balance_check.count()
)

print("\nDaily Balance Calculation Completed Successfully")

spark.stop()
