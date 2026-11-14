from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType
)

from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
    trim
)

# 1. CREATE SPARK SESSION

spark = (
    SparkSession.builder
    .appName("BankAccountIngestion")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Account Spark Session Created")

# 2. DEFINE ACCOUNT SCHEMA

account_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("opening_balance", DecimalType(18, 2), True)
])

# 3. READ ACCOUNT CSV

input_path = "data/landing/accounts/accounts.csv"

accounts_df = (
    spark.read
    .option("header", True)
    .option("mode", "FAILFAST")
    .schema(account_schema)
    .csv(input_path)
)

print("\nAccount Master Data")

accounts_df.show(truncate=False)

# 4. CLEAN ACCOUNT DATA AND ADD METADATA

accounts_bronze = (
    accounts_df
    .withColumn("account_type", trim("account_type"))
    .withColumn("status", trim("status"))
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("batch_id", lit("account_demo_001"))
)

# 5. COUNT ACCOUNT RECORDS

total_accounts = accounts_bronze.count()

print("\nTotal Accounts Ingested:", total_accounts)

# 6. WRITE DATA INTO BRONZE LAYER

output_path = "storage/bronze/accounts/batch_id=account_demo_001"

(
    accounts_bronze.write
    .mode("overwrite")
    .parquet(output_path)
)

print("\nAccount Bronze Data Saved Successfully")

# 7. VERIFY BRONZE DATA

accounts_check = spark.read.parquet(output_path)

print("\nBronze Account Records")

accounts_check.show(truncate=False)

print("\nVerified Account Count:", accounts_check.count())

print("\nAccount Schema")

accounts_check.printSchema()

# 8. STOP SPARK SESSION

spark.stop()

print("\nAccount Ingestion Completed Successfully")
