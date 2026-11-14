from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def test_bronze_transactions_exist():
    path = Path("storage/bronze/transactions")
    assert path.exists()


def test_silver_valid_transactions():
    spark = SparkSession.builder.appName("BankingTests").getOrCreate()

    df = spark.read.parquet("storage/silver/valid")

    assert df.count() == 12
    assert df.filter(col("amount") <= 0).count() == 0

    spark.stop()


def test_invalid_transactions():
    spark = SparkSession.builder.appName("BankingTests").getOrCreate()

    df = spark.read.parquet("storage/silver/invalid")

    assert df.count() == 2

    invalid_ids = [row["transaction_id"] for row in df.select("transaction_id").collect()]

    assert "T004" in invalid_ids
    assert "T007" in invalid_ids

    spark.stop()


def test_anomaly_report():
    spark = SparkSession.builder.appName("BankingTests").getOrCreate()

    df = spark.read.parquet("storage/gold/anomaly_report")

    assert df.count() == 3

    anomaly_ids = [row["transaction_id"] for row in df.select("transaction_id").collect()]

    assert "T005" in anomaly_ids
    assert "T009" in anomaly_ids
    assert "T013" in anomaly_ids

    spark.stop()


def test_streaming_output():
    spark = SparkSession.builder.appName("BankingTests").getOrCreate()

    df = spark.read.parquet("storage/silver/stream_transactions")

    assert df.count() == 17
    assert df.filter(col("stream_alert") == "HIGH_VALUE_DEBIT").count() == 4

    spark.stop()
