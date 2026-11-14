# Banking Transaction Monitoring Pipeline

## 1. Project Overview

This project implements a banking transaction monitoring pipeline using Python and PySpark.

The pipeline ingests transaction and account data, detects duplicate transactions, validates transaction records, identifies suspicious high-value debit transactions, calculates daily account balances, and processes new transactions using Structured Streaming.

## 2. Technologies Used

- Python
- PySpark
- Apache Spark Structured Streaming
- Pytest
- Parquet
- CSV
- Linux / WSL Ubuntu
- Visual Studio Code

## 3. Architecture

The project follows a Medallion-style data architecture.

```text
CSV Source Data
      |
      v
Bronze Layer
Raw transaction and account data
      |
      v
Silver Layer
Deduplication
Validation
Invalid record separation
      |
      v
Gold Layer
Anomaly reports
Daily account balances
      |
      v
Analyst-ready outputs
```

A separate streaming pipeline monitors incoming transaction files and writes processed records to Parquet storage.

## 4. Project Structure

```text
Banking_Transaction_Monitoring/
|
├── data/
│   ├── landing/
│   │   ├── transactions/
│   │   └── accounts/
│   └── streaming/
│       └── transactions/
|
├── storage/
│   ├── bronze/
│   ├── silver/
│   │   ├── valid/
│   │   ├── invalid/
│   │   ├── deduplicated/
│   │   ├── duplicates/
│   │   └── stream_transactions/
│   └── gold/
│       ├── anomaly_report/
│       └── daily_account_balances/
|
├── checkpoints/
│   └── banking_transactions/
|
├── src/
│   ├── ingestion/
│   ├── validation/
│   ├── deduplication/
│   ├── anomaly/
│   ├── transformations/
│   └── streaming/
|
├── tests/
│   └── test_banking_pipeline.py
|
└── README.md
```

## 5. Pipeline Implementation

### Bronze – Data Ingestion

- Reads transaction and account CSV files.
- Stores raw data in Parquet format.
- Adds ingestion metadata and batch identifiers.

### Silver – Data Quality

- Identifies probable duplicate transactions.
- Validates account existence and account status.
- Checks transaction amount and transaction type.
- Separates valid and invalid records.

### Gold – Business Analytics

- Identifies debit transactions of ₹100,000 or more.
- Summarizes repeated high-value transactions by account.
- Calculates daily account balances using opening balances and transaction activity.

### Structured Streaming

- Monitors an incoming transaction folder.
- Processes newly arriving CSV files.
- Flags high-value debit transactions.
- Writes results to Parquet.
- Uses Spark checkpoints to recover processing progress after restart.

## 6. Project Results

| Metric | Result |
|---|---:|
| Original batch transactions | 15 |
| Probable duplicates | 1 |
| Valid transactions | 12 |
| Invalid transactions | 2 |
| High-value debit alerts in batch | 3 |
| Original streamed transactions | 15 |
| Transactions after new batch | 17 |
| Automated tests passed | 5 |

## 7. Running the Project

Activate the virtual environment:

```bash
source venv-linux/bin/activate
```

Run the pipeline scripts from the project root:

```bash
python src/ingestion/read_transactions.py
python src/ingestion/read_accounts.py
python src/deduplication/remove_duplicates.py
python src/validation/validate_transactions.py
python src/anomaly/detect_anomalies.py
python src/transformations/daily_account_balances.py
```

Run the streaming application separately:

```bash
python src/streaming/stream_transactions.py
```

Run automated tests:

```bash
python -m pytest tests/test_banking_pipeline.py -v
```

## 8. Testing

The automated tests verify:

- Bronze data availability.
- Valid transaction records.
- Invalid transaction records.
- Anomaly report output.
- Streaming output and alert counts.

Current result: **5 tests passed.**

## 9. Business Use Case

Banks process large volumes of financial transactions every day. Incorrect records, duplicate transactions, and unusual high-value payments require monitoring.

This project demonstrates how distributed data processing can help banking teams identify data-quality issues, monitor suspicious transaction patterns, and prepare analytical outputs.

## 10. Future Improvements

- Integrate Kafka for real-time transaction ingestion.
- Store curated data in a cloud data lake.
- Add configurable anomaly detection rules.
- Implement transaction-level idempotency.
- Add orchestration using Apache Airflow.
- Add monitoring, logging, and alert notifications.
- Improve automated test coverage.

## 11. Important Note

The high-value transaction threshold is a demonstration rule. An alert does not establish that a transaction is fraudulent. Production banking systems require stronger validation, security, auditability, and fraud investigation controls.
