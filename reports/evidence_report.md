# Banking Transaction Monitoring Pipeline
## Project Evidence Report

**Execution date:** 14 November 2026  
**Technology:** Python, PySpark, Apache Spark Structured Streaming, Pytest  
**Environment:** WSL Ubuntu, VS Code  
**Project status:** Pipeline execution and initial testing completed

---

## 1. Project Overview

This project demonstrates a banking transaction monitoring pipeline that ingests transaction and account data, removes duplicate transactions, validates records, identifies suspicious high-value debits, calculates daily account balances, and processes incoming transactions using Spark Structured Streaming.

The pipeline follows a Medallion-style architecture:

- **Bronze:** Raw ingested data.
- **Silver:** Deduplicated, validated, and processed data.
- **Gold:** Anomaly reports and daily account balance summaries.

## 2. Environment Details

| Component | Version / Details |
|---|---|
| Python | 3.14.4 |
| PySpark | 4.3.0 |
| Pytest | 9.2.0 |
| Operating system | WSL Ubuntu |
| Development environment | Visual Studio Code |
| Storage format | Parquet |
| Streaming engine | Spark Structured Streaming |

## 3. Bronze – Data Ingestion Evidence

Two source datasets were ingested.

| Dataset | Input records | Output location |
|---|---:|---|
| Transactions | 15 | `storage/bronze/transactions/` |
| Accounts | 5 | `storage/bronze/accounts/` |

The ingestion process reads CSV files and stores them in Parquet format.

Additional metadata includes source file, ingestion timestamp, batch ID, and ingestion date.

**Result:** Both datasets were successfully ingested.

## 4. Silver – Deduplication Evidence

The transaction deduplication process compared transaction attributes to identify probable duplicate records.

| Metric | Records |
|---|---:|
| Original transactions | 15 |
| Unique transactions | 14 |
| Probable duplicates | 1 |

Transaction `T003` was identified as a probable duplicate of `T001`.

Output locations:

- Unique records: `storage/silver/deduplicated/`
- Duplicate records: `storage/silver/duplicates/`

**Result:** One probable duplicate was identified and separated.

**Note:** This project uses a demonstration-based matching rule. Production banking systems should use reliable source transaction identifiers and carefully designed duplicate rules to avoid suppressing legitimate repeated payments.

## 5. Silver – Data Validation Evidence

The validation process checked transaction identifiers, account existence and status, transaction amount, date, and transaction type.

| Metric | Records |
|---|---:|
| Input after deduplication | 14 |
| Valid transactions | 12 |
| Invalid transactions | 2 |

Invalid transactions:

| Transaction | Reason |
|---|---|
| T004 | Account A999 does not exist |
| T007 | Transaction amount is negative (-500) |

Output locations:

- Valid: `storage/silver/valid/`
- Invalid: `storage/silver/invalid/`

**Result:** 12 valid transactions and 2 invalid transactions were separated successfully.

## 6. Gold – Anomaly Detection Evidence

The anomaly detection process identified debit transactions with an amount greater than or equal to ₹100,000.

| Transaction ID | Account | Merchant | Amount |
|---|---|---|---:|
| T005 | A103 | Jewellery | ₹500,000 |
| T009 | A103 | Jewellery | ₹450,000 |
| T013 | A103 | Jewellery | ₹600,000 |

Summary:

- Valid transactions analyzed: 12
- High-value debit alerts: 3
- Account A103 high-value debit total: ₹1,550,000

Output location: `storage/gold/anomaly_report/`

**Result:** Three high-value debit transactions were flagged.

**Note:** These are rule-based monitoring alerts, not confirmed fraudulent transactions.

## 7. Gold – Daily Account Balance Evidence

The daily balance transformation combined account opening balances with validated credit and debit transactions.

| Account | Customer | 1 Nov closing balance | 2 Nov closing balance |
|---|---|---:|---:|
| A101 | John | ₹24,000 | ₹29,500 |
| A102 | Jane | ₹28,000 | ₹25,000 |
| A103 | Alice | -₹900,000 | -₹1,500,000 |
| A104 | David | ₹18,000 | ₹16,000 |

Total daily balance records: 8.

Output location: `storage/gold/daily_account_balances/`

**Result:** Eight daily account balance records were generated.

**Note:** The negative balance for A103 reflects the demonstration dataset and calculation. It does not establish overdraft authorization or fraud.

## 8. Structured Streaming Evidence

A file-based Spark Structured Streaming application was configured to monitor:

`data/streaming/transactions/`

The application:

1. Reads incoming CSV files.
2. Applies a predefined transaction schema.
3. Adds source file and processing timestamp metadata.
4. Flags debit transactions of ₹100,000 or more.
5. Writes processed records in Parquet format.

Output location:

`storage/silver/stream_transactions/`

### Initial streaming batch

| Metric | Result |
|---|---:|
| Transactions processed | 15 |
| Normal transactions | 12 |
| High-value debit alerts | 3 |

### Checkpoint recovery test

The streaming application was stopped and restarted using the same checkpoint location:

`checkpoints/banking_transactions/`

After restarting, the output still contained 15 records. This verified that the already-processed input file was not processed again.

### New file processing test

A second input file, `batch_002.csv`, was introduced with two new transactions.

| Transaction | Account | Amount | Alert |
|---|---|---:|---|
| T016 | A101 | ₹120,000 | HIGH_VALUE_DEBIT |
| T017 | A102 | ₹2,500 | NORMAL |

Final streaming results:

| Metric | Result |
|---|---:|
| Previously processed records | 15 |
| New records | 2 |
| Total streamed records | 17 |
| Normal transactions | 13 |
| High-value debit alerts | 4 |

**Result:** Checkpoint recovery and processing of a new input file were successfully demonstrated.

## 9. Automated Testing Evidence

Automated tests were created in:

`tests/test_banking_pipeline.py`

Command executed:

```bash
python -m pytest tests/test_banking_pipeline.py -v
```

Test results:

| Test | Result |
|---|---|
| Bronze transaction output exists | PASSED |
| Silver valid transactions | PASSED |
| Invalid transaction records | PASSED |
| Anomaly report | PASSED |
| Streaming output | PASSED |

**Final test summary:**

```text
5 passed, 1 warning in 27.16s
```

The warning concerned PySpark compatibility with pandas 3.x. It did not cause any test failures.

## 10. Evidence Summary

| Pipeline stage | Verified result |
|---|---|
| Bronze ingestion | 15 transactions and 5 accounts |
| Deduplication | 14 unique, 1 probable duplicate |
| Validation | 12 valid, 2 invalid |
| Anomaly detection | 3 high-value debit alerts |
| Daily balances | 8 records |
| Initial streaming | 15 records |
| Checkpoint recovery | No reprocessing of the original file |
| New streaming batch | 2 additional records |
| Final streaming output | 17 records, 4 alerts |
| Automated tests | 5 passed |

## 11. Project Limitations and Future Improvements

This is an educational demonstration, not a production-ready banking fraud detection system.

Potential improvements:

1. Integrate a real banking transaction API or message broker such as Kafka.
2. Implement stronger transaction idempotency and duplicate handling.
3. Apply account validation and data quality checks to streaming records.
4. Add configurable anomaly thresholds and account-specific behavioral rules.
5. Introduce audit logging, monitoring, and alert notifications.
6. Use a transactional data lake format such as Delta Lake for reliable updates.
7. Add orchestration using Apache Airflow.
8. Add role-based access control, encryption, and sensitive-data protection.

## 12. Conclusion

The Banking Transaction Monitoring project successfully demonstrates batch ingestion, Medallion-style data processing, duplicate detection, validation, anomaly monitoring, daily balance calculations, Structured Streaming, checkpoint recovery, and automated testing using PySpark.

The recorded execution results and five passing automated tests provide evidence that the implemented demonstration pipeline works against the supplied sample datasets.
