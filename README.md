# Mason Bank – Real-Time Transaction Processing System

## Overview

This project implements a **real-time transaction processing system** using **Apache Kafka and AWS services**. It simulates streaming banking transactions, performs real-time monitoring, and builds a scalable data lake for analytics.

The system is designed to handle both:

* Real-time alerting for critical transactions
* Batch-oriented analytical processing
---

## End-to-End Flow

1. AWS Glue job acts as a **Kafka Producer** generating transaction data
2. Apache Kafka (hosted on EC2) ingests and streams data
3. Two consumers process the data:
   * Alert Consumer → Detects failed and high-value transactions
   * Analytics Consumer → Pushes data into S3 (Raw Layer)
4. ETL jobs transform data across layers:
   * Raw → Processed → Curated
5. Curated data is queried using **Amazon Athena**

---

## Tech Stack

| Category        | Tools / Services        |
| --------------- | ----------------------- |
| Streaming       | Apache Kafka            |
| Cloud Platform  | AWS                     |
| Compute         | Amazon EC2              |
| Data Ingestion  | AWS Glue                |
| Data Processing | PySpark (AWS Glue Jobs) |
| Storage         | Amazon S3               |
| Query Engine    | Amazon Athena           |
| Language        | Python                  |

---

## Project Structure

```
mason-bank-realtime-transactions/
│
├── producer/              # Glue job for Kafka producer
├── kafka_setup/           # Kafka setup and configurations
├── consumers/             # Alert and analytics consumers
└── architecture/          # Architecture diagram
└── README.md              # project walkthrough and summary
```

---

## Real-Time Alerting

The alert consumer continuously monitors incoming transactions and triggers alerts for:

* Failed transactions
* High-value transactions (threshold-based)

This enables near real-time detection of anomalies and system issues.

---

## Data Pipeline (Analytics Layer)

The analytics pipeline follows a **layered data lake architecture**:

* **Raw Layer:** Stores streaming data ingested from Kafka
* **Processed Layer:** Data cleansing and transformation using **PySpark (AWS Glue Jobs)**
* **Curated Layer:** Aggregated and optimized datasets built using **PySpark** for analytics

This design ensures scalability, data quality, and efficient querying.

---

## Query Layer

* External tables are created on curated data in S3
* Amazon Athena enables **serverless SQL querying**
* Supports ad-hoc analysis and reporting

---

## Getting Started

### Prerequisites

* AWS Account
* EC2 instance for Kafka setup
* IAM roles for Glue, S3, and Athena
* Python 3.x

### Setup Steps

1. Configure Kafka on EC2 (refer to `kafka_setup/mason-bank-kafka-setup.md`)
2. Deploy and run the Glue Producer job
3. Start Kafka consumers
4. Execute PySpark ETL jobs for data transformation
5. Query curated data using Athena

---

## Future Enhancements

* Dashboard integration (Amazon QuickSight / Tableau)
* Migration to Amazon MSK (Managed Kafka)
* CI/CD pipeline integration

---

## Author

Tanish Kohli
Linkedin: https://www.linkedin.com/in/tanishkohli11/


