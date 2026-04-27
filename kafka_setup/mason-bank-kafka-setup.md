# Mason Bank – Kafka Setup on EC2 (Ubuntu)

A complete guide to set up Apache Kafka on an EC2 instance using Ubuntu and integrate it with AWS Glue jobs for real-time transaction processing.

---

## Overview

This setup supports a real-time streaming architecture with:

- **1 Producer (AWS Glue Job)** → Sends transaction data to Kafka
- **2 Consumers (AWS Glue Jobs)**:
  - **Alert Consumer** → Detects failed and high-value transactions
  - **Analytics Consumer** → Pushes data to Amazon S3

**Kafka Topic:** `transactions_topic`

---

## Architecture Flow

1. AWS Glue Producer generates transaction data
2. Data is published to Kafka topic `transactions_topic`
3. Two consumers process the data:
   - Alert Consumer for real-time monitoring
   - Analytics Consumer for data lake ingestion

---

## Prerequisites

- AWS EC2 instance (Ubuntu 20.04 or later)
- Security Group inbound rules:
  - Port `22` (SSH)
  - Port `9092` (Kafka)
  - Port `2181` (Zookeeper)
- Key pair (`.pem` file)

---

## Step 1 – Connect to EC2

```bash
ssh -i your-key.pem ubuntu@<EC2-PUBLIC-IP>
```

---

## Step 2 – Install Java

```bash
sudo apt update
sudo apt install openjdk-8-jdk -y
java -version
```

---

## Step 3 – Download and Extract Kafka

```bash
wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xvf kafka_2.13-3.6.1.tgz
cd kafka_2.13-3.6.1
```

---

## Step 4 – Configure Kafka

Edit the Kafka server configuration:

```bash
vi config/server.properties
```

Update the following lines:

```properties
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://<EC2-PUBLIC-IP>:9092
```

---

## Step 5 – Start Zookeeper

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

---

## Step 6 – Start Kafka Broker

Open a **new terminal** and run:

```bash
cd kafka_2.13-3.6.1
bin/kafka-server-start.sh config/server.properties
```

---

## Step 7 – Create Kafka Topic

```bash
bin/kafka-topics.sh --create \
  --topic transactions_topic \
  --bootstrap-server <EC2-PUBLIC-IP>:9092 \
  --partitions 1 \
  --replication-factor 1
```

---

## Notes

- Replace `<EC2-PUBLIC-IP>` with your actual EC2 instance public IP throughout this guide.
- Zookeeper must be running before starting the Kafka broker.
- Keep both Zookeeper and Kafka terminals open while running producers/consumers.
