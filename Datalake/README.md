# 🏗️ End-to-End Data Lake Pipeline (MinIO, Trino, Iceberg)

This project demonstrates a complete **data engineering pipeline**, from data ingestion to analytics, using a modern data lake architecture.

It simulates real-world scenarios in financial data processing, integrating multiple systems and tools.

---

## 📌 Architecture Overview

* **MinIO** → S3-compatible object storage (Data Lake)
* **Apache Iceberg** → Table format for large-scale data management
* **Trino** → Distributed SQL query engine
* **Hive Metastore + MariaDB** → Metadata storage & management
* **Oracle** → Source database (transactional data)
* **Python ETL** → Data ingestion & transformation
* **Webhook** → Trigger pipeline execution

---

## ⚙️ Tech Stack

* Data Lake: MinIO
* Table Format: Apache Iceberg
* Query Engine: Trino
* Metadata: Hive Metastore + MariaDB
* Source DB: Oracle
* ETL: Python (pandas, requests, etc.)
* Orchestration: Webhook-triggered pipelines
* Containerization: Docker / Docker Compose

---

## 🔄 Data Pipeline Flow

1. Extract data from **Oracle / external APIs**
2. Trigger pipeline via **Webhook**
3. Run **Python ETL** to:

   * Collect data
   * Clean & transform
4. Load data into **MinIO (Data Lake)** in Parquet/Iceberg format
5. Register tables in **Hive Metastore**
6. Query data using **Trino**

---

## 🧠 Key Features

* End-to-end **data ingestion → storage → query pipeline**
* Implementation of **data lake architecture (raw → processed layers)**
* Use of **Apache Iceberg** for scalable table management
* Integration with **multiple data sources (Oracle, APIs)**
* Automated pipeline triggering via webhook
* Query optimization using columnar storage (Parquet)

---

## 🚀 Getting Started

### 1. Start all services

```bash
docker-compose up -d
```

---

### 2. Upload data to MinIO

```bash
s3cmd --config minio.s3cfg mb s3://data-lake
s3cmd --config minio.s3cfg put data/sample.parquet s3://data-lake
```

---

### 3. Create schema & table in Trino

```bash
./trino --execute "
CREATE SCHEMA IF NOT EXISTS lake.raw
WITH (location = 's3a://data-lake/raw/');

CREATE TABLE lake.raw.sample (
  id INT,
  value DOUBLE
)
WITH (
  format = 'PARQUET'
);"
```

---

### 4. Query data

```bash
./trino --execute "SELECT * FROM lake.raw.sample LIMIT 10;"
```

---

## 📊 Use Case

This project can be applied to:

* Financial data processing (stock, trading data)
* Data warehouse offloading
* Analytics & reporting pipelines
* Building a modern data lake architecture

---

## 📎 Notes

This is a learning project designed to demonstrate core **data engineering concepts**, including ingestion, storage, transformation, and querying in a distributed environment.
