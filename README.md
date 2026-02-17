# 🌦 Incremental Weather Data Lake Pipeline on AWS

This project implements a production-style **batch data engineering pipeline** that ingests historical weather data from the **Open-Meteo Archive API**, stores raw data in Amazon S3, performs distributed transformations using **AWS Glue (PySpark)**, generates analytics-ready datasets, and enables SQL-based querying via **AWS Glue Data Catalog and Amazon Athena** — fully orchestrated by **Apache Airflow running in a Dockerized local environment**.

---

## 🚀 Project Objective

The goal of this pipeline is to demonstrate **real-world cloud data engineering patterns**, including:

✔ Workflow orchestration  
✔ API-driven ingestion  
✔ Multi-layer data lake architecture  
✔ Distributed Spark transformations  
✔ Incremental processing strategy  
✔ Data quality validation  
✔ Metadata-driven analytics  
✔ BI consumption workflows  

---

## 🏗 High-Level Architecture

Pipeline Flow:

Open-Meteo Archive API  
→ Python Extraction Layer  
→ Amazon S3 (Bronze Layer – Raw JSON)  
→ AWS Glue (Silver Layer – PySpark Transformations + Data Quality Checks)  
→ Amazon S3 (Silver Layer – Parquet)  
→ AWS Glue (Gold Layer – Aggregations)  
→ Amazon S3 (Gold Layer – Analytics Ready Parquet)  
→ AWS Glue Crawler  
→ AWS Glue Data Catalog  
→ Amazon Athena  
→ BI / Analytics Tools (Power BI)

---

---

## 🧩 Technologies & Tools Used

| Category | Technology / Tool | Purpose |
|----------|-------------------|---------|
| Orchestration | Apache Airflow (Dockerized) | Workflow scheduling & dependency management |
| Containerization | Docker | Local Airflow environment isolation |
| Language | Python | API ingestion & orchestration logic |
| HTTP Client | Requests Module | REST API communication |
| AWS SDK | Boto3 | Programmatic AWS interaction |
| Cloud Storage | Amazon S3 | Data lake storage layers |
| Distributed Processing | AWS Glue (PySpark) | Spark-based ETL transformations |
| Metadata Discovery | AWS Glue Crawler | Schema inference & partition detection |
| Metadata Catalog | AWS Glue Data Catalog | Athena table definitions |
| Query Engine | Amazon Athena | SQL querying on S3 |
| BI Tool | Power BI | Analytics & visualization |
| Credential Management | AWS CLI + IAM Roles | Secure AWS authentication |

---

---

## 📡 Data Source Layer

**Provider:** Open-Meteo Archive API  

The pipeline retrieves **historical hourly weather observations** for configured cities.

Data retrieved includes:

- temperature_2m  
- precipitation  
- windspeed_10m  
- timestamp  

The API returns nested JSON structures requiring flattening.

---

---

## ⚙ Extraction Layer (Python)

### Responsibilities

✔ Call Open-Meteo API using `requests`  
✔ Fetch previous day's data dynamically  
✔ Preserve raw JSON response  
✔ Upload directly to S3 Bronze layer using `boto3`

---

### Libraries Used

**Requests Module**

Used for external REST API communication.

**Boto3 (AWS SDK)**

Used for:

✔ S3 PutObject operations  
✔ IAM-based authentication  
✔ Secure AWS integration  

No AWS keys hardcoded.

---

### AWS Credential Verification

Authentication handled via:

✔ AWS CLI configuration  
✔ IAM user / role permissions  
✔ Boto3 credential provider chain  

Driver logic:

- Boto3 automatically resolves credentials  
- Uses environment / IAM / config chain  
- No manual secret handling required  

---

---

## 🗂 Bronze Layer – Raw Data Zone

**Storage:** Amazon S3  
**Format:** Raw JSON  
**Partitioning Strategy:**

```
bronze/weather/
    city=XYZ/
        year=YYYY/
            month=MM/
                day=DD/
```

---

### Purpose

✔ Immutable raw storage  
✔ Replay capability  
✔ Debugging & auditing  
✔ Schema recovery  

No transformations applied.

---

---

## 🔄 Silver Layer – Transformation & Validation Zone

**Processing Engine:** AWS Glue (PySpark)

---

### Responsibilities

✔ Read partition-specific Bronze JSON  
✔ Flatten nested hourly arrays  
✔ Parse timestamps  
✔ Cast numeric fields  
✔ Remove duplicates  
✔ Apply Data Quality Checks  

---

### Output Format

✔ Parquet (Columnar, optimized)

Partitioning:

```
silver/weather/date=YYYY-MM-DD/
```

---

### Data Quality Strategy

Implemented directly in Spark:

✔ Null validation  
✔ Domain range validation  
✔ Duplicate detection  
✔ Fail-fast enforcement  

Example checks:

- Null timestamps rejected  
- Temperature bounds enforced  
- Negative windspeed prevented  
- Duplicate city/timestamp removed  

Bad data → Job fails intentionally.

---

---

## 📊 Gold Layer – Analytics Zone

**Processing Engine:** AWS Glue (Aggregation Job)

---

### Responsibilities

Transform Silver hourly records → Daily metrics

Aggregations:

- avg_temperature  
- max_temperature  
- total_precipitation  
- avg_windspeed  

---

### Output

✔ Parquet  
✔ Partitioned by date  

```
gold/weather/date=YYYY-MM-DD/
```

---

### Purpose

✔ BI-ready datasets  
✔ Reduced scan cost  
✔ Faster Athena queries  

---

---

## 🔁 Incremental Processing Strategy

The pipeline follows a **partition-level incremental model**.

Behavior:

✔ Process only `process_date`  
✔ Overwrite only that partition  
✔ Idempotent reruns  
✔ Prevent duplicate records  

Mechanism:

```python
.mode("overwrite")
.option("replaceWhere", "date = 'YYYY-MM-DD'")
```

Industry-standard batch incremental design.

---

---

## ⛓ Orchestration Layer – Apache Airflow

Airflow is the **central control plane** of the pipeline.

---

### Airflow Responsibilities

✔ Schedule pipeline runs  
✔ Manage task dependencies  
✔ Trigger AWS Glue Jobs  
✔ Trigger Glue Crawler  
✔ Retry & failure handling  

Execution order:

1. API Extraction  
2. Silver Glue Job  
3. Gold Glue Job  
4. Glue Crawler  

---

### Dockerized Airflow Environment

Airflow runs locally inside Docker for:

✔ Environment reproducibility  
✔ Dependency isolation  
✔ Easy configuration  
✔ Cloud orchestration simulation  

---

### DAG Location & Module Design

Airflow container contains:

✔ DAG file  
✔ API client module  
✔ Extraction logic  
✔ S3 writer module  

All ingestion modules placed in **same DAG directory** for simplified imports.

Glue jobs run independently on AWS.

---

---

## 🧾 Metadata & Schema Management

### AWS Glue Crawler

✔ Infers Parquet schema  
✔ Detects partitions  
✔ Updates Data Catalog  

---

### AWS Glue Data Catalog

✔ Stores table definitions  
✔ Enables Athena SQL querying  

---

---

## 🔍 Query Layer – Amazon Athena

Athena enables SQL queries directly on S3 datasets.

---

### Benefits

✔ No infrastructure management  
✔ Pay-per-query model  
✔ Works with Glue Catalog  

---

### Example Queries

```sql
SELECT city, avg_temperature
FROM gold_weather
WHERE date = DATE '2026-02-16';
```

Supports analytics, filtering, aggregations.

---

---

## 📈 BI / Visualization Layer – Power BI

Gold datasets are designed for BI consumption.

---

### Integration Strategy

✔ Athena used as query backend  
✔ Power BI connects via Athena connector  
✔ Enables dashboarding & reporting  

---

### Dashboard Intent

Planned visuals:

✔ Temperature trends  
✔ City comparisons  
✔ KPI metrics  
✔ Aggregated insights  

(Dashboard implementation planned as future enhancement.)

---

---

## ✅ Key Data Engineering Concepts Demonstrated

✔ Data Lake Layering (Bronze / Silver / Gold)  
✔ Distributed ETL using Spark  
✔ Incremental Batch Processing  
✔ Partition-Aware Design  
✔ Data Quality Enforcement  
✔ Cloud-Orchestrated Workflows  
✔ Metadata-Driven Analytics  

---

---

## 👨‍💻 Author & Project Context

**Rohit Raj Singh**

This project is part of my professional portfolio and demonstrates a **production-grade cloud data engineering pipeline** using **Apache Airflow and AWS**.

Key skills reflected:

- Workflow orchestration with Apache Airflow (local, Dockerized)  
- REST API ingestion and immutable data lake design  
- AWS Glue–based distributed ETL using PySpark  
- Schema inference and partition management with Glue Crawlers  
- SQL analytics using Amazon Athena  
- Secure AWS integration using boto3 and AWS CLI  
- End-to-end pipeline automation and monitoring  

📬 **LinkedIn:**  
[Connect with me professionally](https://www.linkedin.com/in/rohit-raj-singh-3030172a4?utm_source=share&utm_campaign=share_via&utm_content=profile&utm_medium=android_app)

---
