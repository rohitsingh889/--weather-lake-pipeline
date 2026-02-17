# 🌦 Incremental Serverless Weather Data Lake Pipeline on AWS

A production-style serverless data engineering pipeline that ingests historical weather data from the Open-Meteo API, stores raw data in Amazon S3, performs Spark-based transformations using AWS Glue (PySpark), generates analytics-ready datasets, and enables SQL querying via AWS Glue Data Catalog and Amazon Athena — fully orchestrated by Apache Airflow.

---

## 🚀 Project Overview

This project demonstrates a modern **serverless data lake architecture** on AWS using industry-standard data engineering patterns:

✔ API-Driven Data Ingestion  
✔ Bronze / Silver / Gold Data Lake Design  
✔ Incremental Processing Strategy  
✔ Spark Transformations with AWS Glue  
✔ Data Quality Validation  
✔ Metadata Management with Glue Crawler  
✔ Serverless SQL Analytics via Athena  
✔ Workflow Orchestration using Airflow  

---

## 🏗 Architecture

Pipeline Flow:

Open-Meteo Weather API  
→ Python Extraction (Requests + Boto3)  
→ Amazon S3 (Bronze Layer – Raw JSON)  
→ AWS Glue Job (Silver – PySpark Transformations + Data Quality Checks)  
→ Amazon S3 (Silver Layer – Parquet)  
→ AWS Glue Job (Gold – Aggregations)  
→ Amazon S3 (Gold Layer – Analytics Ready Parquet)  
→ AWS Glue Crawler  
→ AWS Glue Data Catalog  
→ Amazon Athena  
→ BI / Analytics Dashboard  

---

## 🧩 Technologies Used

- **AWS S3** → Data Lake Storage  
- **AWS Glue** → Serverless Spark ETL (PySpark)  
- **AWS Glue Crawler** → Schema & Metadata Discovery  
- **AWS Glue Data Catalog** → Table Definitions for Athena  
- **Amazon Athena** → Serverless SQL Query Engine  
- **Apache Airflow** → Pipeline Orchestration  
- **Python** → API Ingestion & S3 Upload  
- **Boto3** → AWS SDK for Python  
- **Requests Module** → REST API Calls  

---

## 📡 Data Source

**API Provider:** Open-Meteo Archive API  

The pipeline retrieves **historical hourly weather data** including:

- Temperature  
- Precipitation  
- Windspeed  
- Timestamp  

Data is fetched dynamically for configured cities.

---

## ⚙ Extraction Layer (Python)

Data ingestion is handled via Python scripts using:

### ✅ `requests` module
Used to make REST API calls to Open-Meteo.

### ✅ `boto3` (AWS SDK)
Used to upload raw JSON responses directly into Amazon S3.

Example responsibilities:

✔ Fetch previous day's weather data  
✔ Preserve raw API response  
✔ Store immutable JSON in Bronze layer  

---

## 🗂 Bronze Layer – Raw Zone

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

Purpose:

✔ Preserve original API data  
✔ Allow replay & debugging  
✔ Maintain auditability  

No transformations occur here.

---

## 🔄 Silver Layer – Transformation Zone

**Processing Engine:** AWS Glue (PySpark)

Responsibilities:

✔ Flatten nested JSON arrays  
✔ Parse timestamps  
✔ Cast numeric fields  
✔ Remove duplicates  
✔ Apply Data Quality Checks  

### ✅ Data Quality Validations

- Null Checks  
- Domain Range Checks  
- Duplicate Detection  
- Fail-Fast Mechanism  

Output Format:

✔ **Parquet (Columnar, Optimized)**

Partition Strategy:

```
silver/weather/date=YYYY-MM-DD/
```

Benefits:

✔ Faster Athena queries  
✔ Reduced scan cost  
✔ Analytics-friendly layout  

---

## 📊 Gold Layer – Analytics Zone

**Processing Engine:** AWS Glue (Aggregation Job)

Responsibilities:

Transform hourly records → Daily city-level metrics

Generated Metrics:

- Average Temperature  
- Maximum Temperature  
- Total Precipitation  
- Average Windspeed  

Output:

✔ Parquet  
✔ Partitioned by date  

```
gold/weather/date=YYYY-MM-DD/
```

Purpose:

✔ BI / Dashboard consumption  
✔ Small & efficient datasets  
✔ Business-ready structure  

---

## 🔁 Incremental Processing Strategy

The pipeline follows a **partition-level incremental model**.

Behavior:

✔ Processes only the target `process_date`  
✔ Overwrites only that partition  
✔ Safe re-runs (idempotent)  
✔ Prevents duplicates  

Mechanism:

```python
.mode("overwrite")
.option("replaceWhere", "date = 'YYYY-MM-DD'")
```

Industry-standard pattern ✔

---

## ⛓ Orchestration Layer – Apache Airflow

Apache Airflow controls the workflow execution order:

✔ API Extraction  
✔ Silver Glue Job  
✔ Gold Glue Job  
✔ Glue Crawler  

Airflow runs inside a **Dockerized local environment**, simulating real-world orchestration setups.

Benefits:

✔ Clear dependency management  
✔ Retry & failure handling  
✔ Cloud job coordination  

---

## 🐳 Dockerized Airflow Environment

Airflow is deployed locally using Docker for:

✔ Environment isolation  
✔ Reproducibility  
✔ Easy dependency management  

This mimics production orchestration patterns without managing servers.

---

## 🧾 Metadata & Query Layer

### ✅ AWS Glue Crawler
Automatically infers schema from Parquet datasets.

### ✅ AWS Glue Data Catalog
Stores table definitions used by Athena.

### ✅ Amazon Athena
Executes SQL queries directly on S3 data.

Advantages:

✔ Fully serverless  
✔ No cluster management  
✔ Cost-efficient analytics  

---

## 📈 Analytics / BI Layer

Athena-queryable Gold datasets can be consumed by:

✔ BI dashboards  
✔ SQL clients  
✔ Visualization tools  

---

## 📁 Project Structure

Airflow DAG environment contains:

✔ DAG file  
✔ API client logic  
✔ Extraction logic  
✔ S3 writer logic  

All Python ingestion modules reside in the **same Airflow DAG location**, ensuring easy imports and simplified orchestration.

AWS Glue jobs execute independently within AWS.

---

## ✅ Key Engineering Concepts Demonstrated

- Serverless Data Lake Architecture  
- Incremental Data Processing  
- Partition-Aware Storage Design  
- Spark Transformations (PySpark)  
- Data Quality Enforcement  
- Metadata-Driven Analytics  
- Workflow Orchestration  

---

## 👨‍💻 Author

**Rohit Raj Singh**

---

## ⭐ Why This Project Matters

This pipeline mirrors **real data engineering workflows** used in production systems:

✔ API ingestion pipelines  
✔ Cloud-native ETL design  
✔ Analytics-optimized storage  
✔ Failure-resilient processing  

Designed for learning **industry-relevant AWS data engineering practices**.

---
