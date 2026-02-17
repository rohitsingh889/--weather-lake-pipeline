# 🌦 Incremental Weather Data Lake Pipeline on AWS

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/AWS%20pipeline%20main%20architecture.png)
This project implements a production-style **batch data engineering pipeline** that ingests historical weather data from the **Open-Meteo Archive API**, stores immutable raw data in Amazon S3, performs distributed transformations using **AWS Glue (PySpark)**, generates analytics-ready datasets, and enables SQL-based querying via **AWS Glue Data Catalog and Amazon Athena** — fully orchestrated by **Apache Airflow in a Dockerized local environment**.

**Data Source API:** https://open-meteo.com/

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
→ Amazon S3 (Gold Layer – Analytics-Ready Parquet)  
→ AWS Glue Crawler  
→ AWS Glue Data Catalog  
→ Amazon Athena  
→ BI / Analytics Tools (Power BI)

---

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/pipelineinfo.png)

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


## 🔄 End-to-End Data Lifecycle (Step-Wise)

The pipeline follows a structured **Extract → Transform → Load → Analytics** workflow.

---

### 1️⃣ Extract Phase – API Ingestion

✔ Weather data retrieved from Open-Meteo Archive API  
✔ Python used as the ingestion engine  
✔ REST calls executed via the `requests` module  
✔ Raw JSON responses preserved  

**Output:**

→ Raw JSON stored in Bronze layer (Amazon S3)

**S3 Folders**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/s3%20bucket.png)

**Bronze Layer**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/bronze.png)

---

### 2️⃣ Transform Phase – Silver Processing (AWS Glue)

✔ Partition-specific Bronze data read  
✔ Nested arrays flattened  
✔ Timestamps parsed  
✔ Data types standardized  
✔ Duplicate records removed  
✔ Data quality checks enforced  

**Output:**

→ Cleaned Parquet datasets written to Silver layer

**Silver Layer**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/silver.png)

---

### 3️⃣ Load Phase – Gold Aggregation (AWS Glue)

✔ Silver datasets aggregated into daily metrics  
✔ Business-friendly schema produced  
✔ Dataset optimized for analytics  

**Output:**

→ Analytics-ready Parquet datasets written to Gold layer

**Gold Layer**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/gold.png)

---

### 4️⃣ Analytics Phase – Query & BI Consumption

✔ Glue Crawler infers schema  
✔ Glue Data Catalog stores metadata  
✔ Athena executes SQL queries  
✔ Power BI consumes Gold datasets  

**Purpose:**

→ Business intelligence & reporting

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

## ⚙ Extraction Layer (Python)

### Responsibilities

✔ Call Open-Meteo API using `requests`  
✔ Fetch previous day’s data dynamically  
✔ Preserve raw JSON response  
✔ Upload directly to S3 Bronze layer using `boto3`

All source Python scripts are stored in the **same configured DAG location inside the Dockerized Airflow environment**, ensuring consistent execution and simplified orchestration.

---

### Libraries Used

**Requests Module**

Used for external REST API communication.

**Boto3 (AWS SDK)**

Used for:

✔ S3 PutObject operations  
✔ IAM-based authentication  
✔ Secure AWS integration  

No AWS keys are hardcoded.

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

### Purpose

✔ Immutable raw storage  
✔ Replay capability  
✔ Debugging & auditing  
✔ Schema recovery  

No transformations applied.

---

## 🔄 Silver Layer – Transformation & Validation Zone

**Processing Engine:** AWS Glue (PySpark)

### Responsibilities

✔ Read partition-specific Bronze JSON  
✔ Flatten nested hourly arrays  
✔ Parse timestamps  
✔ Cast numeric fields  
✔ Remove duplicates  
✔ Apply data quality checks  

---

### Output Format

✔ Parquet (columnar, optimized)

**Partitioning:**

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

### 🚦 Data Quality Enforcement Strategy

Silver layer transformations implement a **fail-fast validation model**:

✔ Null checks  
✔ Domain range checks  
✔ Duplicate detection  

Invalid records trigger controlled job failure to prevent downstream corruption.

**Future Enhancement – Quarantine Pattern**

✔ Redirect invalid rows to `silver/quarantine/`  
✔ Enable root-cause analysis  
✔ Preserve pipeline continuity  

**Glue Jobs on AWS**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/glue%20jobs%20list.png)

**Silver Job Success**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/silver%20job%20sucess.png)

---

## 📊 Gold Layer – Analytics Zone

**Processing Engine:** AWS Glue (Aggregation Job)

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

**Gold Job Success**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/gold%20job%20sucess.png)

---

### Purpose

✔ BI-ready datasets  
✔ Reduced scan cost  
✔ Faster Athena queries  

---

## 🔁 Incremental Processing Strategy

The pipeline follows a **partition-level incremental model**.

✔ Process only `process_date`  
✔ Overwrite only that partition  
✔ Idempotent reruns  
✔ Prevent duplicate records  

**Mechanism:**

```python
.mode("overwrite")
.option("replaceWhere", "date = 'YYYY-MM-DD'")
```

---

## ⛓ Orchestration Layer – Apache Airflow

Airflow acts as the **central control plane** of the pipeline, coordinating task execution and ensuring reliable workflow management.

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/airflow...dag.png)

### Airflow Responsibilities

✔ Schedule pipeline runs  
✔ Manage task dependencies  
✔ Trigger AWS Glue Jobs  
✔ Trigger Glue Crawler  
✔ Retry & failure handling  

**Gantt Chart**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/Airflow%20gantt%20chart.png)

**Airflow Graph**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/Airflow%20graph.png)

**Airflow DAG**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/airflow.png)

---

### Dockerized Airflow Environment

✔ Local reproducible setup  
✔ Containerized execution  
✔ Cloud orchestration simulation  

---

## 🧾 Metadata & Schema Management

✔ Glue Crawler infers schema  
✔ Glue Data Catalog stores tables  

**Glue Crawler**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/crawler.png)

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/crawler%20sucess.png)

---

## 🔍 Querying Gold Data with Amazon Athena

Amazon Athena enables **SQL querying directly on S3 Parquet datasets**.

### Why Athena?

✔ No infrastructure management  
✔ Pay-per-query pricing  
✔ Glue Catalog integration  

**Athena Querying**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/athena%20.png)

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/athena%20tables.png)

---

### Example Query

```sql
SELECT city, avg_temperature
FROM gold_weather
WHERE date = DATE '2026-02-16';
```

---

### Common Analytics Queries

```sql
-- Hottest city
SELECT city, max_temperature
FROM gold_weather
ORDER BY max_temperature DESC
LIMIT 1;

-- Daily trend
SELECT date, avg_temperature
FROM gold_weather
WHERE city = 'Delhi'
ORDER BY date;
```

---

## 📈 BI / Visualization Layer – Power BI

✔ Athena used as SQL backend  
✔ Gold datasets optimized for dashboards  

Planned visuals:

✔ Temperature trends  
✔ City comparisons  
✔ KPI metrics  

NOTE: BI dashboard upload pending — will be completed soon.

---

**VS Code Project Structure**

![Project Overview](https://github.com/rohitsingh889/--weather-lake-pipeline/blob/main/PICS/vs%20code%20....png)

---

## 💰 Cost Optimization Strategy

✔ Parquet columnar format  
✔ Partitioned datasets  
✔ Reduced Athena scan costs  

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
https://www.linkedin.com/in/rohit-raj-singh-3030172a4

---
