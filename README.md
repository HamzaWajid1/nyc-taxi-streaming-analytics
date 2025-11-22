
# 📊 NYC Taxi Real-Time Streaming & Analytics Pipeline

This project builds a complete **end-to-end real-time data engineering pipeline** using the NYC Yellow Taxi dataset.
The system is developed **locally first** (Kafka + PySpark + Postgres + Power BI), then fully deployed on **AWS (Kinesis + S3 + Glue + RDS)**.

The goal is to simulate real-time taxi rides, process them using a modern data lake architecture (Bronze → Silver → Gold), and build analytics dashboards to extract meaningful insights.

---

## 🚀 Project Architecture (High-Level)

### **Local Pipeline (Phases 1–4)**

* **Python Data Generator** → simulates real-time taxi events
* **Kafka** → streaming event broker
* **PySpark Structured Streaming** → ingestion + raw → Bronze
* **PySpark ETL** → Bronze → Silver → Gold transformation
* **PostgreSQL** → analytics warehouse
* **Power BI** → visual analytics / dashboards

### **AWS Cloud Pipeline (Phase 5+)**

* **Kinesis Data Streams** → real-time ingestion
* **AWS Glue / PySpark** → ETL
* **S3** → Bronze/Silver/Gold data lake
* **AWS RDS (Postgres)** → analytics layer
* **Power BI / QuickSight** → dashboards
* **Step Functions / Airflow** → orchestration
* **CloudWatch** → monitoring

---

## 📁 Repository Structure

```
nyc-taxi-streaming-analytics/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── data/
│   ├── raw/        # original CSV files (not pushed to GitHub)
│   ├── samples/    # small subsets for quick experimentation
│
├── notebooks/
│   └── exploration.ipynb   # dataset understanding (Phase 0)
│
├── src/
│   ├── generator/          # real-time data generator (Phase 1)
│   ├── streaming/          # Kafka + Spark jobs (Phase 1)
│   ├── etl/                # Bronze → Silver → Gold ETL (Phase 2)
│   ├── warehouse/          # Postgres loaders (Phase 3)
│   ├── dashboard/          # Power BI queries, notes (Phase 4)
│
└── docker/
    └── kafka-compose.yml   # Kafka + Zookeeper setup (Phase 1)
```

---

## 🟦 Project Phases

### **🔵 Phase 0 — Environment Setup & Dataset Exploration (Current Phase)**

* Install Python, Kafka, PySpark, PostgreSQL, Power BI
* Set up repo structure
* Download NYC Taxi raw dataset
* Create a sample subset for testing
* Build initial exploration notebook
* Understand schema, missing values, data ranges

✔ *This is the phase we are working on right now.*
✔ *No pipeline logic is written yet.*

---

### **🟢 Phase 1 — Real-Time Data Simulation (Local Kafka)**

* Python script generates taxi ride events
* Events published to Kafka topic: `taxi-events`
* PySpark Structured Streaming consumes Kafka events
* Raw JSON written to Bronze layer (local folder)

---

### **🟡 Phase 2 — ETL Pipeline (Bronze → Silver → Gold)**

* Data cleaning, validation, and enrichment
* Feature engineering: trip duration, borough mapping, tip %
* Aggregations for analytics dashboards

---

### **🟣 Phase 3 — Warehouse Loading (Postgres)**

* Load Gold-layer aggregates into warehouse
* SQL queries for KPIs
* Power BI-ready tables

---

### **🟠 Phase 4 — Analytics Dashboard (Power BI)**

* Peak hours, busiest zones
* Revenue metrics & fare analysis
* Tip behaviour patterns
* Route/zone heatmaps

---

### **🔴 Phase 5 — AWS Deployment**

* Replace Kafka → Kinesis
* Replace local PySpark → AWS Glue
* Store Bronze/Silver/Gold in S3
* Load aggregates into RDS
* Power BI connects to AWS

---

## 🎯 Final Deliverables

* Full **real-time** & **batch** data pipeline
* Proper **Bronze/Silver/Gold** architecture
* Complete **AWS migration**
* Professional **Power BI dashboard**
* Multiple **Medium articles** documenting each phase
* A strong **data engineering portfolio project**

---

## 📌 Current Status: *Phase 0*

Environment setup in progress:
✔ Repository created
✔ Folder structure created
✔ Requirements and gitignore ready
🚧 Dataset download and exploration notebook next

---

## 📬 Contact / Notes

This project is designed for portfolio-building and interview preparation for **Data Engineering** roles.
