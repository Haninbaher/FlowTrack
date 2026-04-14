# 🚚 FlowTrack: Supply Chain Monitoring Pipeline

## 📌 Project Overview

FlowTrack is an end-to-end data pipeline designed to monitor and analyze supply chain operations using both batch and real-time data processing.

The system integrates static reference data (such as warehouses, routes, and carriers) with real-time shipment event streams to generate actionable insights about shipment performance, delivery delays, and operational efficiency.

This project simulates a real-world data platform used in logistics and supply chain companies.

---

## 🎯 Why This Project Matters

Supply chain systems rely heavily on real-time visibility and data-driven decisions. Delays, inefficiencies, or bottlenecks can significantly impact business performance.

This project demonstrates how modern data engineering tools can be combined to:

* Track shipments in real time
* Detect delivery delays early
* Analyze route and warehouse performance
* Build scalable data pipelines
* Combine batch and streaming processing in one system

💡 This makes the project highly relevant for real-world use cases in:

* Logistics companies
* E-commerce platforms
* Delivery services
* Operations analytics teams

---

## 🧠 Project Goals

The main objectives of this project are:

* Build a complete **data pipeline architecture**
* Integrate **batch + streaming data**
* Apply **data transformations using Spark and dbt**
* Orchestrate workflows using Airflow
* Store and query data efficiently using Hive
* Deliver insights through dashboards

---

## 🏗️ Architecture Overview

The pipeline consists of two main flows:

### 1. Static Data Pipeline (Batch)

CSV Files → PostgreSQL → Spark Batch → Hive

### 2. Real-Time Data Pipeline (Streaming)

Python Generator → Kafka → Spark Structured Streaming → Hive

### 3. Modeling Layer

Hive → dbt → Analytics Tables

### 4. Orchestration Layer

Airflow → Workflow Scheduling & Automation

### 5. Visualization Layer

Power BI / Superset → Dashboards & KPIs

---

## 🧰 Technology Stack & Roles

| Tool                           | Role in the Project                                                                  |
| ------------------------------ | ------------------------------------------------------------------------------------ |
| **PostgreSQL**                 | Stores static reference data such as warehouses, routes, carriers, and products      |
| **Python**                     | Generates simulated real-time shipment events                                        |
| **Kafka**                      | Handles real-time data streaming (shipment events pipeline)                          |
| **Spark Batch**                | Loads and transforms static data from PostgreSQL into Hive                           |
| **Spark Structured Streaming** | Processes real-time shipment events from Kafka                                       |
| **HDFS / Hive**                | Stores raw and processed data tables for querying and analytics                      |
| **dbt**                        | Transforms raw data into structured analytical models (staging, intermediate, marts) |
| **Airflow**                    | Orchestrates and schedules pipeline workflows                                        |
| **Power BI / Superset**        | Visualizes insights and KPIs                                                         |
| **Docker Compose**             | Runs the full data stack in a reproducible environment                               |

---

## 📂 Data Sources

### 🔹 Static Data (Batch)

* Warehouses
* Routes
* Carriers
* Products

### 🔹 Real-Time Data (Streaming)

Simulated shipment events:

* shipment_created
* in_transit
* arrived_warehouse
* delayed
* delivered

---

## 🔄 Data Processing Flow

### 🔸 Batch Flow

1. Load static CSV data into PostgreSQL
2. Spark Batch reads from PostgreSQL
3. Data is cleaned and transformed
4. Stored in Hive reference tables

### 🔸 Streaming Flow

1. Python script generates shipment events
2. Events are sent to Kafka
3. Spark Streaming consumes events
4. Data is processed and enriched
5. Stored in Hive event tables

---

## 🧱 Data Layers

### 🟤 Raw Layer

* raw_warehouses
* raw_routes
* raw_carriers
* raw_shipment_events

### ⚪ Staging Layer

* stg_warehouses
* stg_routes
* stg_shipment_events

### 🔵 Intermediate Layer

* int_route_metrics
* int_shipment_status

### 🟢 Mart Layer

* mart_delivery_performance
* mart_route_performance
* mart_warehouse_kpis

---

## 📊 Expected Insights & KPIs

The final dashboard will include:

* Total shipments
* Delayed shipments
* Delay percentage
* Average delivery time
* Top delayed routes
* Warehouse performance
* Carrier performance
* Shipment trends over time

---

## ⚙️ Orchestration (Airflow)

Airflow will manage:

* Batch ingestion jobs
* dbt model execution
* Data refresh schedules
* Pipeline monitoring

---

## 📈 Visualization

Dashboards will be built using:

* **Power BI** (recommended for simplicity)
  or
* **Apache Superset** (for a fully open-source stack)

---

## 🚀 Project Status

This project is being developed step by step.

### Phase 1: Project Initialization

* [x] Define project idea
* [x] Select tools and architecture
* [x] Write project documentation
* [ ] Create repository structure
* [ ] Add architecture diagram

---

## 📌 Future Improvements

* Add real-time alerting for delayed shipments
* Introduce ML-based delay prediction
* Add monitoring with Prometheus & Grafana
* Build API layer for live data access

---

## 👩‍💻 Author

Built as a hands-on data engineering project to demonstrate real-world pipeline design and implementation.
