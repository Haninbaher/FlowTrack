# FlowTrack: Supply Chain Monitoring Pipeline

## Overview
FlowTrack is an end-to-end data pipeline project for monitoring supply chain shipment activity using both batch and streaming workflows.

The project combines static reference data such as warehouses, routes, carriers, and products with real-time shipment event streams to generate analytics-ready tables for reporting and operational monitoring.

## Project Goal
The goal of this project is to build a structured and scalable data pipeline that can:

- ingest static supply chain reference data
- process real-time shipment events
- transform raw data into analytics-ready models
- orchestrate workflows in a clean way
- support visualization for shipment delay and performance KPIs

## Architecture Summary
The pipeline consists of two main flows:

### 1. Static Data Flow
CSV files → PostgreSQL → Spark Batch → Hive

### 2. Real-Time Data Flow
Python Generator → Kafka → Spark Structured Streaming → Hive

### 3. Modeling Layer
Hive → dbt → Analytics Models

### 4. Orchestration Layer
Airflow → schedule and manage pipeline jobs

### 5. Visualization Layer
Power BI or Superset → dashboards and KPIs

## Tools Used

| Tool | Role in the Project |
|------|----------------------|
| PostgreSQL | stores static reference data such as warehouses, routes, carriers, and products |
| Python | generates simulated real-time shipment events |
| Kafka | acts as the message broker for shipment event streaming |
| Spark Batch | ingests and transforms static data from PostgreSQL into Hive |
| Spark Structured Streaming | processes real-time shipment events from Kafka |
| Hive | stores raw and processed tables for analytics |
| dbt | builds staging, intermediate, and mart models on top of Hive |
| Airflow | orchestrates and schedules pipeline workflows |
| Power BI / Superset | visualizes shipment KPIs and operational metrics |
| Docker Compose | runs the project services in a reproducible environment |

## Initial Data Domains
The project will use two categories of data:

### Static Data
- warehouses
- routes
- carriers
- products

### Real-Time Data
- shipment_created
- in_transit
- arrived_warehouse
- delayed
- delivered

## Expected Outputs
The final pipeline will produce analytics tables and dashboards for:

- delayed shipment percentage
- route performance
- warehouse performance
- carrier performance
- delivery trend over time

## Project Status
This project is being built step by step.  
Each completed phase will be documented and added to this README.
