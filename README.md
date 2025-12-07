# ShopVerse Daily Pipeline – Apache Airflow

## 1. Overview

This project implements an **end-to-end daily batch data pipeline** for the
fictitious e-commerce company **ShopVerse** using **Apache Airflow 2+** and **PostgreSQL**.

The pipeline:

- Ingests **customers (CSV)**, **products (CSV)** and **orders (JSON)** files
- Loads them into **staging tables**
- Builds **dim_customers**, **dim_products**, and **fact_orders** with basic transformations
- Performs **data quality checks**
- Uses **branching** to detect low-volume order days and write anomaly summaries

---

## 2. Prerequisites

- Airflow 2.x running (e.g., via Docker)
- PostgreSQL with database: `dwh_shopverse`
- Airflow has access to filesystem path (e.g. `/opt/airflow/data`)

---

## 3. Airflow Variables & Connections

### 3.1 Variables

Create the following in **Admin → Variables**:

1. `shopverse_data_base_path`  
   - Example value: `/opt/airflow/data`

2. `shopverse_min_order_threshold`  
   - Example value: `10`  
   - Used for branching (low volume vs normal).

### 3.2 Connections

Create a connection in **Admin → Connections**:

- **Conn Id**: `postgres_dwh`  
- **Conn Type**: `Postgres`  
- **Host**: `<your_postgres_host or service name>`  
- **Schema**: `dwh_shopverse`  
- **Login / Password**: your DB credentials  
- **Port**: `5432` (or your port)

No credentials are hard-coded in the DAG.

---

## 4. Database Setup

1. Connect to `dwh_shopverse` in PostgreSQL.
2. Run `schema_shopverse_dwh.sql`:

```bash
psql -h <host> -U <user> -d dwh_shopverse -f schema_shopverse_dwh.sql
