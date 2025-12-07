
#  **ShopVerse Daily Data Pipeline (Airflow ETL)**

A production-style Apache Airflow pipeline that loads daily e-commerce data into a warehouse, performs automated data-quality checks, and branches on business rules based on order volume.

---

##  **1. Setting Up Variables & Connections**

###  **Airflow Variables**

Navigate to: **Airflow UI → Admin → Variables → Create**

| **Variable Key**                | **Value**           | **Description**                     |
| ------------------------------- | ------------------- | ----------------------------------- |
| `shopverse_data_base_path`      | `/opt/airflow/data` | Root folder for input files         |
| `shopverse_min_order_threshold` | `10`                | Used for low-volume branching logic |

---

###  **Airflow PostgreSQL Connection**

Navigate to: **Airflow UI → Admin → Connections → Add Connection**

| **Field** | **Value**         |
| --------- | ----------------- |
| Conn Id   | `postgres_dwh`    |
| Conn Type | `Postgres`        |
| Host      | `postgres`        |
| Database  | `airflow`         |
| Login     | `airflow`         |
| Password  | *(your password)* |
| Port      | `5432`            |

This connection is used by staging loaders and warehouse SQL transformations.

---

##  **2. Placing Input Files**

Your files **must follow the folder structure** inside the Airflow container:

```
/opt/airflow/data/
│
└── landing/
    ├── customers/
    │     └── customers_YYYYMMDD.csv
    ├── products/
    │     └── products_YYYYMMDD.csv
    └── orders/
          └── orders_YYYYMMDD.json
```

### ✔ **Example for December 6, 2025**

```
customers_20251206.csv
products_20251206.csv
orders_20251206.json
```

Airflow FileSensors automatically detect the file using:

```
{{ ds_nodash }}
```

---

##  **3. Triggering the DAG & Backfilling Dates**

### **Trigger a run manually**

```
In Airflow UI → Trigger DAG
```

### **Trigger with a specific logical date**

```bash
airflow dags trigger shopverse_daily_pipeline \
  --exec-date 2025-12-06T01:00:00
```

### **Backfill multiple dates**

```bash
airflow dags backfill shopverse_daily_pipeline \
  -s 2025-12-01 -e 2025-12-07
```

For each backfilled date, Airflow expects:

```
customers_YYYYMMDD.csv
products_YYYYMMDD.csv
orders_YYYYMMDD.json
```

---

##  **4. Data Quality Checks (DQ)**

The pipeline uses **dynamic task mapping** to automatically execute SQL-based validation checks.

---

### ✔ **Check 1 — dim_customers_not_empty**

Ensures the customer dimension contains data.

```sql
SELECT COUNT(*) FROM dim_customers;
```

---

### ✔ **Check 2 — no_null_customer_or_product_in_fact_orders**

Ensures there is **no referential integrity break**.

```sql
SELECT COUNT(*)
FROM fact_orders
WHERE customer_id IS NULL OR product_id IS NULL;
```

---

### ✔ **Check 3 — fact_orders_matches_valid_orders_for_day**

Verifies fact table entries match cleaned staging records.
If mismatch → ❌ **DAG fails immediately.**

---

###  **Low Volume Branching Logic**

If **orders < threshold (default: 10)**, Airflow routes to:
`warn_low_volume`

A file is written to:

```
/opt/airflow/data/anomalies/low_volume_YYYYMMDD.json
```

Otherwise → pipeline continues normally.

---

##  **Repository Structure**

```
├── dags/
│   ├── shopverse_daily_pipeline.py
│   └── init_shopverse_schema.py
│
├── sql/
│   └── schema_shopverse_dwh.sql
│
└── README.md
```

