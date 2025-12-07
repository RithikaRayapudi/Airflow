"""
Daily ShopVerse Pipeline 
"""

from datetime import datetime, timedelta
import os
import json
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


# DAG DEFAULTS
default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="shopverse_daily_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 1 * * *",  # run daily at 01:00 UTC
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["shopverse", "dwh"],
)
def shopverse_daily_pipeline():

    # VARIABLES & CONNECTIONS
    base_path = Variable.get("shopverse_data_base_path", "/opt/airflow/data")
    min_order_threshold = int(
        Variable.get("shopverse_min_order_threshold", default_var="10")
    )
    postgres_conn_id = "postgres_dwh"

    # Helper to build filepaths
    def _paths(ds_nodash):
        c = f"{base_path}/landing/customers/customers_{ds_nodash}.csv"
        p = f"{base_path}/landing/products/products_{ds_nodash}.csv"
        o = f"{base_path}/landing/orders/orders_{ds_nodash}.json"
        return c, p, o

    # START / END
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # WAIT FOR FILES
    wait_customers = FileSensor(
        task_id="wait_for_customers_file",
        filepath=f"{base_path}/landing/customers/customers_{{{{ ds_nodash }}}}.csv",
        poke_interval=10,
        timeout=3600,
    )

    wait_products = FileSensor(
        task_id="wait_for_products_file",
        filepath=f"{base_path}/landing/products/products_{{{{ ds_nodash }}}}.csv",
        poke_interval=10,
        timeout=3600,
    )

    wait_orders = FileSensor(
        task_id="wait_for_orders_file",
        filepath=f"{base_path}/landing/orders/orders_{{{{ ds_nodash }}}}.json",
        poke_interval=10,
        timeout=3600,
    )

    # STAGING LAYER
    with TaskGroup(group_id="staging") as staging:

        # Truncate each staging table
        truncate_customers = PostgresOperator(
            task_id="truncate_stg_customers",
            postgres_conn_id=postgres_conn_id,
            sql="TRUNCATE TABLE stg_customers;",
        )

        truncate_products = PostgresOperator(
            task_id="truncate_stg_products",
            postgres_conn_id=postgres_conn_id,
            sql="TRUNCATE TABLE stg_products;",
        )

        truncate_orders = PostgresOperator(
            task_id="truncate_stg_orders",
            postgres_conn_id=postgres_conn_id,
            sql="TRUNCATE TABLE stg_orders;",
        )

        # Load customers
        @task(task_id="load_stg_customers")
        def load_stg_customers(ds):
            ds_nodash = ds.replace("-", "")
            customers_file, _, _ = _paths(ds_nodash)

            df = pd.read_csv(customers_file)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            engine = hook.get_sqlalchemy_engine()
            df.to_sql("stg_customers", engine, if_exists="append", index=False)

        # Load products
        @task(task_id="load_stg_products")
        def load_stg_products(ds):
            ds_nodash = ds.replace("-", "")
            _, products_file, _ = _paths(ds_nodash)

            df = pd.read_csv(products_file)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            engine = hook.get_sqlalchemy_engine()
            df.to_sql("stg_products", engine, if_exists="append", index=False)

        # Load orders
        @task(task_id="load_stg_orders")
        def load_stg_orders(ds):
            ds_nodash = ds.replace("-", "")
            _, _, orders_file = _paths(ds_nodash)

            with open(orders_file) as f:
                raw = f.read().strip()
                if raw.startswith("["):
                    data = json.loads(raw)
                else:
                    data = [json.loads(line) for line in raw.splitlines()]

            df = pd.DataFrame(data)
            hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            engine = hook.get_sqlalchemy_engine()
            df.to_sql("stg_orders", engine, if_exists="append", index=False)

        # Wiring inside staging
        truncate_customers >> load_stg_customers()
        truncate_products >> load_stg_products()
        truncate_orders >> load_stg_orders()

    # TRANSFORM (WAREHOUSE)
    with TaskGroup(group_id="warehouse") as warehouse:

        dim_customers = PostgresOperator(
            task_id="build_dim_customers",
            postgres_conn_id=postgres_conn_id,
            sql="""
                INSERT INTO dim_customers (customer_id, first_name, last_name, email, signup_date, country)
                SELECT DISTINCT
                    customer_id, first_name, last_name, email,
                    signup_date::timestamp AT TIME ZONE 'UTC', country
                FROM stg_customers
                ON CONFLICT (customer_id) DO UPDATE
                    SET first_name = EXCLUDED.first_name,
                        last_name  = EXCLUDED.last_name,
                        email      = EXCLUDED.email,
                        signup_date = EXCLUDED.signup_date,
                        country    = EXCLUDED.country;
            """,
        )

        dim_products = PostgresOperator(
            task_id="build_dim_products",
            postgres_conn_id=postgres_conn_id,
            sql="""
                INSERT INTO dim_products (product_id, product_name, category, unit_price)
                SELECT DISTINCT
                    product_id, product_name, category, unit_price
                FROM stg_products
                ON CONFLICT (product_id) DO UPDATE
                    SET product_name = EXCLUDED.product_name,
                        category     = EXCLUDED.category,
                        unit_price   = EXCLUDED.unit_price;
            """,
        )

        fact_orders = PostgresOperator(
            task_id="build_fact_orders",
            postgres_conn_id=postgres_conn_id,
            sql="""
                INSERT INTO fact_orders (
                    order_id, order_timestamp_utc, order_date,
                    customer_id, product_id, quantity, total_amount,
                    currency, currency_mismatch_flag, status, load_timestamp
                )
                SELECT
                    order_id,
                    (order_timestamp::timestamp AT TIME ZONE 'UTC'),
                    DATE(order_timestamp::timestamp AT TIME ZONE 'UTC'),
                    customer_id, product_id, quantity, total_amount,
                    currency,
                    CASE WHEN currency IS NULL OR currency='USD' THEN 0 ELSE 1 END,
                    status,
                    NOW()
                FROM stg_orders
                WHERE quantity > 0
                  AND total_amount >= 0
                  AND customer_id IS NOT NULL
                  AND product_id IS NOT NULL
                ON CONFLICT (order_id) DO UPDATE
                    SET quantity = EXCLUDED.quantity,
                        total_amount = EXCLUDED.total_amount,
                        status = EXCLUDED.status,
                        load_timestamp = EXCLUDED.load_timestamp;
            """,
        )

        dim_customers >> dim_products >> fact_orders

    # DATA QUALITY CHECKS
    @task(task_id="prepare_dq_checks")
    def prepare_dq_checks():
        return [
            {
                "name": "customers_not_empty",
                "sql": "SELECT COUNT(*) FROM dim_customers;",
                "min": 1,
            },
            {
                "name": "no_null_keys_in_fact",
                "sql": """SELECT COUNT(*) FROM fact_orders
                          WHERE customer_id IS NULL OR product_id IS NULL;""",
                "max": 0,
            },
        ]

    @task(task_id="run_single_dq_check")
    def run_single_dq_check(check):
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        value = hook.get_first(check["sql"])[0]

        if "min" in check and value < check["min"]:
            raise ValueError(f"DQ FAILED: {check['name']} -> {value}")
        if "max" in check and value > check["max"]:
            raise ValueError(f"DQ FAILED: {check['name']} -> {value}")

        return f"{check['name']} OK"

    dq_checks = prepare_dq_checks()
    dq_results = run_single_dq_check.expand(check=dq_checks)

    # COUNT ORDERS FOR BRANCHING
    @task(task_id="get_fact_orders_count")
    def get_fact_orders_count(ds):
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        sql = "SELECT COUNT(*) FROM fact_orders WHERE order_date=%s;"
        return hook.get_first(sql, parameters=[ds])[0]

    @task.branch(task_id="branch_on_volume")
    def branch_on_volume(order_count):
        if order_count < min_order_threshold:
            return "warn_low_volume.log_warning"
        return "normal_completion.success"

    # LOW VOLUME BRANCH
    with TaskGroup(group_id="warn_low_volume") as warn_low_volume:

        @task(task_id="log_warning")
        def log_warning(ds, ti):
            anomalies_dir = f"{base_path}/anomalies"
            os.makedirs(anomalies_dir, exist_ok=True)

            order_count = ti.xcom_pull(task_ids="get_fact_orders_count")

            payload = {
                "date": ds,
                "order_count": order_count,
                "threshold": min_order_threshold,
                "status": "LOW_VOLUME",
            }

            out = f"{anomalies_dir}/low_volume_{ds.replace('-', '')}.json"
            with open(out, "w") as f:
                json.dump(payload, f, indent=2)

        log_warning()

    # NORMAL COMPLETION
    with TaskGroup(group_id="normal_completion") as normal_completion:
        success = EmptyOperator(task_id="success")

    # DAG DEPENDENCIES
    start >> [wait_customers, wait_products, wait_orders] >> staging
    staging >> warehouse >> dq_results

    count_orders = get_fact_orders_count()
    branch = branch_on_volume(count_orders)

    dq_results >> count_orders >> branch

    warn_low_volume >> end
    normal_completion >> end


shopverse_daily_pipeline = shopverse_daily_pipeline()
