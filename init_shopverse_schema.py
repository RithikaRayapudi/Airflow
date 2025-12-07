from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# This DAG runs once and creates all ShopVerse tables in the Postgres DB
# that the "postgres_dwh" connection points to airflow DB.

with DAG(
    dag_id="init_shopverse_schema",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,     # run manually only
    catchup=False,
    template_searchpath=["/opt/airflow/dags/sql"],  # folder that has schema_shopverse_dwh.sql
    tags=["setup", "shopverse"],
) as dag:

    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_dwh",   
        sql="schema_shopverse_dwh.sql",    # filename only
    )
