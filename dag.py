from datetime import datetime
import pandas as pd
import polars as pl
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Fungsi-fungsi untuk memasukkan data ke PostgreSQL (seperti yang sudah Anda definisikan)

def ingest_data_coupons_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    pd.read_json("data/coupons.json").to_sql("coupons", engine, if_exists="replace", index=False)

def ingest_data_customer_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    file_paths = [f"data/customer_{i}.csv" for i in range(10)]
    for file_path in file_paths:
        df = pd.read_csv(file_path)
        df.to_sql("customer", engine, if_exists="append", index=False)

def ingest_data_login_attempts_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    file_paths = [f"data/login_attempts_{i}.json" for i in range(10)]
    for file_path in file_paths:
        df = pd.read_json(file_path)
        df.to_sql("login_attempts", engine, if_exists="append", index=False)

def ingest_data_order_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    pd.read_parquet("data/order.parquet").to_sql("order", engine, if_exists="replace", index=False)

def ingest_data_order_item_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    df = pl.read_avro("data/order_item.avro")
    df_pandas = df.to_pandas()
    df_pandas.to_sql("order_item", engine, if_exists="replace", index=False)

def ingest_data_product_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    pd.read_excel("data/product.xls").to_sql("product", engine, if_exists="replace", index=False)

def ingest_data_product_category_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    pd.read_excel("data/product_category.xls").to_sql("product_category", engine, if_exists="replace", index=False)

def ingest_data_supplier_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    pd.read_excel("data/supplier.xls").to_sql("supplier", engine, if_exists="replace", index=False)

# Definisikan default_args untuk DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Definisikan DAG
dag = DAG(
    "generate_and_ingest_activity_data",
    default_args=default_args,
    description="Generate random activity data and ingest into PostgreSQL",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Definisikan tugas-tugas
t1 = PythonOperator(
    task_id="ingest_data_coupons_to_postgres",
    python_callable=ingest_data_coupons_to_postgres,
    dag=dag,
)

t2 = PythonOperator(
    task_id="ingest_data_customer_to_postgres",
    python_callable=ingest_data_customer_to_postgres,
    dag=dag,
)

t3 = PythonOperator(
    task_id="ingest_data_login_attempts_to_postgres",
    python_callable=ingest_data_login_attempts_to_postgres,
    dag=dag,
)

t4 = PythonOperator(
    task_id="ingest_data_order_to_postgres",
    python_callable=ingest_data_order_to_postgres,
    dag=dag,
)

t5 = PythonOperator(
    task_id="ingest_data_order_item_to_postgres",
    python_callable=ingest_data_order_item_to_postgres,
    dag=dag,
)

t6 = PythonOperator(
    task_id="ingest_data_product_to_postgres",
    python_callable=ingest_data_product_to_postgres,
    dag=dag,
)

t7 = PythonOperator(
    task_id="ingest_data_product_category_to_postgres",
    python_callable=ingest_data_product_category_to_postgres,
    dag=dag,
)

t8 = PythonOperator(
    task_id="ingest_data_supplier_to_postgres",
    python_callable=ingest_data_supplier_to_postgres,
    dag=dag,
)

# Mengatur ketergantungan tugas
t5
