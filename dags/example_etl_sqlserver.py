from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

def extract_data_from_mssql(**context):
    hook = MsSqlHook(mssql_conn_id='mssql_default')  # Conn Id dari Airflow UI
    sql = "SELECT TOP 100 * FROM your_table_name"    # Ganti dengan nama tabel
    df = hook.get_pandas_df(sql)

    output_path = "/opt/airflow/dags/output/mssql_data.csv"
    df.to_csv(output_path, index=False)
    print(f"Data saved to {output_path}")

with DAG(
    dag_id='example_etl_sqlserver',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['mssql', 'extract'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_from_mssql
    )

    extract_task
