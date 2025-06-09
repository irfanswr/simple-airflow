from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

def query_sql_server():
    hook = MsSqlHook(mssql_conn_id='mssql_edcl')
    sql = "SELECT TOP 10 * FROM TB_M_USER;"
    records = hook.get_records(sql)
    for row in records:
        print(row)

with DAG(
    dag_id='example_query_sqlserver',
    default_args=default_args,
    description='DAG to query SQL Server and log results',
    schedule=None,  # manual trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['mssql', 'example'],
) as dag:

    run_query = PythonOperator(
        task_id='run_sql_query',
        python_callable=query_sql_server
    )
