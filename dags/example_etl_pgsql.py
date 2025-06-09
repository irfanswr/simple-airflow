from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='source_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, created_at FROM users WHERE created_at >= current_date - interval '1 day'")
    rows = cursor.fetchall()
    # Kirim data ke task berikutnya pakai XCom
    kwargs['ti'].xcom_push(key='extracted_data', value=rows)

def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='extracted_data', task_ids='extract')
    # Contoh transformasi: filter data, convert tanggal, dll.
    transformed = [(row[0], row[1].upper(), row[2].strftime('%Y-%m-%d')) for row in data]
    ti.xcom_push(key='transformed_data', value=transformed)

def load(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='transformed_data', task_ids='transform')
    pg_hook = PostgresHook(postgres_conn_id='target_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    for row in data:
        cursor.execute(
            "INSERT INTO warehouse_users (id, name, created_date) VALUES (%s, %s, %s) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, created_date = EXCLUDED.created_date",
            row
        )
    conn.commit()

with DAG(
    dag_id='example_etl_pgsql',
    default_args=default_args,
    description='ETL data user dari source ke warehouse',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True,
    )

    t1 >> t2 >> t3
