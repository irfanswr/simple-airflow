from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

def task_hello():
    print("Hello from Airflow!")

def task_step_two():
    print("This is the second step.")

def task_final():
    print("Workflow is complete!")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='example_dag',
    default_args=default_args,
    description='Contoh DAG sederhana',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = PythonOperator(
        task_id='say_hello',
        python_callable=task_hello
    )

    t2 = PythonOperator(
        task_id='step_two',
        python_callable=task_step_two
    )

    t3 = PythonOperator(
        task_id='final_step',
        python_callable=task_final
    )

    t1 >> t2 >> t3