from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_hello_world():
    print("Hello, World!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='Print Hello, World! every 10 seconds',
    schedule_interval=timedelta(seconds=10),
)

print_task = PythonOperator(
    task_id='print_hello_world',
    python_callable=print_hello_world,
    dag=dag,
)
