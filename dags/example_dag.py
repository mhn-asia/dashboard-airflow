from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dashboard_script.test_dag_script.date_print import my_python_function

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'my_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

run_this = PythonOperator(
    task_id='my_task',
    python_callable=my_python_function,
    provide_context=True,  # This is important to pass the execution_date to the function
    dag=dag,
)