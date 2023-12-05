# my_multi_script_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dashboard_script.daily_script.get_data_fhir_update import get_fhir_data_update
from dashboard_script.daily_script.register_summary_update_2 import register_data_update
from dashboard_script.daily_script.add_recordedDate import add_recordedDate
from dashboard_script.daily_script.encounter_summary_2_update import get_encounter2_update
from dashboard_script.daily_script.user_summary_update import get_user_data
from dashboard_script.daily_script.rekod_saya_summary_update import rekod_summary_update
from dashboard_script.vc.appointment_vc_2_summary import get_appointment_vc_data
from dashboard_script.vc.encounter_vc_2_summary import get_encounter_vc_data
from dashboard_script.daily_script.vaccine_defaulted_update import get_vaccine_defaulted_update
from dashboard_script.daily_script.vaccine_administered_update import get_vaccine_administered_update
from dashboard_script.daily_script.sub_record_update import get_sub_record_update
from dashboard_script.daily_script.consent_reject_update import get_consent_reject_update
from dashboard_script.daily_script.consent_reject_update_2 import get_consent_reject_update_2







default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dashboard_daily_script_dag',
    default_args=default_args,
    description='A DAG with multiple tasks for different scripts',
    schedule_interval="01 0 * * *",
)

task_script1 = PythonOperator(
    task_id='get_fhir_data',
    python_callable=get_fhir_data_update,
    provide_context=True,
    dag=dag,
)

task_script2 = PythonOperator(
    task_id='add_recorded_date',
    python_callable=add_recordedDate,
    provide_context=True,
    dag=dag,
)

task_script3 = PythonOperator(
    task_id='register_script',
    python_callable=register_data_update,
    provide_context=True,
    dag=dag,
)

task_script4 = PythonOperator(
    task_id='encounter_script',
    python_callable=get_encounter2_update,
    provide_context=True,
    dag=dag,
)

task_script5 = PythonOperator(
    task_id='user_script',
    python_callable=get_user_data,
    provide_context=True,
    dag=dag,
)

task_script6 = PythonOperator(
    task_id='rekod_saya_script',
    python_callable=rekod_summary_update,
    provide_context=True,
    dag=dag,
)

task_script7 = PythonOperator(
    task_id='vc_appointment_script',
    python_callable=get_appointment_vc_data,
    provide_context=True,
    dag=dag,
)

task_script8 = PythonOperator(
    task_id='vc_encounter_script',
    python_callable=get_encounter_vc_data,
    provide_context=True,
    dag=dag,
)

task_script9 = PythonOperator(
    task_id='vaccine_defaulted_script',
    python_callable=get_vaccine_defaulted_update,
    provide_context=True,
    dag=dag,
)

task_script10 = PythonOperator(
    task_id='vaccine_administered_script',
    python_callable=get_vaccine_administered_update,
    provide_context=True,
    dag=dag,
)

task_script11 = PythonOperator(
    task_id='sub_record_script',
    python_callable=get_sub_record_update,
    provide_context=True,
    dag=dag,
)

task_script12 = PythonOperator(
    task_id='consent_reject1_script',
    python_callable=get_consent_reject_update,
    provide_context=True,
    dag=dag,
)

task_script13 = PythonOperator(
    task_id='consent_reject2_script',
    python_callable=get_consent_reject_update_2,
    provide_context=True,
    dag=dag,
)






# task order
task_script1 >> task_script2 >> task_script3
task_script1 >> [task_script4,task_script5,task_script6,task_script7,task_script8,task_script9,task_script10,task_script11,task_script12,task_script13]
