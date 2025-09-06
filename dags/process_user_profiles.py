from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

dag = DAG(
    dag_id='process_user_profiles',
    default_args=default_args,
    start_date=datetime(2025, 6, 20),
    schedule_interval=None,  # ручний запуск
    catchup=False,
)

process_user_profiles = BashOperator(
    task_id='run_process_user_profiles_to_silver',
    bash_command='/opt/spark/bin/spark-submit /opt/airflow/dags/python_scripts/process_user_profiles_to_silver.py',
    dag=dag,
)