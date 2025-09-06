from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

dag = DAG(
    dag_id='enrich_user_profiles',
    default_args=default_args,
    start_date=datetime(2025, 6, 21),
    schedule_interval=None,  # ручний запуск
    catchup=False,
)

enrich_task = BashOperator(
    task_id='run_enrich_user_profiles',
    bash_command='/opt/spark/bin/spark-submit /opt/airflow/dags/python_scripts/enrich_user_profiles.py',
    dag=dag,
)