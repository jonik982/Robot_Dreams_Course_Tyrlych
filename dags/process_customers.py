from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='process_customers',
    default_args=default_args,
    start_date=datetime(2025, 6, 20),
    schedule_interval='@daily',
    catchup=False
) as dag:

    raw_to_bronze = BashOperator(
        task_id='raw_to_bronze_customers',
        bash_command='/opt/spark/bin/spark-submit /opt/airflow/dags/python_scripts/process_customers_raw_to_bronze.py'
    )

    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver_customers',
        bash_command='/opt/spark/bin/spark-submit /opt/airflow/dags/python_scripts/process_customers_bronze_to_silver.py'
    )

    raw_to_bronze >> bronze_to_silver