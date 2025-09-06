from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='process_sales',
    default_args=default_args,
    start_date=datetime(2025, 6, 20),
    schedule_interval='@daily',
    catchup=False
)


run_spark_job_raw_to_bronze = BashOperator(
        task_id='run_process_sales_raw_to_bronze',
        bash_command='/opt/spark/bin/spark-submit /opt/airflow/dags/python_scripts/process_sales_raw_to_bronze.py',
        dag=dag
    )


run_spark_job_bronze_to_silver = BashOperator(
        task_id='run_process_sales_bronze_to_silver',
        bash_command='/opt/spark/bin/spark-submit /opt/airflow/dags/python_scripts/bronze_to_silver_sales.py',
        dag=dag
    )


run_spark_job_raw_to_bronze >> run_spark_job_bronze_to_silver
