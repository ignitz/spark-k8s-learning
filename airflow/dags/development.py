from datetime import datetime, timedelta
from airflow import DAG
from custom_operator import SparkOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'Development',
    default_args={'max_active_runs': 1},
    description='Spark Submit with sleep 10000 minutes to allow to test and development inside the pod',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    start = DummyOperator(task_id='start', dag=dag)

    tk = SparkOperator(
                task_id='spark_development',
                main_application_file='s3://spark-artifacts/pyspark/develop/development.py',
                pyFiles=['s3://spark-artifacts/lib/jibaro.zip'],
                dag=dag,
            )
        
    tk.set_upstream(start)
        
    