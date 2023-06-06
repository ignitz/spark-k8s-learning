from datetime import datetime, timedelta
from airflow import DAG
from pendulum import today
from custom_operator import SparkOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'Pipeline',
    default_args={'max_active_runs': 1},
    description='Example with SparkOperator a custom operator to submit sparkApplication on kubernetes',
    schedule_interval=None,
    start_date=today(),
    catchup=False,
) as dag:
    from airflow.utils.task_group import TaskGroup

    start = DummyOperator(task_id='start', dag=dag)

    with TaskGroup(group_id='Kafka-Curated') as kafka_group:
        source_list = [
            # {
            #     "project_name": "dbserver1",
            #     "database": "inventory",
            #     "table_name": "customers",
            # },
            # {
            #     "project_name": "dbserver1",
            #     "database": "inventory",
            #     "table_name": "geom",
            # },
            # {
            #     "project_name": "dbserver1",
            #     "database": "inventory",
            #     "table_name": "orders",
            # },
            # {
            #     "project_name": "dbserver1",
            #     "database": "inventory",
            #     "table_name": "products",
            # },
            # {
            #     "project_name": "dbserver1",
            #     "database": "inventory",
            #     "table_name": "products_on_hand"
            # },
            {
                "project_name": "dbserver1",
                "database": "inventory",
                "table_name": "products",
            },
        ]

        for source in source_list:
            kafka_raw = SparkOperator(
                task_id='spark_raw_' +
                f"{source['project_name']}_{source['database']}_{source['table_name']}",
                main_application_file='s3://spark-artifacts/pyspark/develop/kafka_to_raw.py',
                arguments=[
                    f"{source['project_name']}.{source['database']}.{source['table_name']}"],
                pyFiles=['s3://spark-artifacts/lib/jibaro.zip'],
                dag=dag,
            )

            raw_staged = SparkOperator(
                task_id='spark_staged_' +
                f"{source['project_name']}_{source['database']}_{source['table_name']}",
                main_application_file='s3://spark-artifacts/pyspark/develop/raw_to_staged.py',
                arguments=[source['project_name'],
                           source['database'], source['table_name'], "avro"],
                pyFiles=['s3://spark-artifacts/lib/jibaro.zip'],
                dag=dag,
            )

            staged_curated = SparkOperator(
                task_id='spark_curated_' +
                f"{source['project_name']}_{source['database']}_{source['table_name']}",
                main_application_file='s3://spark-artifacts/pyspark/develop/staged_to_curated.py',
                arguments=[source['project_name'],
                           source['database'], source['table_name']],
                pyFiles=['s3://spark-artifacts/lib/jibaro.zip'],
                dag=dag,
            )
            kafka_raw >> raw_staged
            raw_staged >> staged_curated
            staged_curated

        kafka_group.set_upstream(start)
