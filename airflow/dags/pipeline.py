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
        # topic_list = [
        #     'dbserver1.inventory.customers',
        #     'dbserver1.inventory.geom',
        #     'dbserver1.inventory.orders',
        #     'dbserver1.inventory.products',
        #     'dbserver1.inventory.products_on_hand'
        # ]
        topic_list = [
            'dbserver1.inventory.products'
        ]

        for topic in topic_list:
            kafka_raw = SparkOperator(
                task_id='spark_raw_' + topic.replace('.', '_'),
                main_application_file='s3a://spark-artifacts/pyspark/develop/kafka_to_raw.py',
                arguments=[topic],
                pyFiles=['s3a://spark-artifacts/lib/jibaro.zip'],
                dag=dag,
            )

            raw_staged = SparkOperator(
                task_id='spark_staged_' + topic.replace('.', '_'),
                main_application_file='s3a://spark-artifacts/pyspark/develop/raw_to_staged.py',
                arguments=[topic, "avro"],
                pyFiles=['s3a://spark-artifacts/lib/jibaro.zip'],
                dag=dag,
            )

            staged_curated = SparkOperator(
                task_id='spark_curated_' + topic.replace('.', '_'),
                main_application_file='s3a://spark-artifacts/pyspark/develop/staged_to_curated.py',
                arguments=[topic],
                pyFiles=['s3a://spark-artifacts/lib/jibaro.zip'],
                dag=dag,
            )
            kafka_raw >> raw_staged
            raw_staged >> staged_curated

        
        kafka_group.set_upstream(start)
    