from datetime import datetime, timedelta
from airflow import DAG
from pendulum import today
from custom_operator import SparkOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'AirflowDatalake',
    default_args={'max_active_runs': 1},
    description='Example with SparkOperator a custom operator to submit sparkApplication on kubernetes',
    schedule_interval=None,
    start_date=today(),
    catchup=False,
) as dag:
    from airflow.utils.task_group import TaskGroup

    start = DummyOperator(task_id='start', dag=dag)

    with TaskGroup(group_id='Kafka-Curated') as kafka_group:
        topic_list = [
            "airflow.public.ab_permission",
            "airflow.public.ab_permission_view",
            "airflow.public.ab_permission_view_role",
            "airflow.public.ab_role",
            "airflow.public.ab_user",
            "airflow.public.ab_user_role",
            "airflow.public.ab_view_menu",
            "airflow.public.alembic_version",
            "airflow.public.connection",
            "airflow.public.dag",
            "airflow.public.dag_code",
            "airflow.public.dag_run",
            "airflow.public.job",
            "airflow.public.log",
            "airflow.public.log_template",
            "airflow.public.rendered_task_instance_fields",
            "airflow.public.serialized_dag",
            "airflow.public.session",
            "airflow.public.slot_pool",
            "airflow.public.task_fail",
            "airflow.public.task_instance",
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
                arguments=[topic],
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
            kafka_raw >> raw_staged >> staged_curated

        
        kafka_group.set_upstream(start)
    