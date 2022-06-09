#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example DAG which uses SparkKubernetesOperator and SparkKubernetesSensor.
In this example, we create two tasks which execute sequentially.
The first task is to submit sparkApplication on Kubernetes cluster(the example uses spark-pi application).
and the second task is to check the final state of the sparkApplication that submitted in the first state.
Spark-on-k8s operator is required to be already installed on Kubernetes
https://github.com/GoogleCloudPlatform/spark-on-k8s-operator
"""

from datetime import datetime, timedelta
from airflow import DAG
from custom_operator import SparkOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'SparkOperator',
    default_args={'max_active_runs': 1},
    description='Example with SparkOperator a custom operator to submit sparkApplication on kubernetes',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    from airflow.utils.task_group import TaskGroup

    start = DummyOperator(task_id='start', dag=dag)

    with TaskGroup(group_id='Kafka') as kafka_group:
        topic_list = [
            'dbserver1.inventory.customers',
            'dbserver1.inventory.geom',
            'dbserver1.inventory.orders',
            'dbserver1.inventory.products',
            'dbserver1.inventory.products_on_hand'
        ]

        for topic in topic_list:
            tk = SparkOperator(
                task_id='spark_kafka_' + topic.replace('.', '_'),
                application_name=topic.replace('.', '-').replace('_', '-'),
                main_application_file='s3a://spark-artifacts/pyspark/example_jibaro.py',
                arguments=[topic],
                pyFiles=['s3a://spark-artifacts/lib/jibaro.zip'],
                dag=dag,
            )
        
        kafka_group.set_upstream(start)
    