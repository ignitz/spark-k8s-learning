import os
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Any

from kubernetes import client

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
import time
import os
import random
import hashlib

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SparkOperator(BaseOperator):
    """
    Checks sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication
        or
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/user-guide.md

    :param application_name: spark Application resource name
    :param namespace: the kubernetes namespace where the sparkApplication reside in
    :param kubernetes_conn_id: The :ref:`kubernetes connection<howto/connection:kubernetes>`
        to Kubernetes cluster.
    :param attach_log: determines whether logs for driver pod should be appended to the sensor log
    :param api_group: kubernetes api group of sparkApplication
    :param api_version: kubernetes api version of sparkApplication
    # TODO: add remaining params
    """

    template_fields: Sequence[str] = ("application_name", "namespace")
    ui_color = '#f4a460'

    FAILURE_STATES = ("FAILED", "UNKNOWN")
    SUCCESS_STATES = ("COMPLETED",)

    def __init__(
        self,
        *,
        arguments: List[str] = [],
        attach_log: bool = False,
        namespace: str = "spark",
        kubernetes_conn_id: str = "kubernetes_default",
        api_group: str = 'sparkoperator.k8s.io',
        api_version: str = 'v1beta2',
        image: str = 'spark3:latest',
        main_application_file: str = 's3a://spark-artifacts/pyspark/pokeapi.py',
        packages: List[str] = [],
        jars: List[str] = [],
        pyFiles: List[str] = [],
        spark_version: str = "3.1.1",
        service_account: str = "spark",
        driver: Optional[Dict[str, Any]] = None,
        executor: Optional[Dict[str, Any]] = None,
        num_executors: int = 1,
        dynamic_allocation: Dict[str, Any] = {'enabled': False},
        spark_confs: Dict[str, Any] = {},
        **kwargs,
    ) -> None:
        super().__init__(**kwargs, do_xcom_push=False, pool='spark')
        # Cannot be more than 63 chars
        sha1 = hashlib.sha1()
        sha1.update(str(self.task_id).encode('utf-8'))
        self.application_name = (
            self.dag.dag_id[:20].lower()
            .replace('.', '-').replace('_', '-').replace(' ', '')
            + '.' +
            sha1.hexdigest()
        )
        self.attach_log = attach_log
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        self.api_group = api_group
        self.api_version = api_version
        self.plural = "sparkapplications"
        self.arguments = arguments
        self.mainApplicationFile = main_application_file
        self.sparkVersion = spark_version
        self.image = image
        self.packages = packages
        self.jars = jars
        self.pyFiles = pyFiles

        self.restartPolicy = {
            "type": "Never"
        }

        if driver is None:
            self.driver = {
                "cores": 1,
                "coreLimit": "1200m",
                "memory": "2048m",
                "labels": {
                    "version": spark_version
                },
                "serviceAccount": service_account
            }
        else:
            self.driver = driver
        self.driver['labels']['dag_id'] = self.dag.dag_id
        self.driver['labels']['task_id'] = self.task_id[:63]

        if executor is None:
            self.executor = {
                "cores": 1,
                "coreLimit": "1200m",
                "memory": "1024m",
                "labels": {
                    "version": spark_version
                },
                "serviceAccount": service_account
            }
        else:
            self.executor = executor
        self.executor['labels']['dag_id'] = self.dag.dag_id
        self.executor['labels']['task_id'] = self.task_id[:63]

        self.executor["instances"] = num_executors

        # TODO: Validate format of dynamic allocation
        # dynamicAllocation:
        #   enabled: true
        #   initialExecutors: 1
        #   maxExecutors: 5
        #   minExecutors: 1
        self.dynamicAllocation = dynamic_allocation

        default_confs = {
            # S3a protocol
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "miniominio",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            # Delta Lake releted
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
            # Spark Streaming
            "spark.streaming.blockInterval": "200",
            "spark.streaming.receiver.writeAheadLog.enable": "true",
            "spark.streaming.backpressure.enabled": "true",
            "spark.streaming.backpressure.pid.minRate": "10",
            "spark.streaming.receiver.maxRate": "100",
            "spark.streaming.kafka.maxRatePerPartition": "100",
            "spark.streaming.backpressure.initialRate": "30",
            # Spark Histopry
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "s3a://spark-history/logs",
            # "spark.history.provider": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            # "spark.history.fs.logDirectory": "s3a://spark-history/logs",
        }
        self.sparkConf = {**default_confs, **spark_confs}


    def _log_driver(self, application_state: str, response: dict) -> None:
        if not self.attach_log:
            return
        status_info = response["status"]
        if "driverInfo" not in status_info:
            return
        driver_info = status_info["driverInfo"]
        if "podName" not in driver_info:
            return
        driver_pod_name = driver_info["podName"]
        namespace = response["metadata"]["namespace"]
        log_method = self.log.error if application_state in self.FAILURE_STATES else self.log.info
        try:
            log = ""
            for line in self.hook.get_pod_logs(driver_pod_name, namespace=namespace):
                log += line.decode()
            log_method(log)
        except client.rest.ApiException as e:
            self.log.warning(
                "Could not read logs for pod %s. It may have been disposed.\n"
                "Make sure timeToLiveSeconds is set on your SparkApplication spec.\n"
                "underlying exception: %s",
                driver_pod_name,
                e,
            )

    def _create_spark_application(self, context: 'Context') -> dict:
        self.log.info("Creating sparkApplication")
        body_dict = {
            'apiVersion': 'sparkoperator.k8s.io/v1beta2',
            'kind': 'SparkApplication',
            'metadata': {
                'name': self.application_name,
                'namespace': self.namespace
            }
        }

        body_dict['spec'] = {
            'type': 'Python', # TODO: Add support for .py and .jar
            'pythonVersion': '3',
            'mode': 'cluster',
            'image': self.image,
            'imagePullPolicy': 'IfNotPresent',
            'mainApplicationFile': self.mainApplicationFile,
            'arguments': self.arguments,
            'sparkVersion': self.sparkVersion,
            'restartPolicy': self.restartPolicy,
            'driver': self.driver,
            'executor': self.executor,
            'sparkConf': self.sparkConf,
            'deps': {
                'packages': self.packages,
                'jars': self.jars,
                'pyFiles': self.pyFiles,
            }
        }
        if self.dynamicAllocation is not None:
            body_dict['spec']['dynamicAllocation'] = self.dynamicAllocation
        # TODO: pass environment variables

        response = self.hook.create_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            body=body_dict,
            namespace=self.namespace,
        )
        return response

    def poke(self, context: 'Context') -> bool:
        self.log.info("Poking: %s", self.application_name)
        response = self.hook.get_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural="sparkapplications",
            name=self.application_name,
            namespace=self.namespace,
        )
        try:
            application_state = response["status"]["applicationState"]["state"]
        except KeyError:
            return False
        if self.attach_log and application_state in self.FAILURE_STATES + self.SUCCESS_STATES:
            self._log_driver(application_state, response)
        if application_state in self.FAILURE_STATES:
            raise AirflowException(
                f"Spark application failed with state: {application_state}")
        elif application_state in self.SUCCESS_STATES:
            self.log.info("Spark application ended successfully")
            return True
        else:
            self.log.info(
                "Spark application is still in state: %s", application_state)
            return False

    def execute(self, context: 'Context') -> None:
        self.log.info(context)
        self.log.info("Starting SparkApplication: %s", self.application_name)
        # TODO: Delete before send application
        response = self._create_spark_application(context)
        self.log.info("SparkApplication started: %s", self.application_name)
        self.log.info("SparkApplication response: %s", response)
        try:
            while not self.poke(context=context):
                # Sleep for 10 seconds for production environment
                # time.sleep(10.0 + random.uniform(-1.0, 10.0))
                time.sleep(1.0)
        except AirflowException as ae:
            # TODO: get logs from sparkapplications.sparkoperator.k8s.io resource when fail
            self.log.info(self.hook.get_pod_logs(
                self.application_name + '-driver', namespace=self.namespace).data.decode())    
            raise ae
        self.log.info("SparkApplication finished: %s", self.application_name)
        self.log.info(self.hook.get_pod_logs(
            self.application_name + '-driver', namespace=self.namespace).data.decode())
