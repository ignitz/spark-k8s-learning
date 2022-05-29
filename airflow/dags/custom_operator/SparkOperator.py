import os
from typing import TYPE_CHECKING, List, Optional, Sequence

from kubernetes import client

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
import time
import yaml
import os

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SparkOperator(BaseOperator):
    """
    Checks sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

    :param application_name: spark Application resource name
    :param namespace: the kubernetes namespace where the sparkApplication reside in
    :param kubernetes_conn_id: The :ref:`kubernetes connection<howto/connection:kubernetes>`
        to Kubernetes cluster.
    :param attach_log: determines whether logs for driver pod should be appended to the sensor log
    :param api_group: kubernetes api group of sparkApplication
    :param api_version: kubernetes api version of sparkApplication
    """

    template_fields: Sequence[str] = ("application_name", "namespace")
    ui_color = '#f4a460'

    FAILURE_STATES = ("FAILED", "UNKNOWN")
    SUCCESS_STATES = ("COMPLETED",)

    def __init__(
        self,
        *,
        application_name: str,
        arguments: List[str] = [],
        attach_log: bool = False,
        namespace: str = "spark",
        kubernetes_conn_id: str = "kubernetes_default",
        api_group: str = 'sparkoperator.k8s.io',
        api_version: str = 'v1beta2',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs, do_xcom_push=False)
        self.application_name = application_name
        self.attach_log = attach_log
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        self.api_group = api_group
        self.api_version = api_version
        self.plural = "sparkapplications"
        self.arguments = arguments

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
        try:
            filepath = os.path.join(os.path.dirname(
                os.path.realpath(__file__)), 'spark_application.yaml')
            with open(filepath, "r") as stream:
                body_dict = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            raise AirflowException(
                f"Exception when loading resource definition: {e}\n")

        if isinstance(body_dict, str):
            raise AirflowException(
                f"Error on load YAML file\n")

        body_dict['metadata'] = {
            'name': self.application_name,
            'namespace': self.namespace
        }

        body_dict['spec'] = {
            **body_dict['spec'],
            'arguments': self.arguments
        }

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
        response = self._create_spark_application(context)
        self.log.info("SparkApplication started: %s", self.application_name)
        self.log.info("SparkApplication response: %s", response)
        while not self.poke(context=context):
            # Sleep for 10 seconds
            time.sleep(10.0)
        self.log.info("SparkApplication finished: %s", self.application_name)
        self.log.info(self.hook.get_pod_logs(
            self.application_name + '-driver', namespace=self.namespace).data.decode())
