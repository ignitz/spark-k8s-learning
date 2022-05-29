#
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# ARG SPARK_IMAGE=gcr.io/spark-operator/spark:v3.1.1-hadoop3
ARG SPARK_IMAGE=gcr.io/spark-operator/spark-py:v3.1.1-hadoop3
FROM ${SPARK_IMAGE}

# Switch to user root so we can add additional jars and configuration files.
USER root

# Setup dependencies for Google Cloud Storage access.
# RUN rm $SPARK_HOME/jars/guava-14.0.1.jar
# ADD https://repo1.maven.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar $SPARK_HOME/jars
# RUN chmod 644 $SPARK_HOME/jars/guava-23.0.jar
# Add the connector jar needed to access Google Cloud Storage using the Hadoop FileSystem API.
# ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar $SPARK_HOME/jars
# RUN chmod 644 $SPARK_HOME/jars/gcs-connector-latest-hadoop2.jar
# ADD https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest_2.12.jar $SPARK_HOME/jars
# RUN chmod 644 $SPARK_HOME/jars/spark-bigquery-latest_2.12.jar

# Add support to S3a connector.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.1/hadoop-aws-3.1.1.jar $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/hadoop-aws-3.1.1.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.11.534/aws-java-sdk-1.11.534.jar $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/aws-java-sdk-1.11.534.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.11.534/aws-java-sdk-core-1.11.534.jar $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/aws-java-sdk-core-1.11.534.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/1.11.534/aws-java-sdk-dynamodb-1.11.534.jar $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/aws-java-sdk-dynamodb-1.11.534.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-kms/1.11.534/aws-java-sdk-kms-1.11.534.jar $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/aws-java-sdk-kms-1.11.534.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.11.534/aws-java-sdk-s3-1.11.534.jar $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/aws-java-sdk-s3-1.11.534.jar
ADD https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.3/httpclient-4.5.3.jar $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/httpclient-4.5.3.jar
ADD https://repo1.maven.org/maven2/joda-time/joda-time/2.9.9/joda-time-2.9.9.jar $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/joda-time-2.9.9.jar

# Kafka libraries
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.1/spark-sql-kafka-0-10_2.12-3.1.1.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.1.1/spark-token-provider-kafka-0-10_2.12-3.1.1.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.4.1/kafka-clients-2.4.1.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.6.2/commons-pool2-2.6.2.jar $SPARK_HOME/jars
# Avro library
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.1.1/spark-avro_2.12-3.1.1.jar $SPARK_HOME/jars

# Delta libraries
ADD https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.0.1/delta-core_2.12-1.0.1.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/io/delta/delta-contribs_2.12/1.0.1/delta-contribs_2.12-1.0.1.jar $SPARK_HOME/jars

# Setup for the Prometheus JMX exporter.
# Add the Prometheus JMX exporter Java agent jar for exposing metrics sent to the JmxSink to Prometheus.
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.11.0/jmx_prometheus_javaagent-0.11.0.jar /prometheus/
RUN chmod 644 /prometheus/jmx_prometheus_javaagent-0.11.0.jar

USER ${spark_uid}

RUN mkdir -p /etc/metrics/conf
COPY conf/metrics.properties /etc/metrics/conf
COPY conf/prometheus.yaml /etc/metrics/conf

ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_CONF_DIR $HADOOP_HOME/conf
RUN mkdir -p $HADOOP_HOME/conf
COPY conf/core-site.xml $HADOOP_HOME/conf/
COPY conf/spark-env.sh $HADOOP_HOME/conf/
COPY conf/spark-defaults.conf $HADOOP_HOME/conf/

ENV PATH $PATH:/opt/spark/bin

# PIP requirements
COPY ./requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Support Jupyter Notebook
# RUN pip install jupyter
# ENV PYSPARK_DRIVER_PYTHON "jupyter"
# ENV PYSPARK_DRIVER_PYTHON_OPTS "notebook"
# ENV PYSPARK_PYTHON python3

# COPY ./entrypoint.sh /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
