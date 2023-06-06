# Spark K8s Learning

Repository to test and development in Spark on K8s

## Requirements

- Docker and Docker Composer
- Kind
- Kubectl
- Helm

**DO YOUR "JEITO"** #BRHUEHUE

## Create kind cluster

```shell
make create-kind

# To clean-up
kind delete cluster
```

## Install Grafana and Prometheus

```shell
make helm-add
make install-prometheus
```

Access [http://grafana.localhost/](http://grafana.localhost/) with

```
user: admin
pass: prom-operator
```

## External Kafka in Docker-Compose

- Note: The new docker compose use `docker compose up -d` instead `docker-compose up -d`

```shell
make kafka-setup

# To cleanup
# make kafka-destroy
```

Check with [Kowl](http://localhost:7777/topics) if data are sent to Kafka:

![Kowl Topics](docs/img/kowl-topics.png)

### Create Spark Image and sent to Kind Cluster

```shell
# Create Spark Image
make build-spark

# Create Operator Image
make build-operator

# Send image to Kind Cluster
make send-images
```

## Spark Operator

```shell
make helm-add
make install-spark
```

Copy `jibaro` library and scripts with:

```
make copy
```

This command will send files to `s3://spark-artifacts`.

## Test Spark Job

Test Spark submit:

```shell
kubectl apply -f tests/spark-test.yaml
```

Check if job is completed:

```shell
kubectl get sparkapplications -n spark
```

```
NAME                 STATUS      ATTEMPTS   START                  FINISH                 AGE
spark-yuriniitsuma   COMPLETED   1          2022-05-29T06:43:26Z   2022-05-29T06:43:57Z   49s
```

Delete job:

```shell
kubectl delete sparkapplications spark-yuriniitsuma -n spark
```

## Generate data in postgres database

Generate data in `dbserver1.inventory.products`:

```shell
bash external-services/generate_data.sh
```

## Airflow

You need to install airflow on your host computer to easily access kind cluster.
Obviosly you need Python 3 and pip3 installed on your computer.

```shell
# Install Airflow
python3 -m venv venv && source venv/bin/activate && \
pip install apache-airflow==2.4.3 \
    psycopg2-binary \
    apache-airflow-providers-cncf-kubernetes \
    apache-airflow-providers-amazon
```

Run `airflow db init` for the first time to create the config files.

Change the config below in `$HOME/airflow/airflow.cfg`:

```conf
dags_folder = $LOCAL_TO_REPO/airflow/dags
executor = LocalExecutor
load_examples = False
sql_alchemy_conn = postgresql+psycopg2://postgres:postgres@localhost/airflow
remote_logging = True
remote_log_conn_id = AirflowS3Logs
remote_base_log_folder = s3://airflow-logs/logs
```

Change config with sed:

```shell
sed -i '' 's/^executor = .*/executor = LocalExecutor/g' $HOME/airflow/airflow.cfg
sed -i '' 's/^load_examples = .*/load_examples = False/g' $HOME/airflow/airflow.cfg
sed -i '' 's/^sql_alchemy_conn = .*/sql_alchemy_conn = postgresql+psycopg2:\/\/postgres:postgres@localhost\/airflow/g' $HOME/airflow/airflow.cfg
sed -i '' 's:^dags_folder = .*:dags_folder = '`pwd`'/airflow\/dags:g' $HOME/airflow/airflow.cfg
sed -i '' 's/^remote_logging = .*/remote_logging = True/g' $HOME/airflow/airflow.cfg
sed -i '' 's/^remote_log_conn_id =.*/remote_log_conn_id = AirflowS3Logs/g' $HOME/airflow/airflow.cfg
sed -i '' 's/^remote_base_log_folder =.*/remote_base_log_folder = s3:\/\/airflow-logs\/logs/g' $HOME/airflow/airflow.cfg
sed -i '' 's/^web_server_port = .*/web_server_port = 8000/g' $HOME/airflow/airflow.cfg
```

Then run:

```shell
source venv/bin/activate && airflow db init && \
airflow connections add \
   --conn-type 'aws' \
   --conn-extra '{ "aws_access_key_id": "minio", "aws_secret_access_key": "miniominio", "host": "http://localhost:9000" }' \
   AirflowS3Logs &&\
airflow pools set spark 2 'spark on k8s'
```

```shell
# in one terminal run
source venv/bin/activate && PYTHON_PATH=$PWD/airflow/dags airflow webserver

# in another terminal run the scheduler
source venv/bin/activate && PYTHON_PATH=$PWD/airflow/dags airflow scheduler
```

Create an admin user:

```shell
airflow users create \
    --username admin \
    --password admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com
```

Insert password and go to [Airflow UI](http://localhost:8080/)

And run `Pipeline` dag:

![DAG Spark Operator](docs/img/dag-spark-operator.png)

After the DAG is completed, you can check the output in Minio's datalake bucket [http://localhost:9001/buckets/datalake/browse](http://localhost:9001/buckets/datalake/browse):

![Checkpoint and Data](docs/img/minio-checkpoint-data.png)
![Tables in Minio](docs/img/dataset-in-minio.png)

# TODO

- [ ] Export SparkUI with Ingress or a reverse proxy
- [ ] Deploy Spark-History-server
- [x] Export SparkUI to bucket to Spark-History-server
- [ ] Export metrics to Prometheus
- [ ] Documentation of SparkOperator
- [x] Support to spark with `.jar` files
- [x] Create a lib and send with a `.zip` file with `--pyFiles`
- [ ] Support ENV
- [ ] Support secrets
- [ ] Create a new class with family of driver/executors with different configs (CPU/MEM)
- [x] Support with JSON without Schema-Registry
- [x] Support to parse Avro in Key and Value from Kafka
- [ ] Try to support Protobuf.
- [ ] Support to dynamic 'pip install' of packages
- [x] Automatic compact files in Delta Lake
- [x] Automatic vacuum files in Delta Lake
- [x] Create automatic history in `_history` folder to inspect metrics like numberoffiles for each version
- [x] Add Trino to connect to Data Lake
- [x] Add hive metastore with postgresql
- [ ] Add support to Spark use Hive or Jibaro libray use SQL in Trino.
- [ ] Airflow on Kubernetes with Official Helm Chart
- [ ] Airflow allow to send spark job to kubernetes
- [x] Compile spark-operator binary in amd64 and arm64 to Apple Silicon
- [x] Update to Spark 3.2.1
- [x] Update to Spark 3.3.x (after the release of Delta Lake)
- [x] SparkOperator delete spark manifest after COMPLETED
- [x] SparkOperator get status error from SparkApplication and output on Airflow UI
- [x] Rebuild Confluent Kafka-Stack in ARM64 to support Apple Silicon (M1)
