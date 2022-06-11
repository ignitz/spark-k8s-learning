# Spark K8s Learning

Repository to test and development in Spark on K8s

## Requirements

- Docker and Docker Composer
- Kind
- Kubectl
- Helm

## Create kind cluster

```shell
make create-kind
```

## Install Grafana and Prometheus

```shell
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm upgrade --install --debug \
    --create-namespace \
    --namespace monitoring \
    prometheus prometheus-community/kube-prometheus-stack \
    --set grafana.enabled=true

# Create Ingress to access Grafana
bash -c "kubectl create ingress grafana -n monitoring --rule=grafana.localhost/\*=prometheus-grafana:80"
# http://grafana.localhost/
# user: admin
# pass: prom-operator
```

## External Kafka in Docker-Compose

- Note: The new docker compose use `docker compose up -d` instead `docker-compose up -d`

```shell
(
    cd external-services && docker compose up -d && \
    sleep 10 && \
    timeout 90s bash -c "until docker exec postgres pg_isready ; do sleep 5 ; done" && \
    docker exec postgres psql -U postgres -c "CREATE DATABASE airflow;"
) && echo "done"

# To cleanup
# (cd external-services && docker compose down --volumes)
```

### Create Spark Image and sent to Kind Cluster

```shell
# Create Spark Image
(
    cd spark/docker && \
    docker build -f Dockerfile -t spark3:latest .
)
(
    cd spark/docker && \
    docker build -f Dockerfile.spark-operator -t spark-operator:latest .
)

# Send image to Kind Cluster
kind load docker-image spark3 spark-operator
```

## Spark Operator

```shell
make helm-add
make install-spark
```

## Create Kafka-Connector to send data to Kafka

Need to create buckets

```shell
# Create Buckets
bash external-services/create_buckets_minio.sh
```

```shell
# Create Kafka-Connector
(
    cd external-services/ && \
    bash create_connector.sh
)
```

Check with [Kowl](http://localhost:10000/topics) if data is sent to Kafka:

![Kowl Topics](docs/img/kowl-topics.png)

Copy spark scripts from `spark/scripts/` to the bucket `spark-artifacts`.

## Test Spark Job

```shell
# Create Kafka-Connector
bash copy_jibaro.sh
```

test Spark submit:

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
pip install apache-airflow==2.3.2 \
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
```

Change config with sed:

```shell
sed -i 's/^executor = .*/executor = LocalExecutor/g' $HOME/airflow/airflow.cfg
sed -i 's/^load_examples = .*/load_examples = False/g' $HOME/airflow/airflow.cfg
sed -i 's/^sql_alchemy_conn = .*/sql_alchemy_conn = postgresql+psycopg2:\/\/postgres:postgres@localhost\/airflow/g' $HOME/airflow/airflow.cfg
sed -i 's:^dags_folder = .*:dags_folder = '`pwd`'/airflow\/dags:g' $HOME/airflow/airflow.cfg
sed -i 's/^remote_logging = .*/remote_logging = True/g' $HOME/airflow/airflow.cfg
sed -i 's/^remote_log_conn_id = .*/remote_log_conn_id = AirflowS3Logs/g' $HOME/airflow/airflow.cfg
sed -i 's/^remote_base_log_folder = .*/remote_base_log_folder = s3:\/\/airflow-logs\/logs/g' $HOME/airflow/airflow.cfg
```

Then run:

```shell
source venv/bin/activate && airflow db init && \
airflow connections add \
   --conn-type 's3' \
   --conn-extra '{ "aws_access_key_id": "minio", "aws_secret_access_key": "miniominio", "host": "http://localhost:9000" }' \
   AirflowS3Logs
```

```shell
# in one terminal
source venv/bin/activate && PYTHON_PATH=$PWD/airflow/dags airflow webserver

# in another terminal
source venv/bin/activate && PYTHON_PATH=$PWD/airflow/dags airflow scheduler
```

Create an admin user:

```shell
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com
```

Insert password and go to [Airflow UI](http://localhost:8080/)

And run `SparkOperator` dag:

![DAG Spark Operator](docs/img/dag-spark-operator.png)

After the DAG is completed, you can check the output in Minio's datalake bucket [http://localhost:9001/buckets/datalake/browse](http://localhost:9001/buckets/datalake/browse):

![Checkpoint and Data](docs/img/minio-checkpoint-data.png)
![Tables in Minio](docs/img/dataset-in-minio.png)

# TODO

- [ ] Export SparkUI with Ingress or a reverse proxy
- [ ] Deploy Spark-History-server
- [ ] Export SparkUI to bucket to Spark-History-server
- [ ] Export metrics to Prometheus
- [ ] Documentation of SparkOperator
- [ ] Support to spark with `.jar` files
- [x] Create a lib and send with a `.zip` file with `--pyFiles`
- [ ] Support ENV
- [ ] Support secrets
- [ ] Create a new class with family of driver/executors with different configs (CPU/MEM)
- [ ] Support with JSON without Schema-Registry
- [ ] Support to parse Avro in Key and Value from Kafka
- [ ] Try to support Protobuf.
- [ ] Support to dynamic 'pip install' of packages
