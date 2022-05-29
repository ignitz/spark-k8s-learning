# Spark K8s Learning

Repository to test and development in Spark on K8s

## Requirements

- Docker and Docker Composer
- Kind
- Kubectl
- Helm

```shell
# Create Kind cluster
kind create cluster --config kind/config.yaml

# Add Nginx ingress in Kind cluster
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
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
```

## Spark Operator

```shell
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo update

# Create namespace "spark" where will run Jobs
kubectl create namespace spark --dry-run=client -o yaml | kubectl apply -f -

# Create service account "spark" to spark-operator allow submit jobs
kubectl create serviceaccount spark -n spark
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=spark:spark --namespace=spark

# Install spark operator
helm install spark-operator spark-operator/spark-operator --debug \
    --namespace spark-operator \
    --create-namespace \
    --set sparkJobNamespace=spark
```

### Create Spark Image and sent to Kind Cluster

```shell
# Create Spark Image
(cd spark/docker && docker build -t spark3:latest .)

# Send image to Kind Cluster
kind load docker-image spark3
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
(
    cd spark/ && \
    bash copy_scripts.sh
)
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
pip install apache-airflow \
    apache-airflow-providers-cncf-kubernetes \
    apache-airflow-providers-amazon
```

Run `airflow init db` for the first time to create the config files.

Change the config below in `$HOME/airflow/airflow.cfg`:

```conf
dags_folder = $LOCAL_TO_REPO/airflow/dags
executor = LocalExecutor
load_examples = False
sql_alchemy_conn = postgresql+psycopg2://postgres:postgres@localhost/airflow
```

Change config with sed:

```shell
sed -i 's/^executor = .*/executor = LocalExecutor/g' $HOME/airflow/airflow.cfg
sed -i 's/^load_examples = .*/load_examples = False/g' $HOME/airflow/airflow.cfg
sed -i 's/^sql_alchemy_conn = .*/sql_alchemy_conn = postgresql+psycopg2:\/\/postgres:postgres@localhost\/airflow/g' $HOME/airflow/airflow.cfg
sed -i 's:^dags_folder = .*:dags_folder = '`pwd`'/airflow\/dags:g' $HOME/airflow/airflow.cfg
```

Then run:

- `airflow db init`
- `PYTHON_PATH=$PWD/airflow/dags airflow webserver` in one terminal
- `PYTHON_PATH=$PWD/airflow/dags airflow scheduler` in another terminal

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
