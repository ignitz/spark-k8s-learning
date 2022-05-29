# Spark K8s Learning

Repository to test and development in Spark oon K8s

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

helm upgrade --install \
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
    docker exec postgres psql -U postgres -c "CREATE DATABASE airflow;"
) && echo "done"
```

- Kowl [http://localhost:8888](http://localhost:8888)

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
helm install spark-operator spark-operator/spark-operator \
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

Check with Kowl:

TODO: add image

```shell
# Create Kafka-Connector
(
    cd spark/ && \
    bash copy_scripts.sh
)
```

Generate data

```shell
bash external-services/generate_data.sh
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
NAME                 STATUS   ATTEMPTS   START                  FINISH                 AGE
spark-yuriniitsuma   FAILED   4          2022-05-29T05:00:04Z   2022-05-29T05:00:17Z   74m
```

Delete job:

```shell
kubectl delete sparkapplications spark-yuriniitsuma -n spark
```
