.PHONY: help
help: ## Show help menu
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: create-kind
create-kind: ## Setup environment
	@kind create cluster --config kind/config.yaml
	@kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml

.PHONY: helm-add
helm-add: ## Helm Add repositories
	@helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	@helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
	@helm repo add apache-airflow https://airflow.apache.org
	@helm repo update

.PHONY: kafka-setup
kafka-setup: ## Setup Confluent Kafka with docker compose, create buckets and Debezium's connectors
	@bash external-services/setup_kafka_stack.sh

.PHONY: kafka-destroy
kafka-destroy: ## Destroy Confluent Kafka with docker compose
	@bash external-services/destroy_kafka_stack.sh

.PHONY: install-prometheus
install-prometheus: ## Install Prometheus in Kind
	@helm upgrade --install --debug \
		--create-namespace \
		--namespace monitoring \
		prometheus prometheus-community/kube-prometheus-stack \
		--set grafana.enabled=true
	@bash -c "kubectl create ingress grafana -n monitoring --rule=grafana.localhost/\*=prometheus-grafana:80"

.PHONY: build-spark
build-spark: ## Build Spark image
	@docker build -f spark/docker/Dockerfile -t spark3:latest spark/docker

.PHONY: build-operator
build-operator: ## Build Spark Operator image
	@docker build -f spark/docker/Dockerfile.spark-operator -t spark-operator:latest spark/docker

.PHONY: send-images
send-images: ## Send spark images to Kind
	@kind load docker-image spark-operator
	@kind load docker-image spark3

.PHONY: install-spark
install-spark: ## Install spark-on-k8s
	@bash install_spark.sh

.PHONY: copy
copy: ## Copy files to MinIO bucket
	@bash copy_jibaro.sh

