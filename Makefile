.PHONY: help
help: ## Show help menu
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: create-kind
create-kind: ## 🚧 Setup environment
	@kind create cluster --config kind/config.yaml
	@kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml

.PHONY: helm-add
helm-add: ## ⎈ Helm Add repositories
	@helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	@helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
	@helm repo update

.PHONY: lake-setup
lake-setup: ## 🌊 Setup Confluent lake with docker compose, create buckets and Debezium's connectors
	$(MAKE) -C external-services lake-setup

.PHONY: lake-destroy
lake-destroy: ## 🗑 Destroy Confluent lake with docker compose
	$(MAKE) -C external-services lake-destroy

.PHONY: client-setup
client-setup: lake-setup ## 🐰 Setup Trino, Hive and Superset clients
	$(MAKE) -C external-services client-setup

.PHONY: client-destroy
client-destroy: ## 🥕 Destroy Trino, Hive and Superset clients
	$(MAKE) -C external-services client-destroy

.PHONY: install-prometheus
install-prometheus: ## ⎈ Install Prometheus in Kind
	@helm upgrade --install --debug \
		--create-namespace \
		--namespace monitoring \
		prometheus prometheus-community/kube-prometheus-stack \
		--set grafana.enabled=true
	@bash -c "kubectl create ingress grafana -n monitoring --rule=grafana.localhost/\*=prometheus-grafana:80"

.PHONY: build-spark
build-spark: ## 🐳 Build Spark image
	@docker build -f spark/docker/spark-base/Dockerfile.3.2.2 -t spark-base:latest spark/docker/spark-base
	@docker build -f spark/docker/spark-custom/Dockerfile.3.2.2 -t spark-custom:latest spark/docker/spark-custom

.PHONY: build-operator
build-operator: build-spark ## 🐳 Build Spark Operator image
	@docker build -f spark/docker/spark-operator/Dockerfile.3.2.2 -t spark-operator:latest spark/docker/spark-operator

.PHONY: send-images
send-images: ## 📦 Send spark images to Kind
	@kind load docker-image spark-operator
	@kind load docker-image spark-custom

.PHONY: install-spark
install-spark: ## ⎈ Install spark-on-k8s
	@bash install_spark.sh

.PHONY: copy
copy: ## 📁 Copy files to MinIO bucket
	@bash copy_jibaro.sh

