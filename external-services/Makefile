.PHONY: help
help: ## Show help menu
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: lake-setup
lake-setup: ## Setup Confluent lake with docker compose, create buckets and Debezium's connectors
	@bash setup_lake_stack.sh

.PHONY: lake-destroy
lake-destroy: ## Destroy Confluent lake with docker compose
	@bash destroy_lake_stack.sh

.PHONY: client-setup
client-setup: ## Setup Trino, Hive and Superset clients
	@bash setup_clients_stack.sh

.PHONY: client-destroy
client-destroy: ## Destroy Trino, Hive and Superset clients
	@bash destroy_clients_stack.sh
