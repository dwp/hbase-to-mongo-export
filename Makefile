HBASE_TO_MONGO_EXPORT_VERSION=$(cat ./gradle.properties | cut -f2 -d'=')

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build Kafka2Hbase
	./gradlew build

.PHONY: dist
dist: ## Assemble distribution files in build/dist
	./gradlew assembleDist

.PHONY: up
up: ## Bring up Kafka2Hbase in Docker with supporting services
	{
		export HBASE_TO_MONGO_EXPORT_VERSION=$(HBASE_TO_MONGO_EXPORT_VERSION) \
		docker-compose up --build -d hbase hbase-populate hbase-to-mongo-export-file hbase-to-mongo-export-folder
	}

.PHONY: restart
restart: ## Restart Kafka2Hbase and all supporting services
	docker-compose restart

.PHONY: down
down: ## Bring down the hbase container and support services
	docker-compose down

.PHONY: destroy
destroy: down ## Bring down the Kafka2Hbase Docker container and services then delete all volumes
	docker network prune -f
	docker volume prune -f

.PHONY: integration
integration: up ## Run the integration tests in a Docker container
	docker-compose build hbase-to-mongo-export-itest
	docker-compose run hbase-to-mongo-export-itest

.PHONY: hbase-shell
hbase-shell: ## Open an Hbase shell onto the running Hbase container
	docker-compose run --rm hbase shell

