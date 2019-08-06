hbase_to_mongo_version=$(shell cat ./gradle.properties | cut -f2 -d'=')
aws_default_region=eu-west-1
aws_session_token=not_set
aws_access_key_id=not_set
aws_default_profile=not_set
s3_bucket=not_set
s3_prefix_folder=not_set
data_key_service_url=http://localhost:8080

default: help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: echo
echo: ## Echo the current version
	@echo "HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version)"

.PHONY: build
build: ## Build the hbase exporter jar file
	./gradlew build

.PHONY: dist
dist: ## Assemble distribution files in build/dist
	./gradlew assembleDist

.PHONY: add-hbase-to-hosts
add-hbase-to-hosts: ## Update laptop hosts file with reference to hbase container
	./scripts/add-hbase-to-hosts.sh;

.PHONY: build-images
build-images: build ## Build the hbase, population, exporter images
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_SESSION_TOKEN=$(aws_session_token); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_DEFAULT_PROFILE=$(aws_default_profile); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		docker-compose build hbase hbase-populate hbase-to-mongo-export-file hbase-to-mongo-export-directory hbase-to-mongo-export-s3 hbase-to-mongo-export-itest; \
	}

.PHONY: up
up: build-images ## Bring up hbase, population, and sample exporter services
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_SESSION_TOKEN=$(aws_session_token); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_DEFAULT_PROFILE=$(aws_default_profile); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		docker-compose up -d hbase hbase-populate hbase-to-mongo-export-file hbase-to-mongo-export-directory; \
		make add-hbase-to-hosts; \
	}

.PHONY: export-to-s3
export-to-s3: ## Bring up a sample s3-exporter service
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_SESSION_TOKEN=$(aws_session_token); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_DEFAULT_PROFILE=$(aws_default_profile); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		docker-compose up --build -d  hbase-to-mongo-export-s3; \
	}

.PHONY: restart
restart: ## Restart hbase and other services
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_SESSION_TOKEN=$(aws_session_token); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_DEFAULT_PROFILE=$(aws_default_profile); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		docker-compose restart; \
	}

.PHONY: down
down: ## Bring down the hbase and other services
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_SESSION_TOKEN=$(aws_session_token); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_DEFAULT_PROFILE=$(aws_default_profile); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		docker-compose down; \
	}

.PHONY: destroy
destroy: down ## Bring down the hbase and other services then delete all volumes
	docker network prune -f
	docker volume prune -f

.PHONY: integration
integration: up ## Run the integration tests in a Docker container
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_SESSION_TOKEN=$(aws_session_token); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_DEFAULT_PROFILE=$(aws_default_profile); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		docker-compose up --build hbase-to-mongo-export-itest; \
	}

.PHONY: hbase-shell
hbase-shell: ## Open an Hbase shell onto the running hbase container
	docker-compose run --rm hbase shell

.PHONY: logs-file-exporter
logs-file-exporter: ## Show the logs of the file exporter
	docker logs hbase-to-mongo-export-file

.PHONY: logs-directory-exporter
logs-directory-exporter: ## Show the logs of the directory exporter
	docker logs hbase-to-mongo-export-directory

.PHONY: logs-s3-exporter
logs-s3-exporter: ## Show the logs of the s3 exporter
	docker logs hbase-to-mongo-export-s3

.PHONY: reset-all
reset-all: destroy up logs-directory-exporter ## Destroy all, rebuild and up all, and check the export logs
