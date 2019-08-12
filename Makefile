hbase_to_mongo_version=$(shell cat ./gradle.properties | cut -f2 -d'=')
aws_default_region=eu-west-2
aws_secret_access_key=not_set
aws_access_key_id=not_set
s3_bucket=not_set
s3_prefix_folder=not_set
data_key_service_url=http://dks-standalone:8090
follow_flag=--follow

default: help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: echo-version
echo-version: ## Echo the current version
	@echo "HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version)"

.PHONY: build-jar
build-jar: ## Build the hbase exporter jar file
	./gradlew build

.PHONY: dist
dist: ## Assemble distribution files in build/dist
	./gradlew assembleDist

.PHONY: add-containers-to-hosts
add-containers-to-hosts: ## Update laptop hosts file with reference to hbase and dks-standalone containers
	./scripts/add-hbase-to-hosts.sh;
	./scripts/add-dks-to-hosts.sh;

build-all: build-jar build-images ## Build the jar file and then all docker images

.PHONY: build-images
build-images: ## Build the hbase, population, and exporter images
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		docker-compose build hbase dks-standalone hbase-populate hbase-to-mongo-export-file hbase-to-mongo-export-directory hbase-to-mongo-export-s3 hbase-to-mongo-export-itest; \
	}

up: build-all up-all

.PHONY: up-all
up-all: ## Bring up hbase, population, and sample exporter services
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		docker-compose up -d hbase dks-standalone hbase-populate; \
		echo "Waiting for population"; \
		sleep 5; \
		docker-compose up -d hbase-to-mongo-export-file hbase-to-mongo-export-directory; \
	}

.PHONY: export-to-s3
export-to-s3: ## Bring up a sample s3-exporter service exporting to dev AWS
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		docker-compose up --build -d hbase-to-mongo-export-s3; \
	}

.PHONY: restart
restart: ## Restart hbase and other services
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
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
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		docker-compose down; \
	}

.PHONY: destroy
destroy: down ## Bring down the hbase and other services then delete all volumes
	docker network prune -f
	docker volume prune -f

integration-all: build-all up-all integration-tests ## Build the jar and images, put up the containers, run the integration tests

.PHONY: integration-tests
integration-tests: ## (Re-)Run the integration tests in a Docker container
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		echo "Waiting for exporters"; \
		sleep 5; \
		docker-compose up hbase-to-mongo-export-itest; \
	}

.PHONY: hbase-shell
hbase-shell: ## Open an Hbase shell onto the running hbase container
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		docker-compose run --rm hbase shell; \
	}

.PHONY: logs-hbase-populate
logs-hbase-populate: ## Show the logs of the hbase-populater. Update follow_flag as required.
	docker logs $(follow_flag) hbase-populate

.PHONY: logs-file-exporter
logs-file-exporter: ## Show the logs of the file exporter. Update follow_flag as required.
	docker logs $(follow_flag) hbase-to-mongo-export-file

.PHONY: logs-directory-exporter
logs-directory-exporter: ## Show the logs of the directory exporter. Update follow_flag as required.
	docker logs $(follow_flag) hbase-to-mongo-export-directory

.PHONY: logs-s3-exporter
logs-s3-exporter: ## Show the logs of the s3 exporter. Update follow_flag as required.
	docker logs $(follow_flag) hbase-to-mongo-export-s3

.PHONY: reset-all
reset-all: destroy integration-all logs-directory-exporter ## Destroy all, rebuild and up all, and check the export logs

.PHONY: local-all-collections-test
local-all-collections-test: build-jar up add-containers-to-hosts ## Build a local jar, then run it repeat times for each configured collection
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		pushd scripts; \
		./read-topics-csv.sh \
			topics-test.csv \
			$(s3_bucket) \
			$(aws_access_key_id) \
			$(aws_secret_access_key) \
			$(s3_prefix_folder) \
			$(aws_default_region) \
			default \
			http://local-hbase:8080 \
			http://local-dks:8090 ;\
		popd ;\
	}
