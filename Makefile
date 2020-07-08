SHELL:=bash
S3_READY_REGEX=^Ready\.$
hbase_to_mongo_version=$(shell cat ./gradle.properties | cut -f2 -d'=')
aws_default_region=eu-west-2
aws_secret_access_key=DummyKey
aws_access_key_id=DummyKey
s3_bucket=demobucket
s3_manifest_bucket=manifestbucket
s3_prefix_folder=test-exporter
data_key_service_url=http://dks-standalone-http:8080
data_key_service_url_ssl=https://dks-standalone-https:8443
local_hbase_url=local-hbase
local_dks_url=https://local-dks-https:8443
local_s3_service_endpoint=http://localhost:4572
follow_flag=--follow

default: help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: bootstrap
bootstrap: ## Bootstrap local environment for first use
	make git-hooks

.PHONY: git-hooks
git-hooks: ## Set up hooks in .git/hooks
	@{ \
		HOOK_DIR=.git/hooks; \
		for hook in $(shell ls .githooks); do \
			if [ ! -h $${HOOK_DIR}/$${hook} -a -x $${HOOK_DIR}/$${hook} ]; then \
				mv $${HOOK_DIR}/$${hook} $${HOOK_DIR}/$${hook}.local; \
				echo "moved existing $${hook} to $${hook}.local"; \
			fi; \
			ln -s -f ../../.githooks/$${hook} $${HOOK_DIR}/$${hook}; \
		done \
	}

.PHONY: echo-version
echo-version: ## Echo the current version
	@echo "HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version)"

generate-developer-certs:  ## Generate temporary local certs and stores for the local developer containers to use
	pushd resources && ./generate-developer-certs.sh && popd

.PHONY: build-jar
build-jar: ## Build the hbase exporter jar file
	gradle build

.PHONY: dist
dist: ## Assemble distribution files in build/dist
	gradle assembleDist

.PHONY: add-containers-to-hosts
add-containers-to-hosts: ## Update laptop hosts file with reference to containers
	pushd resources && ./add-containers-to-hosts.sh && popd

build-all: build-jar build-images ## Build the jar file and then all docker images

build-base-images: ## Build base images to avoid rebuilding frequently
	@{ \
		pushd resources; \
		docker build --tag dwp-centos-with-java-htme:latest --file Dockerfile_centos_java . ; \
		docker build --tag dwp-python-preinstall-htme:latest --file Dockerfile_python_preinstall . ; \
		cp ../settings.gradle.kts ../gradle.properties . ; \
		docker build --tag dwp-kotlin-slim-gradle:latest --file Dockerfile_java_gradle_base . ; \
		rm settings.gradle.kts gradle.properties ; \
		popd; \
	}

.PHONY: build-images
build-images: build-jar build-base-images ## Build the hbase, population, and exporter images
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_MANIFEST_BUCKET=$(s3_manifest_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		export DATA_KEY_SERVICE_URL_SSL=$(data_key_service_url_ssl); \
		docker-compose build hbase hbase-populate aws aws-init; \
		docker-compose build --no-cache dks-standalone-http dks-standalone-https; \
		docker-compose build --no-cache hbase-to-mongo-export-file hbase-to-mongo-export-directory hbase-to-mongo-export-s3 hbase-to-mongo-export-itest; \
	}

build-dks: build-jar build-base-images ## Build the hbase, population, and exporter images
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_MANIFEST_BUCKET=$(s3_manifest_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		export DATA_KEY_SERVICE_URL_SSL=$(data_key_service_url_ssl); \
		docker-compose build --no-cache dks-standalone-http dks-standalone-https; \
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
		export S3_MANIFEST_BUCKET=$(s3_manifest_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		export DATA_KEY_SERVICE_URL_SSL=$(data_key_service_url_ssl); \
		docker-compose up -d hbase aws; \
		while ! docker logs aws 2> /dev/null | grep -q $(S3_READY_REGEX); do \
			echo Waiting for aws.; \
			sleep 2; \
		done; \
		docker-compose up aws-init; \
		docker-compose up -d dks-standalone-http dks-standalone-https; \
		docker exec -i hbase hbase shell <<< "create_namespace 'claimant_advances'"; \
		docker exec -i hbase hbase shell <<< "create_namespace 'penalties_and_deductions'"; \
		docker exec -i hbase hbase shell <<< "create_namespace 'quartz'"; \
		docker-compose up hbase-populate; \
		docker-compose up -d hbase-to-mongo-export-file hbase-to-mongo-export-directory hbase-to-mongo-export-s3; \
	}

up-some: ## Bring up hbase, population, and sample exporter services
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_MANIFEST_BUCKET=$(s3_manifest_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		export DATA_KEY_SERVICE_URL_SSL=$(data_key_service_url_ssl); \
		docker-compose up -d hbase aws; \
		while ! docker logs aws 2> /dev/null | grep -q $(S3_READY_REGEX); do \
			echo Waiting for aws.; \
			sleep 2; \
		done; \
		docker-compose up aws-init; \
		docker-compose up -d dks-standalone-http dks-standalone-https; \
		docker exec -i hbase hbase shell <<< "create_namespace 'claimant_advances'"; \
		docker exec -i hbase hbase shell <<< "create_namespace 'penalties_and_deductions'"; \
		docker exec -i hbase hbase shell <<< "create_namespace 'quartz'"; \
		docker-compose up hbase-populate; \
	}

export-s3: ## Bring up hbase, population, and sample exporter services
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_MANIFEST_BUCKET=$(s3_manifest_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		export DATA_KEY_SERVICE_URL_SSL=$(data_key_service_url_ssl); \
		docker-compose up -d hbase aws; \
		while ! docker logs aws 2> /dev/null | grep -q $(S3_READY_REGEX); do \
			echo Waiting for aws.; \
			sleep 2; \
		done; \
		docker-compose up hbase-to-mongo-export-s3; \
	}

.PHONY: restart
restart: ## Restart hbase and other services
	@{ \
		export HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		export AWS_ACCESS_KEY_ID=$(aws_access_key_id); \
		export AWS_SECRET_ACCESS_KEY=$(aws_secret_access_key); \
		export S3_BUCKET=$(s3_bucket); \
		export S3_MANIFEST_BUCKET=$(s3_manifest_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		export DATA_KEY_SERVICE_URL_SSL=$(data_key_service_url_ssl); \
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
		export S3_MANIFEST_BUCKET=$(s3_manifest_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		export DATA_KEY_SERVICE_URL_SSL=$(data_key_service_url_ssl); \
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
		export S3_MANIFEST_BUCKET=$(s3_manifest_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		export DATA_KEY_SERVICE_URL_SSL=$(data_key_service_url_ssl); \
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
		export S3_MANIFEST_BUCKET=$(s3_manifest_bucket); \
		export S3_PREFIX_FOLDER=$(s3_prefix_folder); \
		export DATA_KEY_SERVICE_URL=$(data_key_service_url); \
		export DATA_KEY_SERVICE_URL_SSL=$(data_key_service_url_ssl); \
		docker exec -it hbase hbase shell; \
	}

.PHONY: logs-s3-provision
logs-s3-provision: ## Show the logs of the s3 bucket provision. Update follow_flag as required.
	docker logs $(follow_flag) aws-init

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
local-all-collections-test: build-jar ## Build a local jar, then run it repeat times for each configured collection
	@{ \
		pushd resources; \
		./read-topics-csv.sh \
			topics-test.csv \
			$(s3_bucket) \
			$(s3_manifest_bucket) \
			$(local_s3_service_endpoint) \
			$(aws_access_key_id) \
			$(aws_secret_access_key) \
			$(s3_prefix_folder) \
			$(aws_default_region) \
			$(local_hbase_url) \
			$(local_dks_url) ;\
		popd ;\
	}

.PHONY: dks-logs-https
dks-logs-https: ## Cat the logs of dks-standalone-https
	docker exec dks-standalone-https cat /opt/data-key-service/logs/dks.out

.PHONY: dks-logs-http
dks-logs-http: ## Cat the logs of dks-standalone-http
	docker exec dks-standalone-http cat /opt/data-key-service/logs/dks.out
