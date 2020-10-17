SHELL:=bash
S3_READY_REGEX=^Ready\.$
hbase_to_mongo_version=$(shell cat ./gradle.properties | cut -f2 -d'=')

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

echo-version: ## Echo the current version
	@echo "HBASE_TO_MONGO_EXPORT_VERSION=$(hbase_to_mongo_version)"

generate-developer-certs:  ## Generate temporary local certs and stores for the local developer containers to use
	pushd resources && ./generate-developer-certs.sh && popd

build-jar: ## Build the hbase exporter jar file
	gradle build

dist: ## Assemble distribution files in build/dist
	gradle assembleDist

add-containers-to-hosts: ## Update laptop hosts file with reference to containers
	pushd resources && ./add-containers-to-hosts.sh && popd

build-all: build-jar build-images ## Build the jar file and then all docker images

build-base-java:
	@{ \
		pushd resources; \
		docker build --tag dwp-centos-with-java-htme:latest --file Dockerfile_centos_java . ; \
	}

build-base-python:
	@{ \
		pushd resources; \
		docker build --tag dwp-python-preinstall-htme:latest --file Dockerfile_python_preinstall . ; \
	}

build-base-kotlin:
	@{ \
		pushd resources; \
		cp ../settings.gradle.kts ../gradle.properties . ; \
		docker build --tag dwp-kotlin-slim-gradle:latest --file Dockerfile_java_gradle_base . ; \
		rm settings.gradle.kts gradle.properties ; \
		popd; \
	}

build-base-images: build-base-java build-base-python build-base-java

build-hbase-init:
	docker-compose build --no-cache hbase-populate

build-aws-init:
	docker-compose build aws

build-images: build-jar build-base-images
	docker-compose build

service-hbase:
	docker-compose up -d hbase
	@{ \
			echo Waiting for hbase.; \
			while ! docker logs hbase 2>&1 | grep "Master has completed initialization" ; do \
					sleep 2; \
					echo Waiting for hbase.; \
			done; \
			echo HBase ready.; \
	}

service-aws:
	docker-compose up -d aws
	@{ \
		while ! docker logs aws 2> /dev/null | grep -q $(S3_READY_REGEX); do \
			echo Waiting for aws.; \
			sleep 2; \
		done; \
	}

service-dks-insecure:
	docker-compose up -d dks-standalone-http

service-dks-secure:
	docker-compose up -d dks-standalone-https

services-dks: service-dks-insecure service-dks-secure

init-aws: service-aws
	docker-compose up aws-init

init-hbase: service-hbase service-dks-insecure
	docker exec -i hbase hbase shell <<< "create_namespace 'database'"; \
	docker-compose up hbase-init

services: services-dks init-hbase init-aws

export-table-unavailable:
	docker-compose up hbase-to-mongo-export-table-unavailable

export-blocked-topic:
	docker-compose up hbase-to-mongo-export-blocked-topic

export-to-s3:
	docker-compose up hbase-to-mongo-export-s3

exports: export-table-unavailable export-blocked-topic export-to-s3

integration-tests:
	docker-compose up hbase-to-mongo-export-itest
