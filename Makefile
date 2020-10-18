SHELL:=bash
S3_READY_REGEX=^Ready\.$

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

certificates:
	./generate-certificates.sh

build-jar:
	gradle build
	cp build/libs/*.jar images/htme/hbase-to-mongo-export.jar

build-images: build-jar certificates
	docker-compose build

build-all: build-jar build-images ## Build the jar file and then all docker images

build-hbase-init:
	docker-compose build --no-cache hbase-populate

build-aws-init:
	docker-compose build aws

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
	docker exec -i hbase hbase shell <<< "create_namespace 'database'"; \
	docker-compose up hbase-init

service-aws:
	docker-compose up -d aws
	@{ \
		while ! docker logs aws 2> /dev/null | grep -q $(S3_READY_REGEX); do \
			echo Waiting for aws.; \
			sleep 2; \
		done; \
	}
	docker-compose up aws-init

service-dks-insecure:
	docker-compose up -d dks-standalone-http

service-dks-secure:
	docker-compose up -d dks-standalone-https

services-dks: service-dks-insecure service-dks-secure

services: services-dks service-hbase service-aws

export-table-unavailable:
	docker-compose up table-unavailable

export-blocked-topic:
	docker-compose up blocked-topic

export-s3:
	docker-compose up export-s3

exports: services export-table-unavailable export-blocked-topic export-s3

integration-tests: exports
	docker-compose up integration-tests

all: build-images integration-tests
