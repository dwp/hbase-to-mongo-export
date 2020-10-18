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

certificates: ## generate the mutual authentication certificates for communications with dks.
	./generate-certificates.sh

build-jar: ## build the main jar
	gradle build
	cp build/libs/*.jar images/htme/hbase-to-mongo-export.jar

build-images: build-jar certificates ## build the images for local containerized running
	docker-compose build

build-all: build-jar build-images ## Build the jar file and then all docker images

build-hbase-init: ## build the image that populates hbase.
	docker-compose build --no-cache hbase-populate

build-aws-init: ## build the image that prepares aws services.
	docker-compose build aws

service-hbase: ## bring up hbase, populate it.
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

service-aws: ##bring up aws and prepare the services.
	docker-compose up -d aws
	@{ \
		while ! docker logs aws 2> /dev/null | grep -q $(S3_READY_REGEX); do \
			echo Waiting for aws.; \
			sleep 2; \
		done; \
	}
	docker-compose up aws-init

service-dks-insecure: ## bring up dks on 8080
	docker-compose up -d dks-standalone-http

service-dks-secure: ## bring up secure dks on 8443
	docker-compose up -d dks-standalone-https

services-dks: service-dks-insecure service-dks-secure ## bring up the two dkses.

services: services-dks service-hbase service-aws ## bring up dks, hbase, aws.

export-table-unavailable: ## run htme with an unavailable table.
	docker-compose up table-unavailable

export-blocked-topic: ## run htme with a blocked topic.
	docker-compose up blocked-topic

export-s3: ## run htem aginast a valid pre-populated table.
	docker-compose up export-s3

exports: services export-table-unavailable export-blocked-topic export-s3 ## run all the exports.

integration-tests: exports ## run the integration tests
	docker-compose up integration-tests

all: build-images integration-tests ## build the images and run the tests.
