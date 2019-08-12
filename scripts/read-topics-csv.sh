#!/bin/bash

TOPICS_CSV_FILE=$1
S3_BUCKET=$2
AWS_ACCESS_KEY_ID=$3
AWS_SECRET_ACCESS_KEY=$4
S3_PREFIX_FOLDER=${5:-"test-exporter"}
AWS_DEFAULT_REGION=${6:-"eu-west-2"}
AWS_DEFAULT_PROFILE=${7:-"default"}
HBASE_URL=${8:-"http://local-hbase:8090"}
DATA_KEY_SERVICE_URL=${9:-"http://local-dks:8090"}

TODAY=$(date +"%Y-%m-%d")
HBASE_TO_MONGO_VERSION=$(cat ../gradle.properties | cut -f2 -d'=')
S3_FOLDER="${S3_PREFIX_FOLDER}/${TODAY}"
JAR_FILE="../build/libs/hbase-to-mongo-export-${HBASE_TO_MONGO_VERSION}.jar"

echo ""

if [[ -f ${JAR_FILE} ]]; then
  echo "Found jar file at  ${JAR_FILE}"
else
  echo "File not found ${JAR_FILE}"
  exit 1
fi

if [[ -f ${TOPICS_CSV_FILE} ]]; then
  echo "Reading topics csv file ${TOPICS_CSV_FILE}"
else
  echo "File not found ${TOPICS_CSV_FILE}"
  exit 1
fi

# shellcheck disable=SC2002
cat "${TOPICS_CSV_FILE}" | while read -r TOPIC_NAME
  do
    echo "Processing: ${TOPIC_NAME} into folder ${S3_FOLDER}"
    exit 1
    java -jar "${JAR_FILE}" \
      --spring.profiles.active=phoneyCipherService,realHttpClient,httpDataKeyService,realHbaseDataSource,outputToS3,batchRun,strongRng \
      --hbase.zookeeper.quorum="${HBASE_URL}" \
      --data_key_service_url="${DATA_KEY_SERVICE_URL}" \
      --aws_default_region="${AWS_DEFAULT_REGION}" \
      --aws_access_key_id="${AWS_ACCESS_KEY_ID}" \
      --aws_secret_access_key="${AWS_SECRET_ACCESS_KEY}" \
      --aws_default_profile="${AWS_DEFAULT_PROFILE}" \
      --s3_bucket="${S3_BUCKET}" \
      --s3_prefix_folder="${S3_FOLDER}";
  done

echo "Finished topics csv file ${TOPICS_CSV_FILE}"
echo ""