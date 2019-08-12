#!/bin/bash

TOPICS_CSV_FILE=$1
S3_BUCKET=$2
AWS_ACCESS_KEY_ID=$3
AWS_SECRET_ACCESS_KEY=$4
S3_PREFIX_FOLDER=${3:-"test-exporter"}
AWS_DEFAULT_REGION=${5:-"eu-west-2"}
AWS_DEFAULT_PROFILE=${6:-"default"}
DATA_KEY_SERVICE_URL=${8:-"http://localhost:8080"}

TODAY=$(date +"%Y-%m-%d")
HBASE_TO_MONGO_VERSION=$(cat ../gradle.properties | cut -f2 -d'=')
S3_FOLDER="${S3_PREFIX_FOLDER}/${TODAY}"

if [[ -f ${TOPICS_CSV_FILE} ]]; then
  echo "Reading topics csv file ${TOPICS_CSV_FILE}"
else
  echo "File not found ${TOPICS_CSV_FILE}"
  exit 1
fi

cat "${TOPICS_CSV_FILE}" | while read -r TOPIC_NAME
  do
    echo "Processing: ${TOPIC_NAME} into folder ${S3_FOLDER}"
    exit 1
    java -jar ../build/libs/hbase-to-mongo-export-${HBASE_TO_MONGO_VERSION}.jar \
      aws_default_region="${AWS_DEFAULT_REGION}" \
      aws_access_key_id="${AWS_ACCESS_KEY_ID}" \
      aws_secret_access_key="${AWS_SECRET_ACCESS_KEY}" \
      aws_default_profile="${AWS_DEFAULT_PROFILE}" \
      s3_bucket="${S3_BUCKET}" \
      s3_prefix_folder="${S3_FOLDER}" \
      data_key_service_url="${DATA_KEY_SERVICE_URL}"
  done
