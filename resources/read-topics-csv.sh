#!/bin/bash

TOPICS_CSV_FILE=$1
S3_BUCKET=$2
S3_SERVICE_ENDPOINT=$3
AWS_ACCESS_KEY_ID=$4
AWS_SECRET_ACCESS_KEY=$5
S3_PREFIX_FOLDER=$6
AWS_DEFAULT_REGION=$7
HBASE_URL=$8
DATA_KEY_SERVICE_URL=$9

TODAY=$(date +"%Y-%m-%d")
# shellcheck disable=SC2002
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
    echo ""

    java -jar "${JAR_FILE}" \
      --spring.profiles.active=aesCipherService,httpDataKeyService,realHbaseDataSource,localstackConfiguration,outputToS3,batchRun,strongRng,secureHttpClient \
      --hbase.zookeeper.quorum="${HBASE_URL}" \
      --data.key.service.url="${DATA_KEY_SERVICE_URL}" \
      --aws.region="${AWS_DEFAULT_REGION}" \
      --s3.service.endpoint="${S3_SERVICE_ENDPOINT}" \
      --s3.access.key="${AWS_ACCESS_KEY_ID}" \
      --s3.secret.key="${AWS_SECRET_ACCESS_KEY}" \
      --s3.bucket="${S3_BUCKET}" \
      --s3.prefix.folder="${S3_FOLDER}" \
      --data.table.name=ucfs-data \
      --column.family=topic \
      --topic.name="${TOPIC_NAME}" \
      --encrypt.output=true \
      --compress.output=true \
      --output.batch.size.max.bytes=2048 \
      --identity.keystore=../resources/certs/htme/keystore.jks \
      --identity.store.password=changeit \
      --identity.key.password=changeit \
      --identity.store.alias=cid \
      --trust.keystore=../resources/certs/htme/truststore.jks \
      --trust.store.password=changeit ;
  done

echo ""
echo "Finished topics csv file ${TOPICS_CSV_FILE}"
echo ""