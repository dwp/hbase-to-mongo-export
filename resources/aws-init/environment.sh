#!/usr/bin/env bash

aws_local() {
  aws --endpoint-url http://aws:4566 --region=eu-west-2 "$@"
}

init() {
    aws_local configure set aws_access_key_id access_key_id
    aws_local configure set aws_secret_access_key secret_access_key
}

make_bucket() {
    local bucket_name=$1

    if ! aws_local s3 ls s3://$bucket_name 2>/dev/null; then
        echo Making $bucket_name
        aws_local s3 mb s3://$bucket_name
        aws_local s3api put-bucket-acl --bucket $bucket_name --acl public-read
    else
        echo Bucket \'$bucket_name\' exists.
    fi

}

create_export_bucket() {
    make_bucket demobucket
}

create_manifest_bucket() {
    make_bucket manifestbucket
}

create_crl_bucket() {
    make_bucket dw-local-crl
    aws_local s3api put-object --bucket dw-local-crl --key crl/
}

create_uc_ecc_table() {
    local existing_tables=$(aws_local dynamodb list-tables --query "TableNames")
    if [[ ! $existing_tables =~ $(ucc_ecc_table_name) ]]; then
        echo Creating $(ucc_ecc_table_name) table.
        aws_local dynamodb create-table \
                  --table-name $(ucc_ecc_table_name) \
                  --key-schema \
                    AttributeName=CorrelationId,KeyType=HASH \
                    AttributeName=CollectionName,KeyType=RANGE \
                  --attribute-definitions \
                    AttributeName=CorrelationId,AttributeType=S \
                    AttributeName=CollectionName,AttributeType=S \
                  --billing-mode PAY_PER_REQUEST
    else
      echo Table \'$(ucc_ecc_table_name)\' exists.
    fi

}

add_status_item() {
    aws_local dynamodb delete-item \
          --table-name $(ucc_ecc_table_name) \
          --key $(status_item_id) \
          --return-values "ALL_OLD"

    aws_local dynamodb update-item \
          --table-name $(ucc_ecc_table_name) \
          --key $(status_item_id) \
          --update-expression "SET CollectionStatus = :cs, FilesExported = :fe, FilesSent = :fs" \
          --return-values "ALL_NEW" \
          --expression-attribute-values '{":cs": {"S":"Exported"}, ":fe": {"N":"0"}, ":fs": {"N":"0"}}'
}

create_sqs_queue() {
  aws_local sqs create-queue --queue-name integration-queue
}

read_sqs_queue() {
  aws_local sqs receive-message --queue-url $(sqs_queue_url)
}

get_status_item() {
    aws_local dynamodb get-item \
          --table-name $(ucc_ecc_table_name) \
          --key $(status_item_id)
}

status_item_id() {
  echo '{"CorrelationId":{"S":"integration_test_correlation_id"},"CollectionName":{"S":"db.penalties-and-deductions.sanction"}}'
}

ucc_ecc_table_name() {
  echo UCExportToCrownStatus
}

sqs_queue_url() {
  aws_local sqs list-queues --query "QueueUrls[0]" | jq -r .
}
