#!/usr/bin/env bash

init() {
  aws_local configure set aws_access_key_id access_key_id
  aws_local configure set aws_secret_access_key secret_access_key
}

add_status_item() {
  add_item $(status_item_id)
}

add_equality_item() {
  add_item $(equality_item_id)
}

add_completed_product_status_item() {
  add_product_item $(product_status_item_id) "Completed"
}

add_empty_status_item() {
  add_item $(empty_status_item_id)
}

add_empty_product_status_item() {
  add_product_item $(empty_product_status_item_id)
}

add_item() {
  local id=${1:?Usage: $FUNCNAME id}
  local status=${2:-Exporting}
  local files_exported=${3:-0}
  local files_sent=${4:-0}

  aws_local dynamodb delete-item \
    --table-name $(ucc_ecc_table_name) \
    --key "{"$id"}" \
    --return-values "ALL_OLD"

  aws_local dynamodb put-item \
    --table-name $(ucc_ecc_table_name) \
    --item '{'$id', "CollectionStatus": {"S":"'$status'"}, "FilesExported":{"N":"'$files_exported'"},"FilesSent":{"N":"'$files_sent'"}}'
}

add_completed_items() {
  for i in $(seq 50); do

    if [[ $((i % 4)) == 0 ]]; then
      local status=Exported
    elif [[ $((i % 4)) == 1 ]]; then
      local status=Sent
    elif [[ $((i % 4)) == 2 ]]; then
      local status=Received
    else
      local status=Success
    fi

    add_item $(completed_item_id $i) $status 100 100
  done
}

add_product_item() {
  local id=${1:?Usage: $FUNCNAME id}
  local status=${2:-In_Progress}

  aws_local dynamodb delete-item \
    --table-name $(product_table_name) \
    --key "{"$id"}" \
    --return-values "ALL_OLD"

  aws_local dynamodb put-item \
    --table-name $(product_table_name) \
    --item '{'$id', "Status": {"S":"'$status'"}}'
}

read_sqs_queue() {
  aws_local sqs receive-message --queue-url $(sqs_queue_url)
}

get_status_item() {
  get_item $(status_item_id)
}

get_product_status_item() {
  get_item $(product_status_item_id)
}

get_empty_status_item() {
  get_item $(empty_status_item_id)
}

get_empty_product_status_item() {
  get_item $(empty_product_status_item_id)
}

get_item() {
  local id=${1:?Usage: $FUNCNAME id}
  aws_local dynamodb get-item \
    --table-name $(ucc_ecc_table_name) \
    --key $id
}

get_product_item() {
  local id=${1:?Usage: $FUNCNAME id}
  aws_local dynamodb get-item \
    --table-name $(product_table_name) \
    --key $id
}

completed_item_id() {
  local index=${1:?Usage}
  echo '"CorrelationId":{"S":"s3-export"},"CollectionName":{"S":"db.database.collection'$index'"}'
}

status_item_id() {
  echo '"CorrelationId":{"S":"s3-export"},"CollectionName":{"S":"db.database.collection"}'
}

equality_item_id() {
  echo '"CorrelationId":{"S":"equality-export"},"CollectionName":{"S":"data.equality"}'
}

product_status_item_id() {
  echo '"Correlation_Id":{"S":"s3-export"},"DataProduct":{"S":"HTME"}'
}

empty_status_item_id() {
  echo '"CorrelationId":{"S":"empty-export"},"CollectionName":{"S":"db.database.empty"}'
}

empty_product_status_item_id() {
  echo '"Correlation_Id":{"S":"empty-export"},"DataProduct":{"S":"HTME"}'
}

ucc_ecc_table_name() {
  echo UCExportToCrownStatus
}

product_table_name() {
  echo data_pipeline_metadata
}

sqs_queue_url() {
  aws_local sqs list-queues --query "QueueUrls[0]" | jq -r .
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

aws_local() {
  aws --endpoint-url http://aws:4566 --region=eu-west-2 "$@"
}
