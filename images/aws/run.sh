#!/usr/bin/env bash

source ./environment.sh

main() {
  init
  create_export_bucket
  create_manifest_bucket
  create_uc_ecc_table
  add_status_item
  create_sqs_queue
}

main
