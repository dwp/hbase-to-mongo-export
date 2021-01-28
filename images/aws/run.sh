#!/usr/bin/env bash

source ./environment.sh

main() {
  init
  create_uc_ecc_table
  add_status_item
  add_empty_status_item
  add_completed_items
  terraform init
  terraform apply -auto-approve
}

main
