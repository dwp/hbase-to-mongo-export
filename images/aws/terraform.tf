terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  access_key                  = "access_key_id"
  region                      = "eu-west-2"
  s3_force_path_style         = true
  secret_key                  = "secret_access_key"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    dynamodb = "http://aws:4566"
    s3       = "http://aws:4566"
    sns      = "http://aws:4566"
    sqs      = "http://aws:4566"
  }
}

resource "aws_s3_bucket" "export_bucket" {
  bucket = "exports"
  acl    = "public-read"
}

resource "aws_s3_bucket" "manifest_bucket" {
  bucket = "manifests"
  acl    = "public-read"
}

resource "aws_s3_bucket" "crl_bucket" {
  bucket = "dw-local-crl"
  acl    = "public-read"
}

resource "aws_sns_topic" "adg_trigger" {
  name = "trigger-adg-topic"
}

resource "aws_sqs_queue" "trigger_adg_subscriber" {
  name = "trigger-adg-subscriber"
}

resource "aws_sqs_queue" "integration-queue" {
  name = "integration-queue"
}
