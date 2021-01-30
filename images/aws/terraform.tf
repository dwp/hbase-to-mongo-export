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

resource "aws_dynamodb_table" "uc_export_to_crown_status_table" {

  name         = "UCExportToCrownStatus"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "CorrelationId"
  range_key    = "CollectionName"

  attribute {
    name = "CorrelationId"
    type = "S"
  }

  attribute {
    name = "CollectionName"
    type = "S"
  }

  ttl {
    attribute_name = "TimeToExist"
    enabled        = true
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

resource "aws_sqs_queue" "integration_queue" {
  name = "integration-queue"
}

resource "aws_sns_topic" "full_completion_topic" {
  name = "full-completion-topic"
}

resource "aws_sqs_queue" "trigger_adg_subscriber" {
  name = "trigger-adg-subscriber"
}

resource "aws_sns_topic_subscription" "full_completion_subscription" {
  topic_arn = aws_sns_topic.full_completion_topic.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.trigger_adg_subscriber.arn
}
