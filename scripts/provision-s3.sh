#!/usr/bin/env bash

aws configure set aws_access_key_id ${AWS_ACCESS_KEY_ID}
aws configure set aws_secret_access_key ${AWS_SECRET_ACCESS_KEY}
aws configure set default.region ${AWS_DEFAULT_REGION}
aws configure set region ${AWS_REGION}

aws configure list

printenv

#aws s3api delete-bucket --bucket demo-bucket --region ${AWS_REGION}
#echo deleted bucket

echo Creating bucket ${S3_BUCKET}
aws --endpoint-url=http://s3:4572 s3 mb s3://${S3_BUCKET} --region eu-west-2
echo Setting up ACL
aws --endpoint-url=http://s3:4572 s3api put-bucket-acl --bucket ${S3_BUCKET} --acl public-read

# it only works with 18.03 + docker engine
# https://stackoverflow.com/questions/24319662/from-inside-of-a-docker-container-how-do-i-connect-to-the-localhost-of-the-mach