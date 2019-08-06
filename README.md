# hbase-to-mongo-export

Selects the latest records from a single hbase table and writes them out in
mongo backup format, i.e. 1 json record per line.

## Requirements

1. JDK 8+
2. Docker
3. docker-compose

## Configuration

* The main class is
  ```app.HBaseToMongoExport```

* The program arguments are

  | Parameter name           | Default Value         | Further info
  |--------------------------|-----------------------|--------------
  | `compress.output`          | false                 | Whether to compress the output.
  | `data.key.service.url`     | http://localhost:8080 | Url of remote data key service.
  | `directory.output`         |                       | Directory to write output files to.
  | `encrypt.output`           | true                  | Whether to encrypt the output.
  | `file.output`              |                       | File to write output to - only needed if 'outputToFile' spring profile is active so not used in production.
  | `aws.region`               | eu-west-1             | AWS Region to use for client auth - required when 'outputToS3' is used
  | `s3.bucket`                |                       | S3 bucket to write output to - required when 'outputToS3' spring profile is active. I.e. `bucket` in `s3://bucket/folder/`
  | `s3.folder`                |                       | S3 folder to write to in the bucket - required when 'outputToS3' spring profile is active. I.e. `folder` in  `s3://bucket/folder/`
  | `hbase.zookeeper.quorum`   | hbase                 | Name of the hbase host (set this to 'localhost' to run from IDE).
  | `output.batch.size.max`    |                       | The maximum size of each  batch of output (calculated before compression and encryption). Max is `Int.MAX_VALUE` = `2147483647`
  | `source.cipher.algorithm`  | AES/CTR/NoPadding     | The algorithm that was used to encrypt the source data.
  | `source.table.name`        | ucdata                | Table in hbase to read data from.
  | `target.cipher.algorithm`  | AES/CTR/NoPadding     | The algorithm that should be used to encrypt the output data.


* The available spring profiles are

  | Profile name         | Production profile | Result
  |----------------------|--------------------|-------
  | `aesCipherService`     | Yes                | Use the actual cipher service that does real encryption and decryption.
  | `batchRun`             | Yes                | Activates the batch run (omit for unit tests).
  | `httpDataKeyService`   | Yes                | Use a remote (i.e. real) data key service (not a fake stubbed one)
  | `localDataSource`      | No                 | Indicates hbase is running locally i.e. not on a cluster.
  | `outputToConsole`      | No                 | Output is written to console as is (not encrypted or compressed).
  | `outputToDirectory`    | No                 | Output is chunked and written to the configured directory.
  | `outputToFile`         | No                 | Output is written to configured local file as is (used for the hbase integration test).
  | `outputToS3`           | Yes                | Output is chunked and written to configured S3 folder.
  | `phoneyCipherService`  | No                 | Use a cipher service that does not do real encryption.
  | `phoneyDataKeyService` | No                 | Use a dummy key service that does not require a configured DKS instance.
  | `production`           | Yes                | Use real http client and a strong random number generator (contrast with 'unitTest').
  | `unitTest`             | No                 | Use mock http client and psuedo random number generator (contrast with 'production').


## Run locally containerized

There are makefile commands for all your common actions;

 | Command                 | Description
 |-------------------------|--------------------
 | `add-hbase-to-hosts`      | Update laptop hosts file with reference to hbase container
 | `build-images`            | Build the hbase, population, exporter images
 | `build`                   | Build the hbase exporter jar file
 | `destroy`                 | Bring down the hbase and other services then delete all volumes
 | `dist`                    | Assemble distribution files in build/dist
 | `down`                    | Bring down the hbase and other services
 | `echo`                    | Echo the current version
 | `hbase-shell`             | Open an Hbase shell onto the running hbase container
 | `integration`             | Run the integration tests in a Docker container
 | `logs-directory-exporter` | Show the logs of the directory exporter
 | `logs-file-exporter`      | Show the logs of the file exporter
 | `logs-s3-exporter`        | Show the logs of the S3 exporter
 | `reset-all`               | Destroy all, rebuild and up all, and check the export logs
 | `restart`                 | Restart hbase and other services
 | `up`                      | Bring up hbase, population, and sample exporter services

### Stand up the hbase container and populate it, and execute sample exporters

Create all:
```
    make up
``` 
Check the logs:
```
    make logs-file-exporter
    make logs-directory-eporter
``` 

### Additionally run the integration tests against local containerized setup
```
    make integration
```

### Run the s3 exporter

You will need to have valid aws credentials to access S3 with a bucket you already have created and have permissions for.
* This is a sample only
* This uses some default values - further settings (like changing profiles for the real DWP Data Key Service) 
can be updated in the `docker-compose` file

```
    make export-to-s3 --aws_default_region=eu-west-2 \
                      --aws_access_key_id=keykeykey \
                      --aws_session_token=tokentokentoken \
                      --aws_default_profile=profile \
                      --s3_bucket=9876543210 -
                      --s3_prefix_folder=hbase-export/2019-07-11/ \
                      --data_key_service_url=dummy.com:8080
```
then check the logs
```
    make logs-s3-eporter
```

## Run locally in IDE or as a jar

### First, you must stand up the hbase container and populate it

This is slightly cumbersome as zookeeper gives the docker network name of the
hbase host which can't be resolved outside of docker, however this can be remedied
if the name given by zookeeper is then entered into the local `/etc/hosts` file.

1. Bring up the hbase container and populate it with test data:

```
   docker-compose up -d hbase hbase-populate
```

2. Add hbase entry in local /etc/hosts file:

```
    make add-hbase-to-hosts
```

It should now be possible to run code in an IDE against the local instance.

### Running locally in IDE

See the makefile command `up` and the docker-compose file for `hbase-to-mongo-export-directory` for a sample of the arguments you need

#### Console output

Make a run configuration and add arguments as per `export-to-s3`:
```
--spring.profiles.active=phoneyCipherService,phoneyDataKeyService,localDataSource,outputToConsole,batchRun,strongRng
--source.table.name=ucdata
--data.ready.flag.location=data/ready
```
...it should the print out what it has exported from the local containerised hbase

#### S3 Output

To test a running it from local HBase to S3, you can try this - 
See the makefile command `export-to-s3` and the docker-compose file for `hbase-to-mongo-export-s3` for a sample of the arguments you need

* Env vars:
```
aws_access_key_id=keykeykey
aws_secret_access_key=secretsecretsecret
```

* Arguments:
```
--spring.profiles.active=phoneyCipherService,phoneyDataKeyService,localDataSource,outputToS3,batchRun,strongRng
--source.table.name=ucdata
--hbase.zookeeper.quorum=localhost
--aws.region=eu-west-1
--s3.bucket=9876543210
--s3.folder=test/businessdata/mongo/ucdata
```
