# hbase-to-mongo-export

Selects the latest records from a single hbase table and writes them out in
mongo backup format, i.e. 1 json record per line.

## Requirements

1. JDK 8+
2. Docker
3. docker-compose

## Self signed certificates

The integration tests standup various services as docker containers and the
export and dks containers require keystores and truststores so that they can
communicate over 2-way https. To generate these:

```bash
    cd resources
    ./certificates.sh
```

This will create the required keystores where the docker build processes expect
to find them.

## Configuration

* The main class is
  ```app.HBaseToMongoExport```

* The program arguments are

  | Parameter name                | Sample Value               | Further info
  |-------------------------------|----------------------------|--------------
  | `aws.region`                  | eu-west-2                  | AWS Region to use for client auth - required when `outputToS3` is used
  | `compress.output`             | true                       | Whether to compress the output.
  | `data.key.service.url`        | http://localhost:8080      | Url of remote data key service.
  | `directory.output`            | mongo-export/2019080       | Directory to write output files to.
  | `encrypt.output`              | true                       | Whether to encrypt the output.
  | `file.output`                 | export20190808.txt         | File to write output to - only needed if `outputToFile` spring profile is active so not used in production.
  | `hbase.zookeeper.quorum`      | hbase                      | Name of the hbase host (set this to `localhost` to run from IDE).
  | `output.batch.size.max.bytes` | 100000                     | The maximum size of each  batch of output (calculated before compression and encryption). Max is `Int.MAX_VALUE` = `2147483647`
  | `s3.bucket`                   | a1b2c3d                    | S3 bucket to write output to - required when `outputToS3` spring profile is active. I.e. `bucket` in `s3://bucket/folder/`
  | `s3.prefix.folder`            | mongo-export/2019080       | S3 folder to write to in the bucket - required when `outputToS3` spring profile is active. I.e. `folder` in  `s3://bucket/folder/`
  | `source.cipher.algorithm`     | AES/CTR/NoPadding          | The algorithm that was used to encrypt the source data.
  | `source.table.name`           | k2hb:ingest                | Table in hbase to read data from.
  | `target.cipher.algorithm`     | AES/CTR/NoPadding          | The algorithm that should be used to encrypt the output data.
  | `column.family`               | topic                      | The common column family that the encrypted record data column is in.
  | `topic.name`                  | db.core.addressDeclaration | The column qualifier, also is the name of the topic that the data came in on.
  | `data.table.name`             | k2hb:ingest                | The table to which all the kafka messages have been persisted.
  | `identity.keystore`           | resources/identity.jks     | For mutual auth - the client cert and key truststore.
  | `trust.keystore`              | resources/truststore.jks   | For mutual auth - the DKS cert.
  | `identity.store.password`     | changeit                   | client cert store password.
  | `identity.key.password`       | changeit                   | the client key password.
  | `trust.store.password`        | changeit                   | the trust store password.
  | `identity.store.alias`        | cid                        | The name of the cert in to present to DKS.

* The available spring profiles are

  | Profile name           | Production profile | Result
  |------------------------|--------------------|-------
  | `aesCipherService`     | Yes                | Use the actual cipher service that does real encryption and decryption.
  | `batchRun`             | Yes                | Activates the batch run (omit for unit tests).
  | `httpDataKeyService`   | Yes                | Use a remote (i.e. real) data key service (not a fake stubbed one)
  | `insecureHttpClient`   | No                 | Connects to DKS over unencrypted http.
  | `outputToConsole`      | No                 | Output is written to console as is (not encrypted or compressed).
  | `outputToDirectory`    | No                 | Output is chunked and written to the configured directory.
  | `outputToFile`         | No                 | Output is written to configured local file as is (used for the hbase integration test).
  | `realS3Client`         | Yes                | AWS S3 Client to communicate to the real AWS S3 service
  | `dummyS3Client`        | No                 | Dummy AWS S3 Client to communicate to the localstack S3 docker container
  | `outputToS3`           | Yes                | Output is chunked and written to configured S3 folder.
  | `phoneyCipherService`  | No                 | Use a cipher service that does not do real encryption.
  | `phoneyDataKeyService` | No                 | Use a dummy key service that does not require a configured DKS instance.
  | `realHbaseDataSource`  | Yes                | Not needed if using `unitTest`. Indicates a real hbase connection i.e. not a mock
  | `secureHttpClient`     | Yes                | Connects to DKS over TLS with mutual authorisation.
  | `strongRng`            | Yes                | Not needed if using `unitTest`. Create a strong random number generator.
  | `unitTest`             | No                 | Overrides `strongRng`, `realHttpClient` and `realHbaseDataSource`. Use mock http client and psuedo random number generator.


## Run locally containerized

There are makefile commands for all your common actions;

 | Command                      | Description
 |------------------------------|--------------------
 | `add-hbase-to-hosts`         | Update laptop hosts file with reference to hbase container (`http://local-hbase:8080`) and dks-standalone container (`http://local-dks:8080`)
 | `build-all`                  | Build the jar file and then all docker images
 | `build-images`               | Build the hbase, population, and exporter images
 | `build-jar`                  | Build the hbase exporter jar file
 | `destroy`                    | Bring down the hbase and other services then delete all volumes
 | `dist`                       | Assemble distribution files in build/dist
 | `down`                       | Bring down the hbase and other services
 | `echo-version`               | Echo the current version Jar version from Gradle settings
 | `hbase-shell`                | Open an Hbase shell onto the running hbase container
 | `integration-all`            | Build the jar and images, put up the containers, run the integration tests
 | `integration-tests`          | (Re-)Run the integration tests in a Docker container
 | `logs-directory-exporter`    | Show the logs of the directory exporter. Update follow_flag as required.
 | `logs-file-exporter`         | Show the logs of the file exporter. Update follow_flag as required.
 | `logs-s3-exporter`           | Show the logs of the s3 exporter. Update follow_flag as required.
 | `logs-hbase-populate`        | Show the logs of the hbase-populater. Update follow_flag as required.
 | `reset-all`                  | Destroy all, rebuild and up all, and check the export logs
 | `restart`                    | Restart hbase and other services
 | `up`                         | Run `build-all` then start the services with `up-all`
 | `up-all`                     | Bring up hbase, population, and sample exporter services
 | `local-all-collections-test` | Runs a sample test of local collections, similar to how we do in a VM. Only works properly on Unix hosts with native docker.

### Stand up the hbase container and populate it, and execute sample exporters

Create all:
```
    make up-all
```
Check the logs:
```
    make logs-file-exporter
    make logs-directory-eporter
    make logs-s3-exporter
```
Verify the data in the localstack dummy s3 container:
* Browse to http:localhost:4572

### Run the integration tests against local containerized setup.

```
    make integration-all
```
...this also executes `build-all` and `up-all`.
It is a simple test that the File exporter generated the correct sample set of records.
It does not check that the encryption/decryption is correct

## Run locally in IDE or as a jar (Unix)

* Note this only works properly on Unix hosts with native docker.

### First, you must stand up the hbase container and populate it

This is slightly cumbersome as zookeeper gives the docker network name of the
hbase host which can't be resolved outside of docker, however this can be remedied
if the name given by zookeeper is then entered into the local `/etc/hosts` file.

1. Bring up the hbase container and populate it with test data:

```
   make up-all
```

2. Add hbase and dks-standalone entry in local /etc/hosts file:
See Make targets above for the names this puts in
```
    make add-containers-to-hosts
```

It should now be possible to run code in an IDE against the local instance.

### Running locally in IDE (Unix)

* Only works properly on Unix, not Mac or Windows, as they run docker in VMs.

Run the application from class file `HBaseToMongoExport` and add arguments to the profile:
```
--spring.profiles.active=phoneyCipherService,realHttpClient,httpDataKeyService,realHbaseDataSource,outputToS3,dummyS3Client,batchRun,strongRng
--hbase.zookeeper.quorum=local-hbase
--data.key.service.url=http://local-dks-insecure:8080
--data.table.name=ucfs-data
--column.family=topic
--topic.name=db.core.addressDeclaration
--encrypt.output=true
--compress.output=true
--output.batch.size.max.bytes=2048
--aws.region=eu-west-1
--s3.bucket=9876543210
--s3.folder=test/businessdata/mongo/ucdata
--s3.service.endpoint=http://localhost:4572
--s3.access.key=DummyKey
--s3.secret.key=DummySecret
```

### Running on the command line (Unix)

You will need to update the active profiles to suit your needs...
```bash
 SPRING_CONFIG_LOCATION=./resources/application.properties ./gradlew bootRun
```

#### Console output

Make a run configuration and add arguments as per `hbase-to-mongo-export-file` in the docker-compose file, and update as you need.

...it should the print out what it has exported from the local containerised hbase
