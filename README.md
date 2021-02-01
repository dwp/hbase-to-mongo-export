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
   make certificates
```

## Configuration

* The main class is
  ```app.HBaseToMongoExport```

* The program arguments are

  | Parameter name                | Sample Value               | Further info
  |-------------------------------|----------------------------|--------------
  | `aws.region`                  | eu-west-2                  | AWS Region to use for client auth - required when `outputToS3` is used
  | `data.key.service.url`        | http://localhost:8080      | Url of remote data key service.
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
  | `identity.store.password`     | changeit                   | Client cert store password.
  | `identity.key.password`       | changeit                   | The client key password.
  | `trust.store.password`        | changeit                   | The trust store password.
  | `identity.store.alias`        | cid                        | The name of the cert in to present to DKS.
  | `snapshot.type`               | full                       | Full or incremental for the type of snapshots being generated.
  | `dynamodb.retry.delay`        | 1000                       | Initial delay of the first dynamodb retry (ms) |
  | `dynamodb.retry.maxAttempts`  |    5                       | The number of retries to attempt before giving up |
  | `dynamodb.retry.multiplier`   |    2                       | The backoff multiplier (the retry delay is this multiple of the previous delay) |
  | `keyservice.retry.delay`      | 1000                       | Initial delay of the first keyservice retry (ms) |
  | `keyservice.retry.maxAttempts` |   5                       | The number of retries to attempt before giving up |
  | `keyservice.retry.multiplier` |    2                       | The backoff multiplier (the retry delay is this multiple of the previous delay) |
  | `manifest.retry.delay`        | 1000                       | Initial delay of the first manifest write retry (ms) |
  | `manifest.retry.maxAttempts`  |    5                       | The number of retries to attempt before giving up |
  | `manifest.retry.multiplier`   |    2                       | The backoff multiplier (the retry delay is this multiple of the previous delay) |
  | `sqs.retry.delay`             | 1000                       | Initial delay of the first sqs retry (ms) | 
  | `sqs.retry.maxAttempts`       |    5                       | The number of retries to attempt before giving up |
  | `sqs.retry.multiplier`        |    2                       | The backoff multiplier (the retry delay is this multiple of the previous delay) |

* The available spring profiles are

  | Profile name           | Production profile | Result
  |------------------------|--------------------|-------
  | `aesCipherService`     | Yes                | Use the actual cipher service that does real encryption and decryption.
  | `batchRun`             | Yes                | Activates the batch run (omit for unit tests).
  | `httpDataKeyService`   | Yes                | Use a remote (i.e. real) data key service (not a fake stubbed one)
  | `insecureHttpClient`   | No                 | Connects to DKS over unencrypted http.
  | `awsConfiguration`         | Yes                | AWS S3 Client to communicate to the real AWS S3 service
  | `localstackConfiguration`        | No                 | Dummy AWS S3 Client to communicate to the localstack S3 docker container
  | `outputToS3`           | Yes                | Output is chunked and written to configured S3 folder.
  | `realHbaseDataSource`  | Yes                | Not needed if using `unitTest`. Indicates a real hbase connection i.e. not a mock
  | `secureHttpClient`     | Yes                | Connects to DKS over TLS with mutual authorisation.
  | `strongRng`            | Yes                | Not needed if using `unitTest`. Create a strong random number generator.
  | `unitTest`             | No                 | Overrides `strongRng`, `realHttpClient` and `realHbaseDataSource`. Use mock http client and psuedo random number generator.


## Run locally containerized

### Stand up the hbase container and populate it, and execute sample exporters

Create all:
```
    make exports
```

### Run the integration tests against local containerized setup.

```
    make integration-all
```

## Run locally in IDE or as a jar (Unix)

* Note this only works properly on Unix hosts with native docker.

### First, you must stand up the hbase container and populate it

This is slightly cumbersome as zookeeper gives the docker network name of the
hbase host which can't be resolved outside of docker, however this can be remedied
if the name given by zookeeper is then entered into the local `/etc/hosts` file.

1. Bring up the hbase container and populate it with test data:

```
   make services
```

It should now be possible to run code in an IDE against the local instance.

### Running locally in IDE (Unix)

* Only works properly on Unix, not Mac or Windows, as they run docker in VMs.

Run the class `HBaseToMongoExport` and add the following VM 
arguments to the run configuration:

```
-Dlogging.config=resources/logback.xml -Dcorrelation_id=s3-export
```

And the following environment variables:

```sql
SPRING_CONFIG_LOCATION=resources/application.properties
```
