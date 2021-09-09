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


### Spring profiles

  | Profile name           | Production profile | Result
  |------------------------|--------------------|-------
  | `awsConfiguration`     | Yes                | Configure actual AWS S3 services, if not set a localstack configuration is enabled
  | `outputToS3`           | Yes                | Output is chunked and written to configured S3 folder.
  | `weakRng`              | Yes                | Us a psuedo-random number generator for cipher activities. If not set a strong rng is enabled.

### Spring configuration parameters

  | Parameter name                | Default Value               | Description
  |-------------------------------|-----------------------------|--------------
  | `allow.partial.results` | true |
  | `aws.region` | eu-west-2 | Location of s3 services |
  | `aws.s3.max.connections` | 256 | Max no. of concurrent connection with s3 |
  | `blocked.topics` | NOT_SET | List of topics which the app will not export |
  | `chunk.size` | 1000 | The number of reacords to read at a time (per thread) from hbase |
  | `cipher.transformation` | AES/CTR/NoPadding | The cipher transformation to use on the snapshot files|
  | `data.key.service.url` | | The url of the data key service |
  | `delete.metrics` | true | Wipe the metrics from the pushgateway at the end of the run |
  | `dynamodb.retry.delay`        | 1000                       | Initial delay of the first dynamodb retry (ms) |
  | `dynamodb.retry.maxAttempts`  |    5                       | The number of retries to attempt before giving up |
  | `dynamodb.retry.multiplier`   |    2                       | The backoff multiplier (the retry delay is this multiple of the previous delay) |
  | `dynamodb.status.table.name` |UCExportToCrownStatus | The export stats table name |
  | `dynamodb.status.product.table.name` |data_pipeline_metadata | The product stats table name |
  | `hbase.client.timeout.ms` | 3600000 | See [hbase.client.operation.timeout](https://hbase.apache.org/1.4/book.html#hbase.client.operation.timeout)  in the HBase manual |
  | `hbase.rpc.timeout.ms` | 1800000 | See [hbase.rpc.timeout](https://hbase.apache.org/1.4/book.html#hbase.rpc.timeout) in the HBase manual |
  | `hbase.scanner.timeout.ms` | 1800000 | See [hbase.client.scanner.timeout.period](https://hbase.apache.org/1.4/book.html#hbase.client.scanner.timeout.period) in the HBase manual |
  | `hbase.zookeeper.quorum` | | The url to go to to establish an hbase connection |
  | `identity.key.password`       | changeit                   | The client key password.
  | `identity.keystore`           | resources/identity.jks     | For mutual auth - the client cert and key truststore.
  | `identity.store.alias`        | cid                        | The name of the cert in to present to DKS.
  | `identity.store.password`     | changeit                   | Client cert store password.
  | `instance.name` | | The ec2 instance name - for the metrics to allow metrics from a single instance to be queried |
  | `keyservice.retry.delay`      | 1000                       | Initial delay of the first keyservice retry (ms) |
  | `keyservice.retry.maxAttempts` |   5                       | The number of retries to attempt before giving up |
  | `keyservice.retry.multiplier` |    2                       | The backoff multiplier (the retry delay is this multiple of the previous delay) |
  | `manifest.output.directory` | . | Where to write manifests on the local machine and from where they will be written to s3 |
  | `manifest.retry.delay`        | 1000                       | Initial delay of the first manifest write retry (ms) |
  | `manifest.retry.maxAttempts`  |    5                       | The number of retries to attempt before giving up |
  | `manifest.retry.multiplier`   |    2                       | The backoff multiplier (the retry delay is this multiple of the previous delay) |
  | `output.batch.size.max.bytes` | | The maximum amount of uncompressed data to place in each snapshot |
  | `prometheus.scrape.interval` | 70000 | How long to wait before deleting matrics from the pushgateway |
  | `pushgateway.host` | | The name of the pushgateway instance to which metrics must be pushed |
  | `pushgateway.port` | 9091 | The port on which the pushgateway instance is listening |
  | `s3.bucket` | | The bucket into which snapshot files are written |
  | `s3.manifest.bucket` | | The bucket into which manifest files are written |
  | `s3.manifest.prefix.folder` | | The common prefix of all manifest files |
  | `s3.max.retries` | 20 | How many attempts should be made to write a snapshot file before giving up |
  | `s3.prefix.folder` | | The common prefix of all snapshot files |
  | `s3.socket.timeout` | 1800000 | How long to wait for an s3 connection before giving up |
  | `scan.cache.blocks` | true | Whether to enable scan cache blocks on the hbase scanner |
  | `scan.cache.size` |-1 | The size of the client side hbase scan cache |
  | `scan.max.result.size` | -1 | he maximum no. of rows to place in the client side cache |
  | `scan.max.retries` | 100 | How many attempt should be made to scan a slice of the tablesppace before giving up |
  | `scan.retry.sleep.ms` | 10000 | How long to wait in between scan attempts |
  | `scan.time.range.end` | | For incremental feeds - any cells newer than this are omitted |
  | `scan.time.range.start` | | For incremental feeds - any cells older than this are omitted |
  | `scan.width` | 5 | How much of the keyspace each scanner should scan. |
  | `snapshot.sender.export.date` | | Passed on to snapshot sender  |
  | `snapshot.sender.reprocess.files` | | To be passed on to snapshot sender |
  | `snapshot.sender.shutdown.flag` | | Passed on to snapshot sender |
  | `snapshot.sender.sqs.queue.url` | | The queue to which notifications of exported files are sent |
  | `snapshot.sender.sqs.message.group.id` | `daily_export` | A string representing the group for SQS that the message will go in to |
  | `snapshot.type` | | `full` or `incremental` |
  | `sns.retry.delay`             | 1000                       | Initial delay of the first sns retry (ms) |
  | `sns.retry.maxAttempts`       |    5                       | The number of retries to attempt before giving up |
  | `sns.retry.multiplier`        |    2                       | The backoff multiplier (the retry delay is this multiple of the previous delay) |
  | `sqs.retry.delay`             | 1000                       | Initial delay of the first sqs retry (ms) |
  | `sqs.retry.maxAttempts`       |    5                       | The number of retries to attempt before giving up |
  | `sqs.retry.multiplier`        |    2                       | The backoff multiplier (the retry delay is this multiple of the previous delay)
  | `thread.count` | 256 | How many threads spring batch should place into the thread pool |
  | `topic.arn.completion.full` | | Where to send the adg triggering message for full exports |
  | `topic.arn.completion.incremental` | | Where to send the adg triggering message for incremental exports |
  | `topic.arn.monitoring` | | Where to send the monitoring message at the end of the run |
  | `topic.name` | | The topic that should be exported (uniquely identifies a table in HBase) |
  | `trigger.adg` | false | Whether to send the SNS message that kicks off ADG. |
  | `trigger.snapshot.sender` | | Whether to send the messages that trigger snapshot sender |
  | `trust.keystore`              | resources/truststore.jks   | For mutual auth - the DKS CA certificate |
  | `trust.store.password`        | changeit                   | The truststore password. |
  | `use.timeline.consistency` | true | Whether to scan the region replicas or only the master |
  | `data.egress.sqs.queue.url` | | The queue to which notifications of exported RIS files are sent |
  | `send.to.ris` | false | Whether to send the Sqs message to Data egress. |


## Run locally containerized

### Stand up the hbase container and populate it, and execute sample exporters

Create all:
```
    make exports
```

### Run the integration tests against local containerized setup.

```
    make integration-tests
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
